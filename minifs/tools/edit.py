"""``Edit``: exact string replacement with opencode's fallback matchers (trimmed lines, block anchors, whitespace,
indentation, escapes, ...). Keeps the file's BOM and line endings. ladderframe's ``Edit`` (``ladderframe.tools.edit``,
which follows opencode's edit tool), over a ``VFSView``; the replacers below are ladderframe's, unchanged.

ladderframe serialises editors of a path with a thread lock; here an edit reads the whole file and its version,
replaces in memory, and writes back with a compare-and-swap on that version, so a concurrent writer anywhere is
detected. A lost race is redone once from the start, since the old string may have moved or gone (then it is the
replacer's "Could not find" retry); a second loss is the ``ModelRetry`` of ``tool_error``. Files over
``EditSettings.max_bytes`` are refused, since the whole file is held in memory.

The matching approaches are sourced, as in opencode, from
https://github.com/cline/cline/blob/main/evals/diff-edits/diff-apply/diff-06-23-25.ts
https://github.com/google-gemini/gemini-cli/blob/main/packages/core/src/utils/editCorrector.ts
https://github.com/cline/cline/blob/main/evals/diff-edits/diff-apply/diff-06-26-25.ts
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Iterator

from pydantic_ai import ModelRetry, RunContext
from pydantic_ai.messages import ToolReturn

from minifs.tools._text import (
    TooLarge,
    convert_to_line_ending,
    create_file,
    detect_line_ending,
    diff_metadata,
    encode_text,
    normalize_line_endings,
    read_file,
    split_bom,
)
from minifs.tools.base import vfs_tool
from minifs.tools.deps import HasVFS, resolve, settings
from minifs.tools.errors import tool_error
from minifs.vfs.core import VFSError
from minifs.vfs.view import VFSView

DESCRIPTION = """Performs exact string replacements in files.

Usage:
- You must use your `Read` tool at least once in the conversation before editing. This tool will error if you attempt an edit without reading the file.
- When editing text from Read tool output, ensure you preserve the exact indentation (tabs/spaces) as it appears AFTER the line number prefix. The line number prefix format is: line number + colon + space (e.g., `1: `). Everything after that space is the actual file content to match. Never include any part of the line number prefix in the oldString or newString.
- ALWAYS prefer editing existing files in the codebase. NEVER write new files unless explicitly required.
- Only use emojis if the user explicitly requests it. Avoid adding emojis to files unless asked.
- The edit will FAIL if `oldString` is not found in the file with an error "oldString not found in content".
- The edit will FAIL if `oldString` is found multiple times in the file with an error "Found multiple matches for oldString. Provide more surrounding lines in oldString to identify the correct match." Either provide a larger string with more surrounding context to make it unique or use `replaceAll` to change every instance of `oldString`.
- Use `replaceAll` for replacing and renaming strings across the file. This parameter is useful if you want to rename a variable for instance."""

BOM = "\ufeff"
EMPTY_OLD_STRING = (
    "oldString cannot be empty when editing an existing file. Provide the exact text to replace, "
    "or use write for an intentional full-file replacement."
)


class _Lost(Exception):
    """Another writer got in between the read and the write of an ``_edit``; starting over resolves it."""


async def edit(
    ctx: RunContext[HasVFS], filePath: str, oldString: str, newString: str, replaceAll: bool | None = None
) -> str | ToolReturn:
    """Replace text in a file.

    Args:
        filePath: The absolute path to the file to modify
        oldString: The text to replace
        newString: The text to replace it with (must be different from oldString)
        replaceAll: Replace all occurrences of oldString (default false)
    """
    if not filePath:
        raise ModelRetry("filePath is required")
    if oldString == newString:
        raise ModelRetry("No changes to apply: oldString and newString are identical.")
    path = resolve(ctx, filePath)
    max_bytes = settings(ctx).edit.max_bytes
    try:
        for _ in range(2):
            try:
                content_old, content_new = await _edit(
                    ctx.deps.vfs, path, oldString, newString, bool(replaceAll), max_bytes
                )
                break
            except _Lost:
                continue
        else:
            raise VFSError("ESTALE", path)
    except VFSError as e:
        return tool_error(e, "write", ENOENT="File {path} not found", ENOTDIR="File {path} not found")
    diff = await asyncio.to_thread(
        diff_metadata, path, normalize_line_endings(content_old), normalize_line_endings(content_new)
    )
    return ToolReturn("Edit applied successfully.", metadata={"filepath": path, **diff})


async def _edit(
    vfs: VFSView, path: str, old_string: str, new_string: str, replace_all: bool, max_bytes: int
) -> tuple[str, str]:
    """Edits ``path``: ``(its old text, the new text)``, without BOMs. ``_Lost`` where another writer got in
    between: a create beaten to the file, a file changed, removed or replaced by a directory after its stat."""
    if not vfs.policy.allows(vfs.actor, "read", path):
        # As in ``Write``: ladderframe checks its one permission first, here the write. Allowed to write but not
        # read, an empty ``oldString`` may still create the file; anything else needs the old text.
        vfs.policy.check(vfs.actor, "write", path)
        if old_string != "":
            vfs.policy.check(vfs.actor, "read", path)
        bom, content_new = split_bom(new_string)
        try:
            await create_file(vfs, path, encode_text(content_new, bom))
        except VFSError as e:
            if e.errno == "EEXIST":
                vfs.policy.check(vfs.actor, "read", path)
            raise
        return "", content_new
    try:
        entry = await vfs.stat(path)
    except VFSError as e:
        if e.errno != "ENOENT" or old_string != "":
            raise
        bom, content_new = split_bom(new_string)
        try:
            await create_file(vfs, path, encode_text(content_new, bom))
        except VFSError as c:
            if c.errno == "EEXIST":  # created meanwhile
                raise _Lost from None
            raise
        return "", content_new
    vfs.policy.check(vfs.actor, "write", entry.path)
    if old_string == "":
        raise ModelRetry(EMPTY_OLD_STRING)
    if entry.is_dir:
        raise ModelRetry(f"Path is a directory, not a file: {path}")
    if entry.size > max_bytes:
        raise ModelRetry(_too_large(path, entry.size, max_bytes))
    try:
        try:
            data, version = await read_file(vfs, path, max_bytes)
        except TooLarge as e:  # grew since the stat
            raise ModelRetry(_too_large(path, e.size, max_bytes)) from None
        try:
            source_bom, content_old = split_bom(data.decode("utf-8"))
        except UnicodeDecodeError:
            raise ModelRetry(f"Cannot edit {path}: it is not a UTF-8 text file") from None
        ending = detect_line_ending(content_old)
        old = convert_to_line_ending(normalize_line_endings(old_string), ending)
        replacement = convert_to_line_ending(normalize_line_endings(new_string), ending)
        try:
            # In a thread, as pydantic-ai runs ladderframe's sync Edit: the fallback replacers scan the whole file
            # (seconds for a miss in a few MB), which would stall every other coroutine on the loop.
            replaced = await asyncio.to_thread(replace, content_old, old, replacement, replace_all)
        except ValueError as exc:
            raise ModelRetry(str(exc)) from None
        next_bom, content_new = split_bom(replaced)
        await vfs.write(path, encode_text(content_new, source_bom or next_bom), expect=version)
    except VFSError as e:
        if e.errno in ("ESTALE", "ENOENT", "EISDIR"):  # changed, removed, or replaced by a directory since the stat
            raise _Lost from None
        raise
    return content_old, content_new


def _too_large(path: str, size: int, max_bytes: int) -> str:
    return (
        f"Cannot edit {path}: it is {size} bytes, over the {max_bytes}-byte limit for Edit. "
        "Use the Write tool to replace the whole file instead."
    )


# ---------------------------------------------------------------- replacers

Replacer = Callable[[str, str], Iterator[str]]

SINGLE_CANDIDATE_SIMILARITY_THRESHOLD = 0.65
MULTIPLE_CANDIDATES_SIMILARITY_THRESHOLD = 0.65
MAX_SCORED_LINE_LENGTH = 200
"""Longer lines (minified code) are compared for equality only: `levenshtein` is quadratic in the line length."""


def levenshtein(a: str, b: str) -> int:
    if a == "" or b == "":
        return max(len(a), len(b))
    previous = list(range(len(b) + 1))
    for i in range(1, len(a) + 1):
        current = [i] + [0] * len(b)
        for j in range(1, len(b) + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            current[j] = min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost)
        previous = current
    return previous[len(b)]


def _span(lines: list[str], start: int, end: int) -> tuple[int, int]:
    """Character offsets of lines `start..end` (inclusive) in the text the lines were split from."""
    begin = sum(len(line) + 1 for line in lines[:start])
    finish = begin + sum(len(line) for line in lines[start : end + 1]) + (end - start)
    return begin, finish


def simple_replacer(content: str, find: str) -> Iterator[str]:
    yield find


def line_trimmed_replacer(content: str, find: str) -> Iterator[str]:
    original = content.split("\n")
    search = find.split("\n")
    if search[-1] == "":
        search.pop()
    for i in range(len(original) - len(search) + 1):
        if all(original[i + j].strip() == search[j].strip() for j in range(len(search))):
            begin, finish = _span(original, i, i + len(search) - 1)
            yield content[begin:finish]


def _middle_similarity(original: list[str], start: int, size: int, search: list[str], early_exit: bool) -> float:
    lines_to_check = min(len(search) - 2, size - 2)
    if lines_to_check <= 0:
        return 1.0  # no middle lines to compare: accept on the anchors
    similarity = 0.0
    for j in range(1, min(len(search) - 1, size - 1)):
        original_line = original[start + j].strip()
        search_line = search[j].strip()
        longest = max(len(original_line), len(search_line))
        if longest == 0:
            continue
        if original_line == search_line:
            score = 1.0
        elif longest > MAX_SCORED_LINE_LENGTH:
            score = 0.0
        else:
            score = 1 - levenshtein(original_line, search_line) / longest
        if early_exit:
            similarity += score / lines_to_check
            if similarity >= SINGLE_CANDIDATE_SIMILARITY_THRESHOLD:
                break
        else:
            similarity += score
    return similarity if early_exit else similarity / lines_to_check


def block_anchor_replacer(content: str, find: str) -> Iterator[str]:
    original = content.split("\n")
    search = find.split("\n")
    if len(search) < 3:
        return
    if search[-1] == "":
        search.pop()
    first, last = search[0].strip(), search[-1].strip()
    max_delta = max(1, len(search) // 4)

    candidates: list[tuple[int, int]] = []
    for i, line in enumerate(original):
        if line.strip() != first:
            continue
        for j in range(i + 2, len(original)):
            if original[j].strip() == last:
                if abs((j - i + 1) - len(search)) <= max_delta:
                    candidates.append((i, j))
                break  # only the first occurrence of the last line
    if not candidates:
        return

    if len(candidates) == 1:
        start, end = candidates[0]
        if _middle_similarity(original, start, end - start + 1, search, True) >= SINGLE_CANDIDATE_SIMILARITY_THRESHOLD:
            begin, finish = _span(original, start, end)
            yield content[begin:finish]
        return

    best: tuple[int, int] | None = None
    best_similarity = -1.0
    for start, end in candidates:
        similarity = _middle_similarity(original, start, end - start + 1, search, False)
        if similarity > best_similarity:
            best_similarity, best = similarity, (start, end)
    if best is not None and best_similarity >= MULTIPLE_CANDIDATES_SIMILARITY_THRESHOLD:
        begin, finish = _span(original, *best)
        yield content[begin:finish]


def whitespace_normalized_replacer(content: str, find: str) -> Iterator[str]:
    def normalize(text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    normalized_find = normalize(find)
    lines = content.split("\n")
    for line in lines:
        normalized_line = normalize(line)
        if normalized_line == normalized_find:
            yield line
        elif normalized_find in normalized_line:
            words = find.strip().split()
            if words:
                match = re.search(r"\s+".join(re.escape(word) for word in words), line)
                if match:
                    yield match.group(0)

    find_lines = find.split("\n")
    if len(find_lines) > 1:
        for i in range(len(lines) - len(find_lines) + 1):
            block = "\n".join(lines[i : i + len(find_lines)])
            if normalize(block) == normalized_find:
                yield block


def indentation_flexible_replacer(content: str, find: str) -> Iterator[str]:
    def remove_indentation(text: str) -> str:
        lines = text.split("\n")
        non_empty = [line for line in lines if line.strip()]
        if not non_empty:
            return text
        least = min(len(line) - len(line.lstrip()) for line in non_empty)
        return "\n".join(line if not line.strip() else line[least:] for line in lines)

    normalized_find = remove_indentation(find)
    content_lines = content.split("\n")
    find_lines = find.split("\n")
    for i in range(len(content_lines) - len(find_lines) + 1):
        block = "\n".join(content_lines[i : i + len(find_lines)])
        if remove_indentation(block) == normalized_find:
            yield block


_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "'": "'", '"': '"', "`": "`", "\\": "\\", "\n": "\n", "$": "$"}
_ESCAPE = re.compile(r"\\(n|t|r|'|\"|`|\\|\n|\$)")


def _unescape(text: str) -> str:
    return _ESCAPE.sub(lambda m: _ESCAPES[m.group(1)], text)


def escape_normalized_replacer(content: str, find: str) -> Iterator[str]:
    unescaped_find = _unescape(find)
    if unescaped_find in content:
        yield unescaped_find
    lines = content.split("\n")
    find_lines = unescaped_find.split("\n")
    for i in range(len(lines) - len(find_lines) + 1):
        block = "\n".join(lines[i : i + len(find_lines)])
        if _unescape(block) == unescaped_find:
            yield block


def multi_occurrence_replacer(content: str, find: str) -> Iterator[str]:
    """Every exact match, so `replace` can decide between unique and `replaceAll`."""
    start = 0
    while (index := content.find(find, start)) != -1:
        yield find
        start = index + len(find)


def trimmed_boundary_replacer(content: str, find: str) -> Iterator[str]:
    trimmed = find.strip()
    if trimmed == find:
        return
    if trimmed in content:
        yield trimmed
    lines = content.split("\n")
    find_lines = find.split("\n")
    for i in range(len(lines) - len(find_lines) + 1):
        block = "\n".join(lines[i : i + len(find_lines)])
        if block.strip() == trimmed:
            yield block


def context_aware_replacer(content: str, find: str) -> Iterator[str]:
    find_lines = find.split("\n")
    if len(find_lines) < 3:
        return  # needs at least 3 lines to have meaningful context
    if find_lines[-1] == "":
        find_lines.pop()
    content_lines = content.split("\n")
    first, last = find_lines[0].strip(), find_lines[-1].strip()
    for i, line in enumerate(content_lines):
        if line.strip() != first:
            continue
        for j in range(i + 2, len(content_lines)):
            if content_lines[j].strip() != last:
                continue
            block_lines = content_lines[i : j + 1]
            if len(block_lines) == len(find_lines):
                # at least half of the non-empty middle lines must match when trimmed
                matching = total = 0
                for k in range(1, len(block_lines) - 1):
                    block_line, find_line = block_lines[k].strip(), find_lines[k].strip()
                    if block_line or find_line:
                        total += 1
                        matching += block_line == find_line
                if total == 0 or matching / total >= 0.5:
                    yield "\n".join(block_lines)
            break


REPLACERS: tuple[Replacer, ...] = (
    simple_replacer,
    line_trimmed_replacer,
    block_anchor_replacer,
    whitespace_normalized_replacer,
    indentation_flexible_replacer,
    escape_normalized_replacer,
    trimmed_boundary_replacer,
    context_aware_replacer,
    multi_occurrence_replacer,
)


def replace(content: str, old_string: str, new_string: str, replace_all: bool = False) -> str:
    """Replace `old_string`, trying each replacer in turn until one finds a unique match (or any, with
    `replace_all`). Raises `ValueError` with the message for the model when nothing fits."""
    if old_string == new_string:
        raise ValueError("No changes to apply: oldString and newString are identical.")
    if old_string == "":
        raise ValueError(EMPTY_OLD_STRING)

    not_found = True
    for replacer in REPLACERS:
        for search in replacer(content, old_string):
            index = content.find(search)
            if index == -1:
                continue
            not_found = False
            if _is_disproportionate_match(search, old_string):
                raise ValueError(
                    "Refusing replacement because the matched span is much larger than oldString. "
                    "Re-read the file and provide the full exact oldString for the intended replacement."
                )
            if replace_all:
                return content.replace(search, new_string)
            if index != content.rfind(search):
                continue
            return content[:index] + new_string + content[index + len(search) :]

    if not_found:
        raise ValueError(
            "Could not find oldString in the file. It must match exactly, including whitespace, indentation, "
            "and line endings."
        )
    raise ValueError("Found multiple matches for oldString. Provide more surrounding context to make the match unique.")


def _is_disproportionate_match(search: str, old_string: str) -> bool:
    old_lines = old_string.count("\n") + 1
    search_lines = search.count("\n") + 1
    if search_lines >= max(old_lines + 3, old_lines * 2):
        return True
    if old_lines == 1:
        return False
    return len(search.strip()) > max(len(old_string.strip()) + 500, len(old_string.strip()) * 4)


Edit = vfs_tool(edit, "Edit", DESCRIPTION, sequential=True)
