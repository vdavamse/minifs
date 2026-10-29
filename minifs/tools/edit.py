"""``Edit``: exact string replacement with opencode's fallback matchers (trimmed lines, block anchors, whitespace,
indentation, escapes, ...). Keeps the file's BOM and line endings. ladderframe's ``Edit`` (``ladderframe.tools.edit``,
which follows opencode's edit tool), over a ``VFSView``; the replacers below give ladderframe's answers (the same
candidates in the same order, checked by ``tests/tools/test_edit_replacers.py`` against its code), but each is at
most O(n·m) in content lines and ``oldString`` lines, where ladderframe's block scans were O(n²), and ``replace``
runs under a deadline (``EditSettings.timeout``), since the whole search runs in a worker thread that can't be
cancelled and holds the GIL while it works (#80).

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
import time
from array import array
from bisect import bisect_left
from collections.abc import Callable, Iterable, Iterator
from contextvars import ContextVar
from itertools import accumulate

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
    config = settings(ctx).edit
    try:
        for _ in range(2):
            try:
                content_old, content_new = await _edit(
                    ctx.deps.vfs, path, oldString, newString, bool(replaceAll), config.max_bytes, config.timeout
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
    vfs: VFSView, path: str, old_string: str, new_string: str, replace_all: bool, max_bytes: int, timeout: float
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
            # In a thread, as pydantic-ai runs ladderframe's sync Edit: the fallback replacers scan the whole file,
            # which would stall every other coroutine on the loop. The thread still holds the GIL while it works
            # and can't be cancelled, so the search runs under ``timeout``.
            replaced = await asyncio.to_thread(replace, content_old, old, replacement, replace_all, timeout)
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

_TICK = 4096
"""Loop iterations between two looks at the clock in ``_Deadline.tick``."""


class ReplaceTimeout(ValueError):
    """``replace`` ran past its deadline. A ``ValueError``, so ``_edit`` reports it as a retry like the replacers'
    own errors, with a message that tells the model what to do."""


class _Deadline:
    """When ``replace`` must stop: ``timeout`` seconds after it started, or never (None). ``check`` looks at the
    clock, for a loop whose iterations cost more than a clock read (a Levenshtein distance, a block test, a scan
    of the file); ``tick`` does so every ``_TICK`` calls, for a tight loop over lines."""

    __slots__ = ("at", "timeout", "_n")

    def __init__(self, timeout: float | None) -> None:
        self.timeout = timeout
        self.at = None if timeout is None else time.monotonic() + timeout
        self._n = 0

    def tick(self) -> None:
        if self.at is None:
            return
        self._n += 1
        if self._n >= _TICK:
            self._n = 0
            self.check()

    def check(self) -> None:
        if self.at is not None and time.monotonic() > self.at:
            assert self.timeout is not None
            raise ReplaceTimeout(timeout_message(self.timeout))


_NO_DEADLINE = _Deadline(None)
_deadline: ContextVar[_Deadline] = ContextVar("minifs_edit_deadline", default=_NO_DEADLINE)
"""The deadline of the ``replace`` call running in this context, for the replacers: each binds it once when it
starts (``deadline = _deadline.get()``). A context variable rather than a parameter, so the replacers keep
ladderframe's ``(content, find)`` signature. A replacer called on its own, outside ``replace``, never stops."""


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


def _line_starts(lines: Iterable[str]) -> array[int]:
    """``starts[k]``: the character offset of line ``k`` in the text ``lines`` were split from (on ``\\n``); one
    more entry past the last line. Built once, so a span of lines is two lookups (ladderframe's ``_span`` summed
    the lines before each candidate). An array, not a list: a file of millions of short lines has millions of
    entries, and the replacers already hold its lines."""
    return array("q", accumulate((len(line) + 1 for line in lines), initial=0))


def _lines_equal_to(lines: Iterable[str], wanted: str) -> array[int]:
    """The sorted line numbers whose stripped text is ``wanted``."""
    return array("q", (k for k, line in enumerate(lines) if line.strip() == wanted))


def simple_replacer(content: str, find: str) -> Iterator[str]:
    yield find


def line_trimmed_replacer(content: str, find: str) -> Iterator[str]:
    """Every run of lines that equals ``find``'s lines once both are stripped, as the lines of ``content``.

    The stripped lines are joined with ``\\n`` and searched for the joined stripped ``find`` with ``str.find``: a
    hit that starts and ends at line boundaries is a run of equal stripped lines, and the hits come in line order,
    as ladderframe's window loop gave them. The hit's line is the count of ``\\n`` before it, kept up as the hits
    advance, so the stripped lines are never held as a list."""
    original = content.split("\n")
    search = find.split("\n")
    if search[-1] == "":
        search.pop()
    joined = "\n".join(map(str.strip, original))
    needle = "\n".join(line.strip() for line in search)
    starts = _line_starts(original)
    tick = _deadline.get().tick
    line = counted = 0  # the line at `counted`
    pos = joined.find(needle)
    while pos != -1:
        tick()
        end = pos + len(needle)
        if (pos == 0 or joined[pos - 1] == "\n") and (end == len(joined) or joined[end] == "\n"):
            line += joined.count("\n", counted, pos)
            counted = pos
            yield content[starts[line] : starts[line + len(search)] - 1]
        pos = joined.find(needle, pos + 1)


def _middle_similarity(original: list[str], start: int, size: int, search: list[str], early_exit: bool) -> float:
    lines_to_check = min(len(search) - 2, size - 2)
    if lines_to_check <= 0:
        return 1.0  # no middle lines to compare: accept on the anchors
    similarity = 0.0
    deadline = _deadline.get()
    for j in range(1, min(len(search) - 1, size - 1)):
        deadline.tick()
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
            deadline.check()  # a distance between two 200-character lines is milliseconds of work
            score = 1 - levenshtein(original_line, search_line) / longest
        if early_exit:
            similarity += score / lines_to_check
            if similarity >= SINGLE_CANDIDATE_SIMILARITY_THRESHOLD:
                break
        else:
            similarity += score
    return similarity if early_exit else similarity / lines_to_check


def _next_line(positions: array[int], at: int, n: int) -> int:
    """The first of the sorted line numbers ``positions`` at or after ``at``, or ``n`` if there is none: what a
    forward scan for that line from ``at`` finds before it stops, without the scan."""
    k = bisect_left(positions, at)
    return positions[k] if k < len(positions) else n


def block_anchor_replacer(content: str, find: str) -> Iterator[str]:
    original = content.split("\n")
    search = find.split("\n")
    if len(search) < 3:
        return
    if search[-1] == "":
        search.pop()
    first, last = search[0].strip(), search[-1].strip()
    max_delta = max(1, len(search) // 4)

    last_at = _lines_equal_to(original, last)
    n = len(original)
    tick = _deadline.get().tick
    candidates: list[tuple[int, int]] = []
    for i, line in enumerate(original):
        tick()
        if line.strip() != first:
            continue
        j = _next_line(last_at, i + 2, n)  # only the first occurrence of the last line
        if j < n and abs((j - i + 1) - len(search)) <= max_delta:
            candidates.append((i, j))
    if not candidates:
        return

    starts = _line_starts(original)
    if len(candidates) == 1:
        start, end = candidates[0]
        if _middle_similarity(original, start, end - start + 1, search, True) >= SINGLE_CANDIDATE_SIMILARITY_THRESHOLD:
            yield content[starts[start] : starts[end + 1] - 1]
        return

    best: tuple[int, int] | None = None
    best_similarity = -1.0
    for start, end in candidates:
        similarity = _middle_similarity(original, start, end - start + 1, search, False)
        if similarity > best_similarity:
            best_similarity, best = similarity, (start, end)
    if best is not None and best_similarity >= MULTIPLE_CANDIDATES_SIMILARITY_THRESHOLD:
        start, end = best
        yield content[starts[start] : starts[end + 1] - 1]


def whitespace_normalized_replacer(content: str, find: str) -> Iterator[str]:
    def normalize(text: str) -> str:
        # What re.sub(r"\s+", " ", text).strip() gives: str.split() splits on the same whitespace as \s.
        return " ".join(text.split())

    normalized_find = normalize(find)
    lines = content.split("\n")
    deadline = _deadline.get()
    for line in lines:
        deadline.tick()
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
        m = len(find_lines)
        for i in range(len(lines) - m + 1):
            deadline.tick()
            # A block's normalised text starts with its first line's and ends with its last line's (whitespace
            # runs collapse within a line as they do across one), so a block whose first line isn't a prefix of
            # the find, or whose last line isn't a suffix, can't match.
            if not normalized_find.startswith(normalize(lines[i])) or not normalized_find.endswith(
                normalize(lines[i + m - 1])
            ):
                continue
            deadline.check()  # the block test below costs the whole block, not one line
            block = "\n".join(lines[i : i + m])
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
    # Removing a common indent keeps each line's text after its indent, so equal de-indented blocks have equal
    # lines once left-stripped: a block whose left-stripped lines differ from the find's can't match. The first
    # and last lines are checked before the rest, so most windows cost two comparisons.
    m = len(find_lines)
    wanted = [line.lstrip() for line in find_lines]
    deadline = _deadline.get()
    for i in range(len(content_lines) - m + 1):
        deadline.tick()
        if content_lines[i].lstrip() != wanted[0] or content_lines[i + m - 1].lstrip() != wanted[-1]:
            continue
        deadline.check()  # the rest costs the whole block
        if any(content_lines[i + k].lstrip() != wanted[k] for k in range(1, m - 1)):
            continue
        block = "\n".join(content_lines[i : i + m])
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
    m = len(find_lines)
    deadline = _deadline.get()
    for i in range(len(lines) - m + 1):
        deadline.tick()
        first, last = lines[i], lines[i + m - 1]
        # An escape never spans a line break unless the line ends in a backslash (``\`` + newline is one), so a
        # block's unescaped text starts with its first line's unless that line ends in one, and always ends with
        # its last line's: those must be a prefix and a suffix of the find.
        if not unescaped_find.endswith(_unescape(last) if "\\" in last else last):
            continue
        if not first.endswith("\\") and not unescaped_find.startswith(_unescape(first) if "\\" in first else first):
            continue
        deadline.check()  # the block test below costs the whole block
        block = "\n".join(lines[i : i + m])
        if _unescape(block) == unescaped_find:
            yield block


def multi_occurrence_replacer(content: str, find: str) -> Iterator[str]:
    """Every exact match, so `replace` can decide between unique and `replaceAll`."""
    start = 0
    tick = _deadline.get().tick
    while (index := content.find(find, start)) != -1:
        tick()
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
    m = len(find_lines)
    deadline = _deadline.get()
    for i in range(len(lines) - m + 1):
        deadline.tick()
        # A stripped block starts with its first line's stripped text (or with a later line's, when the first is
        # blank, whose stripped text is then empty) and ends with its last line's, so those must be a prefix and
        # a suffix of the stripped find.
        if not trimmed.startswith(lines[i].strip()) or not trimmed.endswith(lines[i + m - 1].strip()):
            continue
        deadline.check()  # the block test below costs the whole block
        block = "\n".join(lines[i : i + m])
        if block.strip() == trimmed:
            yield block


def context_aware_replacer(content: str, find: str) -> Iterator[str]:
    find_lines = find.split("\n")
    if len(find_lines) < 3:
        return  # needs at least 3 lines to have meaningful context
    if find_lines[-1] == "":
        find_lines.pop()
    content_lines = content.split("\n")
    wanted = [line.strip() for line in find_lines]
    first, last = wanted[0], wanted[-1]
    last_at = _lines_equal_to(content_lines, last)
    n = len(content_lines)
    starts: array[int] | None = None
    deadline = _deadline.get()
    for i, line in enumerate(content_lines):
        deadline.tick()
        if line.strip() != first:
            continue
        # Only the first occurrence of the last line, and only a block of the find's length.
        j = _next_line(last_at, i + 2, n)
        if j < n and j - i + 1 == len(find_lines):
            deadline.check()  # the middle check costs the whole block
            # at least half of the non-empty middle lines must match when trimmed
            matching = total = 0
            for k in range(1, len(find_lines) - 1):
                block_line, find_line = content_lines[i + k].strip(), wanted[k]
                if block_line or find_line:
                    total += 1
                    matching += block_line == find_line
            if total == 0 or matching / total >= 0.5:
                if starts is None:
                    starts = _line_starts(content_lines)
                yield content[starts[i] : starts[j + 1] - 1]


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

_SEEN_BYTES = 1 << 20
"""Characters of candidates ``replace`` remembers to skip repeats of, before it forgets them: enough to collapse
the repeats of a few candidates, bounded so a flood of distinct ones doesn't pile up in memory."""


def timeout_message(timeout: float) -> str:
    """The retry for a ``replace`` that ran past ``timeout`` seconds."""
    return (
        f"Could not match oldString within {timeout:g} seconds: the file is large and oldString does not match it "
        "exactly. Read the file again and provide the exact text to replace, including whitespace and indentation."
    )


def replace(
    content: str, old_string: str, new_string: str, replace_all: bool = False, timeout: float | None = None
) -> str:
    """Replace `old_string`, trying each replacer in turn until one finds a unique match (or any, with
    `replace_all`). Raises `ValueError` with the message for the model when nothing fits, or `ReplaceTimeout` (a
    `ValueError`) once the search has run for `timeout` seconds (None: no limit)."""
    if old_string == new_string:
        raise ValueError("No changes to apply: oldString and newString are identical.")
    if old_string == "":
        raise ValueError(EMPTY_OLD_STRING)

    deadline = _Deadline(timeout)
    token = _deadline.set(deadline)
    try:
        not_found = True
        seen: set[str] = set()  # a candidate already tried gave its answer: the same find, the same ambiguity
        seen_size = 0
        for replacer in REPLACERS:
            deadline.check()
            for search in replacer(content, old_string):
                deadline.check()  # each candidate costs two scans of the file
                if search in seen:
                    continue
                if seen_size + len(search) > _SEEN_BYTES:
                    seen.clear()
                    seen_size = 0
                seen.add(search)
                seen_size += len(search)
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
    finally:
        _deadline.reset(token)

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
