"""``Read``: files (with line numbers), directories, images and PDFs, as ladderframe's ``Read`` (opencode's read
tool), over the view. Lines come from ``VFSView.pread_lines``, so a page is read without the rest of the file."""

from __future__ import annotations

import asyncio
import mimetypes
from collections.abc import Awaitable
from pathlib import PurePosixPath
from typing import Any
from uuid import UUID

from pydantic_ai import ModelRetry, RunContext
from pydantic_ai.exceptions import UserError
from pydantic_ai.messages import BinaryContent, ToolReturn, ToolReturnPart

from minifs.tools.base import vfs_tool
from minifs.tools.deps import HasVFS, ReadSettings, resolve, settings, workdir
from minifs.tools.errors import tool_error
from minifs.vfs.core import Entry, LineReadResult, VFSError, basename, join, split

DESCRIPTION = """Read a file or directory from the filesystem. If the path does not exist, an error is returned.

Usage:
- The filePath parameter should be an absolute path.
- By default, this tool returns up to 2000 lines from the start of the file.
- The offset parameter is the line number to start from (1-indexed).
- To read later sections, call this tool again with a larger offset.
- Use the Grep tool to find specific content in large files or files with long lines.
- If you are unsure of the correct file path, use the Glob tool to look up filenames by glob pattern.
- Contents are returned with each line prefixed by its line number as `<line>: <content>`. For example, if a file has contents "foo\\n", you will receive "1: foo\\n". For directories, entries are returned one per line (without line numbers) with a trailing `/` for subdirectories.
- Any line longer than 2000 characters is truncated.
- Call this tool in parallel when you know there are multiple files you want to read.
- Avoid tiny repeated slices (30 line chunks). If you need more context, read a larger window.
- This tool can read image files and PDFs and return them as file attachments."""

SAMPLE_BYTES = 4096
IMAGE_MIMES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
BINARY_EXTENSIONS = {
    ".zip", ".tar", ".gz", ".exe", ".dll", ".so", ".class", ".jar", ".war", ".7z", ".doc", ".docx", ".xls",
    ".xlsx", ".ppt", ".pptx", ".odt", ".ods", ".odp", ".bin", ".dat", ".obj", ".o", ".a", ".lib", ".wasm",
    ".pyc", ".pyo",
}  # fmt: skip
INSTRUCTION_FILES = ("AGENTS.md", "CLAUDE.md")
MAX_ATTACHMENT_BYTES = 3_750_000
"""Largest image or PDF attached to a result: about 5 MB once base64-encoded, the providers' usual limit."""
_PAGE_ATTEMPTS = 3
"""Reads of a file that changes under the tool (between the sample and the page or attachment, or between a page's
batches) before it settles for what the last attempt read."""
_RACES = frozenset({"ENOENT", "ENOTDIR", "EISDIR"})
"""Errors of a later call that mean the path changed since the sample: the read starts again."""
_MAX_LINES = 2**31 - 1  # pread_lines' max_lines and max_bytes, and pread's limit, are int
_MAX_LINE = 2**63 - 1  # and first_line a bigint; no file has that many lines


async def read(
    ctx: RunContext[HasVFS], filePath: str, offset: int | None = None, limit: int | None = None
) -> str | ToolReturn:
    """Read a file or directory.

    Args:
        filePath: The absolute path to the file or directory to read
        offset: The line number to start reading from (1-indexed)
        limit: The maximum number of lines to read (defaults to 2000)
    """
    config = settings(ctx).read
    if (offset is not None and offset < 0) or (limit is not None and limit < 0):
        raise ModelRetry("offset and limit must be non-negative integers")
    path = resolve(ctx, filePath)
    vfs = ctx.deps.vfs
    start = offset or 1
    count = limit if limit is not None else config.default_limit
    instructions: list[str] | BaseException | None = None  # looked up with the first attempt's sample
    try:
        for attempt in range(_PAGE_ATTEMPTS):
            settle = attempt + 1 == _PAGE_ATTEMPTS
            # The sample, the page's first batch and, once, the instruction files at once: one round trip for most
            # reads, on at most three connections.
            calls: list[Awaitable[Any]] = [
                vfs.pread(path, 0, SAMPLE_BYTES),
                vfs.pread_lines(
                    path,
                    min(start, _MAX_LINE),
                    min(count, _MAX_LINES),
                    _fetch(config),
                    max_line_bytes=_line_cap(config),
                ),
            ]
            if instructions is None:
                calls.append(_nearby_instructions(ctx, path))
            sample, first, *looked_up = await asyncio.gather(*calls, return_exceptions=True)
            if looked_up:
                instructions = looked_up[0]
            if isinstance(sample, VFSError):
                if sample.errno == "EISDIR" and sample.path == path:
                    return _directory(path, await vfs.readdir(path), start, count)
                if sample.errno in ("ENOENT", "ENOTDIR"):
                    raise ModelRetry(await _not_found(ctx, path))
                raise sample
            if isinstance(sample, BaseException):
                raise sample
            version = (sample.id, sample.generation)

            mime = _sniff_mime(sample.content, path)
            if mime in IMAGE_MIMES or mime == "application/pdf":
                if sample.size > MAX_ATTACHMENT_BYTES:
                    raise ModelRetry(
                        f"{path} is too large to attach ({sample.size} bytes, the limit is {MAX_ATTACHMENT_BYTES})"
                    )
                data = sample.content
                if not sample.eof:
                    whole = await vfs.pread(path, 0, MAX_ATTACHMENT_BYTES)
                    if (whole.id, whole.generation) != version and not settle:
                        continue  # replaced since the sample: sniff it again
                    if not whole.eof:  # grew past the limit since the sample
                        raise ModelRetry(
                            f"{path} is too large to attach ({whole.size} bytes, the limit is {MAX_ATTACHMENT_BYTES})"
                        )
                    data = whole.content
                message = "PDF read successfully" if mime == "application/pdf" else "Image read successfully"
                return ToolReturn(message, content=[BinaryContent(data=data, media_type=mime)])
            if _is_binary(path, sample.content):
                raise ModelRetry(f"Cannot read binary file: {path}")

            if isinstance(first, VFSError) and first.errno in _RACES and not settle:
                continue  # gone, or replaced by a directory, since the sample
            if isinstance(first, BaseException):
                raise first
            if (first.id, first.generation) != version and not settle:
                continue  # the page isn't the sampled file's
            if first.lines < start and not (first.lines == 0 and start == 1):
                raise ModelRetry(f"Offset {start} is out of range for this file ({first.lines} lines)")
            page = await _read_lines(ctx, path, start, count, config, first, None if settle else version)
            if page is None:
                continue  # changed between the page's batches
            lines, total, more, capped = page
            if total < start and not (total == 0 and start == 1):
                raise ModelRetry(f"Offset {start} is out of range for this file ({total} lines)")
            break
        else:  # pragma: no cover - the last attempt never continues
            raise AssertionError("not reached")
    except VFSError as e:
        return tool_error(e, "read")
    if isinstance(instructions, BaseException):
        raise instructions
    assert instructions is not None

    output = f"<path>{path}</path>\n<type>file</type>\n<content>\n"
    output += "\n".join(f"{start + i}: {line}" for i, line in enumerate(lines))
    last = start + len(lines) - 1
    if capped:
        output += (
            f"\n\n(Output capped at {config.max_bytes // 1024} KB. "
            f"Showing lines {start}-{last}. Use offset={last + 1} to continue.)"
        )
    elif more:
        output += f"\n\n(Showing lines {start}-{last} of {total}. Use offset={last + 1} to continue.)"
    else:
        output += f"\n\n(End of file - total {total} lines)"
    output += "\n</content>"

    if instructions:
        output += "\n\n<system-reminder>\n" + "\n\n".join(instructions) + "\n</system-reminder>"
    return output


Read = vfs_tool(read, "Read", DESCRIPTION)


async def _not_found(ctx: RunContext[HasVFS], path: str) -> str:
    """The not-found message, with up to three names in the parent directory that contain the missing name or
    that it contains, from the view, so nothing outside it is suggested."""
    segments = split(path)
    parent = join(*segments[:-1])
    base = basename(path).lower()
    try:
        entries = sorted(e.name for e in await ctx.deps.vfs.readdir(parent)) if segments else []
    except VFSError:
        entries = []
    similar = [join(*segments[:-1], e) for e in entries if base in e.lower() or e.lower() in base][:3]
    if similar:
        return f"File not found: {path}\n\nDid you mean one of these?\n" + "\n".join(similar)
    return f"File not found: {path}"


def _directory(path: str, entries: list[Entry], offset: int, limit: int) -> str:
    names = sorted((f"{e.name}/" if e.is_dir else e.name for e in entries), key=lambda n: (n.lower(), n.swapcase()))
    start = offset - 1
    shown = names[start : start + limit]
    truncated = start + len(shown) < len(names)
    footer = (
        f"\n(Showing {len(shown)} of {len(names)} entries. "
        f"Use 'offset' parameter to read beyond entry {offset + len(shown)})"
        if truncated
        else f"\n({len(names)} entries)"
    )
    return "\n".join(
        [f"<path>{path}</path>", "<type>directory</type>", "<entries>", "\n".join(shown), footer, "</entries>"]
    )


def _sniff_mime(sample: bytes, path: str) -> str:
    if sample.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if sample.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if sample.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if sample[:4] == b"RIFF" and sample[8:12] == b"WEBP":
        return "image/webp"
    if sample.startswith(b"%PDF-"):
        return "application/pdf"
    return mimetypes.guess_type(basename(path))[0] or "application/octet-stream"


def _is_binary(path: str, sample: bytes) -> bool:
    if PurePosixPath(path).suffix.lower() in BINARY_EXTENSIONS:
        return True
    if not sample:
        return False
    if 0 in sample:
        return True
    non_printable = sum(1 for b in sample if b < 9 or 13 < b < 32)
    return non_printable / len(sample) > 0.3


async def _read_lines(
    ctx: RunContext[HasVFS],
    path: str,
    start: int,
    limit: int,
    config: ReadSettings,
    first: LineReadResult,
    version: tuple[UUID, int] | None,
) -> tuple[list[str], int, bool, bool] | None:
    """Lines from ``start`` (1-indexed): ``(lines, the file's line count, more after these, stopped at the byte
    cap)``, as ladderframe's ``_read_lines`` gives them from the whole file. ``first`` is the page's first batch,
    already read.

    The page comes in batches of ``pread_lines``, each line cut to ``_line_cap`` bytes, which decode to more than
    ``max_line_length`` characters, so a cut line is truncated to the same text as the whole line would be (its
    ``\\r``, if any, is past the cut). A batch is capped at raw bytes, big enough for a page of cut lines
    (``_fetch``), so most pages are one batch. A line the byte cap cut is decoded as is when it's the first of its
    batch, for the same reason; otherwise it's left to the next batch. None when a batch isn't
    from file version ``version`` (a new generation, or another file moved to the path), or the path is gone; with no ``version`` the
    page is taken as it comes. A ``limit`` or ``start`` beyond what ``pread_lines`` takes is clamped, as no file
    has that many lines."""
    fetch = _fetch(config)
    lines: list[str] = []
    size = 0
    line = start
    batch = first
    while True:
        if version is not None and (batch.id, batch.generation) != version:
            return None
        total = batch.lines
        for raw in _batch_lines(batch):
            text = raw.decode("utf-8", errors="replace")
            if line not in batch.cut:
                text = text.removesuffix("\r")
            if len(text) > config.max_line_length:
                text = text[: config.max_line_length] + f"... (line truncated to {config.max_line_length} chars)"
            line_size = len(text.encode("utf-8")) + (1 if lines else 0)
            if size + line_size > config.max_bytes:
                return lines, total, True, True
            lines.append(text)
            size += line_size
            line += 1
        if batch.eof or len(lines) >= limit or not batch.content:
            return lines, total, start + len(lines) <= total, False
        try:
            batch = await ctx.deps.vfs.pread_lines(
                path, min(line, _MAX_LINE), min(limit - len(lines), _MAX_LINES), fetch, max_line_bytes=_line_cap(config)
            )
        except VFSError as e:
            if version is not None and e.errno in _RACES:
                return None  # gone, or replaced by a directory, mid-page
            raise


def _line_cap(config: ReadSettings) -> int:
    """The bytes of a line ``pread_lines`` returns: more than ``max_line_length`` characters however they decode, as
    a decoded character takes at most 4 bytes."""
    return min(4 * (config.max_line_length + 1), _MAX_LINES)


def _fetch(config: ReadSettings) -> int:
    """The raw byte cap of a batch, so a page is one batch: a line of the page takes at most 4 times its share of
    ``max_bytes`` in raw bytes (a cut line ``_line_cap`` bytes and a ``\\n``, for over ``max_line_length``
    characters; a whole line at most its text, a ``\\r`` and a ``\\n``), plus the line that overflows
    ``max_bytes``, whole. That's also more than a line, so a first line the cap cut truncates as the whole line
    would."""
    return min(4 * config.max_bytes + _line_cap(config) + 8, _MAX_LINES)


def _batch_lines(batch: LineReadResult) -> list[bytes]:
    """The lines of ``batch``, without their ``\\n``: a last line the byte cap cut is kept only when it's the
    batch's first line."""
    if not batch.content:
        return []
    rows = batch.content.split(b"\n")
    if batch.content.endswith(b"\n"):
        rows.pop()
    elif not batch.eof and len(rows) > 1:
        rows.pop()  # cut by the cap, after other lines: the next batch starts with it
    return rows


async def _nearby_instructions(ctx: RunContext[HasVFS], path: str) -> list[str]:
    """AGENTS.md / CLAUDE.md files between the file and the working directory, once per conversation: in each
    directory from the file's up to, not including, the working directory, the first of the two that is a file
    the view lets the actor read. Every candidate is looked up, and the wanted ones read, in one ``pread_many``,
    so the cost doesn't grow with the depth; the file itself and those already shown are looked up, not read."""
    root = split(workdir(ctx))
    current = split(path)[:-1]
    if len(current) <= len(root) or current[: len(root)] != root:
        return []
    seen = _loaded_instructions(ctx)
    candidates = [
        join(*current[:end], name) for end in range(len(current), len(root), -1) for name in INSTRUCTION_FILES
    ]
    reads = [(c, 0, 0 if c == path or c in seen else _MAX_LINES) for c in candidates]
    try:
        found = await ctx.deps.vfs.pread_many(reads)
    except VFSError:
        return []  # a failed lookup (EIO): picking another name then could attach a file ladderframe never would
    texts: list[str] = []
    for i in range(0, len(candidates), len(INSTRUCTION_FILES)):
        group = slice(i, i + len(INSTRUCTION_FILES))
        for (candidate, _, limit), result in zip(reads[group], found[group], strict=True):
            if isinstance(result, VFSError):
                if result.errno in _ABSENT:
                    continue
                break  # any other outcome: skip the directory, as for EIO
            text = result.content.decode("utf-8", errors="replace").strip()
            if limit and text:
                texts.append(f"Instructions from: {candidate}\n{text}")
            break
    return texts


_ABSENT = frozenset({"ENOENT", "ENOTDIR", "EISDIR", "EACCES"})
"""What a lookup of an instruction file says when it's missing, a directory or the actor may not read it."""


def _loaded_instructions(ctx: RunContext[HasVFS]) -> set[str]:
    # ladderframe also takes the list from a Temporal run context, which has no ``messages``; minifs has no
    # Temporal integration, so under durable execution without ``messages`` the instructions repeat.
    try:
        return already_loaded(ctx.messages)
    except (AttributeError, UserError):
        return set()


def already_loaded(messages: list[Any]) -> set[str]:
    """Instruction files that earlier Read results in the conversation already included."""
    loaded: set[str] = set()
    for message in messages:
        for part in getattr(message, "parts", []):
            if isinstance(part, ToolReturnPart) and part.tool_name == "Read" and isinstance(part.content, str):
                for line in part.content.splitlines():
                    if line.startswith("Instructions from: "):
                        loaded.add(line.removeprefix("Instructions from: "))
    return loaded
