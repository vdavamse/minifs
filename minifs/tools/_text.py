"""What ``Write`` and ``Edit`` share: ladderframe's text helpers (BOM, line endings, and the jsdiff-style patch with
its change counts and trimming, from ``ladderframe.tools.edit``), reading a whole file with its version, and
creating one.
"""

from __future__ import annotations

import difflib
import re

from minifs.vfs.core import FileVersion, VFSError
from minifs.vfs.view import VFSView

BOM = "\ufeff"

PAGE = 16 * 1024 * 1024
"""The most bytes one page statement of ``read_file`` asks for: big enough that Edit's default 10 MB cap is one
statement. Not small, because a line longer than ~64 KB is one pglz extent, and a page that starts inside it
makes the server decompress the extent from its start (migration 013): small pages would make a long-line file
cost about size²/page. A page costs about 3x its size in client memory while it arrives (hex text). Every page
of a read is on one snapshot (``VFSView.pread_file``)."""


class TooLarge(Exception):
    """The file is over ``read_file``'s ``max_size``: ``size`` bytes."""

    def __init__(self, size: int) -> None:
        super().__init__(size)
        self.size = size


class ReadTimedOut(Exception):
    """A read of ``path`` was cancelled by a server timeout (ETIMEDOUT): the file was too slow to read, not the
    write."""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.path = path


async def read_file(vfs: VFSView, path: str, max_size: int | None = None) -> tuple[bytes, FileVersion]:
    """The whole content of file ``path`` and the version it is: what a compare-and-swap write expects.
    ``TooLarge``, having read at most one page, for a file over ``max_size`` bytes; ``ReadTimedOut`` where a
    server timeout cancelled the read.

    The file is read on one snapshot (``VFSView.pread_file``), in pages of at most ``PAGE`` bytes, so it never
    mixes two versions, whatever its size or the caps that apply: a file within ``max_size`` comes back whole,
    and one that grew past it is refused having read at most a page of it. Pages are large (``PAGE``), since a
    page into a long line's pglz extent decompresses it from its start.

    A page costs about three times its size in client memory while it arrives (bytea comes as hex text, then is
    decoded), and the server holds it whole before sending it (spilling to disk past ``work_mem``). Reads set no
    ``statement_timeout`` of their own, so a server one applies to each page statement, and is a slow read to
    report (``ReadTimedOut``), not a slow write."""
    page = PAGE if max_size is None else max(1, min(max_size, PAGE))
    try:
        result = await vfs.pread_file(path, page, max_size)
    except VFSError as e:
        if e.errno == "ETIMEDOUT":
            raise ReadTimedOut(path) from None
        raise
    if max_size is not None and result.size > max_size:
        raise TooLarge(result.size)
    return result.content, FileVersion(result.id, result.generation, result.size)


async def create_file(vfs: VFSView, path: str, data: bytes) -> None:
    """Creates file ``path``, with its missing parent directories. ``parents`` only when the parent is missing (the
    create without it is ENOENT), since the view checks ``write`` on every directory a ``parents`` create could
    make, existing or not."""
    try:
        await vfs.create(path, data)
    except VFSError as e:
        if e.errno != "ENOENT":
            raise
        await vfs.create(path, data, parents=True)


def split_bom(text: str) -> tuple[bool, str]:
    return (True, text[1:]) if text.startswith(BOM) else (False, text)


def encode_text(text: str, bom: bool) -> bytes:
    """``text`` as UTF-8, starting with one BOM if ``bom`` (ladderframe's ``write_text``)."""
    return ((BOM if bom else "") + split_bom(text)[1]).encode("utf-8")


def normalize_line_endings(text: str) -> str:
    return text.replace("\r\n", "\n")


def detect_line_ending(text: str) -> str:
    return "\r\n" if "\r\n" in text else "\n"


def convert_to_line_ending(text: str, ending: str) -> str:
    return text if ending == "\n" else text.replace("\n", "\r\n")


def _lines(text: str) -> list[str]:
    """Lines with their `\\n`, split on `\\n` only like jsdiff (`str.splitlines` also splits on `\\f`, `\\r`, ...)."""
    return re.findall(r"[^\n]*\n|[^\n]+$", text)


def unified_diff(path: str, old: str, new: str) -> str:
    """A patch in the format of jsdiff's `createTwoFilesPatch`, which opencode puts in tool metadata."""
    out = [f"Index: {path}\n", "=" * 67 + "\n"]
    lines = difflib.unified_diff(_lines(old), _lines(new), path, path)
    for line in lines:
        out.append(line if line.endswith("\n") else line + "\n\\ No newline at end of file\n")
    if len(out) == 2:
        out += [f"--- {path}\n", f"+++ {path}\n"]
    return "".join(out)


def count_changes(diff: str) -> tuple[int, int]:
    """`(lines added, lines removed)` in a `unified_diff`, so the counts always agree with the patch.
    (`difflib.ndiff` or a non-autojunk matcher take minutes on long runs of similar or repeated lines.)"""
    additions = deletions = 0
    in_hunks = False
    for line in diff.split("\n"):
        if line.startswith("@@"):
            in_hunks = True
        elif in_hunks and line.startswith("+"):
            additions += 1
        elif in_hunks and line.startswith("-"):
            deletions += 1
    return additions, deletions


def _is_hunk_line(line: str) -> bool:
    return line[:1] in ("+", "-", " ") and not line.startswith(("---", "+++"))


def trim_diff(diff: str) -> str:
    """Remove the indentation every changed or context line shares, so deeply nested edits read well."""
    lines = diff.split("\n")
    content_lines = [line for line in lines if _is_hunk_line(line)]
    if not content_lines:
        return diff
    indents = [len(c) - len(c.lstrip()) for c in (line[1:] for line in content_lines) if c.strip()]
    least = min(indents, default=0)
    if least == 0:
        return diff
    return "\n".join(line[0] + line[1:][least:] if _is_hunk_line(line) else line for line in lines)


def diff_metadata(path: str, old: str, new: str) -> dict[str, str | int]:
    """The ``diff``, ``additions`` and ``deletions`` of the tools' metadata, as ladderframe computes them."""
    diff = unified_diff(path, old, new)
    additions, deletions = count_changes(diff)
    return {"diff": trim_diff(diff), "additions": additions, "deletions": deletions}
