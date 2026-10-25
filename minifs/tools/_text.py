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

PAGE = 1024 * 1024
"""Bytes per ``pread`` when reading a whole file."""


async def read_file(vfs: VFSView, path: str) -> tuple[bytes, FileVersion]:
    """The whole content of file ``path`` and the version it is, read in pages: what a compare-and-swap write
    expects. ESTALE if the file changed between pages (the content would mix two versions)."""
    first = await vfs.pread(path, 0, PAGE)
    version = FileVersion(first.id, first.generation, first.size)
    chunks = [first.content]
    page = first
    while not page.eof:
        page = await vfs.pread(path, page.next_offset, PAGE)
        if (page.id, page.generation, page.size) != (version.id, version.generation, version.size):
            raise VFSError("ESTALE", path, "changed while it was read")
        chunks.append(page.content)
    return b"".join(chunks), version


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
