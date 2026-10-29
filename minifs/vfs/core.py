"""The VFS library's value types, its error, and path normalisation.

Paths are absolute and ``/``-separated. ``normalize`` removes ``.``, ``..`` and repeated slashes before anything
else looks at a path, as ladderframe's ``_path_subject`` does, so ``/workspace/../docs//a.md`` and
``/docs/a.md`` name the same file everywhere (resolution, the chroot view, the write policy). ``..`` at the root
stays at the root, as in POSIX. Unlike POSIX, a trailing slash is dropped too, so ``/notes.md/`` names the file
``/notes.md`` rather than failing with ENOTDIR: paths are names here, not walks through the kernel.

A path with a control character is EINVAL (``CONTROL_CHARACTERS``): names can't contain one (migration 016), and
refusing the path here keeps a newline or an escape sequence from reaching a round trip, a log line or a listing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

import psycopg

from minifs.db.errors import errno_name

Kind = Literal["file", "dir"]

CONTROL_CHARACTERS = re.compile("[\x00-\x1f\x7f-\x9f\u2028-\u202e\u2066-\u2069]")
"""What a name can't contain, as ``vfs_check_name`` tests it (migration 016): the C0 controls, DEL, the C1
controls, the Unicode line and paragraph separators, and the bidi embeddings, overrides and isolates. NUL is a
C0 control the server never sees: refused here, it can't surface as an encoding error (EIO) from psycopg."""


class VFSError(Exception):
    """A filesystem error: ``errno`` is the errno name (``"ENOENT"``, ...), ``path`` the path it concerns.

    ``message`` is the client's own explanation, in terms of ``path``. ``detail`` is the SQL function's DETAIL,
    for logs: it names node ids and real path components, so it is not part of ``str()``, and a view that
    rewrites paths (the chroot) must translate ``path`` and never show ``detail``. ``db_error`` is the psycopg
    error it came from, for logs too; it isn't chained as ``__cause__``, so tracebacks don't show it, and it isn't
    pickled.
    """

    def __init__(self, errno: str, path: str, message: str = "", detail: str = "") -> None:
        super().__init__(f"{errno}: {path}" + (f": {message}" if message else ""))
        self.errno = errno
        self.path = path
        self.message = message
        self.detail = detail
        self.db_error: psycopg.Error | None = None

    def __reduce__(self) -> tuple[type[VFSError], tuple[str, str, str, str]]:
        return (VFSError, (self.errno, self.path, self.message, self.detail))

    def at(self, path: str, errno: str | None = None, message: str | None = None) -> VFSError:
        """This error reported for ``path`` (and as ``errno``, with ``message``, if given), keeping its detail and
        database error."""
        error = VFSError(errno or self.errno, path, self.message if message is None else message, self.detail)
        error.db_error = self.db_error
        return error

    @classmethod
    def from_db(cls, error: psycopg.Error, path: str) -> VFSError | None:
        """The ``VFSError`` for an error from the minifs SQL functions or constraints, None for any other error."""
        errno = errno_name(error)
        if errno is None:
            return None
        return cls(errno, path, detail=error.diag.message_detail or "")


@dataclass(frozen=True)
class Entry:
    """A file or directory. The root ``/`` has ``id`` None. ``generation`` is None where the source lacks it
    (directory listings)."""

    path: str
    id: UUID | None
    kind: Kind
    size: int
    mtime: datetime | None
    generation: int | None = None

    @property
    def name(self) -> str:
        return basename(self.path)

    @property
    def is_dir(self) -> bool:
        return self.kind == "dir"

    @property
    def version(self) -> FileVersion | None:
        """What a compare-and-swap ``write`` expects, for a file entry with a generation; else None."""
        if self.kind != "file" or self.id is None or self.generation is None:
            return None
        return FileVersion(self.id, self.generation, self.size)


@dataclass(frozen=True)
class FileVersion:
    """A file's identity and content version, as returned by a create or a write: what a compare-and-swap
    write expects."""

    id: UUID
    generation: int
    size: int


@dataclass(frozen=True)
class GlobResult:
    """What ``VFSView.glob`` returns: the readable files it matched, newest first; ``truncated`` if it saw at least
    one more readable match past them (``VFSView.glob`` says when it can't tell)."""

    entries: list[Entry]
    truncated: bool


@dataclass(frozen=True)
class ReadResult:
    """Bytes ``[offset, next_offset)`` of file ``id``, with its ``size`` and ``generation`` from the same snapshot.

    ``offset`` is the resolved start (a negative or past-the-end request is clamped to ``[0, size]``), and
    ``eof`` says the read reached the end of the file.
    """

    id: UUID
    content: bytes
    offset: int
    next_offset: int
    size: int
    generation: int
    eof: bool


@dataclass(frozen=True)
class LineReadResult:
    """Lines ``first_line`` onwards of file ``id``: ``content`` is bytes ``[offset, next_offset)``, whole lines
    with their ``\\n``, except that the last one is cut when the read hit its byte cap (perhaps inside a UTF-8
    character, so decode with ``errors="replace"``), and the file's last line may have no ``\\n``. ``size``,
    ``generation`` and ``lines`` (the file's line count; a trailing ``\\n`` adds no empty line) come from the
    same snapshot.

    A ``first_line`` past the last line returns no content, ``offset == size`` and ``eof``; ``eof`` says the read
    reached the end of the file.

    With a line cap, ``cut`` holds the numbers of the lines that came back cut: each is the line's first
    ``max_line_bytes`` bytes and a ``\\n`` (added if the line had none). ``content`` is then shorter than
    ``[offset, next_offset)``, which still spans the lines read.
    """

    id: UUID
    content: bytes
    first_line: int
    offset: int
    next_offset: int
    size: int
    generation: int
    lines: int
    eof: bool
    cut: frozenset[int] = frozenset()


def normalize(path: str) -> str:
    """The canonical form of absolute ``path``: no ``.``, ``..``, repeated or trailing slashes.

    EINVAL for a relative path, an empty one, or one containing a control character (``CONTROL_CHARACTERS``).
    """
    return "/" + "/".join(split(path))


def split(path: str) -> tuple[str, ...]:
    """The components of absolute ``path`` after normalisation; ``()`` for the root. EINVAL as ``normalize``."""
    if not path.startswith("/"):
        raise VFSError("EINVAL", path, "path must be absolute")
    if CONTROL_CHARACTERS.search(path):
        raise VFSError("EINVAL", path, "path contains a control character")
    parts: list[str] = []
    for part in path.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return tuple(parts)


def join(*segments: str) -> str:
    """The absolute path of ``segments`` (already split)."""
    return "/" + "/".join(segments)


def basename(path: str) -> str:
    """The last component of normalised ``path``; ``""`` for the root."""
    return path.rsplit("/", 1)[-1]
