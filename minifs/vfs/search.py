"""Search: ripgrep's line search, run in Postgres over the current bytes of the files in scope (README: Search).

A search runs in one REPEATABLE READ transaction under ``SET LOCAL statement_timeout``, which applies to each of
its statements (resolving the path, the scope, the search; ``regex.validate`` uses its own, shorter one):

1. ``find_scope``: the files from ``vfs_glob``, with their sizes, read from ``vfs_nodes`` only. ``check_scope``
   refuses a scope over the file or byte cap, so an oversized search costs no content read.
2. ``search``: ``vfs_search`` goes through the files in path order, filters each extent with the pattern's file
   form, checks the lines of the extents that pass with its line form, and stops at the limit.

``VFSClient.grep`` is the entry point; these functions take a connection already in such a transaction.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from psycopg import AsyncConnection

from minifs.vfs import regex
from minifs.vfs.core import VFSError
from minifs.vfs.glob import compile_excludes, compile_glob

DEFAULT_LIMIT = 100  # hits, as ladderframe's Grep
MAX_FILES = 10_000  # files in scope
MAX_BYTES = 50 * 1024 * 1024  # bytes in scope, from vfs_nodes.size
MAX_LINE_CHARS = 2000  # characters of each hit's line returned; the Grep tool cuts lines there
TIMEOUT = 2.0  # seconds: the statement_timeout of each statement

# The scope when there is no include pattern: every file below the root.
_ALL_FILES = "**"


class SearchRefused(Exception):
    """A search refused before or while scanning: ``reason`` is ``"files"`` or ``"bytes"`` (the scope is over a
    cap; nothing was read) or ``"timeout"``. ``str()`` is a message for the model, with a hint to narrow the
    search."""

    def __init__(self, reason: Literal["files", "bytes", "timeout"], message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass(frozen=True)
class Matcher:
    """A pattern as ``vfs_search`` tests it: two regexes (file and line forms), or one literal for both."""

    file_pattern: str
    line_pattern: str
    fixed: bool
    translation: regex.Translation | None  # None for a literal, which Postgres needn't validate


@dataclass(frozen=True)
class Scope:
    """The files to search: their ids, the paths to report them by and their sizes, in the same order."""

    ids: tuple[UUID, ...]
    paths: tuple[str, ...]
    sizes: tuple[int, ...]

    @property
    def files(self) -> int:
        return len(self.ids)

    @property
    def bytes(self) -> int:
        return sum(self.sizes)

    def only(self, keep: Callable[[str], bool]) -> Scope:
        """The files whose path ``keep`` accepts."""
        kept = [i for i, path in enumerate(self.paths) if keep(path)]
        return Scope(
            tuple(self.ids[i] for i in kept), tuple(self.paths[i] for i in kept), tuple(self.sizes[i] for i in kept)
        )


@dataclass(frozen=True)
class GrepHit:
    """A matching line: ``line`` is its 1-based number and ``offset`` the file byte offset of its start, which
    ``VFSClient.pread`` accepts. ``text`` is the line without its ``\\n`` (a ``\\r`` before it is kept), cut to
    the search's ``max_line_chars`` characters if ``cut``."""

    path: str
    line: int
    offset: int
    text: str
    cut: bool


@dataclass(frozen=True)
class GrepResult:
    """The hits in order of path, then line; ``truncated`` if there were more than the limit. ``files`` and
    ``bytes`` are the scope's totals (from ``vfs_nodes``); the search may stop before reading all of it."""

    hits: list[GrepHit]
    truncated: bool
    files: int
    bytes: int


def compile_pattern(pattern: str, *, ignore_case: bool = False, fixed: bool = False) -> Matcher:
    """The matcher for a ripgrep pattern (``fixed``: a literal string, rg ``-F``). Raises ``RegexError`` with
    ripgrep's message for a pattern it rejects, such as one with a literal newline."""
    if fixed:
        translation = regex.translate_fixed(pattern, ignore_case)  # rejects what rg -F does
        if not ignore_case and "\0" not in pattern:  # a text parameter can't hold NUL; the regex spells it
            return Matcher(pattern, pattern, True, None)  # strpos is cheaper than a regex
    else:
        translation = regex.translate(pattern, ignore_case)
    return Matcher(translation.file_form, translation.line_form, False, translation)


async def find_scope(
    conn: AsyncConnection,
    root: UUID | None,
    prefix: str,
    include: str | None = None,
    *,
    excludes: Sequence[str] = (),
    hidden: bool = True,
    max_files: int = MAX_FILES,
    keep: Callable[[str], bool] | None = None,
) -> Scope:
    """The files below directory ``root`` (None: ``/``) that match glob ``include`` (None: all), as ``vfs_glob``
    walks them without the files and directories ``excludes`` match (``minifs.vfs.glob.compile_excludes``), each
    reported as ``prefix`` plus its path relative to ``root``, and only those whose reported path ``keep`` accepts
    (None: all). Reads only ``vfs_nodes``. At most ``max_files + 1`` files are listed, so ``check_scope`` can tell
    the cap was passed; a listing over the cap is returned unfiltered, for ``check_scope`` to refuse. ``.git`` is
    never walked. Raises GlobError for a bad ``include`` or exclude, ``VFSError`` ENOENT / ENOTDIR (for ``prefix``)
    for a bad ``root``."""
    plan = compile_glob(include or _ALL_FILES)
    ex = compile_excludes(excludes)
    cur = await conn.execute(
        "SELECT coalesce(array_agg(id ORDER BY path) FILTER (WHERE err IS NULL), '{}'),"
        " coalesce(array_agg(%s || path ORDER BY path) FILTER (WHERE err IS NULL), '{}'),"
        " coalesce(array_agg(size ORDER BY path) FILTER (WHERE err IS NULL), '{}'), min(err), min(err_detail)"
        " FROM vfs_glob(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            prefix,
            root,
            plan.regex,
            plan.name_like,
            plan.max_depth,
            list(ex.regexes),
            hidden,
            max_files + 1,
            list(plan.prefix),
            list(ex.prune_dirs),
        ),
    )
    row = await cur.fetchone()
    assert row is not None
    ids, paths, sizes, err, detail = row
    if err is not None:
        raise VFSError(err, prefix.removesuffix("/") or "/", detail=detail or "")
    scope = Scope(tuple(ids), tuple(paths), tuple(sizes))
    if keep is None or scope.files > max_files:  # over the cap: not every file was listed, so refuse it whole
        return scope
    return scope.only(keep)


def check_scope(scope: Scope, *, max_files: int = MAX_FILES, max_bytes: int = MAX_BYTES) -> None:
    """Raises SearchRefused if ``scope`` has more than ``max_files`` files or ``max_bytes`` bytes."""
    if scope.files > max_files:
        raise SearchRefused("files", f"Search scope is over {max_files} files; narrow the path or include pattern")
    if scope.bytes > max_bytes:
        raise SearchRefused(
            "bytes",
            f"Search scope is {_megabytes(scope.bytes)} (over the {_megabytes(max_bytes)} cap); narrow the path or"
            " include pattern",
        )


async def search(
    conn: AsyncConnection,
    scope: Scope,
    matcher: Matcher,
    *,
    limit: int = DEFAULT_LIMIT,
    max_line_chars: int | None = MAX_LINE_CHARS,
    timeout: float | None = None,
) -> GrepResult:
    """The lines of ``scope``'s files that match, at most ``limit`` (``vfs_search``). Validates the translated
    pattern in Postgres first (see ``minifs.vfs.regex.validate``), within ``timeout`` seconds if given (the
    transaction's statement timeout, which validation would otherwise lengthen to its own)."""
    if matcher.translation is not None:
        await regex.validate(conn, matcher.translation, timeout)
    if not scope.ids:
        return GrepResult([], False, 0, 0)
    cur = await conn.execute(
        "SELECT path, line_no, byte_offset, line, cut FROM vfs_search(%s, %s, %s, %s, %s, %s, %s)",
        (
            list(scope.ids),
            list(scope.paths),
            matcher.file_pattern,
            matcher.line_pattern,
            matcher.fixed,
            limit + 1,
            max_line_chars,
        ),
    )
    hits = [GrepHit(*row) for row in await cur.fetchall()]
    return GrepResult(hits[:limit], len(hits) > limit, scope.files, scope.bytes)


async def set_timeout(conn: AsyncConnection, seconds: float = TIMEOUT) -> None:
    """``SET LOCAL statement_timeout`` to ``seconds`` (rounded up to a millisecond), for the rest of the current
    transaction."""
    await conn.execute("SELECT set_config('statement_timeout', %s, true)", (f"{max(1, math.ceil(seconds * 1000))}ms",))


def timed_out() -> SearchRefused:
    """What a cancelled search statement raises. The server reports a statement timeout and an administrator's
    pg_cancel_backend with the same SQLSTATE and only a localized message to tell them apart, so both count as a
    timeout. (A cancelled asyncio task raises CancelledError instead.)"""
    return SearchRefused("timeout", "Search timed out; narrow the pattern or path")


def _megabytes(size: int) -> str:
    mb = size / (1024 * 1024)
    return f"{mb:.0f} MB" if mb >= 10 else f"{mb:.1f} MB"
