"""``VFSClient``: async path-based operations, each a thin wrapper over one call to a minifs SQL function.

Every operation takes absolute paths, normalises them (``minifs.vfs.core.normalize``), and runs as one statement
in its own transaction on a connection from the write pool, retried on a serialization failure or a deadlock.
Errors from the SQL layer come back as ``VFSError`` with the errno the function reported: an outcome (ENOENT,
EEXIST, ESTALE, ...) that it returned in its ``err`` column, or a fault (EINVAL, ENAMETOOLONG) that it raised
(migration 009). Any other database error propagates unchanged.

Path resolution: a statement resolves its own paths on the server (``vfs_lookup`` in a subquery), so an
operation costs one round trip. The function is called only if every lookup succeeded, so it never sees the NULL
id of a failed one, which for a directory would mean the root. A ``DirCache`` (the directory-entry cache of #19)
saves the walk: when every component of a path is cached, the statement checks the cached chain instead, every
``(parent, name) -> id`` link in one index lookup each, and falls back to ``vfs_lookup`` when any link no longer
holds. So a stale cache costs a slower resolution, never the wrong node. Each statement returns the id it
resolved: the client stores what it learns (listings, creates), evicts what it renames or removes, and evicts a
cached id that turned out stale. Keeping the cache fresh against other clients (NOTIFY, TTL) is the cache's job.
Without a cache, every path is resolved on the server.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol, cast
from uuid import UUID

import psycopg
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs.db.pool import retry_serializable
from minifs.vfs import glob as globbing
from minifs.vfs import search as searching
from minifs.vfs.core import (
    Entry,
    FileVersion,
    Kind,
    LineReadResult,
    ReadResult,
    VFSError,
    join,
    normalize,
    split,
)

DEFAULT_READ_LIMIT = 16_000
DEFAULT_LINE_LIMIT = 2000
DEFAULT_LINE_BYTES = 50 * 1024
_MAX_INT = 2**31 - 1  # vfs_pread's limit, vfs_pread_lines' limits and vfs_compact's target are int
_MAX_BIGINT = 2**63 - 1
_MAX_TIMEOUT = _MAX_INT / 1000  # statement_timeout is an int of milliseconds
_log = logging.getLogger(__name__)
_EVICT_TIMEOUT = 1.0  # seconds to wait for a connection to find stale cache links

# A node argument is a FROM item of one row (id, err, err_detail): the node's id, or the errno of a failed lookup.
_LOOKUP = "(SELECT id, err, err_detail FROM vfs_lookup(%s))"
# The last cached id if every cached link (id, parent, name) still holds, else the server's lookup.
_LOOKUP_CACHED = """(SELECT l.id, l.err, l.err_detail
   FROM (SELECT bool_and(n.id IS NOT NULL) AS ok, (array_agg(c.id ORDER BY c.i DESC))[1] AS last
           FROM unnest(%s::uuid[], %s::uuid[], %s::text[]) WITH ORDINALITY AS c(id, parent, name, i)
           LEFT JOIN vfs_nodes n
                  ON n.id = c.id AND n.parent_id IS NOT DISTINCT FROM c.parent AND n.name = c.name) v
  CROSS JOIN LATERAL (SELECT v.last AS id, NULL::text AS err, NULL::text AS err_detail WHERE v.ok
                      UNION ALL
                      SELECT r.id, r.err, r.err_detail FROM vfs_lookup(%s) r WHERE NOT v.ok) l)"""
_NULL = "(SELECT NULL::uuid AS id, NULL::text AS err, NULL::text AS err_detail)"


class DirCache(Protocol):
    """The directory-entry cache hook: ``(parent id, name) -> (id, kind)``, with None for the root. Implemented by
    ``minifs.vfs.cache.DirEntryCache``."""

    def lookup(self, parent: UUID | None, name: str) -> tuple[UUID, Kind] | None: ...

    def store(self, parent: UUID | None, name: str, node: UUID, kind: Kind) -> None: ...

    def evict(self, node: UUID) -> None: ...


type _Link = tuple[UUID, UUID | None, str]  # a cached (id, parent id, name)


class _Ref:
    """A node argument of a statement: a FROM item that yields the node's id (NULL for the root) or a lookup error
    (``_LOOKUP``), and its parameters.
    ``links`` is the cached ``(id, parent id, name)`` of every component, if the cache held the whole path;
    ``cached`` the last id."""

    def __init__(self, sql: str, params: Sequence[object], links: Sequence[_Link] = ()) -> None:
        self.sql = sql
        self.params = list(params)
        self.links = list(links)
        self.cached = self.links[-1][0] if self.links else None


_ROOT = _Ref(_NULL, [])


class VFSClient:
    """Path-based filesystem operations over the minifs SQL functions.

    ``pool`` serves every operation; ``search_pool`` (default: ``pool``) serves the scans (glob, grep).
    Both come from ``minifs.db.pool``. ``cache`` is an optional ``DirCache``.
    """

    def __init__(
        self,
        pool: AsyncConnectionPool[AsyncConnection[Any]],
        *,
        search_pool: AsyncConnectionPool[AsyncConnection[Any]] | None = None,
        cache: DirCache | None = None,
    ) -> None:
        self.pool = pool
        self.search_pool = search_pool or pool
        self.cache = cache
        self._background: set[asyncio.Task[None]] = set()
        self._evicting: dict[tuple[_Link, ...], asyncio.Task[None]] = {}  # by links, see _evict_stale_task

    # Reads

    async def stat(self, path: str) -> Entry:
        """The file or directory at ``path``. ENOENT; ENOTDIR if a non-final component is a file."""
        segments = split(path)
        if not segments:
            return Entry("/", None, "dir", 0, None)
        ref = self._ref(segments)
        rows = await self._run(
            path,
            f"SELECT r.id, r.err, r.err_detail, n.kind, n.size, n.mtime, n.generation"
            f" FROM {ref.sql} r LEFT JOIN LATERAL (SELECT * FROM vfs_nodes n WHERE n.id = r.id) n ON true",
            ref.params,
            [ref],
        )
        node, err, detail, kind, size, mtime, generation = rows[0]
        await self._outcome(path, [ref], (err, detail))
        self._checked(ref, node)
        return Entry(join(*segments), node, kind, size, mtime, generation)

    async def readdir(self, path: str) -> list[Entry]:
        """The entries of directory ``path``, in byte order of name. ENOENT, ENOTDIR."""
        segments = split(path)
        ref = self._dir_ref(segments)
        rows = await self._call(path, [ref], "vfs_readdir({0})", [])
        parent = rows[0][0]
        self._checked(ref, parent)
        entries: list[Entry] = []
        for node, name, kind, size, mtime in (row[3:8] for row in rows):
            if name is None:  # an empty directory
                continue
            if self.cache is not None:
                self.cache.store(parent, name, node, kind)
            entries.append(Entry(join(*segments, name), node, kind, size, mtime))
        return entries

    async def pread(self, path: str, offset: int = 0, limit: int = DEFAULT_READ_LIMIT) -> ReadResult:
        """Up to ``limit`` bytes of file ``path`` from byte ``offset``, like pread(2); a negative ``offset`` counts
        from the end (``tail -c``). Reading past the end returns no bytes and ``eof``. ENOENT, EISDIR; EINVAL for a
        negative ``limit``."""
        segments = split(path)
        if not segments:
            raise VFSError("EISDIR", "/")
        if not 0 <= limit <= _MAX_INT:
            raise VFSError("EINVAL", join(*segments), f"invalid limit {limit}")
        if not -_MAX_BIGINT - 1 <= offset <= _MAX_BIGINT:
            raise VFSError("EINVAL", join(*segments), f"invalid offset {offset}")
        ref = self._ref(segments)
        rows = await self._call(path, [ref], "vfs_pread({0}, %s::bigint, %s::int)", [offset, limit])
        node, size, generation, start = rows[0][0], *rows[0][3:6]
        self._checked(ref, node)
        content = b"".join(row[6] for row in rows if row[6] is not None)
        end = start + len(content)
        return ReadResult(node, content, start, end, size, generation, end >= size)

    async def pread_lines(
        self,
        path: str,
        first_line: int = 1,
        max_lines: int = DEFAULT_LINE_LIMIT,
        max_bytes: int = DEFAULT_LINE_BYTES,
    ) -> LineReadResult:
        """Up to ``max_lines`` whole lines of file ``path`` from line ``first_line`` (1-based), capped at
        ``max_bytes`` bytes, so the last line may come back cut. Lines split on ``\\n`` only. Reading past the last
        line returns no bytes and ``eof``, with the line count. ENOENT, EISDIR; EINVAL for a ``first_line`` below 1
        or a negative limit."""
        segments = split(path)
        if not segments:
            raise VFSError("EISDIR", "/")
        if not 1 <= first_line <= _MAX_BIGINT:
            raise VFSError("EINVAL", join(*segments), f"invalid first line {first_line}")
        if not (0 <= max_lines <= _MAX_INT and 0 <= max_bytes <= _MAX_INT):
            raise VFSError("EINVAL", join(*segments), f"invalid limits {max_lines} lines, {max_bytes} bytes")
        ref = self._ref(segments)
        rows = await self._call(
            path, [ref], "vfs_pread_lines({0}, %s::bigint, %s::int, %s::int)", [first_line, max_lines, max_bytes]
        )
        node = rows[0][0]
        size, generation, lines, _, start, content = rows[0][3:9]
        self._checked(ref, node)
        end = start + len(content)
        return LineReadResult(node, content, first_line, start, end, size, generation, lines, end >= size)

    async def glob(
        self,
        pattern: str,
        path: str = "/",
        *,
        excludes: Sequence[str] = (),
        hidden: bool = False,
        limit: int | None = None,
    ) -> list[Entry]:
        """The files below directory ``path`` that match glob ``pattern`` (``minifs.vfs.glob``), newest first, at
        most ``limit``. Resolving ``path`` and the walk share one snapshot on a search-pool connection, so the
        returned paths describe the tree the walk saw. ``GlobError`` for a bad pattern; ENOENT, ENOTDIR for ``path``;
        EINVAL for a bad ``limit``."""
        segments = split(path)
        if limit is not None and not 0 <= limit <= _MAX_INT:
            raise VFSError("EINVAL", join(*segments), f"invalid limit {limit}")
        globbing.compile_glob(pattern)  # a bad pattern is reported before anything about the path
        globbing.compile_excludes(excludes)
        ref = self._dir_ref(segments)

        async def attempt() -> tuple[_Error, UUID | None, list[globbing.GlobMatch]]:
            async with self.search_pool.connection() as conn, conn.transaction():
                await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")  # one snapshot for both
                root: UUID | None = None
                if segments:
                    node, error, kind, _ = await _lookup(conn, ref)
                    if error[0] is None and kind != "dir":
                        error = ("ENOTDIR", "")
                    if error[0] is not None:
                        return error, None, []  # committed, not rolled back: nothing failed on the server
                    root = node
                matches = await globbing.glob(conn, root, pattern, excludes=excludes, hidden=hidden, limit=limit)
                return (None, None), root, matches

        try:
            error, root, matches = await _mapped(path, attempt)
        except VFSError as e:
            if e.errno in ("ENOENT", "ENOTDIR"):
                await self._await_eviction([ref])
            raise
        await self._outcome(path, [ref], error)
        self._checked(ref, root)
        return [Entry(join(*segments, m.path), m.id, "file", m.size, m.mtime) for m in matches]

    async def grep(
        self,
        pattern: str,
        path: str = "/",
        *,
        include: str | None = None,
        excludes: Sequence[str] = (),
        ignore_case: bool = False,
        fixed: bool = False,
        hidden: bool = True,
        limit: int = searching.DEFAULT_LIMIT,
        max_files: int = searching.MAX_FILES,
        max_bytes: int = searching.MAX_BYTES,
        max_line_chars: int | None = searching.MAX_LINE_CHARS,
        timeout: float = searching.TIMEOUT,
    ) -> searching.GrepResult:
        """The lines matching ripgrep ``pattern`` (``fixed``: a literal string) in the files below directory
        ``path`` that match glob ``include`` (None: all) and none of ``excludes`` (as in ``glob``), or in file ``path``
        alone (``include``, ``excludes`` and ``hidden`` don't apply, as for a path given to rg), in order of path
        (bytewise), then line, at most ``limit`` (``minifs.vfs.search``). Unlike ``glob``, hidden files are searched
        unless ``hidden`` is false, as ladderframe's Grep does; ``.git`` never is. Files that aren't UTF-8 are skipped.

        One snapshot on a search-pool connection, each statement under ``timeout`` seconds: the scope is read from
        the tree first, and refused over ``max_files`` files or ``max_bytes`` bytes before any content is read.
        ``SearchRefused`` for a refused or timed-out search; ``RegexError`` for a bad pattern, ``GlobError`` for a
        bad ``include`` or exclude; ENOENT, ENOTDIR for ``path``; EINVAL for a bad limit or timeout."""
        segments = split(path)
        if not 0 <= limit < _MAX_INT:  # limit + 1 is asked for
            raise VFSError("EINVAL", join(*segments), f"invalid limit {limit}")
        if not 0 <= max_files < _MAX_INT or max_bytes < 0:
            raise VFSError("EINVAL", join(*segments), f"invalid caps {max_files} files, {max_bytes} bytes")
        if max_line_chars is not None and not 0 <= max_line_chars <= _MAX_INT:
            raise VFSError("EINVAL", join(*segments), f"invalid line length {max_line_chars}")
        if not 0 < timeout <= _MAX_TIMEOUT:
            raise VFSError("EINVAL", join(*segments), f"invalid timeout {timeout}")
        # Bad patterns are reported before anything about the path.
        matcher = searching.compile_pattern(pattern, ignore_case=ignore_case, fixed=fixed)
        if include is not None:
            globbing.compile_glob(include)
        globbing.compile_excludes(excludes)
        ref = self._dir_ref(segments)

        async def attempt() -> tuple[_Error, UUID | None, searching.GrepResult | None]:
            async with self.search_pool.connection() as conn, conn.transaction():
                await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")  # one snapshot for both
                await searching.set_timeout(conn, timeout)
                try:
                    node: UUID | None = None
                    scope: searching.Scope | None = None
                    if segments:
                        node, error, kind, size = await _lookup(conn, ref)
                        if error[0] is not None:
                            return error, None, None  # committed, not rolled back: nothing failed on the server
                        if kind == "file":
                            assert node is not None and size is not None
                            scope = searching.Scope((node,), (join(*segments),), size)
                    if scope is None:
                        scope = await searching.find_scope(
                            conn,
                            node,
                            join(*segments, ""),
                            include,
                            excludes=excludes,
                            hidden=hidden,
                            max_files=max_files,
                        )
                    searching.check_scope(scope, max_files=max_files, max_bytes=max_bytes)
                    result = await searching.search(
                        conn, scope, matcher, limit=limit, max_line_chars=max_line_chars, timeout=timeout
                    )
                except psycopg.errors.QueryCanceled:
                    raise searching.timed_out() from None
                return (None, None), node, result

        try:
            error, node, result = await _mapped(path, attempt)
        except VFSError as e:
            if e.errno in ("ENOENT", "ENOTDIR"):
                await self._await_eviction([ref])
            raise
        await self._outcome(path, [ref], error)
        assert result is not None
        self._checked(ref, node)
        return result

    # Writes

    async def create(self, path: str, data: bytes | None = None, *, parents: bool = False) -> FileVersion:
        """Creates file ``path`` with ``data``, like open(O_CREAT|O_EXCL). With ``parents``, creates the missing
        directories too (in the same transaction). EEXIST, ENOENT, ENOTDIR, EINVAL, ENAMETOOLONG."""
        return await self._create(path, "file", data, parents)

    async def mkdir(self, path: str, *, parents: bool = False) -> UUID:
        """Creates directory ``path``. With ``parents``, creates the missing directories above it too, but the
        final one must not exist (unlike ``mkdir_p``). EEXIST, ENOENT, ENOTDIR, EINVAL, ENAMETOOLONG."""
        return (await self._create(path, "dir", None, parents)).id

    async def mkdir_p(self, path: str) -> UUID | None:
        """Like ``mkdir -p``: creates every missing directory of ``path`` and returns the last one's id (None for
        the root). ENOTDIR if a non-final component is a file, EEXIST if the final one is."""
        segments = split(path)
        rows = await self._run(path, "SELECT id, err, err_detail FROM vfs_mkdir_p(%s)", [list(segments)])
        node, err, detail = rows[0]
        await self._outcome(path, [], (err, detail))
        return node

    async def append(self, path: str, data: bytes) -> int:
        """Appends ``data`` to file ``path``, like write(2) with O_APPEND, and returns the offset it landed at.
        ENOENT, EISDIR."""
        segments = split(path)
        if not segments:
            raise VFSError("EISDIR", "/")
        ref = self._ref(segments)
        rows = await self._call(path, [ref], "vfs_append({0}, %s)", [data])
        self._checked(ref, rows[0][0])
        return rows[0][3]

    async def write(self, path: str, data: bytes, *, expect: FileVersion | None = None) -> FileVersion:
        """Replaces the content of file ``path`` with ``data`` as a new generation, like open(O_TRUNC) + write(2).

        With ``expect``, it is a compare-and-swap: it writes node ``expect.id`` only if that node is still at
        ``path`` (same parent directory and name) with ``expect``'s generation and size, and raises ESTALE
        otherwise. ENOENT, EISDIR."""
        segments = split(path)
        if not segments:
            raise VFSError("EISDIR", "/")
        if expect is None:
            ref = self._ref(segments)
            rows = await self._call(path, [ref], "vfs_write({0}, %s)", [data])
            self._checked(ref, rows[0][0])
            return FileVersion(rows[0][0], *rows[0][3:5])
        parent = self._dir_ref(segments[:-1])
        rows = await self._call(
            path,
            [parent],
            "vfs_write(%s, %s, %s, %s, {0}, %s)",
            [expect.id, data, expect.generation, expect.size, segments[-1]],
        )
        return FileVersion(expect.id, *rows[0][3:5])

    async def rename(self, src: str, dst: str) -> None:
        """Moves ``src`` to ``dst``, like rename(2) with RENAME_NOREPLACE. ENOENT, ENOTDIR (on ``src`` or ``dst``,
        whichever the error concerns), EEXIST (``dst`` exists), EINVAL (a directory into its own subtree, or the
        root)."""
        await self._rename(src, dst, claim=False)

    async def claim(self, src: str, dst: str) -> bool:
        """Moves ``src`` to ``dst`` only if it is still in ``src``'s directory when the move locks it, and returns
        whether it moved: of clients claiming the same file, exactly one gets True (a Maildir claim from
        ``tasks/new`` to ``tasks/cur``) and the others False, including those that find ``src`` already gone.
        Errors as ``rename``; ENOENT if ``src``'s directory is missing, EINVAL for a ``src`` in the root."""
        return await self._rename(src, dst, claim=True)

    async def unlink(self, path: str) -> None:
        """Removes file or empty directory ``path``, like unlink(2) / rmdir(2). ENOENT, ENOTEMPTY; EINVAL for
        the root."""
        segments = split(path)
        if not segments:
            raise VFSError("EINVAL", "/", "cannot remove the root")
        ref = self._ref(segments)
        rows = await self._call(path, [ref], "vfs_unlink({0})", [])
        self._evict(ref, rows[0][0])

    async def compact(self, path: str, target: int = 65536) -> int:
        """Merges the extents of file ``path`` into pieces of about ``target`` bytes and returns how many it
        removed. Content, size, generation and mtime don't change. ENOENT, EISDIR, EINVAL."""
        segments = split(path)
        if not segments:
            raise VFSError("EISDIR", "/")
        if not 0 < target <= _MAX_INT:
            raise VFSError("EINVAL", join(*segments), f"invalid target size {target}")
        ref = self._ref(segments)
        rows = await self._call(path, [ref], "vfs_compact({0}, %s::int)", [target])
        self._checked(ref, rows[0][0])
        return rows[0][3]

    # Internals

    async def _create(self, path: str, kind: Kind, data: bytes | None, parents: bool) -> FileVersion:
        segments = split(path)
        if not segments:
            raise VFSError("EEXIST", "/")
        parent = _Ref("vfs_mkdir_p(%s)", [list(segments[:-1])]) if parents else self._dir_ref(segments[:-1])
        try:
            rows = await self._call(path, [parent], "vfs_create({0}, %s, %s, %s)", [segments[-1], kind, data])
        except VFSError as e:
            # vfs_mkdir_p reports a file at its last component, our parent, as EEXIST: for us it is ENOTDIR.
            if (
                parents
                and e.errno == "EEXIST"
                and len(segments) > 1
                and not (await self.stat(join(*segments[:-1]))).is_dir
            ):
                raise e.at(join(*segments), "ENOTDIR", "the parent is a file") from None
            raise
        parent_id = rows[0][0]
        self._checked(parent, parent_id)
        version = FileVersion(*rows[0][3:6])
        if self.cache is not None:
            self.cache.store(parent_id, segments[-1], version.id, kind)
        return version

    async def _rename(self, src: str, dst: str, *, claim: bool) -> bool:
        src_segments, dst_segments = split(src), split(dst)
        if not src_segments:
            raise VFSError("EINVAL", "/", "cannot move the root")
        if not dst_segments:
            raise VFSError("EEXIST", "/")
        if claim and len(src_segments) == 1:
            raise VFSError("EINVAL", join(*src_segments), "a claim needs a source directory other than the root")
        node = self._ref(src_segments)
        parent = self._dir_ref(dst_segments[:-1])
        expect = self._dir_ref(src_segments[:-1]) if claim else _Ref(_NULL, [])  # NULL: no compare-and-swap
        refs = [node, parent, expect]
        try:
            # Raised errors too (a create that commits during the move raises EEXIST), so both get the same paths.
            rows = await self._run(
                src,
                _gated(refs, "vfs_rename({0}, {1}, %s, {2})"),
                [*node.params, *parent.params, *expect.params, dst_segments[-1]],
                refs,
            )
            if rows[0][-2] == "ESTALE":  # lost the compare-and-swap: someone else moved it first
                self._evict(node, rows[0][0])
                return False
            await self._outcome(src, refs, *_lookup_errors(rows[0], len(refs)), (rows[0][-2], rows[0][-1]))
        except VFSError as e:
            if e.errno == "EEXIST":
                raise e.at(join(*dst_segments)) from None
            if e.errno not in ("ENOENT", "ENOTDIR"):
                raise
            # Which path is it about? Only the error path pays for finding out.
            try:
                await self.stat(join(*src_segments))
            except VFSError as src_error:
                if claim and src_error.errno == "ENOENT" and src_error.path == join(*src_segments):
                    # Lost the race, unless the claim could never succeed: check what vfs_rename didn't get to.
                    await self.stat(join(*src_segments[:-1]))
                    await self._check_destination(dst_segments)
                    return False
                raise src_error from None
            raise e.at(join(*dst_segments)) from None
        self._evict(node, rows[0][0])
        return True

    async def _check_destination(self, segments: tuple[str, ...]) -> None:
        """Raises what a move to ``segments`` would: a missing or file parent directory, an invalid name."""
        path = join(*segments)
        if segments[:-1]:
            try:
                parent = await self.stat(join(*segments[:-1]))
            except VFSError as e:
                raise e.at(path) from None
            if not parent.is_dir:
                raise VFSError("ENOTDIR", path, "the parent is a file")
        if len(segments[-1].encode()) > 255:
            raise VFSError("ENAMETOOLONG", path, "name over 255 bytes")

    def _ref(self, segments: tuple[str, ...]) -> _Ref:
        chain = self._cached_chain(segments)
        if chain is None:
            return _Ref(_LOOKUP, [list(segments)])
        ids = [node for node, _ in chain]
        parents = [parent for _, parent in chain]
        links = list(zip(ids, parents, segments, strict=True))
        return _Ref(_LOOKUP_CACHED, [ids, parents, list(segments), list(segments)], links)

    def _dir_ref(self, segments: tuple[str, ...]) -> _Ref:
        """Like ``_ref``, for a directory argument, where the root is NULL (and needs no subquery)."""
        return self._ref(segments) if segments else _ROOT

    def _cached_chain(self, segments: tuple[str, ...]) -> list[tuple[UUID, UUID | None]] | None:
        """``(id, parent id)`` of every component of ``segments`` if the cache holds them all, else None."""
        if self.cache is None or not segments:
            return None
        chain: list[tuple[UUID, UUID | None]] = []
        parent: UUID | None = None
        for name in segments:
            hit = self.cache.lookup(parent, name)
            if hit is None:
                return None
            chain.append((hit[0], parent))
            parent = hit[0]
        return chain

    def _checked(self, ref: _Ref, resolved: UUID | None) -> None:
        """Evicts the stale links of ``ref`` if the server resolved the path to another node.

        This runs after the operation committed, so it runs no query: finding the stale links does, in the
        background (see ``settle``), so a slow pool can't widen the window in which a cancelled caller sees a
        committed write as failed. (A cancellation from COMMIT on is ambiguous anyway: re-check with ``stat``.)"""
        if self.cache is not None and ref.cached is not None and ref.cached != resolved:
            self._evict_stale_task([ref])

    def _evict_stale_task(self, refs: Sequence[_Ref]) -> asyncio.Task[None] | None:
        """A background ``_evict_stale(refs)``, shared by every caller that finds the same links stale before it sends
        its query (typically while it waits for a connection): concurrent operations on one stale path cost one
        query, not one each. A caller that comes later gets a new task, since the running query may predate what
        that caller saw. None if there is nothing to check."""
        key = tuple(link for ref in refs for link in ref.links)
        if self.cache is None or not key:
            return None
        task = self._evicting.get(key)
        if task is None:
            task = asyncio.create_task(
                self._evict_stale(refs, started=lambda: self._unshare(key, asyncio.current_task()))
            )
            self._evicting[key] = task
            self._background.add(task)
            task.add_done_callback(lambda done: self._unshare(key, done))
            task.add_done_callback(self._background_done)
        return task

    def _unshare(self, key: tuple[_Link, ...], task: asyncio.Task[Any] | None) -> None:
        if self._evicting.get(key) is task:
            del self._evicting[key]

    async def _await_eviction(self, refs: Sequence[_Ref]) -> None:
        """Evicts the stale links of ``refs`` before an error is raised, so a retry doesn't reuse them. Cancelling
        the wait doesn't cancel the shared eviction, and the eviction failing or being cancelled doesn't change what
        the caller raises (``_background_done`` logs a failure)."""
        task = self._evict_stale_task(refs)
        if task is not None:
            await asyncio.wait({task})

    def _background_done(self, task: asyncio.Task[None]) -> None:
        self._background.discard(task)
        if not task.cancelled() and (error := task.exception()) is not None:
            _log.warning("evicting stale cache entries failed", exc_info=error)

    async def settle(self) -> None:
        """Waits for the background cache maintenance started so far (for tests and orderly shutdown). Cancelling
        the wait doesn't cancel the work."""
        pending = set(self._background)
        if pending:
            await asyncio.wait(pending)

    async def _evict_stale(self, refs: Sequence[_Ref], started: Callable[[], None] | None = None) -> None:
        """Evicts the cached links of ``refs`` that no longer hold, found in one query. Links are keyed by parent id
        and ids are never reused, so the links below a stale one stay valid and are kept. ``started`` is called once,
        just before the query is sent (or when getting a connection fails)."""
        links = [link for ref in refs for link in ref.links]
        if self.cache is None or not links:
            return
        try:
            # A short wait: on the error path, the caller's error waits for this.
            async with self.pool.connection(timeout=_EVICT_TIMEOUT) as conn:
                if started is not None:
                    started()
                    started = None
                cur = await conn.execute(
                    "SELECT c.id FROM unnest(%s::uuid[], %s::uuid[], %s::text[]) AS c(id, parent, name)"
                    " WHERE NOT EXISTS (SELECT FROM vfs_nodes n"
                    "                    WHERE n.id = c.id AND n.parent_id IS NOT DISTINCT FROM c.parent"
                    "                      AND n.name = c.name)",
                    [[link[0] for link in links], [link[1] for link in links], [link[2] for link in links]],
                )
                stale = {row[0] for row in await cur.fetchall()}
        except psycopg.Error:
            if started is not None:
                started()
            stale = {link[0] for link in links}  # can't tell: drop them all
        for node in stale:
            self.cache.evict(node)

    def _evict(self, ref: _Ref, resolved: UUID) -> None:
        """Evicts a node this client moved or removed (and stale links of its path)."""
        if self.cache is not None:
            self._checked(ref, resolved)
            self.cache.evict(resolved)

    async def _call(
        self, path: str, refs: Sequence[_Ref], call: str, params: Sequence[object]
    ) -> list[tuple[Any, ...]]:
        """Runs ``_gated(refs, call)`` with ``params`` (the call's own) and raises the error it returned, if any:
        the first failed lookup's, else the call's. Returns the rows (see ``_gated``)."""
        rows = await self._run(path, _gated(refs, call), [*(p for ref in refs for p in ref.params), *params], refs)
        await self._outcome(path, refs, *_lookup_errors(rows[0], len(refs)), (rows[0][-2], rows[0][-1]))
        return rows

    async def _outcome(self, path: str, refs: Sequence[_Ref], *errors: _Error) -> None:
        """Raises the first of ``errors``, the ``(err, err_detail)`` pairs a statement returned, as a ``VFSError``
        for ``path``: the one place a returned outcome becomes an exception, with the same errno and detail a raised
        one had (and no ``db_error``). ENOENT and ENOTDIR first evict the links of ``refs`` that went stale, as
        ``_run`` does for raised errors, so a retry doesn't reuse them."""
        for err, detail in errors:
            if err is not None:
                if err in ("ENOENT", "ENOTDIR"):
                    await self._await_eviction(refs)
                raise VFSError(err, normalize(path), detail=detail or "")

    async def _run(
        self, path: str, query: str, params: Sequence[object], refs: Sequence[_Ref] = ()
    ) -> list[tuple[Any, ...]]:
        """Runs ``query`` in its own transaction, retrying serialization failures; maps raised SQL errors (faults,
        and the constraints' backstop) to VFSError. A path that no longer resolves evicts the links of ``refs``
        that went stale. Returned errors are the caller's to check (``_outcome``)."""

        async def attempt() -> list[tuple[Any, ...]]:
            async with self.pool.connection() as conn, conn.transaction():
                cur = await conn.execute(cast(Any, query), params)
                return await cur.fetchall()

        try:
            return await _mapped(path, attempt)
        except VFSError as e:
            if e.errno in ("ENOENT", "ENOTDIR"):
                await self._await_eviction(refs)
            raise


type _Error = tuple[str | None, str | None]  # a returned (err, err_detail); (None, None) for success


def _gated(refs: Sequence[_Ref], call: str) -> str:
    """A query that looks up ``refs`` (as ``r0``, ``r1``, ...) and runs ``call``, a set-returning function call over
    their ids, only if every lookup succeeded. Each row has every ref's id, err and err_detail, then
    the call's columns, ending with its err and err_detail; there is always at least one row, with the call's
    columns NULL when it didn't run or returned no rows.

    ``call`` names the ids ``{0}``, ``{1}``, ...: each is the ref's id through ``vfs_gate``. The call is in a
    subquery whose WHERE refers only to the lookups, so the planner makes it a one-time filter, checked before the
    call runs (OFFSET 0 keeps the subquery from being merged into the join). Were it ever not, ``vfs_gate`` would
    raise EINVAL rather than pass on a failed lookup's NULL, which for a directory would mean the root."""
    call = call.format(*(f"vfs_gate(r{i}.id, r{i}.err)" for i in range(len(refs))))
    lookups = " CROSS JOIN ".join(f"{ref.sql} r{i}" for i, ref in enumerate(refs))
    columns = "".join(f"r{i}.id, r{i}.err, r{i}.err_detail, " for i in range(len(refs)))
    gate = " AND ".join(f"r{i}.err IS NULL" for i in range(len(refs)))
    return (
        f"SELECT {columns}f.* FROM {lookups} LEFT JOIN LATERAL (SELECT * FROM {call} WHERE {gate} OFFSET 0) f ON true"
    )


def _lookup_errors(row: Sequence[Any], count: int) -> list[_Error]:
    """The lookup errors of the first ``count`` refs in a row of ``_gated``."""
    return [(row[3 * i + 1], row[3 * i + 2]) for i in range(count)]


async def _lookup(conn: AsyncConnection[Any], ref: _Ref) -> tuple[UUID | None, _Error, Kind | None, int | None]:
    """The id, lookup error, kind and size of the node ``ref`` names, on ``conn``."""
    query = (
        f"SELECT r.id, r.err, r.err_detail, n.kind, n.size"
        f" FROM {ref.sql} r LEFT JOIN LATERAL (SELECT * FROM vfs_nodes n WHERE n.id = r.id) n ON true"
    )
    cur = await conn.execute(cast(Any, query), ref.params)
    row = await cur.fetchone()
    assert row is not None
    return row[0], (row[1], row[2]), row[3], row[4]


async def _mapped[T](path: str, call: Callable[[], Awaitable[T]]) -> T:
    try:
        return await retry_serializable(call)
    except psycopg.Error as e:
        error = VFSError.from_db(e, normalize(path))
        if error is None:
            raise
        error.db_error = e
        raise error from None  # the psycopg error's text has the server DETAIL
