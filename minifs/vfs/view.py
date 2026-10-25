"""``VFSView``: an actor's view of the tree (a chroot) with the write policy enforced (README: *The agent's view*).

Mounts map visible prefixes to real ones, like bind mounts: ``/workspace`` to ``/workspaces/<workspace_id>``,
``/docs`` to ``/docs``. A path is normalised before it is mapped, so ``..`` can't climb out of a mount, and the
longest mount prefix wins (mounts may not nest, so there is at most one). A path outside every mount is ENOENT,
whether or not something is there, so the view never reveals other workspaces. The directories above the mount
points (``/`` for the mounts above) are virtual: they list the mount points below them and can't be written,
moved or removed. There are no symlinks in the tree (``vfs_nodes.kind`` is ``file`` or ``dir``), so a mapped path
never leads elsewhere.

Every operation checks the ``Policy`` for the view's actor on the visible path before it runs (EACCES, counted
in ``policy.denials``), then runs as the ``VFSClient`` operation on the real path. The check needs no
transaction of its own: it depends on the path alone, and the operation resolves that same path in its one
statement. Results and errors come back in visible paths: a ``VFSError`` keeps its errno and message, gets the
visible path, and drops ``detail`` and ``db_error``, which name real paths and node ids. Entries the actor may
not read are left out of listings and glob results; a grep needs ``read`` on everything it searches.

What each operation needs:

- ``read`` for ``stat``, ``readdir``, ``pread``, ``pread_lines``, ``glob`` and ``grep``;
- ``write`` on the path for ``create``, ``mkdir``, ``mkdir_p``, ``write`` and ``unlink``, and on every directory
  between the mount point and the path when a call may create missing parents (``mkdir_p``, ``parents=True``);
- ``write`` on both paths for ``rename`` and ``claim``, and ``read`` on the source. Moving a directory also
  needs ``write`` everywhere below both paths and ``read`` everywhere below the source
  (``Policy.allows(subtree=True)``), so a move can't carry a file out from under a more specific rule or land
  an unreadable file where it can be read. A file has nothing below it: when the policy would refuse a
  directory there, the view checks that the source is a file and moves only that node (``expect``);
- ``append`` (or ``write``) for ``append``.

An actor may learn a little it can't read: ``append`` returns the new size of a file it may append to, and
``create`` is EEXIST if something is there; a glob with a ``limit`` that returns fewer than ``limit`` entries, or
is not ``truncated``, while a glob without a limit finds more readable files, tells that unreadable newer files
fill its refetch window; and an ``expect`` naming some node id outside the path tells whether that id exists (ids
are random, and the view hands out only ids inside the mounts). Moving or removing a mount point or a virtual
directory is EBUSY. A mount point is a directory: a file can't be created there, and creating one with ``parents``
(or by ``mkdir_p``) also creates the real directories above it. A database error other than a ``VFSError`` comes
back as EIO, since its text may name real paths.
"""

from __future__ import annotations

import dataclasses
import logging
import re
from collections.abc import Awaitable, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import psycopg

from minifs.vfs import glob as globbing
from minifs.vfs import search as searching
from minifs.vfs.client import DEFAULT_LINE_BYTES, DEFAULT_LINE_LIMIT, DEFAULT_READ_LIMIT, MAX_TIMEOUT, VFSClient
from minifs.vfs.core import Entry, FileVersion, GlobResult, LineReadResult, ReadResult, VFSError, join, split
from minifs.vfs.policy import Action, Policy

_MAX_LIMIT = 2**31 - 1  # vfs_glob's limit is an int
MAX_GLOB_LIMIT = _MAX_LIMIT - 1
"""The largest ``limit`` ``VFSView.glob`` takes (it asks for ``limit + 1``)."""
_REFETCH_FACTOR = 8
_NO_NODE = UUID(int=0)  # never a node id: a pin that nothing matches
_log = logging.getLogger(__name__)


def workspace_mounts(workspace_id: UUID | str) -> dict[str, str]:
    """The agent's mounts: its workspace at ``/workspace``, and the shared ``/docs``. ValueError if
    ``workspace_id`` isn't a UUID (``""`` or ``..`` would mount other workspaces)."""
    return {"/workspace": f"/workspaces/{UUID(str(workspace_id))}", "/docs": "/docs"}


class _Mount:
    def __init__(self, visible: tuple[str, ...], real: tuple[str, ...]) -> None:
        self.visible = visible
        self.real = real


class _Target:
    """A visible path mapped through ``mount`` (None for a virtual directory) to ``real``."""

    def __init__(self, path: tuple[str, ...], mount: _Mount | None) -> None:
        self.path = path
        self.mount = mount
        self.real = join(*mount.real, *path[len(mount.visible) :]) if mount else None

    @property
    def visible(self) -> str:
        return join(*self.path)

    @property
    def is_mount_point(self) -> bool:
        return self.mount is not None and self.path == self.mount.visible


class VFSView:
    """``client``'s tree as ``actor`` sees it through ``mounts`` (visible prefix -> real prefix), under
    ``policy``. The operations are ``VFSClient``'s, in visible paths. ValueError for an actor the policy has no
    rules for, or for mounts that nest, visibly or in the real tree (two visible names for one node would let
    the policy on one name be bypassed through the other)."""

    def __init__(self, client: VFSClient, mounts: Mapping[str, str], policy: Policy, actor: str) -> None:
        if actor not in policy.actors:
            raise ValueError(f"the policy has no rules for actor {actor!r}")
        self._client = client
        self._mounts = [_Mount(split(v), split(r)) for v, r in mounts.items()]
        for a in self._mounts:
            for b in self._mounts:
                if a is b:
                    continue
                if b.visible[: len(a.visible)] == a.visible:
                    raise ValueError(f"mount {join(*b.visible)} is inside mount {join(*a.visible)}")
                if b.real[: len(a.real)] == a.real:
                    raise ValueError(f"mount {join(*b.visible)} reaches into mount {join(*a.visible)}'s tree")
        self.policy = policy
        self.actor = actor

    # Reads

    async def stat(self, path: str) -> Entry:
        """The file or directory at ``path``; a virtual directory has id None."""
        target = self._target(path, "read")
        if target.real is None:
            return Entry(target.visible, None, "dir", 0, None)
        entry = await self._mapped(self._client.stat(_real(target)), target)
        return self._entry(entry, target)

    async def readdir(self, path: str) -> list[Entry]:
        """The entries of directory ``path`` the actor may read, in byte order of name."""
        target = self._target(path, "read")
        if target.real is None:
            entries = await self._virtual_entries(target.path)
        else:
            listing = await self._mapped(self._client.readdir(_real(target)), target)
            entries = [self._entry(e, target) for e in listing]
        return [e for e in entries if self.policy.allows(self.actor, "read", e.path)]

    async def pread(self, path: str, offset: int = 0, limit: int = DEFAULT_READ_LIMIT) -> ReadResult:
        """``VFSClient.pread`` of ``path``."""
        target = self._target(path, "read")
        if target.real is None:
            raise VFSError("EISDIR", target.visible)
        return await self._mapped(self._client.pread(_real(target), offset, limit), target)

    async def pread_lines(
        self, path: str, first_line: int = 1, max_lines: int = DEFAULT_LINE_LIMIT, max_bytes: int = DEFAULT_LINE_BYTES
    ) -> LineReadResult:
        """``VFSClient.pread_lines`` of ``path``."""
        target = self._target(path, "read")
        if target.real is None:
            raise VFSError("EISDIR", target.visible)
        return await self._mapped(self._client.pread_lines(_real(target), first_line, max_lines, max_bytes), target)

    async def glob(
        self,
        pattern: str,
        path: str = "/",
        *,
        excludes: Sequence[str] = (),
        hidden: bool = False,
        limit: int | None = None,
    ) -> GlobResult:
        """``VFSClient.glob`` below directory ``path``: the first ``limit`` files the actor may read, and whether
        there is at least one more (``truncated``).

        Mounts the actor can't read anything in are not searched. The glob asks for ``limit + 1`` matches, so
        exactly ``limit`` readable ones are not ``truncated``. When all ``limit + 1`` came back and the policy hid
        some of them, it looks further, at ``min(_REFETCH_FACTOR * max(limit, 1) + 1, 2**31 - 1)`` matches, so a
        policy that hides most of the matches can return fewer than ``limit``. Files the actor can't read never
        count toward ``limit`` and never make the result ``truncated``: when hidden files fill that window, the
        glob can't tell whether a readable match follows, and reports ``truncated`` only for one it has seen. So a
        result can be not ``truncated`` though readable files lie past the window.

        Below a virtual directory, the glob runs in each mount and the results are merged, newest first. The
        pattern and excludes still match paths relative to ``path``, restated relative to each mount point. Where
        that can't be done exactly, the glob is EINVAL (glob from inside a mount instead). Only a pattern or
        exclude with one of these can be, and only where it reaches a mount point:

        - an escaped ``/`` (``a\\/b``);
        - a brace or class with a ``/`` in it (``{a/b,c}/x``, ``[!/]*.md``);
        - a ``**`` segment followed by a segment that can match a directory at or above a mount point:
          ``**/workspace/*.md`` from ``/``, and for an exclude, which matches directories too, also
          ``**/w*`` or ``**/*s/``;
        - for an exclude, a trailing ``//`` (``**//``).

        Every mount's pattern and excludes are restated before any is searched, so whether a glob is EINVAL
        doesn't depend on the order of the mounts. EINVAL too for a ``limit`` outside 0 to 2**31 - 2."""
        target = self._target(path, "read")
        if limit is not None and not 0 <= limit < _MAX_LIMIT:  # limit + 1 is asked for
            raise VFSError("EINVAL", target.visible, f"invalid limit {limit}")
        if limit is None:
            return GlobResult(self._readable(await self._glob(target, pattern, excludes, hidden, None)), False)
        fetch = limit + 1
        entries = await self._glob(target, pattern, excludes, hidden, fetch)
        readable = self._readable(entries)
        wider = min(max(limit, 1) * _REFETCH_FACTOR + 1, _MAX_LIMIT)
        if len(entries) == fetch and len(readable) <= limit and wider > fetch:
            # The policy hid some of the first `limit + 1`: look past them, a bounded distance.
            readable = self._readable(await self._glob(target, pattern, excludes, hidden, wider))
        return GlobResult(readable[:limit], len(readable) > limit)

    def _readable(self, entries: list[Entry]) -> list[Entry]:
        return [e for e in entries if self.policy.allows(self.actor, "read", e.path)]

    async def _glob(
        self, target: _Target, pattern: str, excludes: Sequence[str], hidden: bool, limit: int | None
    ) -> list[Entry]:
        if target.real is not None:
            if not self.policy.allows_any(self.actor, "read", target.visible):
                return []
            matches = await self._mapped(
                self._client.glob(pattern, target.real, excludes=excludes, hidden=hidden, limit=limit), target
            )
            return [self._entry(e, target) for e in matches]
        globbing.compile_glob(pattern)  # a bad pattern is reported as such, not restated
        globbing.compile_excludes(excludes)
        return await self._virtual_glob(target, pattern, excludes, hidden, limit)

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
        """``VFSClient.grep`` of ``path``, with hits in visible paths.

        The actor must be allowed to read everything it searches, or the grep is EACCES (grep a narrower path):
        filtering hits afterwards would let unreadable files count against ``limit`` and the caps, and show in
        ``files`` and ``bytes``. A file ``path`` needs ``read`` on the file only. Mounts the actor can read
        nothing in are not searched.

        Below a virtual directory, the grep runs in each mount in turn, in path order, until ``limit`` hits, with
        ``include`` and ``excludes`` restated as for ``glob`` (EINVAL in the same cases). Each mount is its own
        snapshot, and the caps and the timeout apply to each mount's search; ``files`` and ``bytes`` are the
        totals of the mounts searched. Once there are ``limit`` hits, a later mount only tells whether there are
        more (``truncated``), and is taken to have more if its search is refused (over a cap, before anything is
        read, or on timeout, before the search finished), since there is no telling whether it holds a match. That
        is the one case where ``truncated`` may be True with no more hits."""
        target = self._target(path, "read")
        if target.real is not None:
            return await self._grep_in_mount(
                target,
                pattern,
                include,
                excludes,
                hidden,
                limit,
                ignore_case=ignore_case,
                fixed=fixed,
                max_files=max_files,
                max_bytes=max_bytes,
                max_line_chars=max_line_chars,
                timeout=timeout,
            )
        # As the client does: bad arguments and patterns are reported before anything about the path.
        if not 0 <= limit < _MAX_LIMIT:
            raise VFSError("EINVAL", target.visible, f"invalid limit {limit}")
        if not 0 <= max_files < _MAX_LIMIT or max_bytes < 0:
            raise VFSError("EINVAL", target.visible, f"invalid caps {max_files} files, {max_bytes} bytes")
        if max_line_chars is not None and not 0 <= max_line_chars <= _MAX_LIMIT:
            raise VFSError("EINVAL", target.visible, f"invalid line length {max_line_chars}")
        if not 0 < timeout <= MAX_TIMEOUT:
            raise VFSError("EINVAL", target.visible, f"invalid timeout {timeout}")
        searching.compile_pattern(pattern, ignore_case=ignore_case, fixed=fixed)
        if include is not None:
            globbing.compile_glob(include)
        globbing.compile_excludes(excludes)
        hits: list[searching.GrepHit] = []
        files = size = 0
        truncated = False
        mounts = self._restated(target, include, excludes, hidden)
        for mount_target, _, _ in mounts:  # all before any search
            self.policy.check(self.actor, "read", mount_target.visible, subtree=True)
        for mount_target, mount_include, mount_excludes in mounts:
            try:
                result = await self._mapped(
                    self._client.grep(
                        pattern,
                        _real(mount_target),
                        include=mount_include,
                        excludes=mount_excludes,
                        ignore_case=ignore_case,
                        fixed=fixed,
                        hidden=hidden,
                        limit=limit - len(hits),
                        max_files=max_files,
                        max_bytes=max_bytes,
                        max_line_chars=max_line_chars,
                        timeout=timeout,
                    ),
                    mount_target,
                )
            except VFSError as e:
                if e.errno in ("ENOENT", "ENOTDIR"):
                    continue  # nothing to search in this mount
                raise
            except searching.SearchRefused:
                if len(hits) < limit:
                    raise
                truncated = True  # the limit was reached before this mount: there may be more
                break
            hits.extend(self._hit(hit, mount_target) for hit in result.hits)
            files, size = files + result.files, size + result.bytes
            if result.truncated:
                truncated = True
                break
        return searching.GrepResult(hits, truncated, files, size)

    async def _grep_in_mount(
        self,
        target: _Target,
        pattern: str,
        include: str | None,
        excludes: Sequence[str],
        hidden: bool,
        limit: int,
        **options: Any,
    ) -> searching.GrepResult:
        expect: UUID | None = None
        if not self.policy.allows(self.actor, "read", target.visible, subtree=True):
            # Only a file may be searched, since nothing is below it: pin it, in case a directory takes its place.
            entry = await self._mapped(self._client.stat(_real(target)), target)
            if entry.is_dir:
                self.policy.check(self.actor, "read", target.visible, subtree=True)  # EACCES
            expect = entry.id
        result = await self._mapped(
            self._client.grep(
                pattern,
                _real(target),
                include=include,
                excludes=excludes,
                hidden=hidden,
                limit=limit,
                expect=expect,
                **options,
            ),
            target,
        )
        return searching.GrepResult(
            [self._hit(hit, target) for hit in result.hits], result.truncated, result.files, result.bytes
        )

    # Writes

    async def create(self, path: str, data: bytes | None = None, *, parents: bool = False) -> FileVersion:
        """``VFSClient.create`` of ``path``."""
        target = self._creatable(path, parents)
        if target.is_mount_point:  # the mount's root is missing: it can only become a directory
            raise VFSError("EISDIR", target.visible, "a mount point")
        return await self._mapped(self._client.create(_real(target), data, parents=parents), target)

    async def mkdir(self, path: str, *, parents: bool = False) -> UUID:
        """``VFSClient.mkdir`` of ``path``."""
        target = self._creatable(path, parents)
        return await self._mapped(self._client.mkdir(_real(target), parents=parents), target)

    async def mkdir_p(self, path: str) -> UUID | None:
        """``VFSClient.mkdir_p`` of ``path``; None for a virtual directory, which exists already."""
        target = self._target(path)
        if target.real is None:
            return None
        self._check_parents(target)
        self.policy.check(self.actor, "write", target.visible)
        return await self._mapped(self._client.mkdir_p(_real(target)), target)

    async def append(self, path: str, data: bytes) -> int:
        """``VFSClient.append`` to ``path``: the new size, even for an actor that may not read the file."""
        target = self._target(path, "append")
        if target.real is None:
            raise VFSError("EISDIR", target.visible)
        return await self._mapped(self._client.append(_real(target), data), target)

    async def write(self, path: str, data: bytes, *, expect: FileVersion | None = None) -> FileVersion:
        """``VFSClient.write`` of ``path``, a compare-and-swap with ``expect``."""
        target = self._target(path, "write")
        if target.real is None:
            raise VFSError("EISDIR", target.visible)
        return await self._mapped(self._client.write(_real(target), data, expect=expect), target)

    async def rename(self, src: str, dst: str) -> None:
        """``VFSClient.rename``; both paths may be in different mounts. ESTALE if the policy allows moving only a
        file from ``src`` and a directory took its place while the move was checked."""
        source, dest = self._moved(src), self._movable(dst)
        pin = await self._pin(source, dest, claim=False)
        await self._mapped(self._client.rename(_real(source), _real(dest), expect=pin), source, dest)

    async def claim(self, src: str, dst: str) -> bool:
        """``VFSClient.claim``: whether this caller moved ``src`` to ``dst``."""
        source, dest = self._moved(src), self._movable(dst)
        pin = await self._pin(source, dest, claim=True)
        return await self._mapped(self._client.claim(_real(source), _real(dest), expect=pin), source, dest)

    async def unlink(self, path: str) -> None:
        """``VFSClient.unlink`` of ``path``."""
        target = self._movable(path)
        await self._mapped(self._client.unlink(_real(target)), target)

    # Mapping

    def _target(self, path: str, action: Action | None = None) -> _Target:
        """``path`` mapped to the real tree, after checking ``action`` on it. ENOENT outside every mount."""
        segments = split(path)
        target: _Target | None = None
        for mount in self._mounts:
            if segments[: len(mount.visible)] == mount.visible:
                target = _Target(segments, mount)
            elif mount.visible[: len(segments)] == segments:
                target = target or _Target(segments, None)  # above a mount point
        if target is None:
            raise VFSError("ENOENT", join(*segments))
        if action is not None:
            self.policy.check(self.actor, action, target.visible)
        return target

    def _creatable(self, path: str, parents: bool) -> _Target:
        """``path``, to be created (with its missing parents); EEXIST for a virtual directory."""
        target = self._target(path)
        if target.real is None:
            raise VFSError("EEXIST", target.visible)
        if parents:
            self._check_parents(target)
        self.policy.check(self.actor, "write", target.visible)
        return target

    def _check_parents(self, target: _Target) -> None:
        """Checks ``write`` on every directory between the mount point and ``target``, which creating
        ``target``'s missing parents may create. Conservative: it doesn't look which are missing, so a rule that
        refuses ``write`` on an existing directory refuses creating files below it with ``parents`` too."""
        assert target.mount is not None
        for end in range(len(target.mount.visible) + 1, len(target.path)):
            self.policy.check(self.actor, "write", join(*target.path[:end]))

    def _movable(self, path: str) -> _Target:
        """``path``, to be moved or removed (or replaced by a move), after checking ``write`` on it. EBUSY for a
        mount point or a virtual directory. An unlink needs no more: it removes a file or an empty directory."""
        target = self._target(path)
        if target.real is None or target.is_mount_point:
            raise VFSError("EBUSY", target.visible, "a mount point")
        self.policy.check(self.actor, "write", target.visible)
        return target

    def _moved(self, src: str) -> _Target:
        """The source of a move: as ``_movable``, and readable, since a move can land it where the actor may
        read it."""
        source = self._movable(src)
        self.policy.check(self.actor, "read", source.visible)
        return source

    async def _pin(self, source: _Target, dest: _Target, *, claim: bool) -> UUID | None:
        """None when the policy allows moving a directory from ``source`` to ``dest``: ``write`` below both and
        ``read`` below ``source``. Otherwise only a file may move, since nothing is below it: the id of the file
        at ``source``, for the move to expect, or EACCES for a directory."""
        checks: list[tuple[Action, _Target]] = [("write", source), ("write", dest), ("read", source)]
        if all(self.policy.allows(self.actor, action, t.visible, subtree=True) for action, t in checks):
            return None
        try:
            entry = await self._mapped(self._client.stat(_real(source)), source)
        except VFSError as e:
            if claim and e.errno == "ENOENT" and e.path == source.visible:
                return _NO_NODE  # the claim lost the race, or never could: the client tells which
            raise
        if entry.is_dir:
            for action, t in checks:
                self.policy.check(self.actor, action, t.visible, subtree=True)
        assert entry.id is not None
        return entry.id

    def _entry(self, entry: Entry, target: _Target) -> Entry:
        return Entry(self._visible(entry.path, target), entry.id, entry.kind, entry.size, entry.mtime, entry.generation)

    def _hit(self, hit: searching.GrepHit, target: _Target) -> searching.GrepHit:
        return dataclasses.replace(hit, path=self._visible(hit.path, target))

    def _visible(self, real: str, *targets: _Target) -> str:
        """The visible path of ``real``, a path the operation on ``targets`` reported: one of their real paths, or
        a path below or above one in its mount. A path above a mount point is reported as the mount point."""
        segments = split(real)
        for target in targets:
            if real == target.real:
                return target.visible
        best: _Mount | None = None  # the mount with the longest real prefix of `real`
        for target in targets:
            mount = target.mount
            if mount and segments[: len(mount.real)] == mount.real and (not best or len(mount.real) > len(best.real)):
                best = mount
        if best is not None:
            return join(*best.visible, *segments[len(best.real) :])
        for target in targets:
            if target.mount is not None and target.mount.real[: len(segments)] == segments:
                return join(*target.mount.visible)
        return targets[0].visible  # not reached: the client reports the paths it was given

    async def _mapped[T](self, call: Awaitable[T], *targets: _Target) -> T:
        """Awaits ``call``, reporting a ``VFSError`` at the visible path, without the real detail, and any other
        database error, whose text may name real paths, as EIO (logged)."""
        try:
            return await call
        except VFSError as e:
            error = VFSError(e.errno, self._visible(e.path, *targets), e.message)
        except psycopg.Error as e:
            _log.warning("database error in an operation on %s", targets[0].visible, exc_info=e)
            error = VFSError("EIO", targets[0].visible, "database error")
        raise error  # outside the except block, so it doesn't carry the real error as __context__

    async def _virtual_entries(self, path: tuple[str, ...]) -> list[Entry]:
        """The entries of virtual directory ``path``: the mount points and virtual directories just below it."""
        children: dict[str, Entry] = {}
        for mount in self._mounts:
            if mount.visible[: len(path)] != path or mount.visible == path:
                continue
            name = mount.visible[len(path)]
            if len(mount.visible) > len(path) + 1:
                children.setdefault(name, Entry(join(*path, name), None, "dir", 0, None))
                continue
            target = _Target(mount.visible, mount)
            try:
                entry = await self._mapped(self._client.stat(_real(target)), target)
            except VFSError as e:
                if e.errno in ("ENOENT", "ENOTDIR"):
                    continue  # the mount's real root doesn't exist (yet)
                raise
            children[name] = self._entry(entry, target)
        return [children[name] for name in sorted(children)]

    async def _virtual_glob(
        self, target: _Target, pattern: str, excludes: Sequence[str], hidden: bool, limit: int | None
    ) -> list[Entry]:
        entries: list[Entry] = []
        for mount_target, mount_pattern, mount_excludes in self._restated(target, pattern, excludes, hidden):
            assert mount_pattern is not None
            try:
                matches = await self._mapped(
                    self._client.glob(
                        mount_pattern, _real(mount_target), excludes=mount_excludes, hidden=hidden, limit=limit
                    ),
                    mount_target,
                )
            except VFSError as e:
                if e.errno in ("ENOENT", "ENOTDIR"):
                    continue  # nothing to search in this mount
                raise
            entries.extend(self._entry(e, mount_target) for e in matches)
        entries.sort(key=lambda e: e.path)
        entries.sort(key=lambda e: _mtime(e), reverse=True)  # newest first, then by path, as vfs_glob
        return entries if limit is None else entries[:limit]

    def _restated(
        self, target: _Target, pattern: str | None, excludes: Sequence[str], hidden: bool
    ) -> list[tuple[_Target, str | None, list[str]]]:
        """The mounts below virtual directory ``target`` to search for ``pattern`` (None: every file) without
        ``excludes``, in path order, each with the pattern and excludes restated relative to its mount point
        (``_below``). Leaves out the mounts the patterns rule out, hidden ones unless ``hidden``, and those the
        actor can read nothing in. EINVAL, before anything is searched, where a pattern can't be restated."""
        mounts: list[tuple[_Target, str | None, list[str]]] = []
        for mount in sorted(self._mounts, key=lambda m: join(*m.visible) + "/"):  # as their files sort
            prefix = mount.visible[len(target.path) :]
            if mount.visible[: len(target.path)] != target.path or not prefix:
                continue
            if not hidden and any(name.startswith(".") for name in prefix):
                continue
            if not self.policy.allows_any(self.actor, "read", join(*mount.visible)):
                continue
            restated = pattern if pattern is None else _below(pattern, prefix, dirs=False, path=target.visible)
            mount_excludes = [_below(exclude, prefix, dirs=True, path=target.visible) for exclude in excludes]
            if (pattern is not None and restated is None) or restated is True or True in mount_excludes:
                continue  # the pattern matches nothing in the mount, or an exclude covers all of it
            kept = [e for e in mount_excludes if isinstance(e, str)]
            mounts.append((_Target(mount.visible, mount), restated, kept))
        return mounts


def _mtime(entry: Entry) -> datetime:
    assert entry.mtime is not None  # glob results have one
    return entry.mtime


def _real(target: _Target) -> str:
    assert target.real is not None
    return target.real


def _below(pattern: str, prefix: tuple[str, ...], *, dirs: bool, path: str) -> str | Literal[True] | None:
    """Glob ``pattern``, relative to virtual directory ``path``, restated relative to the mount point ``prefix``
    below it: None if it matches nothing in the mount, True if it matches the mount point or a directory above it
    (only for ``dirs``, an exclude, which can match directories). EINVAL when it can't be restated exactly: the
    cases ``VFSView.glob`` lists. ``tests/test_view.py`` checks it against compiling the pattern itself."""
    if "\\/" in pattern:  # an escaped / is still a separator to glob.py: don't guess where segments are
        raise _unrestatable(pattern, path)
    body = pattern[:-1] if dirs and pattern.endswith("/") else pattern
    suffix = pattern[len(body) :]
    if "/" not in body:  # a name at any depth
        if not dirs:
            return pattern  # a file name is never a directory above the mount
        return True if _matches_any(body, prefix, pattern, path) else pattern
    segments = (body[1:] if body.startswith("/") else body).split("/")
    if segments[-1] == "":  # globset reads `**//` as `**/`, any directory: don't guess what it means
        raise _unrestatable(pattern, path)
    if any(segment in ("", ".", "..") for segment in segments):  # never in a path: `a//x` matches nothing
        return None
    for i, name in enumerate(prefix):
        if i == len(segments):  # the pattern names a directory above the mount point
            return True if dirs else None
        segment = segments[i]
        if segment == "**":
            tail = segments[i + 1 :]
            if not tail:
                return True if dirs else "/**"
            # From above the mount point, `**/tail` matches in the mount what it matches from the mount point,
            # provided no match can start at a directory above it.
            if (not dirs and len(tail) == 1) or not _matches_any(tail[0], prefix[i:], pattern, path):
                return "/".join(["**", *tail]) + suffix
            raise _unrestatable(pattern, path)
        if not _matches_any(segment, (name,), pattern, path):
            return None
    rest = segments[len(prefix) :]
    if not rest:  # the mount point itself
        return True if dirs else None
    return "/" + "/".join(rest) + suffix  # the leading / keeps it anchored at the mount point


def _matches_any(segment: str, names: Sequence[str], pattern: str, path: str) -> bool:
    """Whether the glob path segment ``segment`` (of ``pattern``) matches one of ``names``."""
    try:
        regex = globbing.compile_glob(segment).regex  # literals and explicit ranges only: Python reads it the same
    except globbing.GlobError:
        raise _unrestatable(pattern, path) from None  # a brace or class split by a /
    return any(re.fullmatch(regex, name) for name in names)


def _unrestatable(pattern: str, path: str) -> VFSError:
    return VFSError("EINVAL", path, f"{pattern!r} spans mount points here: glob from inside a mount")
