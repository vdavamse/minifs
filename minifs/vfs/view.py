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
not read are left out of listings and glob results.

What each operation needs: ``read`` for ``stat``, ``readdir``, ``pread`` and ``glob``; ``write`` on the path for
``create``, ``mkdir``, ``mkdir_p`` (directories a call creates above the path come with it), ``write`` and
``unlink``, and on both paths for ``rename`` and ``claim``; ``append`` (or ``write``) for ``append``. A move or
an unlink also needs ``write`` everywhere below its paths (``Policy.allows(subtree=True)``), so moving a directory
can't carry a file out from under a more specific rule. Moving or removing a mount point or a virtual directory
is EBUSY. A database error other than a ``VFSError`` comes back as EIO, since its text may name real paths.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Mapping, Sequence
from datetime import datetime
from typing import Literal
from uuid import UUID

import psycopg

from minifs.vfs import glob as globbing
from minifs.vfs.client import DEFAULT_READ_LIMIT, VFSClient
from minifs.vfs.core import Entry, FileVersion, ReadResult, VFSError, join, split
from minifs.vfs.policy import Action, Policy

_MAX_LIMIT = 2**31 - 1  # vfs_glob's limit is an int
_log = logging.getLogger(__name__)


def workspace_mounts(workspace_id: UUID | str) -> dict[str, str]:
    """The agent's mounts: its workspace at ``/workspace``, and the shared ``/docs``."""
    return {"/workspace": f"/workspaces/{workspace_id}", "/docs": "/docs"}


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
    ``policy``. The operations are ``VFSClient``'s, in visible paths. ValueError for nested mounts or an actor
    the policy has no rules for."""

    def __init__(self, client: VFSClient, mounts: Mapping[str, str], policy: Policy, actor: str) -> None:
        if actor not in policy.actors:
            raise ValueError(f"the policy has no rules for actor {actor!r}")
        self._client = client
        self._mounts = [_Mount(split(v), split(r)) for v, r in mounts.items()]
        for a in self._mounts:
            for b in self._mounts:
                if a is not b and b.visible[: len(a.visible)] == a.visible:
                    raise ValueError(f"mount {join(*b.visible)} is inside mount {join(*a.visible)}")
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

    async def glob(
        self,
        pattern: str,
        path: str = "/",
        *,
        excludes: Sequence[str] = (),
        hidden: bool = False,
        limit: int | None = None,
    ) -> list[Entry]:
        """``VFSClient.glob`` below directory ``path``: the first ``limit`` files the actor may read.

        Below a virtual directory, the glob runs in each mount and the results are merged, newest first. The
        pattern and excludes still match paths relative to ``path``, restated relative to each mount point. That
        is exact unless a ``**`` segment is followed by one that can match a directory above or at a mount point
        (``**/workspace/*.md`` from ``/``), which is EINVAL: glob from inside a mount instead."""
        target = self._target(path, "read")
        if limit is not None and not 0 <= limit <= _MAX_LIMIT:
            raise VFSError("EINVAL", target.visible, f"invalid limit {limit}")
        entries = await self._glob(target, pattern, excludes, hidden, limit)
        readable = [e for e in entries if self.policy.allows(self.actor, "read", e.path)]
        if limit is not None and len(entries) == limit and len(readable) < limit:
            # The policy hid some of the first `limit`: look past them.
            entries = await self._glob(target, pattern, excludes, hidden, None)
            readable = [e for e in entries if self.policy.allows(self.actor, "read", e.path)][:limit]
        return readable

    async def _glob(
        self, target: _Target, pattern: str, excludes: Sequence[str], hidden: bool, limit: int | None
    ) -> list[Entry]:
        if target.real is not None:
            matches = await self._mapped(
                self._client.glob(pattern, target.real, excludes=excludes, hidden=hidden, limit=limit), target
            )
            return [self._entry(e, target) for e in matches]
        globbing.compile_glob(pattern)  # a bad pattern is reported as such, not restated
        globbing.compile_excludes(excludes)
        return await self._virtual_glob(target, pattern, excludes, hidden, limit)

    # Writes

    async def create(self, path: str, data: bytes | None = None, *, parents: bool = False) -> FileVersion:
        """``VFSClient.create`` of ``path``."""
        target = self._creatable(path)
        return await self._mapped(self._client.create(_real(target), data, parents=parents), target)

    async def mkdir(self, path: str, *, parents: bool = False) -> UUID:
        """``VFSClient.mkdir`` of ``path``."""
        target = self._creatable(path)
        return await self._mapped(self._client.mkdir(_real(target), parents=parents), target)

    async def mkdir_p(self, path: str) -> UUID | None:
        """``VFSClient.mkdir_p`` of ``path``; None for a virtual directory, which exists already."""
        target = self._target(path)
        if target.real is None:
            return None
        self.policy.check(self.actor, "write", target.visible)
        return await self._mapped(self._client.mkdir_p(_real(target)), target)

    async def append(self, path: str, data: bytes) -> int:
        """``VFSClient.append`` to ``path``."""
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
        """``VFSClient.rename``; both paths may be in different mounts."""
        source, dest = self._movable(src), self._movable(dst)
        await self._mapped(self._client.rename(_real(source), _real(dest)), source, dest)

    async def claim(self, src: str, dst: str) -> bool:
        """``VFSClient.claim``: whether this caller moved ``src`` to ``dst``."""
        source, dest = self._movable(src), self._movable(dst)
        return await self._mapped(self._client.claim(_real(source), _real(dest)), source, dest)

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

    def _creatable(self, path: str) -> _Target:
        """``path``, to be created; EEXIST for a virtual directory."""
        target = self._target(path, "write")
        if target.real is None:
            raise VFSError("EEXIST", target.visible)
        return target

    def _movable(self, path: str) -> _Target:
        """``path``, to be moved or removed (or replaced by a move). What is below it moves too, so the actor must
        be allowed to write there as well. EBUSY for a mount point or a virtual directory."""
        target = self._target(path)
        if target.real is None or target.is_mount_point:
            raise VFSError("EBUSY", target.visible, "a mount point")
        self.policy.check(self.actor, "write", target.visible, subtree=True)
        return target

    def _entry(self, entry: Entry, target: _Target) -> Entry:
        return Entry(self._visible(entry.path, target), entry.id, entry.kind, entry.size, entry.mtime, entry.generation)

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
        for mount in self._mounts:
            prefix = mount.visible[len(target.path) :]
            if mount.visible[: len(target.path)] != target.path or not prefix:
                continue
            if not hidden and any(name.startswith(".") for name in prefix):
                continue
            mount_pattern = _below(pattern, prefix, dirs=False, path=target.visible)
            if mount_pattern is None or mount_pattern is True:
                continue
            mount_excludes: list[str] = []
            for exclude in excludes:
                restated = _below(exclude, prefix, dirs=True, path=target.visible)
                if restated is True:
                    break  # the exclude covers the whole mount
                if restated is not None:
                    mount_excludes.append(restated)
            else:
                mount_target = _Target(mount.visible, mount)
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


def _mtime(entry: Entry) -> datetime:
    assert entry.mtime is not None  # glob results have one
    return entry.mtime


def _real(target: _Target) -> str:
    assert target.real is not None
    return target.real


def _below(pattern: str, prefix: tuple[str, ...], *, dirs: bool, path: str) -> str | Literal[True] | None:
    """Glob ``pattern``, relative to virtual directory ``path``, restated relative to the mount point ``prefix``
    below it: None if it matches nothing in the mount, True if it matches the mount point or a directory above it
    (only for ``dirs``, an exclude, which can match directories). EINVAL when it can't be restated exactly."""
    body = pattern[:-1] if dirs and pattern.endswith("/") and not pattern.endswith("\\/") else pattern
    suffix = pattern[len(body) :]
    if "/" not in body:  # a name at any depth
        if not dirs:
            return pattern  # a file name is never a directory above the mount
        return True if _matches_any(body, prefix, pattern, path) else pattern
    segments = (body[1:] if body.startswith("/") else body).split("/")
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
    if segment in ("", ".", ".."):
        raise _unrestatable(pattern, path)
    try:
        regex = globbing.compile_glob(segment).regex  # literals and explicit ranges only: Python reads it the same
    except globbing.GlobError:
        raise _unrestatable(pattern, path) from None  # a brace or class split by a /
    return any(re.fullmatch(regex, name) for name in names)


def _unrestatable(pattern: str, path: str) -> VFSError:
    return VFSError("EINVAL", path, f"{pattern!r} spans mount points here: glob from inside a mount")
