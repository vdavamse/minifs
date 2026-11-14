"""``ViewWatch``: a ``PathWatch`` as an actor sees it through a ``VFSView`` (``VFSView.watch``).

``Watcher.watch`` is the operator's API: it takes a real path, applies no policy and reports node ids and names
from anywhere in the tree. Code that acts for an actor (an agent, a tenant) watches through its view instead,
which checks ``read`` on the path when the watch starts and reports each event in visible paths, without node ids,
as far as the actor may see it:

- A directory watch reports a child only when the actor may read the child's path, for every op: a ``create``,
  ``modify``, ``move`` or ``delete`` of a file the policy hides is not delivered, so neither its name nor its size
  is learnt. A rename between a readable and a hidden name is reported as the ``create`` or ``delete`` the actor
  can see.
- A move into or out of the watched directory is a ``create`` or a ``delete``: the other side may be outside the
  view, so it is never named.
- The policy is applied to the watched node's path, so the watch must know that path is still true. It follows
  the node through a rename in place to a name the actor may read, and reports it as a ``move``. Anything else that
  can change the path ends the watch, after a ``delete`` at the old path: a move of the node to another directory
  or to a name the actor may not read, a move or removal of any directory above it (a mount point's included), or
  a path that no longer leads to the node when the watch checks after a ``rescan`` (a move may have been missed).
  Watch the new path again to follow it.
- Events are hints: the node's own ``move``, its ancestors' ``move`` and ``delete``, and a ``rescan`` are checked
  against the database (one path lookup) before the watch acts on them, so one that changed nothing (a forged or
  repeated notification) is dropped. When that lookup fails, the watch delivers a ``rescan`` and ends.
- A child's ``modify`` notification carries no name. The watch keeps the names of the children it has seen, and
  looks a missing one up with one query (on the watcher's pool) the first time the child is modified. It forgets
  them on a ``rescan``, since a rename may have been missed. A child that is not in the directory is skipped (and
  remembered as such, a bounded number of them), and a failed lookup is reported as a ``rescan``.
- A name that no node can have (empty, ``.``, ``..``, or with a ``/``) comes only from a forged payload, and its
  event is dropped.
- An event is never lost to a caller's timeout: if ``get()`` is cancelled while it checks the database, the event
  and the check are kept, and the next ``get()`` resumes them.

The underlying ``PathWatch`` still does the rest: it subscribes before resolving the path, confirms a ``rescan``
and the node's own ``delete`` against the database, rate-limits rescans and ends when its node is gone.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Self
from uuid import UUID

from minifs.vfs.core import VFSError, escape_controls, join
from minifs.vfs.policy import Policy
from minifs.vfs.watch import Event, Op, PathWatch

logger = logging.getLogger(__name__)

_MAX_MISSING = 1024  # children remembered as not in the watched directory, before the set is forgotten


@dataclass(frozen=True, slots=True)
class WatchEvent:
    """One change as the view reports it, in visible paths.

    ``path`` is the node the event is about: the watched node itself, or a child of a watched directory; for a
    ``rescan``, the watched path. ``old_path`` is set for ``move`` only (a rename inside the watched directory, or
    of the watched node in place). ``kind`` is set for create, move and delete, and for the watched node's own
    events; ``size`` and ``generation`` for modify.
    """

    op: Op
    path: str
    kind: str | None = None
    size: int | None = None
    generation: int | None = None
    old_path: str | None = None


class ViewWatch:
    """The events of a ``PathWatch`` that ``actor`` may see under ``policy``, at visible paths. Iterate it; it
    ends when the underlying watch ends, or after the event it reports when its path may no longer be true. Built
    by ``VFSView.watch``: ``path`` is the visible path and ``real`` the real one, both split."""

    def __init__(
        self,
        inner: PathWatch,
        path: tuple[str, ...],
        real: tuple[str, ...],
        *,
        mount_point: bool,
        policy: Policy,
        actor: str,
    ) -> None:
        self._inner = inner
        self._path = path  # the watched node's visible path, raw (the policy matches names as stored)
        self._real = real
        self._mount_point = mount_point
        self._policy = policy
        self._actor = actor
        self._names: dict[UUID, str] = {}  # the watched directory's children seen so far, by id
        self._missing: set[UUID] = set()  # ids a lookup found not to be children
        self._ended = False
        self._pending: tuple[Event, asyncio.Task[WatchEvent | None]] | None = None  # survives a cancelled get()

    @property
    def path(self) -> str:
        """The watched node's visible path now (it follows a rename in place)."""
        return escape_controls(join(*self._path))

    @property
    def kind(self) -> str:
        """``file`` or ``dir``."""
        return self._inner.kind

    def close(self) -> None:
        self._ended = True
        if self._pending is not None:
            self._pending[1].cancel()
            self._pending = None
        self._inner.close()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> WatchEvent:
        return await self.get()

    async def get(self) -> WatchEvent:
        """The next event the actor may see. StopAsyncIteration once the watch has ended."""
        while True:
            if self._ended:
                raise StopAsyncIteration
            if self._pending is None:
                event = await self._inner.get()
                self._pending = (event, asyncio.ensure_future(self._seen(event)))
            task = self._pending[1]
            try:
                seen = await asyncio.shield(task)
            except asyncio.CancelledError:
                if task.cancelled():  # close() cancelled it
                    raise StopAsyncIteration from None
                raise  # our caller gave up waiting: the event and its task stay pending for the next get()
            except BaseException:
                self._pending = None
                raise
            self._pending = None
            if seen is not None:
                return seen

    async def _seen(self, event: Event) -> WatchEvent | None:
        node = self._inner.node
        if event.op == "rescan":
            self._names.clear()
            self._missing.clear()
            return await self._confirmed(WatchEvent("rescan", self.path))
        if event.id == node:
            return await self._own(event)
        if event.id in self._inner.ancestors:  # a move or delete of a directory above the node
            return await self._confirmed(None)
        return await self._child(event)

    async def _own(self, event: Event) -> WatchEvent | None:
        """An event of the watched node itself."""
        if event.op == "move":
            if not self._mount_point and event.old_parent == event.parent and _valid(event.name):
                assert event.name is not None
                real = (*self._real[:-1], event.name)
                if real == self._real:
                    return None  # it is where the watch has it: a repeated notification
                try:
                    there = await self._at(real)
                except Exception as error:
                    return self._failed(error)
                path = (*self._path[:-1], event.name)
                if there == self._inner.node and self._readable(path):
                    old, self._path, self._real = self.path, path, real
                    return WatchEvent("move", self.path, kind=self.kind, old_path=old)
            return await self._confirmed(None)
        if event.op == "create":  # not for a node that exists: a forged payload
            return None
        return WatchEvent(event.op, self.path, kind=self.kind, size=event.size, generation=event.generation)

    async def _confirmed(self, event: WatchEvent | None) -> WatchEvent | None:
        """``event`` if the watched path still leads to the node; otherwise a ``delete`` at that path, and the
        watch ends."""
        try:
            there = await self._at(self._real)
        except Exception as error:
            return self._failed(error)
        if there == self._inner.node:
            return event
        return self._end(WatchEvent("delete", self.path, kind=self.kind))

    async def _child(self, event: Event) -> WatchEvent | None:
        """An event of a child of the watched directory."""
        node = self._inner.node
        assert event.id is not None
        if event.op == "move":
            into = event.parent == node
            out_of = event.old_parent == node
            if (into and not _valid(event.name)) or (out_of and not _valid(event.old_name)):
                return None
            old = self._child_path(event.old_name) if out_of else None
            new = self._child_path(event.name) if into else None
            if into:
                assert event.name is not None
                self._names[event.id] = event.name
                self._missing.discard(event.id)
            else:
                self._names.pop(event.id, None)
            if old is not None and new is not None:
                return WatchEvent("move", new, kind=event.kind, old_path=old)
            if new is not None:
                return WatchEvent("create", new, kind=event.kind)
            if old is not None:
                return WatchEvent("delete", old, kind=event.kind)
            return None
        if event.parent != node:
            return None
        if event.op in ("create", "delete"):
            if not _valid(event.name):
                return None
            assert event.name is not None
            if event.op == "create":
                self._names[event.id] = event.name
                self._missing.discard(event.id)
            else:
                self._names.pop(event.id, None)
            path = self._child_path(event.name)
            return None if path is None else WatchEvent(event.op, path, kind=event.kind)
        # modify: the payload has no name
        name = self._names.get(event.id)
        if name is None:
            if event.id in self._missing:
                return None
            try:
                name = await self._lookup(event.id)
            except Exception as error:
                logger.warning("minifs watch: could not look up a child's name (%r), sending a rescan", error)
                self._names.clear()
                self._missing.clear()
                return WatchEvent("rescan", self.path)
            if name is None:
                if len(self._missing) >= _MAX_MISSING:
                    self._missing.clear()
                self._missing.add(event.id)
                return None  # not in the watched directory: a later event says where it went
            self._names[event.id] = name
        path = self._child_path(name)
        return None if path is None else WatchEvent("modify", path, size=event.size, generation=event.generation)

    def _child_path(self, name: str | None) -> str | None:
        """The visible path of the child ``name``, reported as listings do, or None if the actor may not read it."""
        if name is None:
            return None
        path = (*self._path, name)
        return escape_controls(join(*path)) if self._readable(path) else None

    def _readable(self, path: tuple[str, ...]) -> bool:
        try:
            return self._policy.allows(self._actor, "read", join(*path))
        except VFSError:  # a name the path rules refuse: never reported
            return False

    async def _at(self, real: tuple[str, ...]) -> UUID | None:
        """The node at real path ``real`` now, None if there is none."""
        async with self._inner._watcher._query() as conn:  # pyright: ignore[reportPrivateUsage]
            cur = await conn.execute("SELECT id FROM vfs_lookup(%s)", (list(real),))
            row = await cur.fetchone()
        return None if row is None else row[0]

    async def _lookup(self, child: UUID) -> str | None:
        """The name of ``child`` if it is in the watched directory."""
        async with self._inner._watcher._query() as conn:  # pyright: ignore[reportPrivateUsage]
            cur = await conn.execute(
                "SELECT name FROM vfs_nodes WHERE id = %s AND parent_id = %s", (child, self._inner.node)
            )
            row = await cur.fetchone()
        return None if row is None else row[0]

    def _failed(self, error: Exception) -> WatchEvent:
        """A ``rescan`` that ends the watch: the check of its path failed, so it can't vouch for it."""
        logger.warning("minifs watch: could not check a watched path (%r), ending the watch", error)
        return self._end(WatchEvent("rescan", self.path))

    def _end(self, event: WatchEvent) -> WatchEvent:
        """``event``, the last one: the watch ends after it."""
        self._ended = True
        self._inner.close()
        return event


def _valid(name: str | None) -> bool:
    """Whether a node could have ``name``: none is empty, ``.`` or ``..``, or has a ``/``."""
    return name is not None and name not in ("", ".", "..") and "/" not in name
