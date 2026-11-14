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
- A watch follows its node through a rename in place to a name the actor may read. A move to another directory, to
  a name it may not read, or of a mount point's real directory, is reported as a ``delete`` and ends the watch:
  the view can't tell where the node went. Watch the new path again to follow it.
- A child's ``modify`` notification carries no name. The watch keeps the names of the children it has seen, and
  looks a missing one up with one query (on the watcher's pool) the first time the child is modified; a
  ``rescan`` forgets them, since a rename may have been missed. A child that is no longer there is skipped, and a
  failed lookup is reported as a ``rescan``.

The underlying ``PathWatch`` still does the rest: it subscribes before resolving the path, confirms a ``rescan``
and the node's own ``delete`` against the database, rate-limits rescans and ends when its node is gone.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Self
from uuid import UUID

from minifs.vfs.core import VFSError, escape_controls, join
from minifs.vfs.policy import Policy
from minifs.vfs.watch import Event, Op, PathWatch

logger = logging.getLogger(__name__)


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
    ends when the underlying watch ends, or after the ``delete`` it reports when its node leaves where the view can
    follow it. Built by ``VFSView.watch``."""

    def __init__(self, inner: PathWatch, path: tuple[str, ...], *, mount_point: bool, policy: Policy, actor: str):
        self._inner = inner
        self._path = path  # the watched node's visible path, raw (the policy matches names as stored)
        self._mount_point = mount_point
        self._policy = policy
        self._actor = actor
        self._names: dict[UUID, str] = {}  # the watched directory's children seen so far, by id
        self._ended = False

    @property
    def path(self) -> str:
        """The watched node's visible path now (it follows a rename in place)."""
        return escape_controls(join(*self._path))

    @property
    def kind(self) -> str:
        """``file`` or ``dir``."""
        return self._inner.kind

    def close(self) -> None:
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
            event = await self._inner.get()
            seen = await self._seen(event)
            if seen is not None:
                return seen

    async def _seen(self, event: Event) -> WatchEvent | None:
        if event.op == "rescan":
            self._names.clear()
            return WatchEvent("rescan", self.path)
        if event.id == self._inner.node:
            return self._own(event)
        return await self._child(event)

    def _own(self, event: Event) -> WatchEvent | None:
        """An event of the watched node itself."""
        if event.op == "move":
            parent = self._path[:-1]
            if not self._mount_point and event.old_parent == event.parent and event.name is not None:
                new = (*parent, event.name)
                if self._readable(new):
                    old, self._path = self.path, new
                    return WatchEvent("move", self.path, kind=self.kind, old_path=old)
            return self._end(WatchEvent("delete", self.path, kind=self.kind))
        if event.op == "create":  # not for a node that exists: a forged payload
            return None
        return WatchEvent(event.op, self.path, kind=self.kind, size=event.size, generation=event.generation)

    async def _child(self, event: Event) -> WatchEvent | None:
        """An event of a child of the watched directory (PathWatch only delivers those besides the node's own)."""
        node = self._inner.node
        assert event.id is not None
        if event.op == "move":
            into = event.parent == node and event.name is not None
            out_of = event.old_parent == node and event.old_name is not None
            old = self._child_path(event.old_name) if out_of else None
            new = self._child_path(event.name) if into else None
            if into:
                assert event.name is not None
                self._names[event.id] = event.name
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
        if event.op == "create":
            if event.name is None:
                return None
            self._names[event.id] = event.name
            path = self._child_path(event.name)
            return None if path is None else WatchEvent("create", path, kind=event.kind)
        if event.op == "delete":
            self._names.pop(event.id, None)
            path = self._child_path(event.name) if event.name is not None else None
            return None if path is None else WatchEvent("delete", path, kind=event.kind)
        # modify: the payload has no name
        name = self._names.get(event.id)
        if name is None:
            try:
                name = await self._lookup(event.id)
            except Exception as error:
                logger.warning("minifs watch: could not look up a child's name (%r), sending a rescan", error)
                self._names.clear()
                return WatchEvent("rescan", self.path)
            if name is None:
                return None  # no longer in the watched directory: a later event says where it went
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

    async def _lookup(self, child: UUID) -> str | None:
        """The name of ``child`` if it is still in the watched directory."""
        node = self._inner.node
        async with self._inner._watcher._query() as conn:  # pyright: ignore[reportPrivateUsage]
            if node is None:
                cur = await conn.execute("SELECT name FROM vfs_nodes WHERE id = %s AND parent_id IS NULL", (child,))
            else:
                cur = await conn.execute("SELECT name FROM vfs_nodes WHERE id = %s AND parent_id = %s", (child, node))
            row = await cur.fetchone()
        return None if row is None else row[0]

    def _end(self, event: WatchEvent) -> WatchEvent:
        """``event``, the last one: the watch ends after it."""
        self._ended = True
        self._inner.close()
        return event
