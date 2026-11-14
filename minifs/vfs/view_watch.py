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
  or to a name the actor may not read, a rename it has already left again (even back to the watched name: the
  events in between happened under a path the watch never had), a move or removal of any directory above it (a
  mount point's included, even one moved back). Watch the new path again to follow it.
- Every event is relative to the path the watch resolved. Nothing committed before the watch's fence is
  delivered (``PathWatch`` ``fence``): no child event that happened before the watch returned, perhaps under
  another path, and a move or delete above the node in that window that may have changed the path is a
  ``rescan``. A ``rescan`` ends the watch, after it is delivered: events were lost, a move among them perhaps, and
  no check made after the fact can vouch for the events queued meanwhile. Watch again, and re-list.
- Events are hints: the node's own ``move`` and ``delete`` and its ancestors' ``delete`` are checked against the
  database before the watch acts on them: the watched path must still lead to the node through the same
  directories (the same ids) it resolved through when the watch started, so a directory above it that was
  replaced by another of the same name counts as moved. A move to where the watch already has the node, or a
  delete that removed nothing, can only be forged, and is dropped; a forged move to anywhere else ends the watch
  (forging takes EXECUTE on ``vfs_channel()``). Each check costs a query on the watcher's pool; when one fails, the
  watch delivers a ``rescan`` and ends.
- Each child event is judged by the name in its own notification (the name the child had when it changed; the
  ``modify`` payload carries it since migration 021), so no name is remembered or looked up. A child ``modify``
  without a name (a schema before 021) is dropped. Postgres folds identical notifications within a transaction,
  so a child renamed back and forth in one transaction may be seen at a stale name: the actor gets ENOENT on it,
  never the hidden name.
- A name that no node can have (empty, ``.``, ``..``, with a ``/``, over 255 bytes, or not UTF-8) comes only
  from a forged payload, and its event is dropped.
- An event is never lost to a caller's timeout: if ``get()`` is cancelled while it checks the database, the event
  and the check are kept, and the next ``get()`` resumes them.

The underlying ``PathWatch`` still does the rest: it subscribes before resolving the path, sends the fence,
confirms the node's own ``delete`` against the database, rate-limits rescans and ends when its node is gone.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Self
from uuid import UUID

from psycopg import AsyncConnection

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
        self._path: tuple[str, ...] = path  # the watched node's visible path, raw (the policy matches names as stored)
        self._real: tuple[str, ...] = real
        self._mount_point = mount_point
        self._policy = policy
        self._actor = actor
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
            if self._ended and self._pending is None:  # a pending task may hold the last event
                raise StopAsyncIteration
            if self._pending is None:
                event = await self._inner.get()
                self._pending = (event, asyncio.ensure_future(self._seen(event)))
            task = self._pending[1]
            try:
                seen = await asyncio.shield(task)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if task.cancelled() and (current is None or current.cancelling() == 0):  # only close() cancelled
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
        if event.op == "rescan":  # events may have been lost, a move among them: no later check can vouch for
            return self._end(WatchEvent("rescan", self.path))  # the events queued meanwhile, so the watch ends
        if event.id == node:
            return await self._own(event)
        if event.id in self._inner.ancestors:  # a move or delete of a directory above the node
            if event.op == "move" and not self._inner.in_place(event):  # in place: forged, the fence drops real ones
                return self._end(WatchEvent("delete", self.path, kind=self.kind))
            return await self._confirmed(None)
        return await self._child(event)

    async def _own(self, event: Event) -> WatchEvent | None:
        """An event of the watched node itself."""
        if event.op == "move":
            return await self._moved(event)
        if event.op == "delete":  # PathWatch checked the node exists; the path may still lead to it (forged)
            return await self._confirmed(None)
        if event.op == "create":  # not for a node that exists: a forged payload
            return None
        return WatchEvent("modify", self.path, kind=self.kind, size=event.size, generation=event.generation)

    async def _moved(self, event: Event) -> WatchEvent | None:
        """The node moved, by its notification. To where the watch has it (before the watch started, or forged):
        nothing, once the path is checked. Renamed in place to a readable name and still there now: followed, as
        a ``move``. Anything else ends the watch with a ``delete``: another directory, a hidden name, a mount
        point, or a node that has moved again since (the events queued after this one happened under a path the
        watch never had, so it can't judge them)."""
        chain = self._inner.chain
        parent = chain[-2] if len(chain) > 1 else None
        if event.parent == parent and event.name == self._real[-1]:  # to where the watch has it: forged (the fence
            return await self._confirmed(None)  # dropped a real one committed before the watch)
        gone = WatchEvent("delete", self.path, kind=self.kind)
        if event.parent != parent or self._mount_point or not _valid(event.name):
            return self._end(gone)
        assert event.name is not None  # _valid
        path: tuple[str, ...] = (*self._path[:-1], event.name)
        real: tuple[str, ...] = (*self._real[:-1], event.name)
        if not self._readable(path):
            return self._end(gone)
        try:
            async with self._inner._watcher._query() as conn:  # pyright: ignore[reportPrivateUsage]
                there = await self._at(real, conn)
        except Exception as error:
            return self._failed(error)
        if there != chain:  # not there now (moved again since, or a forged name)
            return self._end(gone)
        old, self._path, self._real = self.path, path, real
        return WatchEvent("move", self.path, kind=self.kind, old_path=old)

    async def _confirmed(self, event: WatchEvent | None) -> WatchEvent | None:
        """``event`` if the watched path still leads to the node through the directories it resolved through;
        otherwise a ``delete`` at that path, and the watch ends."""
        try:
            async with self._inner._watcher._query() as conn:  # pyright: ignore[reportPrivateUsage]
                there = await self._at(self._real, conn)
        except Exception as error:
            return self._failed(error)
        if there == self._inner.chain:
            return event
        return self._end(WatchEvent("delete", self.path, kind=self.kind))

    async def _child(self, event: Event) -> WatchEvent | None:
        """An event of a child of the watched directory, judged by the name in its notification."""
        node = self._inner.node
        if event.op == "move":
            into = event.parent == node
            out_of = event.old_parent == node
            if (into and not _valid(event.name)) or (out_of and not _valid(event.old_name)):
                return None
            old = self._child_path(event.old_name) if out_of else None
            new = self._child_path(event.name) if into else None
            if old is not None and new is not None:
                return WatchEvent("move", new, kind=event.kind, old_path=old)
            if new is not None:
                return WatchEvent("create", new, kind=event.kind)
            if old is not None:
                return WatchEvent("delete", old, kind=event.kind)
            return None
        if event.parent != node or not _valid(event.name):
            return None
        path = self._child_path(event.name)
        if path is None:
            return None
        if event.op == "modify":
            return WatchEvent("modify", path, size=event.size, generation=event.generation)
        return WatchEvent(event.op, path, kind=event.kind)

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

    @staticmethod
    async def _at(real: tuple[str, ...], conn: AsyncConnection[Any]) -> tuple[UUID, ...] | None:
        """The ids along real path ``real`` now (as ``PathWatch.chain``), None if nothing is there."""
        cur = await conn.execute("SELECT id, chain FROM vfs_lookup(%s)", (list(real),))
        row = await cur.fetchone()
        return None if row is None or row[0] is None else tuple(row[1])

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
    """Whether a node could have ``name``: none is empty, ``.`` or ``..``, has a ``/``, is over 255 bytes or isn't
    valid UTF-8 (a lone surrogate, which JSON allows)."""
    if name is None or name in ("", ".", "..") or "/" in name:
        return False
    try:
        return len(name.encode()) <= 255
    except UnicodeEncodeError:
        return False
