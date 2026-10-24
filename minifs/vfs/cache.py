"""The directory-entry cache: ``(parent id, name) -> (id, kind)``, per process and in memory (README: *Caching*).

``DirEntryCache`` implements ``minifs.vfs.client.DirCache``. The client checks every cached chain against
``vfs_nodes`` in the statement that uses it, so a stale entry costs a slower resolution, never the wrong node; this
cache only has to keep stale entries rare:

- Only positive entries are cached. A missing name is never cached, so a name another client creates is visible at
  once.
- ``follow(watcher)`` keeps it fresh from the ``vfs`` NOTIFY channel: a ``delete`` evicts the node by id (through
  the reverse map ``id -> (parent, name)``), and so does a ``move``, unless the node is already cached under its new
  name (the client stored it after the move, and the event arrived late). A ``rescan`` (after the LISTEN connection
  reconnects, when the subscription overflows, or on a TRUNCATE) clears everything.
- Every entry expires ``ttl`` seconds (2 by default) after it was stored, whatever its use. The TTL is the fallback
  for what notifications can't cover: events lost while the LISTEN connection is down (the ``rescan`` only comes
  once it is back), and a listing whose snapshot predates a move or delete but which is stored after that change's
  notification was handled.

``evict`` removes whatever name the node has now. When the client evicts links it found stale, a fresher name
stored meanwhile can go too: that costs a miss, never a wrong answer.

Entries below a removed directory are keyed by its id, so they can't be reached any more; the TTL sweeps them out.
The cache is not thread-safe: use it from one event loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from collections import OrderedDict
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import NamedTuple
from uuid import UUID

from minifs.vfs.core import Kind
from minifs.vfs.watch import Event, Subscription, Watcher

_log = logging.getLogger(__name__)

DEFAULT_TTL = 2.0
DEFAULT_MAX_ENTRIES = 10_000  # about 500 bytes each, so about 5 MB per process


@dataclass(frozen=True, slots=True)
class CacheStats:
    """Counters since the cache was created. ``misses`` includes ``expired``: lookups that found an entry past its
    TTL. ``evictions`` counts entries removed by ``evict`` (by the client, for its own renames and removals and for
    links it found stale, or by a notification), ``clears`` the calls to ``clear`` (each ``rescan``, and entering
    ``follow``); entries dropped by the TTL or the size cap count as neither.

    ``hit_rate`` counts path components, not paths: a path resolves from the cache only if all its components hit.
    A path is cached when a listing of its directory or its creation went through this process, so a low rate with
    few ``expired`` means paths that were never cached, and with many, the TTL is short for the access pattern."""

    lookups: int
    hits: int
    misses: int
    expired: int
    stores: int
    evictions: int
    clears: int
    size: int

    @property
    def hit_rate(self) -> float:
        """Hits per lookup, 0.0 before the first lookup."""
        return self.hits / self.lookups if self.lookups else 0.0


class _Entry(NamedTuple):
    node: UUID
    kind: Kind
    expires: float


type _Key = tuple[UUID | None, str]


class DirEntryCache:
    """Positive directory entries with a reverse map by node id, a TTL and a size cap (see the module docstring).

    ``max_entries`` bounds memory under a burst of stores: past it, the oldest entries go first. An entry costs about
    500 bytes, so the default of 10,000 is about 5 MB; within a 2 s TTL, that is more than a process's agents list.
    ``clock`` (seconds, monotonic) is for tests.
    """

    def __init__(
        self,
        *,
        ttl: float = DEFAULT_TTL,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not math.isfinite(ttl) or ttl <= 0:
            raise ValueError("ttl must be a positive finite number")
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        self._ttl = ttl
        self._max_entries = max_entries
        self._clock = clock
        # In store order, so the entries that expire first are at the front. A store moves its key to the end.
        self._entries: OrderedDict[_Key, _Entry] = OrderedDict()
        self._names: dict[UUID, _Key] = {}  # id -> its entry's key; a node has one name (no hard links)
        self._lookups = self._hits = self._expired = self._stores = self._evictions = self._clears = 0

    @property
    def ttl(self) -> float:
        """Seconds an entry lives after it is stored. Fixed, so entries expire in store order."""
        return self._ttl

    @property
    def max_entries(self) -> int:
        return self._max_entries

    def __len__(self) -> int:
        return len(self._entries)

    # The DirCache protocol

    def lookup(self, parent: UUID | None, name: str) -> tuple[UUID, Kind] | None:
        self._lookups += 1
        key = (parent, name)
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires <= self._clock():
            self._expired += 1
            self._remove(key)
            return None
        self._hits += 1
        return entry.node, entry.kind

    def store(self, parent: UUID | None, name: str, node: UUID, kind: Kind) -> None:
        self._stores += 1
        now = self._clock()
        key = (parent, name)
        old_key = self._names.get(node)
        if old_key is not None and old_key != key:
            self._remove(old_key)  # the node was moved: its old name is stale
        if key in self._entries:
            self._remove(key)  # another node had this name, or a refresh: either way it goes to the end
        self._entries[key] = _Entry(node, kind, now + self._ttl)
        self._names[node] = key
        self._sweep(now)

    def evict(self, node: UUID) -> None:
        key = self._names.get(node)
        if key is not None:
            self._evictions += 1
            self._remove(key)

    # Maintenance

    def clear(self) -> None:
        """Drops every entry."""
        self._clears += 1
        self._entries.clear()
        self._names.clear()

    def stats(self) -> CacheStats:
        lookups = self._lookups
        return CacheStats(
            lookups=lookups,
            hits=self._hits,
            misses=lookups - self._hits,
            expired=self._expired,
            stores=self._stores,
            evictions=self._evictions,
            clears=self._clears,
            size=len(self._entries),
        )

    @contextlib.asynccontextmanager
    async def follow(self, watcher: Watcher) -> AsyncGenerator[DirEntryCache]:
        """Keeps the cache fresh from ``watcher``'s events until the block exits.

        Clears the cache first, since entries stored before may have missed events. RuntimeError if the watcher was
        not started or is closed. If the watcher closes while the block runs, eviction falls back to the TTL alone.
        """
        if not watcher.started:
            raise RuntimeError("follow() needs a started Watcher: no events would arrive")
        subscription = watcher.subscribe(lambda e: e.op in ("move", "delete"))
        self.clear()
        leaving = asyncio.Event()
        task = asyncio.create_task(self._consume(subscription, leaving), name="minifs-dir-cache")
        task.add_done_callback(_consumer_done)
        try:
            yield self
        finally:
            leaving.set()
            subscription.close()
            task.cancel()
            await asyncio.wait({task})  # unlike awaiting the task, never swallows a cancel aimed at us

    # Internals

    async def _consume(self, subscription: Subscription, leaving: asyncio.Event) -> None:
        async for event in subscription:
            self._apply(event)
        if not leaving.is_set():
            _log.warning("minifs directory cache: the watcher closed, entries now expire by TTL only")

    def _apply(self, event: Event) -> None:
        if event.op == "rescan":
            self.clear()
        elif event.id is not None:
            if event.op == "move" and self._names.get(event.id) == (event.parent, event.name):
                return  # already cached under the name the move gave it
            self.evict(event.id)

    def _remove(self, key: _Key) -> None:
        entry = self._entries.pop(key)
        if self._names.get(entry.node) == key:
            del self._names[entry.node]

    def _sweep(self, now: float) -> None:
        """Drops expired entries from the front, then the oldest beyond ``max_entries``."""
        while self._entries:
            key, entry = next(iter(self._entries.items()))
            if entry.expires > now and len(self._entries) <= self._max_entries:
                break
            self._remove(key)


def _consumer_done(task: asyncio.Task[None]) -> None:
    if not task.cancelled() and (error := task.exception()) is not None:
        _log.error(
            "minifs directory cache: following the watcher failed, entries now expire by TTL only", exc_info=error
        )
