"""Watch: inotify-style change events from the ``vfs`` NOTIFY channel (migration 005).

One ``Watcher`` per process holds one dedicated LISTEN connection and fans the events out to its subscriptions:
``watch(path)`` for callers, and ``subscribe()`` for the library's own consumers such as the directory cache.
Use a direct connection for it, never one through a transaction-mode pooler, which would drop the LISTEN.

LISTEN lasts only as long as its connection, and Lakebase closes idle connections (scale to zero) and expires
credentials after an hour. So the watcher gets every connection from a factory, and when the connection drops it
reconnects with backoff, listens again, and then sends every subscription a ``rescan`` event: events may have
been lost in the gap, so callers re-check whatever they watch (saved cursors make that cheap) and caches clear.

A connection can also die without the socket closing. Every ``keepalive`` seconds the watcher runs ``SELECT 1``,
and treats no answer within ``probe_timeout`` as a lost connection. The probe keeps a Lakebase compute awake
while a watcher runs; that is the price of noticing a dead connection.

Events are hints, not facts: anyone who can connect can NOTIFY on the channel, and PostgreSQL folds identical
payloads within one transaction. React to an event by reading the database, never by trusting its fields alone.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Literal, Self, cast
from uuid import UUID

from psycopg import AsyncConnection, sql
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

CHANNEL = "vfs"  # the trigger in migration 005 notifies on this channel

Op = Literal["create", "modify", "move", "delete", "rescan"]

ConnectionFactory = Callable[[], Awaitable[AsyncConnection[Any]]]
"""Returns a new connection, with fresh credentials. The watcher closes the connections it gets."""


@dataclass(frozen=True, slots=True)
class Event:
    """One change to a node, as the ``vfs_notify`` trigger reports it, or a ``rescan``.

    ``parent`` is the node's directory after the change, None for the root; ``old_parent`` and ``old_name`` are
    set for ``move`` only. ``name`` and ``kind`` are set for create, move and delete; ``size`` and ``generation``
    for modify. A ``rescan`` has no fields: events may have been lost, so re-check what you watch.
    """

    op: Op
    id: UUID | None = None
    parent: UUID | None = None
    name: str | None = None
    kind: str | None = None
    size: int | None = None
    generation: int | None = None
    old_parent: UUID | None = None
    old_name: str | None = None

    @classmethod
    def from_payload(cls, payload: str) -> Event:
        """Parses a ``vfs`` NOTIFY payload. ValueError (or a subclass) if it isn't one."""
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError(f"not a JSON object: {payload!r}")
        fields = cast(dict[str, object], data)
        op = fields.get("op")
        if op == "rescan":  # TRUNCATE
            return RESCAN
        if op not in ("create", "modify", "move", "delete"):
            raise ValueError(f"unknown op in {payload!r}")
        node = _uuid(fields.get("id"))
        if node is None:
            raise ValueError(f"no id in {payload!r}")
        return cls(
            op=cast(Op, op),
            id=node,
            parent=_uuid(fields.get("parent")),
            name=_field(fields, "name", str),
            kind=_field(fields, "kind", str),
            size=_field(fields, "size", int),
            generation=_field(fields, "gen", int),
            old_parent=_uuid(fields.get("old_parent")),
            old_name=_field(fields, "old_name", str),
        )


RESCAN = Event("rescan")


def _uuid(value: object) -> UUID | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"not a uuid: {value!r}")
    return UUID(value)


def _field[T](fields: dict[str, object], key: str, kind: type[T]) -> T | None:
    value = fields.get(key)
    if value is None:
        return None
    if not isinstance(value, kind) or isinstance(value, bool):
        raise ValueError(f"{key} is not a {kind.__name__}: {value!r}")
    return value


class Subscription:
    """The events a predicate accepts, plus every ``rescan``, in commit order. An async iterator.

    The queue holds at most ``maxsize`` events. A subscriber that falls that far behind loses them and gets a
    ``rescan`` in their place, like inotify's queue overflow; so does one whose predicate raises. Iteration ends
    after ``close()`` or when the watcher closes, once the queued events are consumed.
    """

    def __init__(self, watcher: Watcher, predicate: Callable[[Event], bool] | None, maxsize: int) -> None:
        self._watcher = watcher
        self._predicate = predicate
        self._maxsize = maxsize
        self._events: deque[Event] = deque()
        self._wakeup = asyncio.Event()
        self._closed = False

    def _offer(self, event: Event) -> None:
        if self._closed or (event.op != "rescan" and self._predicate is not None and not self._predicate(event)):
            return
        if len(self._events) >= self._maxsize:
            self._events.clear()
            event = RESCAN
        self._events.append(event)
        self._wakeup.set()

    def _end(self) -> None:
        self._closed = True
        self._wakeup.set()

    def close(self) -> None:
        """Stops receiving events."""
        self._watcher._unsubscribe(self)  # pyright: ignore[reportPrivateUsage]
        self._end()

    async def get(self) -> Event:
        """The next event. StopAsyncIteration once the subscription is closed and drained."""
        while not self._events:
            if self._closed:
                raise StopAsyncIteration
            self._wakeup.clear()
            await self._wakeup.wait()
        return self._events.popleft()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Event:
        return await self.get()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class PathWatch(Subscription):
    """The events for the node a path named when the watch started, like an inotify watch.

    A file is matched by node id. A directory is matched by its id and by parent id, so a move into or out of it
    is seen from both sides. The watch follows its node, not the path: it keeps watching a node that is renamed,
    and ends (after delivering the event) when the node is deleted, or when a ``rescan`` finds it gone. Watch the
    path again to follow whatever it names now.
    """

    def __init__(self, watcher: Watcher, segments: list[str], maxsize: int) -> None:
        super().__init__(watcher, None, maxsize)
        self._segments = segments
        self.node: UUID | None = None
        """The watched node; None is the root."""
        self.kind = "dir"
        self._resolved = False
        self._gone = False

    def _offer(self, event: Event) -> None:
        # Until the path is resolved every event is kept and filtered by _open, so none is lost between
        # subscribing and resolving.
        if self._resolved and event.op != "rescan" and not self.matches(event):
            return
        super()._offer(event)

    def matches(self, event: Event) -> bool:
        if event.op == "rescan" or (self.node is not None and event.id == self.node):
            return True
        if self.kind != "dir":
            return False
        return event.parent == self.node or (event.op == "move" and event.old_parent == self.node)

    async def _open(self) -> None:
        async with self._watcher._query() as conn:  # pyright: ignore[reportPrivateUsage]
            cur = await conn.execute("SELECT id, kind FROM vfs_resolve(%s)", (self._segments,))
            row = await cur.fetchone()
        assert row is not None
        self.node, self.kind = row[0], row[1]
        self._resolved = True
        self._events = deque(e for e in self._events if self.matches(e))

    async def _exists(self) -> bool:
        if self.node is None:
            return True
        try:
            async with self._watcher._query() as conn:  # pyright: ignore[reportPrivateUsage]
                cur = await conn.execute("SELECT FROM vfs_nodes WHERE id = %s", (self.node,))
                return await cur.fetchone() is not None
        except Exception as error:
            # The rescan is delivered regardless; the caller re-checks, and a deleted node still ends the watch.
            logger.warning("minifs watch: could not check that %s still exists (%r)", self.node, error)
            return True

    def _finish(self) -> None:
        self._gone = True
        self.close()

    async def get(self) -> Event:
        while True:
            if self._gone:
                raise StopAsyncIteration
            event = await super().get()
            if event.op == "rescan":
                if not await self._exists():
                    self._finish()
                return event
            if self.matches(event):
                if event.op == "delete" and event.id == self.node:
                    self._finish()
                return event


class Watcher:
    """One LISTEN connection per process, shared by every watch and subscription. An async context manager.

    ``connect`` must return a new connection each time (with fresh credentials). Entering the watcher opens the
    first connection and listens, so errors in the configuration surface there. After that, connection failures
    are retried forever, with jittered exponential backoff from ``retry_min`` to ``retry_max`` seconds that resets
    only once a connection has answered a keepalive probe, so a connection that keeps dropping at once backs off.

    ``watch()`` runs a query when it starts and on each ``rescan``: on a connection from ``pool`` if given, else on
    a short-lived one from ``connect``. Each is bounded by ``connect_timeout``.
    """

    def __init__(
        self,
        connect: ConnectionFactory,
        *,
        pool: AsyncConnectionPool[Any] | None = None,
        keepalive: float = 30.0,
        probe_timeout: float = 10.0,
        connect_timeout: float = 30.0,
        retry_min: float = 0.1,
        retry_max: float = 10.0,
        maxsize: int = 10_000,
    ) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        self._connect = connect
        self._pool = pool
        self._keepalive = keepalive
        self._probe_timeout = probe_timeout
        self._connect_timeout = connect_timeout
        self._retry_min = retry_min
        self._retry_max = retry_max
        self._maxsize = maxsize
        self._subscriptions: set[Subscription] = set()
        self._conn: AsyncConnection[Any] | None = None
        self._healthy = False  # the current connection has answered a probe
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def backend_pid(self) -> int | None:
        """The backend pid of the LISTEN connection, None while reconnecting."""
        return None if self._conn is None else self._conn.info.backend_pid

    async def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("a Watcher can be started only once")
        self._conn = await self._listen()
        self._task = asyncio.create_task(self._run(), name="minifs-watch")

    async def aclose(self) -> None:
        """Closes the connection and ends every subscription."""
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        await self._discard()
        for subscription in list(self._subscriptions):
            subscription._end()  # pyright: ignore[reportPrivateUsage]
        self._subscriptions.clear()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.aclose()

    def subscribe(self, predicate: Callable[[Event], bool] | None = None) -> Subscription:
        """Every event that ``predicate`` accepts (all of them if None), and every ``rescan``."""
        return self._add(Subscription(self, predicate, self._maxsize))

    @contextlib.asynccontextmanager
    async def watch(self, path: str) -> AsyncGenerator[PathWatch]:
        """Events for the node at ``path`` ("/"-separated, from the root), until the block exits (see PathWatch).

        Subscribes before resolving the path, so a change committed after this returns is always delivered. A
        change committed just before the call may be delivered too, if its notification was still on its way:
        events err towards repeating a change the caller already sees, never towards missing one.
        Raises the psycopg error from ``vfs_resolve`` (``errno_name`` gives ENOENT or ENOTDIR) if the path
        doesn't resolve.
        """
        watch = self._add(PathWatch(self, [s for s in path.split("/") if s], self._maxsize))
        try:
            await watch._open()  # pyright: ignore[reportPrivateUsage]
            yield watch
        finally:
            watch.close()

    @contextlib.asynccontextmanager
    async def _query(self) -> AsyncGenerator[AsyncConnection[Any]]:
        """A connection for a watch's own queries, which must finish within connect_timeout."""
        async with asyncio.timeout(self._connect_timeout):
            if self._pool is not None:
                async with self._pool.connection() as conn:
                    yield conn
            else:
                async with await self._connect() as conn:
                    yield conn

    def _add[S: Subscription](self, subscription: S) -> S:
        if self._closed:
            raise RuntimeError("the Watcher is closed")
        self._subscriptions.add(subscription)
        return subscription

    def _unsubscribe(self, subscription: Subscription) -> None:
        self._subscriptions.discard(subscription)

    def _broadcast(self, event: Event) -> None:
        for subscription in list(self._subscriptions):
            try:
                subscription._offer(event)  # pyright: ignore[reportPrivateUsage]
            except Exception:
                logger.exception("minifs watch: a subscription's predicate failed, sending it a rescan")
                subscription._offer(RESCAN)  # pyright: ignore[reportPrivateUsage]

    async def _listen(self) -> AsyncConnection[Any]:
        async with asyncio.timeout(self._connect_timeout):
            conn = await self._connect()
            try:
                if not conn.autocommit:
                    await conn.set_autocommit(True)
                await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(CHANNEL)))
            except BaseException:
                await conn.close()
                raise
        self._healthy = False
        return conn

    async def _discard(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()

    async def _run(self) -> None:
        delay = self._retry_min
        while True:
            try:
                await self._pump()
            except Exception as error:  # any failure means the connection is gone
                logger.warning("minifs watch: LISTEN connection lost (%r), reconnecting", error)
            if self._healthy:
                delay = self._retry_min
            await self._discard()
            while True:
                await asyncio.sleep(delay * random.uniform(0.5, 1.0))
                delay = min(delay * 2, self._retry_max)
                try:
                    self._conn = await self._listen()
                    break
                except Exception as error:
                    logger.warning("minifs watch: reconnect failed (%r), retrying", error)
            # LISTEN is active again: anything committed from now on is delivered, so re-checking now misses nothing.
            self._broadcast(RESCAN)

    async def _pump(self) -> None:
        conn = self._conn
        assert conn is not None
        while True:
            # notifies() returns after keepalive seconds, however many notifications arrived meanwhile.
            async for notify in conn.notifies(timeout=self._keepalive):
                if notify.channel != CHANNEL:
                    continue
                try:
                    event = Event.from_payload(notify.payload)
                except ValueError:
                    logger.warning("minifs watch: ignoring malformed payload %r", notify.payload)
                    continue
                self._broadcast(event)
            # Notifications that arrive during the probe are kept by psycopg for the next notifies().
            async with asyncio.timeout(self._probe_timeout):
                await conn.execute("SELECT 1")
            self._healthy = True
