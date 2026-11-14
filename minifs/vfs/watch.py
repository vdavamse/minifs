"""Watch: inotify-style change events from the schema's NOTIFY channel (migrations 005 and 016).

One ``Watcher`` per process holds one dedicated LISTEN connection and fans the events out to its subscriptions:
``watch(path)`` for callers, and ``subscribe()`` for the library's own consumers such as the directory cache.
Use a direct connection for it, never one through a transaction-mode pooler, which would drop the LISTEN.

LISTEN lasts only as long as its connection, and Lakebase closes idle connections (scale to zero) and expires
credentials after an hour. So the watcher gets every connection from a factory, and when the connection drops it
reconnects with backoff, listens again, and then sends every subscription a ``rescan`` event: events may have
been lost in the gap, so callers re-check whatever they watch (saved cursors make that cheap) and caches clear.

A connection can also die without the socket closing. Every ``keepalive`` seconds the watcher runs ``SELECT 1``,
and treats no answer within ``probe_timeout`` as a lost connection, which it closes without waiting for the
server. The probe keeps a Lakebase compute awake while a watcher runs; that is the price of noticing a dead
connection.

The channel is the schema's own (``vfs_channel()``, migration 016): a random name, read from the database when the
watcher connects, so two minifs schemas in one database don't see each other's events. Channels are database-wide,
and anyone who knows a channel's name can LISTEN and NOTIFY on it, so the trust boundary is who can learn the name:
a role with EXECUTE on ``vfs_channel()``, which is the schema's owner until it grants it. PostgreSQL also folds identical payloads within one transaction. So events are
hints, not facts: react to one by reading the database, never by trusting its fields alone. What forged events can
cost is bounded where it is cheap to: each subscription delivers at most one ``rescan`` per ``rescan_interval``
(the rest collapse into it; none is lost, only delayed), and a malformed payload is logged cut short and at most
once every few seconds. A forged ``delete`` of a watched node still costs that watch one database check each.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import random
from collections import deque
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Literal, Self, cast
from uuid import UUID

from psycopg import AsyncConnection, errors, sql
from psycopg_pool import AsyncConnectionPool

from minifs.db.pool import ConnectionFactory
from minifs.vfs.core import VFSError

logger = logging.getLogger(__name__)

DEFAULT_RESCAN_INTERVAL = 1.0
"""Seconds a subscription waits after delivering a ``rescan`` before it delivers the next (``Watcher``)."""
_LOGGED_PAYLOAD_CHARS = 200
_MALFORMED_LOG_INTERVAL = 10.0  # seconds between two warnings about malformed payloads

Op = Literal["create", "modify", "move", "delete", "rescan"]


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
            raise ValueError("not a JSON object")
        fields = cast(dict[str, object], data)
        op = fields.get("op")
        if op == "rescan":  # TRUNCATE
            return RESCAN
        if op not in ("create", "modify", "move", "delete"):
            raise ValueError("unknown op")
        node = _uuid(fields.get("id"))
        if node is None:
            raise ValueError("no id")
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


def _shown(payload: str) -> str:
    """``payload`` as a log line shows it: its ``repr``, cut to ``_LOGGED_PAYLOAD_CHARS`` characters with its length,
    so an 8 KB payload isn't a log-volume attack."""
    if len(payload) <= _LOGGED_PAYLOAD_CHARS:
        return repr(payload)
    return f"{payload[:_LOGGED_PAYLOAD_CHARS]!r}... ({len(payload)} characters)"


def _uuid(value: object) -> UUID | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("id is not a string")
    return UUID(value)


def _field[T](fields: dict[str, object], key: str, kind: type[T]) -> T | None:
    value = fields.get(key)
    if value is None:
        return None
    if not isinstance(value, kind) or isinstance(value, bool):
        raise ValueError(f"{key} is not a {kind.__name__}")
    return value


class Subscription:
    """The events a predicate accepts, plus every ``rescan``, in commit order. An async iterator.

    The queue holds at most ``maxsize`` events. A subscriber that falls that far behind loses them and gets a
    ``rescan`` in their place, like inotify's queue overflow; so does one whose predicate raises. Iteration ends
    after ``close()`` or when the watcher closes, once the queued events are consumed.

    Rescans are delivered at most once per ``rescan_interval`` seconds: one that arrives sooner after the last is
    queued when the interval is up, and any number arriving meanwhile collapse into it. So a flood of forged
    rescans (anyone who knows the channel can send them) costs a ``PathWatch`` one database check per interval,
    and a directory cache one clear, while a real rescan is delayed by at most the interval, never lost. The
    rescan of an overflow is not delayed: it stands where the lost events were, and the queue's size bounds it.
    """

    def __init__(
        self,
        watcher: Watcher,
        predicate: Callable[[Event], bool] | None,
        maxsize: int,
        rescan_interval: float = DEFAULT_RESCAN_INTERVAL,
    ) -> None:
        self._watcher = watcher
        self._predicate = predicate
        self._maxsize = maxsize
        self._rescan_interval = rescan_interval
        self._events: deque[Event] = deque()
        self._wakeup = asyncio.Event()
        self._closed = False
        self._last_rescan = -math.inf  # loop time the last rescan was queued at
        self._rescan_timer: asyncio.TimerHandle | None = None  # a deferred rescan, within the interval

    def _offer(self, event: Event) -> None:
        if self._closed or (event.op != "rescan" and self._predicate is not None and not self._predicate(event)):
            return
        if len(self._events) >= self._maxsize:
            self._events.clear()
            self._offer_rescan(now=True)  # in place of the events just dropped, whatever the interval
            return
        if event is RESCAN:
            self._offer_rescan()
            return
        self._events.append(event)
        self._wakeup.set()

    def _offer_rescan(self, *, now: bool = False) -> None:
        """Queues a rescan, or defers it to the end of the interval unless ``now``."""
        self._on_rescan()
        if self._events and self._events[-1] is RESCAN:
            return  # one rescan covers both
        loop = asyncio.get_running_loop()
        wait = self._last_rescan + self._rescan_interval - loop.time()
        if wait > 0 and not now:
            if self._rescan_timer is None:  # else one is already due, and covers this one too
                self._rescan_timer = loop.call_later(wait, self._deferred_rescan)
            return
        if self._rescan_timer is not None:  # the one queued here covers the deferred one
            self._rescan_timer.cancel()
            self._rescan_timer = None
        self._last_rescan = loop.time()
        self._events.append(RESCAN)
        self._wakeup.set()

    def _deferred_rescan(self) -> None:
        self._rescan_timer = None
        if not self._closed:
            self._offer_rescan()  # the interval is up: queued now, unless a rescan is still waiting at the tail

    def _on_rescan(self) -> None:
        """Called whenever a rescan arrives, including one folded into the last, deferred, or made by an overflow."""

    def _end(self) -> None:
        self._closed = True
        if self._rescan_timer is not None:
            self._rescan_timer.cancel()
            self._rescan_timer = None
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
    is seen from both sides. The watch follows its node, not the path: it keeps watching a node that is renamed.
    After a ``rescan``, or a ``delete`` of the node itself, it checks the database, and ends (after delivering
    that event) if the node is gone or the check fails: a watch is never left on a node it can't vouch for. Watch
    the path again to follow whatever it names now.
    """

    def __init__(
        self, watcher: Watcher, segments: list[str], maxsize: int, rescan_interval: float = DEFAULT_RESCAN_INTERVAL
    ) -> None:
        super().__init__(watcher, None, maxsize, rescan_interval)
        self._segments = segments
        self.node: UUID | None = None
        """The watched node; None is the root."""
        self.kind = "dir"
        self._resolved = False
        self._gone = False
        self._check: asyncio.Task[bool] | None = None  # survives a cancelled get(), so the next one resumes it
        self._checks: set[asyncio.Task[bool]] = set()  # every check still running, so closing can cancel them

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
            cur = await conn.execute("SELECT id, kind, err, err_detail FROM vfs_lookup(%s)", (self._segments,))
            row = await cur.fetchone()
        assert row is not None
        if row[2] is not None:
            raise VFSError(row[2], "/" + "/".join(self._segments), detail=row[3] or "")
        self.node, self.kind = row[0], row[1]
        self._resolved = True
        self._events = deque(e for e in self._events if self.matches(e))

    async def _still_there(self) -> bool:
        """Whether the node still exists. False if the check fails: the caller re-watches, which is safe."""
        if self.node is None:
            return True
        try:
            async with self._watcher._query() as conn:  # pyright: ignore[reportPrivateUsage]
                cur = await conn.execute("SELECT FROM vfs_nodes WHERE id = %s", (self.node,))
                return await cur.fetchone() is not None
        except Exception as error:
            logger.warning(
                "minifs watch: could not check that %s still exists (%r), ending the watch", self.node, error
            )
            return False

    def _on_rescan(self) -> None:
        # Events may have been lost since a pending check started, so its answer can't serve this rescan. Stop
        # reusing it (without cancelling it: a get() may be awaiting it for an earlier event).
        self._check = None

    def _finish(self) -> None:
        self._gone = True
        self.close()

    def _end(self) -> None:
        # Every way of closing (close(), _finish(), Watcher.aclose()) comes here: a consumer awaiting a check then
        # ends at once instead of after the check, and no check outlives the watch.
        for check in list(self._checks):
            check.cancel()
        self._check = None
        super()._end()

    async def get(self) -> Event:
        while True:
            if self._gone:
                raise StopAsyncIteration
            event = await super().get()
            if not self.matches(event):
                continue
            # Events are hints: a rescan or the node's own delete is confirmed against the database.
            if event.op == "rescan" or (event.op == "delete" and event.id == self.node):
                if self._check is None:
                    if self._closed:
                        raise StopAsyncIteration  # no queries for a watch that is already closed
                    self._check = asyncio.ensure_future(self._still_there())
                    self._checks.add(self._check)
                    self._check.add_done_callback(self._checks.discard)
                check = self._check
                try:
                    there = await asyncio.shield(check)
                except asyncio.CancelledError:
                    task = asyncio.current_task()
                    if self._closed and task is not None and task.cancelling() == 0:
                        raise StopAsyncIteration from None  # close() cancelled the check, not our caller
                    # The caller gave up waiting (a timeout around get()). Keep the event and the check running,
                    # so the next get() resumes the check rather than restarting it, and a rescan is never lost.
                    self._events.appendleft(event)
                    self._wakeup.set()
                    raise
                if self._check is check:
                    self._check = None
                if not there:
                    self._finish()
            return event


class Watcher:
    """One LISTEN connection per process, shared by every watch and subscription. An async context manager.

    ``connect`` must return a new connection each time (with fresh credentials). Entering the watcher opens the
    first connection and listens, so errors in the configuration surface there. After that, connection failures
    are retried forever, with jittered exponential backoff from ``retry_min`` to ``retry_max`` seconds that resets
    only once a connection has answered a keepalive probe, so a connection that keeps dropping at once backs off.

    ``watch()`` runs a query when it starts, on each ``rescan`` and on its node's ``delete``: on a connection from
    ``pool`` if given, else on a short-lived one from ``connect``. Each is bounded by ``connect_timeout``. With many
    watches per process, pass a pool, so a reconnect doesn't open one connection per watch at once.

    The channel is read from the database (``vfs_channel()`` of the schema ``vfs_nodes`` resolves to) whenever a
    connection opens, unless ``channel`` names one, and each keepalive probe checks it, so a watcher follows a
    changed name within ``keepalive`` seconds, with a ``rescan``. RuntimeError from ``start()`` if the schema has no
    such function (it predates migration 016), or the role may not call it: EXECUTE on ``vfs_channel()`` is the
    owner's alone until granted, and it is what lets a role read the schema's events. ``rescan_interval`` is the least time between two ``rescan`` deliveries to
    one subscription (see ``Subscription``); 0 delivers every one.
    """

    def __init__(
        self,
        connect: ConnectionFactory,
        *,
        pool: AsyncConnectionPool[Any] | None = None,
        channel: str | None = None,
        keepalive: float = 30.0,
        probe_timeout: float = 10.0,
        connect_timeout: float = 30.0,
        retry_min: float = 0.1,
        retry_max: float = 10.0,
        maxsize: int = 10_000,
        rescan_interval: float = DEFAULT_RESCAN_INTERVAL,
    ) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        if not math.isfinite(rescan_interval) or rescan_interval < 0:
            raise ValueError("rescan_interval must be a finite number of seconds, at least 0")
        self._connect = connect
        self._pool = pool
        self._fixed_channel = channel
        self._channel = channel
        self._schema: str | None = None  # the minifs schema, when the channel is read from it
        self._keepalive = keepalive
        self._probe_timeout = probe_timeout
        self._connect_timeout = connect_timeout
        self._retry_min = retry_min
        self._retry_max = retry_max
        self._maxsize = maxsize
        self._rescan_interval = rescan_interval
        self._malformed_logged_at = -math.inf
        self._malformed_suppressed = 0
        self._subscriptions: set[Subscription] = set()
        self._conn: AsyncConnection[Any] | None = None
        self._healthy = False  # the current connection has answered a probe
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def started(self) -> bool:
        """Whether ``start()`` ran (entering the watcher runs it). Stays True after ``aclose()``, when events no longer
        arrive."""
        return self._task is not None

    @property
    def channel(self) -> str | None:
        """The NOTIFY channel the watcher listens on: None until the first connection read it from the database,
        and read again on every reconnect."""
        return self._channel

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
        try:
            if self._task is not None:
                self._task.cancel()
                await asyncio.wait({self._task})  # unlike awaiting the task, never swallows a cancel aimed at us
        finally:
            # Runs even if we are cancelled while waiting; none of it suspends (close() only calls finish()).
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
        return self._add(Subscription(self, predicate, self._maxsize, self._rescan_interval))

    @contextlib.asynccontextmanager
    async def watch(self, path: str) -> AsyncGenerator[PathWatch]:
        """Events for the node at ``path`` ("/"-separated, from the root), until the block exits (see PathWatch).

        Subscribes before resolving the path, so a change committed after this returns is delivered, or covered by
        a later ``rescan`` if the connection drops. A change committed just before the call may be delivered too,
        if its notification was still on its way: events err towards repeating a change, never towards missing one.
        Raises ``VFSError`` ENOENT or ENOTDIR if the path doesn't resolve.
        """
        watch = self._add(PathWatch(self, [s for s in path.split("/") if s], self._maxsize, self._rescan_interval))
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
                if self._fixed_channel is None:
                    # Read on every connection, not once: the name can change (a rotation, a schema migrated anew).
                    self._schema, self._channel = await self._read_channel(conn)
                assert self._channel is not None
                await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self._channel)))
            except BaseException:
                await conn.close()
                raise
        self._healthy = False
        return conn

    @staticmethod
    async def _read_channel(conn: AsyncConnection[Any]) -> tuple[str, str]:
        """The minifs schema of the connection and its channel. The schema is the one ``vfs_nodes`` resolves to,
        which is where the client's statements go, and the function is looked up in that schema alone: through the
        search_path, a schema that predates migration 016 would get the channel of a later schema on the path that
        has one, and listen to that schema's events instead of its own."""
        cur = await conn.execute(
            "SELECT n.nspname, to_regprocedure(quote_ident(n.nspname) || '.vfs_channel()') IS NOT NULL"
            " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass('vfs_nodes')"
        )
        row = await cur.fetchone()
        if row is None:
            raise RuntimeError("no vfs_nodes table on the connection's search_path: is the schema migrated?")
        schema, found = row
        if not found:
            raise RuntimeError(
                f"no vfs_channel() in schema {schema!r}: it predates migration 016 (run `minifs db migrate`)"
            )
        function = sql.SQL("{}.vfs_channel()").format(sql.Identifier(schema))
        try:
            cur = await conn.execute(sql.SQL("SELECT {}").format(function))
        except errors.InsufficientPrivilege as error:
            raise RuntimeError(
                f"the watcher's role may not call {function.as_string(conn)}: the schema's owner must "
                f"GRANT EXECUTE ON FUNCTION {function.as_string(conn)} to it (and USAGE on the schema)"
            ) from error
        row = await cur.fetchone()
        if row is None or not isinstance(row[0], str) or not row[0]:
            raise RuntimeError(
                f"no channel in {schema}.vfs_channel_name: the table must hold one row "
                "(change the name with an UPDATE, never a DELETE)"
            )
        return schema, row[0]

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
                if notify.channel != self._channel:
                    continue
                try:
                    event = Event.from_payload(notify.payload)
                except ValueError as error:
                    self._log_malformed(notify.payload, error)
                    continue
                self._broadcast(event)
            # Notifications that arrive during the probe are kept by psycopg for the next notifies().
            await self._probe(conn)
            self._healthy = True

    def _log_malformed(self, payload: str, error: ValueError) -> None:
        """Warns about a malformed ``payload`` (shown cut short), at most once every ``_MALFORMED_LOG_INTERVAL``
        seconds, with how many the warning stands for."""
        now = asyncio.get_running_loop().time()
        if now - self._malformed_logged_at < _MALFORMED_LOG_INTERVAL:
            self._malformed_suppressed += 1
            return
        more = f" (and {self._malformed_suppressed} more since the last warning)" if self._malformed_suppressed else ""
        logger.warning("minifs watch: ignoring malformed payload %s (%s)%s", _shown(payload), error, more)
        self._malformed_logged_at = now
        self._malformed_suppressed = 0

    async def _probe_query(self, conn: AsyncConnection[Any]) -> str | None:
        """The channel as the database has it now; the one listened on if it was given, after a SELECT 1."""
        if self._schema is None:
            await conn.execute("SELECT 1")
            return self._channel
        cur = await conn.execute(sql.SQL("SELECT {}.vfs_channel()").format(sql.Identifier(self._schema)))
        row = await cur.fetchone()
        return None if row is None else row[0]

    async def _probe(self, conn: AsyncConnection[Any]) -> None:
        """A trivial query, or TimeoutError after probe_timeout. Unless the channel was given, the query is the
        schema's ``vfs_channel()``, and ConnectionError if the name is no longer the one this connection listens on:
        the caller then reconnects, which reads the new name, and every subscriber gets a ``rescan``.

        On a timeout the connection is closed here rather than by cancelling the query: psycopg's cancel waits for
        the server, and how long depends on the psycopg and libpq versions.
        """
        probe = asyncio.ensure_future(self._probe_query(conn))
        probe.add_done_callback(lambda task: task.cancelled() or task.exception())  # may be abandoned: retrieve it
        try:
            done, _ = await asyncio.wait({probe}, timeout=self._probe_timeout)
        except BaseException:  # cancelled by aclose(): don't leave the probe running on a connection closed under it
            await self._discard()
            probe.cancel()
            raise
        if probe in done:
            channel = probe.result()
            if channel != self._channel:
                raise ConnectionError(f"the schema's NOTIFY channel changed (it was {self._channel!r})")
            return
        await self._discard()
        probe.cancel()
        raise TimeoutError(f"no answer to the keepalive probe within {self._probe_timeout}s")
