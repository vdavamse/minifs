"""Watch: the vfs_notify trigger (migration 005) and minifs.vfs.watch."""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import AsyncConnection, errors, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool

from minifs.db.migrate import migrate
from minifs.vfs.core import VFSError
from minifs.vfs.watch import RESCAN, ConnectionFactory, Event, Subscription, Watcher
from tests.test_tree import create, mkdir_p, rename, unlink


def factory(db: str) -> ConnectionFactory:
    async def connect() -> AsyncConnection[Any]:
        return await AsyncConnection.connect(db, autocommit=True)

    return connect


async def next_event(events: Subscription) -> Event:
    return await asyncio.wait_for(events.get(), 10)


async def drain(events: Subscription) -> list[Event]:
    """Every event until the subscription ends, failing instead of hanging if it doesn't."""

    async def collect() -> list[Event]:
        return [e async for e in events]

    return await asyncio.wait_for(collect(), 10)


async def catch_up(watcher: Watcher, conn: AsyncConnection) -> None:
    """Waits until the watcher has received every notification committed so far.

    A watch can see a change committed just before it started, so tests call this after their setup. Each
    listener receives notifications in commit order, so once a marker sent now arrives, everything before it has.
    """
    marker = uuid4()
    with watcher.subscribe(lambda e: e.id == marker) as events:
        payload = json.dumps({"op": "modify", "id": str(marker), "parent": str(uuid4())})
        await conn.execute("SELECT pg_notify(vfs_channel(), %s)", (payload,))
        assert (await next_event(events)).id == marker


@pytest.fixture
async def listener(db: str) -> AsyncIterator[AsyncConnection]:
    """A plain connection listening on the schema's channel, to check the trigger's payloads."""
    async with await AsyncConnection.connect(db, autocommit=True) as conn:
        await conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(await channel(conn))))
        yield conn


async def channel(conn: AsyncConnection) -> str:
    """The schema's NOTIFY channel, as the trigger and the watcher get it."""
    row = await (await conn.execute("SELECT vfs_channel()")).fetchone()
    assert row is not None and isinstance(row[0], str)
    return row[0]


async def collect(events: Subscription, seconds: float) -> list[tuple[float, Event]]:
    """Every event delivered within ``seconds``, each with the loop time it was taken."""
    loop = asyncio.get_running_loop()
    end = loop.time() + seconds
    taken: list[tuple[float, Event]] = []
    while (left := end - loop.time()) > 0:
        try:
            event = await asyncio.wait_for(events.get(), left)
        except TimeoutError:
            break
        taken.append((loop.time(), event))
    return taken


@pytest.fixture
async def watcher(db: str) -> AsyncIterator[Watcher]:
    async with Watcher(factory(db)) as w:
        yield w


async def payloads(listener: AsyncConnection, count: int) -> list[dict[str, Any]]:
    received = [json.loads(n.payload) async for n in listener.notifies(timeout=10, stop_after=count)]
    assert len(received) == count, received
    return received


async def terminate(conn: AsyncConnection, pid: int | None) -> None:
    """Terminates backend ``pid`` and waits until it has exited."""
    assert pid is not None
    await conn.execute("SELECT pg_terminate_backend(%s, 10000)", (pid,))


async def append(conn: AsyncConnection, node: UUID, size: int) -> None:
    """Appends a line of ``size`` bytes (nothing for 0), so each append is an extent of its own."""
    await conn.execute("SELECT vfs_append(%s, %s)", (node, (b"x" * (size - 1) + b"\n") if size else b""))


# The trigger


async def test_payload_of_each_op(conn: AsyncConnection, listener: AsyncConnection) -> None:
    d = await mkdir_p(conn, "d")
    e = await mkdir_p(conn, "e")
    f = await create(conn, d, "f")
    await append(conn, f, 5)
    await conn.execute("SELECT * FROM vfs_write(%s, %s)", (f, b"ab"))  # an overwrite: a new generation
    assert await rename(conn, f, e, "g")
    await unlink(conn, f)
    got = await payloads(listener, 7)
    assert got[:2] == [
        {"op": "create", "id": str(d), "parent": None, "name": "d", "kind": "dir"},
        {"op": "create", "id": str(e), "parent": None, "name": "e", "kind": "dir"},
    ]
    assert got[2:] == [
        {"op": "create", "id": str(f), "parent": str(d), "name": "f", "kind": "file"},
        {"op": "modify", "id": str(f), "parent": str(d), "size": 5, "gen": 0},
        {"op": "modify", "id": str(f), "parent": str(d), "size": 2, "gen": 1},
        {
            "op": "move",
            "id": str(f),
            "parent": str(e),
            "name": "g",
            "old_parent": str(d),
            "old_name": "f",
            "kind": "file",
        },
        {"op": "delete", "id": str(f), "parent": str(e), "name": "g", "kind": "file"},
    ]


async def test_rename_in_place_is_a_move(conn: AsyncConnection, listener: AsyncConnection) -> None:
    f = await create(conn, None, "f")
    assert await rename(conn, f, None, "g")
    _, moved = await payloads(listener, 2)
    assert moved["op"] == "move"
    assert (moved["parent"], moved["old_parent"], moved["name"], moved["old_name"]) == (None, None, "g", "f")


async def test_rolled_back_write_emits_nothing(conn: AsyncConnection, db: str, listener: AsyncConnection) -> None:
    async with await AsyncConnection.connect(db) as other:
        await create(other, None, "rolled-back")
        await other.rollback()
    marker = await create(conn, None, "marker")
    [event] = await payloads(listener, 1)
    assert event["id"] == str(marker)


async def test_events_are_delivered_at_commit(conn: AsyncConnection, db: str, listener: AsyncConnection) -> None:
    async with await AsyncConnection.connect(db) as other:
        late = await create(other, None, "late")  # uncommitted
        early = await create(conn, None, "early")
        [first] = await payloads(listener, 1)
        assert first["id"] == str(early)
        await other.commit()
    [second] = await payloads(listener, 1)
    assert second["id"] == str(late)


async def test_compaction_emits_nothing(conn: AsyncConnection, listener: AsyncConnection) -> None:
    f = await create(conn, None, "log")
    for _ in range(5):
        await append(conn, f, 10)
    await payloads(listener, 6)
    cur = await conn.execute("SELECT removed, err FROM vfs_compact(%s)", (f,))
    assert await cur.fetchone() == (4, None)  # five extents merged into one
    marker = await create(conn, None, "marker")
    [event] = await payloads(listener, 1)
    assert event["id"] == str(marker)


async def test_create_with_content_is_one_create(conn: AsyncConnection, listener: AsyncConnection) -> None:
    cur = await conn.execute("SELECT id FROM vfs_create(NULL, 'f', 'file', %s)", (b"hello",))
    row = await cur.fetchone()
    assert row is not None
    marker = await create(conn, None, "marker")
    assert [e["id"] for e in await payloads(listener, 2)] == [str(row[0]), str(marker)]


async def test_unchanged_inode_emits_nothing(conn: AsyncConnection, listener: AsyncConnection) -> None:
    """An UPDATE that changes nothing is silent, and so is an empty append."""
    d = await mkdir_p(conn, "d")
    f = await create(conn, d, "f")
    await payloads(listener, 2)
    await conn.execute("UPDATE vfs_nodes SET size = size, mtime = mtime WHERE id = %s", (f,))
    assert await rename(conn, f, d, "f")  # to where it already is
    await append(conn, f, 0)
    marker = await create(conn, None, "marker")
    [event] = await payloads(listener, 1)
    assert event["id"] == str(marker)


# Watcher


async def test_watch_file_sees_appends_as_modify(conn: AsyncConnection, watcher: Watcher) -> None:
    logs = await mkdir_p(conn, "logs")
    log = await create(conn, logs, "app.log")
    other = await create(conn, logs, "other.log")
    await catch_up(watcher, conn)
    async with watcher.watch("/logs/app.log") as events:
        assert (events.node, events.kind) == (log, "file")
        await append(conn, other, 3)
        await append(conn, log, 10)
        assert await next_event(events) == Event("modify", id=log, parent=logs, size=10, generation=0)


async def test_watch_directory_sees_its_entries_only(conn: AsyncConnection, watcher: Watcher) -> None:
    a = await mkdir_p(conn, "a")
    b = await mkdir_p(conn, "b")
    await catch_up(watcher, conn)
    async with watcher.watch("a") as events:
        x = await create(conn, a, "x")
        await create(conn, b, "y")
        await append(conn, x, 4)
        assert await rename(conn, x, a, "z")
        await unlink(conn, x)
        assert [(e.op, e.id, e.name) for e in [await next_event(events) for _ in range(4)]] == [
            ("create", x, "x"),
            ("modify", x, None),
            ("move", x, "z"),
            ("delete", x, "z"),
        ]


async def test_maildir_claim_is_a_move_seen_from_both_directories(conn: AsyncConnection, watcher: Watcher) -> None:
    new = await mkdir_p(conn, "tasks", "new")
    cur = await mkdir_p(conn, "tasks", "cur")
    task = await create(conn, new, "1")
    await catch_up(watcher, conn)
    async with watcher.watch("tasks/new") as inbox, watcher.watch("tasks/cur") as claimed:
        assert await rename(conn, task, cur, "1", expect_parent=new)
        claim = Event("move", id=task, parent=cur, name="1", kind="file", old_parent=new, old_name="1")
        assert await next_event(inbox) == claim
        assert await next_event(claimed) == claim


async def test_watch_root(conn: AsyncConnection, watcher: Watcher) -> None:
    deep = await mkdir_p(conn, "deep")
    await catch_up(watcher, conn)
    async with watcher.watch("/") as events:
        assert (events.node, events.kind) == (None, "dir")
        await create(conn, deep, "not-at-root")
        top = await create(conn, None, "top")
        assert await next_event(events) == Event("create", id=top, parent=None, name="top", kind="file")


async def test_watch_ends_when_its_node_is_deleted(conn: AsyncConnection, watcher: Watcher) -> None:
    d = await mkdir_p(conn, "d")
    assert d is not None
    await catch_up(watcher, conn)
    async with watcher.watch("d") as events:
        await unlink(conn, d)
        await mkdir_p(conn, "d")  # a new node at the same path: not this watch's
        assert await drain(events) == [Event("delete", id=d, parent=None, name="d", kind="dir")]


async def test_watch_follows_its_node_through_a_rename(conn: AsyncConnection, watcher: Watcher) -> None:
    a = await mkdir_p(conn, "a")
    assert a is not None
    await catch_up(watcher, conn)
    async with watcher.watch("a") as events:
        assert await rename(conn, a, None, "b")
        assert (await next_event(events)).op == "move"
        f = await create(conn, a, "f")
        assert await next_event(events) == Event("create", id=f, parent=a, name="f", kind="file")


async def test_watch_missing_path(conn: AsyncConnection, watcher: Watcher) -> None:
    await create(conn, None, "f")
    for path, errno in [("missing", "ENOENT"), ("f/x", "ENOTDIR")]:
        with pytest.raises(VFSError) as info:
            async with watcher.watch(path):
                pass
        assert (info.value.errno, info.value.path, info.value.db_error) == (errno, "/" + path, None)


async def test_terminated_listener_reconnects_with_rescan(conn: AsyncConnection, db: str) -> None:
    connects = 0
    connect = factory(db)

    async def counting() -> AsyncConnection[Any]:
        nonlocal connects
        connects += 1
        return await connect()

    async with Watcher(counting, retry_min=0.01) as watcher:
        events = watcher.subscribe()
        pid = watcher.backend_pid
        assert pid is not None and connects == 1
        await terminate(conn, pid)
        assert await next_event(events) == RESCAN
        assert connects == 2  # a fresh connection from the factory, so fresh credentials
        assert watcher.backend_pid not in (None, pid)
        f = await create(conn, None, "after")
        assert await next_event(events) == Event("create", id=f, parent=None, name="after", kind="file")


async def test_reconnect_retries_a_failing_factory(conn: AsyncConnection, db: str) -> None:
    calls = 0
    connect = factory(db)

    async def flaky() -> AsyncConnection[Any]:
        nonlocal calls
        calls += 1
        if calls in (2, 3):  # the first two reconnect attempts
            raise OSError("token endpoint unavailable")
        return await connect()

    async with Watcher(flaky, retry_min=0.01) as watcher:
        events = watcher.subscribe()
        await terminate(conn, watcher.backend_pid)
        assert await next_event(events) == RESCAN
        assert calls == 4
        f = await create(conn, None, "after")
        assert (await next_event(events)).id == f


async def test_rescan_ends_a_watch_whose_node_is_gone(conn: AsyncConnection, db: str) -> None:
    """A directory replaced while the connection was down: the rescan is delivered, then the watch ends."""
    allow = asyncio.Event()
    allow.set()
    connect = factory(db)

    async def gated() -> AsyncConnection[Any]:
        await allow.wait()
        return await connect()

    old = await mkdir_p(conn, "inbox")
    assert old is not None
    async with Watcher(gated, retry_min=0.01) as watcher, watcher.watch("inbox") as events:
        allow.clear()
        await terminate(conn, watcher.backend_pid)
        await unlink(conn, old)
        await mkdir_p(conn, "inbox")
        allow.set()
        assert await drain(events) == [RESCAN]


async def test_rescan_keeps_a_watch_whose_node_exists(conn: AsyncConnection, db: str) -> None:
    d = await mkdir_p(conn, "d")
    async with Watcher(factory(db), retry_min=0.01) as watcher, watcher.watch("d") as events:
        await terminate(conn, watcher.backend_pid)
        assert await next_event(events) == RESCAN
        f = await create(conn, d, "f")
        assert (await next_event(events)).id == f


async def test_failed_check_after_rescan_ends_the_watch(conn: AsyncConnection, db: str) -> None:
    """The rescan is delivered, and a watch whose node can't be vouched for ends rather than going stale."""
    calls = 0
    connect = factory(db)

    async def flaky() -> AsyncConnection[Any]:
        nonlocal calls
        calls += 1
        if calls == 4:  # listen, resolve, listen again, then the rescan's check
            raise OSError("token endpoint unavailable")
        return await connect()

    await mkdir_p(conn, "d")
    async with Watcher(flaky, retry_min=0.01) as watcher, watcher.watch("d") as events:
        await terminate(conn, watcher.backend_pid)
        assert await drain(events) == [RESCAN]
        assert calls == 4


async def test_a_check_from_before_a_later_rescan_is_not_reused(conn: AsyncConnection, db: str) -> None:
    """A check that finished before a second gap can't answer the rescan for that gap."""
    slow = asyncio.Event()
    gate = asyncio.Event()
    gate.set()
    connect = factory(db)

    async def gated() -> AsyncConnection[Any]:
        await gate.wait()
        if slow.is_set():
            await asyncio.sleep(0.5)
        return await connect()

    async def reconnected(watcher: Watcher) -> None:
        while watcher.backend_pid is None:
            await asyncio.sleep(0.01)

    d = await mkdir_p(conn, "d")
    assert d is not None
    async with Watcher(gated, retry_min=0.01) as watcher, watcher.watch("d") as events:
        await terminate(conn, watcher.backend_pid)
        await reconnected(watcher)
        slow.set()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(events.get(), 0.1)  # gives up while the check runs; the check finds d
        await asyncio.sleep(1)
        slow.clear()
        gate.clear()
        await terminate(conn, watcher.backend_pid)
        await unlink(conn, d)  # lost in the gap
        gate.set()
        await reconnected(watcher)
        assert await drain(events) == [RESCAN]  # checked again: d is gone, so the watch ends


@pytest.mark.parametrize("how", ["watch", "watch after a second rescan", "watcher"])
async def test_close_during_a_check_ends_the_consumer(conn: AsyncConnection, db: str, how: str) -> None:
    """Leaving the watch block (even after a later rescan orphaned the check) or the watcher's ends a consumer
    that is waiting on a check at once, without cancelling it."""
    calls = 0
    connect = factory(db)

    async def slow_check() -> AsyncConnection[Any]:
        nonlocal calls
        calls += 1
        if calls >= 4:  # listen, resolve, listen again, then checks
            await asyncio.sleep(30)
        return await connect()

    await mkdir_p(conn, "d")
    got: list[Event] = []
    consumer: asyncio.Task[None] | None = None
    async with Watcher(slow_check, retry_min=0.01) as watcher:
        async with watcher.watch("d") as events:

            async def consume() -> None:
                async for event in events:
                    got.append(event)

            consumer = asyncio.create_task(consume())
            await terminate(conn, watcher.backend_pid)
            while calls < 4:  # the consumer is waiting on the check
                await asyncio.sleep(0.01)
            if how == "watch after a second rescan":
                watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
            if how == "watcher":
                await watcher.aclose()
                await asyncio.wait_for(consumer, 2)
        await asyncio.wait_for(consumer, 2)  # ends: not cancelled, no error, no wait for the check
    assert got == []
    assert calls == 4  # no check started after closing


async def test_forged_delete_does_not_end_the_watch(conn: AsyncConnection, watcher: Watcher) -> None:
    d = await mkdir_p(conn, "d")
    assert d is not None
    await catch_up(watcher, conn)
    async with watcher.watch("d") as events:
        forged = {"op": "delete", "id": str(d), "parent": None, "name": "d", "kind": "dir"}
        await conn.execute("SELECT pg_notify(vfs_channel(), %s)", (json.dumps(forged),))
        assert (await next_event(events)).op == "delete"
        f = await create(conn, d, "f")
        assert (await next_event(events)).id == f


async def test_channel_is_the_schemas(conn: AsyncConnection, watcher: Watcher) -> None:
    """The trigger notifies on vfs_channel(), a random name, and the watcher listens there."""
    name = await channel(conn)
    assert re.fullmatch(r"vfs_[0-9a-f]{32}", name) and watcher.channel == name
    with watcher.subscribe() as events:
        f = await create(conn, None, "f")
        assert await next_event(events) == Event("create", id=f, parent=None, name="f", kind="file")


async def test_a_schema_without_the_channel_function_fails_at_start(conn: AsyncConnection, db: str) -> None:
    """A schema that predates migration 017 has no vfs_channel(): the watcher says so instead of listening on a
    channel nothing notifies, even when a later schema on the search_path has the function."""
    await conn.execute("CREATE SCHEMA old")

    async def connect_old() -> AsyncConnection[Any]:
        return await AsyncConnection.connect(db, autocommit=True, options="-c search_path=old,public")

    with pytest.raises(RuntimeError, match="no vfs_channel.. in schema 'old'.*migration 017"):
        await Watcher(connect_old).start()


async def test_each_schema_has_its_own_channel(conn: AsyncConnection, db: str) -> None:
    """A second minifs schema in the same database gets its own channel, so a watcher on one never sees the
    other's events. Its name needs quoting, which the migration and the watcher both do."""
    schema = 'Other "one"'
    await conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    options = "-c search_path=" + sql.Identifier(schema).as_string(conn).replace(" ", r"\ ") + ",public"

    async def connect_other() -> AsyncConnection[Any]:
        return await AsyncConnection.connect(db, autocommit=True, options=options)

    async with await connect_other() as c:
        await migrate(c)
        theirs = await channel(c)
    assert re.fullmatch(r"vfs_[0-9a-f]{32}", theirs) and theirs != await channel(conn)
    async with Watcher(connect_other) as other:
        assert other.channel == theirs
        events = other.subscribe()
        await create(conn, None, "in-public")  # public's trigger: not for this watcher
        async with await connect_other() as c:
            f = await create(c, None, "in-other")
        assert (await next_event(events)).id == f
        assert await collect(events, 0.3) == []


async def test_only_a_role_with_usage_on_the_schema_learns_the_channel(db: str) -> None:
    """The channel's name is what keeps other roles out, so it must not leak: the table is the owner's, the name
    isn't in vfs_channel()'s source, and the function takes USAGE on the schema. A role with USAGE and table grants,
    but none on vfs_channel_name, still gets the name and still writes (the trigger calls vfs_channel() as it)."""
    role = sql.Identifier(f"minifs_watch_{uuid4().hex[:8]}")
    async with await AsyncConnection.connect(db, autocommit=True) as admin:
        name = await channel(admin)
        await admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(role))
        try:
            await admin.execute(
                sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON vfs_nodes, vfs_extents TO {}").format(role)
            )
            await admin.execute(sql.SQL("SET ROLE {}").format(role))
            assert await channel(admin) == name
            with pytest.raises(errors.InsufficientPrivilege):
                await admin.execute("SELECT channel FROM vfs_channel_name")
            cur = await admin.execute("SELECT count(*) FROM pg_proc WHERE prosrc LIKE '%%' || %s || '%%'", (name,))
            assert await cur.fetchone() == (0,)
            async with await AsyncConnection.connect(db, autocommit=True) as listener:
                await listener.execute(sql.SQL("LISTEN {}").format(sql.Identifier(name)))
                f = await create(admin, None, "f")
                notify = await anext(listener.notifies(timeout=10))
                assert json.loads(notify.payload)["id"] == str(f)

            await admin.execute("RESET ROLE")
            await admin.execute("REVOKE USAGE ON SCHEMA public FROM PUBLIC")
            await admin.execute(sql.SQL("SET ROLE {}").format(role))
            with pytest.raises(errors.InsufficientPrivilege):
                await admin.execute("SELECT public.vfs_channel()")
        finally:
            await admin.execute("RESET ROLE")
            await admin.execute(sql.SQL("DROP OWNED BY {}").format(role))
            await admin.execute(sql.SQL("DROP ROLE {}").format(role))


async def test_rescan_floods_are_rate_limited(db: str) -> None:
    """Forged rescans (anyone who knows the channel can send them) collapse to one per interval; none is lost,
    and real events still flow. Driven through _broadcast, so no database latency falls inside the interval."""
    async with Watcher(factory(db), rescan_interval=0.3) as watcher:
        events = watcher.subscribe()
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        assert await next_event(events) == RESCAN  # the first is delivered at once
        queued = events._last_rescan  # pyright: ignore[reportPrivateUsage]
        real = Event("modify", id=uuid4())
        for _ in range(20):  # a flood while the consumer keeps up: one deferred rescan stands for all of them
            watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        watcher._broadcast(real)  # pyright: ignore[reportPrivateUsage]
        taken = await collect(events, 0.8)
        assert [e for _, e in taken] == [real, RESCAN]
        assert taken[1][0] - queued >= 0.3  # the deferred one came when the interval was up
        await asyncio.sleep(0.3)
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        assert await collect(events, 0.1) == [(pytest.approx(asyncio.get_running_loop().time(), abs=0.2), RESCAN)]


async def test_forged_rescans_reach_the_limiter(conn: AsyncConnection, db: str) -> None:
    """The same through the database: rescans NOTIFYed on the channel are folded like the watcher's own."""
    async with Watcher(factory(db), rescan_interval=30) as watcher:
        await catch_up(watcher, conn)
        events = watcher.subscribe()  # after the catch-up, whose marker it would otherwise see
        for _ in range(5):
            await conn.execute("SELECT pg_notify(vfs_channel(), %s)", ('{"op": "rescan"}',))
        await catch_up(watcher, conn)
        # One rescan, then the second catch-up's marker: the other four wait for the interval.
        assert [e.op for _, e in await collect(events, 0.2)] == ["rescan", "modify"]


async def test_a_path_watch_checks_once_per_interval(conn: AsyncConnection, db: str) -> None:
    """A rescan flood costs a watch one database check per interval, not one per rescan."""
    await mkdir_p(conn, "d")
    async with Watcher(factory(db), rescan_interval=0.3) as watcher, watcher.watch("d") as events:
        checks = 0
        still_there = events._still_there  # pyright: ignore[reportPrivateUsage]

        async def counted() -> bool:
            nonlocal checks
            checks += 1
            return await still_there()

        events._still_there = counted  # pyright: ignore[reportPrivateUsage]
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        assert [e for _, e in await collect(events, 0.1)] == [RESCAN]
        assert checks == 1
        for _ in range(20):  # all inside the interval: one deferred rescan, one more check
            watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        assert [e for _, e in await collect(events, 0.6)] == [RESCAN]
        assert checks == 2


async def test_an_overflow_rescan_is_not_deferred(db: str) -> None:
    """Inside the interval too, the rescan of an overflow stands where the lost events were: the consumer learns
    of the gap before it sees a later event. It also covers a deferred rescan."""
    async with Watcher(factory(db), maxsize=3, rescan_interval=30) as watcher:
        events = watcher.subscribe()
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        assert await next_event(events) == RESCAN
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        assert events._rescan_timer is not None  # pyright: ignore[reportPrivateUsage]
        sent = [Event("modify", id=uuid4()) for _ in range(5)]
        for event in sent:
            watcher._broadcast(event)  # pyright: ignore[reportPrivateUsage]
        assert events._rescan_timer is None  # pyright: ignore[reportPrivateUsage]
        assert [e for _, e in await collect(events, 0.2)] == [RESCAN, sent[4]]


async def test_rescan_interval_zero_delivers_every_rescan(db: str) -> None:
    async with Watcher(factory(db), rescan_interval=0) as watcher:
        events = watcher.subscribe()
        for _ in range(3):
            watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
            assert await next_event(events) == RESCAN
        assert events._rescan_timer is None  # pyright: ignore[reportPrivateUsage]


async def test_a_deferred_rescan_dies_with_its_subscription(db: str) -> None:
    """Whether the subscription closes or the watcher does."""
    async with Watcher(factory(db), rescan_interval=0.2) as watcher:
        closed, left = watcher.subscribe(), watcher.subscribe()
        for events in (closed, left):
            watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
            assert await next_event(events) == RESCAN
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        assert closed._rescan_timer is not None  # pyright: ignore[reportPrivateUsage]
        closed.close()
        assert closed._rescan_timer is None  # pyright: ignore[reportPrivateUsage]
        assert left._rescan_timer is not None  # pyright: ignore[reportPrivateUsage]
    assert left._rescan_timer is None  # pyright: ignore[reportPrivateUsage]
    await asyncio.sleep(0.3)
    assert await drain(closed) == [] and await drain(left) == []


async def test_malformed_payloads_are_logged_cut_short_and_seldom(
    conn: AsyncConnection, watcher: Watcher, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("WARNING", logger="minifs.vfs.watch")
    # Not JSON, then JSON whose bad field is long: neither the payload nor the error's text carries it whole.
    for payload in ("x" * 7000, json.dumps({"op": "create", "id": ["A" * 7000]}), "x", "x", "x"):
        await conn.execute("SELECT pg_notify(vfs_channel(), %s)", (payload,))
    await catch_up(watcher, conn)  # every payload before the marker has been handled
    warnings = [r.getMessage() for r in caplog.records if "malformed" in r.getMessage()]
    assert len(warnings) == 1, warnings  # the other four are counted into the next warning
    assert len(warnings[0]) < 400 and "7000 characters" in warnings[0]
    watcher._malformed_logged_at = -1e9  # pyright: ignore[reportPrivateUsage]
    await conn.execute("SELECT pg_notify(vfs_channel(), %s)", (json.dumps({"op": "create", "id": ["A" * 7000]}),))
    await catch_up(watcher, conn)
    warnings = [r.getMessage() for r in caplog.records if "malformed" in r.getMessage()]
    assert len(warnings) == 2 and len(warnings[1]) < 400 and "and 4 more since the last warning" in warnings[1]


async def test_consecutive_rescans_are_folded(db: str) -> None:
    async with Watcher(factory(db)) as watcher:
        events = watcher.subscribe()
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
    assert await drain(events) == [RESCAN]


async def test_aclose_keeps_the_callers_cancellation_and_still_cleans_up(db: str) -> None:
    watcher = Watcher(factory(db))
    await watcher.start()
    events = watcher.subscribe()
    pid = watcher.backend_pid
    task = asyncio.current_task()
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await watcher.aclose()
    task.uncancel()
    assert watcher.backend_pid is None
    assert await drain(events) == []
    async with await AsyncConnection.connect(db, autocommit=True) as conn:
        row = None
        for _ in range(100):  # the backend exits shortly after the client closes
            cur = await conn.execute("SELECT count(*) FROM pg_stat_activity WHERE pid = %s", (pid,))
            row = await cur.fetchone()
            if row == (0,):
                break
            await asyncio.sleep(0.05)
        assert row == (0,)


async def test_short_get_timeouts_resume_a_slow_check(conn: AsyncConnection, db: str) -> None:
    """A caller polling get() with a timeout shorter than the check still gets the rescan and later events."""
    slow = asyncio.Event()
    connects = 0
    connect = factory(db)

    async def gated() -> AsyncConnection[Any]:
        nonlocal connects
        if slow.is_set():
            connects += 1
            await asyncio.sleep(1)
        return await connect()

    d = await mkdir_p(conn, "d")
    async with Watcher(gated, retry_min=0.01) as watcher, watcher.watch("d") as events:
        slow.set()
        await terminate(conn, watcher.backend_pid)

        async def poll() -> Event:
            for _ in range(40):
                with contextlib.suppress(TimeoutError):
                    return await asyncio.wait_for(events.get(), 0.3)
            raise AssertionError("no event")

        assert await poll() == RESCAN
        f = await create(conn, d, "f")
        assert (await poll()).id == f
        assert connects == 2  # the reconnect and one check, resumed rather than restarted


async def test_watch_queries_use_the_pool(conn: AsyncConnection, db: str) -> None:
    calls = 0
    connect = factory(db)

    async def counting() -> AsyncConnection[Any]:
        nonlocal calls
        calls += 1
        return await connect()

    d = await mkdir_p(conn, "d")
    async with (
        AsyncConnectionPool(db, min_size=1, open=False) as pool,
        Watcher(counting, pool=pool) as watcher,
        watcher.watch("d") as events,
    ):
        f = await create(conn, d, "f")
        assert (await next_event(events)).id == f
        assert calls == 1  # only the LISTEN connection


async def test_failing_predicate_gets_a_rescan_and_spares_the_others(conn: AsyncConnection, watcher: Watcher) -> None:
    pid = watcher.backend_pid
    broken = watcher.subscribe(lambda e: 1 / 0 > 0)
    others = watcher.subscribe()
    f = await create(conn, None, "f")
    assert (await next_event(others)).id == f
    assert await next_event(broken) == RESCAN
    assert watcher.backend_pid == pid  # the connection was kept


async def test_silently_dead_connection_is_detected(conn: AsyncConnection, db: str) -> None:
    """A connection that stops answering without closing (a dropped route, a NAT timeout) is replaced."""
    async with Proxy(db) as proxy, Watcher(factory(proxy.conninfo), keepalive=0.2, probe_timeout=0.5) as watcher:
        events = watcher.subscribe()
        pid = watcher.backend_pid
        proxy.freeze()
        assert await asyncio.wait_for(events.get(), 30) == RESCAN
        assert watcher.backend_pid not in (None, pid)
        f = await create(conn, None, "f")
        assert (await next_event(events)).id == f


async def test_queue_overflow_becomes_rescan(conn: AsyncConnection, db: str) -> None:
    async with Watcher(factory(db), maxsize=2) as watcher:
        slow = watcher.subscribe()
        everything = watcher.subscribe(lambda e: e.name == "marker")
        for name in ("a", "b", "c"):
            await create(conn, None, name)
        marker = await create(conn, None, "marker")
        assert (await next_event(everything)).id == marker  # so slow has been offered all four
        assert await next_event(slow) == RESCAN
        assert (await next_event(slow)).id == marker


async def test_keepalive_keeps_events(conn: AsyncConnection, db: str) -> None:
    """Probing the connection between waits loses no notification."""
    async with Watcher(factory(db), keepalive=0.05, probe_timeout=10) as watcher:
        events = watcher.subscribe()
        names = [f"f{i}" for i in range(20)]
        for name in names:
            await create(conn, None, name)
            await asyncio.sleep(0.01)
        assert [(await next_event(events)).name for _ in names] == names


async def test_close_ends_subscriptions(db: str) -> None:
    async with Watcher(factory(db)) as watcher:
        events = watcher.subscribe()
    assert await drain(events) == []
    with pytest.raises(RuntimeError):
        watcher.subscribe()


async def test_closed_subscription_stops_receiving(conn: AsyncConnection, watcher: Watcher) -> None:
    with watcher.subscribe() as events:
        pass
    await create(conn, None, "f")
    assert await drain(events) == []


async def test_truncate_is_a_rescan(conn: AsyncConnection, watcher: Watcher) -> None:
    events = watcher.subscribe()
    await conn.execute("TRUNCATE vfs_nodes, vfs_extents")
    assert await next_event(events) == RESCAN


def test_malformed_payloads_are_rejected() -> None:
    node = str(uuid4())
    for payload in (
        "[]",
        "nope",
        json.dumps({"op": "chmod", "id": node}),
        '{"op": "create"}',
        '{"op": "create", "id": 7}',
        json.dumps({"op": "modify", "id": node, "size": "big"}),
        json.dumps({"op": "modify", "id": node, "size": True}),
    ):
        with pytest.raises(ValueError):
            Event.from_payload(payload)


def test_maxsize_must_be_positive() -> None:
    with pytest.raises(ValueError):
        Watcher(factory("unused"), maxsize=0)


@pytest.mark.parametrize("interval", [-1.0, float("nan"), float("inf")])
def test_rescan_interval_must_be_a_finite_non_negative_number(interval: float) -> None:
    with pytest.raises(ValueError):
        Watcher(factory("unused"), rescan_interval=interval)


class Proxy:
    """A TCP proxy in front of the test server whose current connections can be frozen: they stay open but
    forward nothing, like a connection whose route died. New connections still work."""

    def __init__(self, db: str) -> None:
        self._db = db
        self._target = conninfo_to_dict(db)
        self._open: list[asyncio.Event] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self._writers: list[asyncio.StreamWriter] = []
        self.conninfo = ""

    async def __aenter__(self) -> Proxy:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        port = self._server.sockets[0].getsockname()[1]
        self.conninfo = make_conninfo(self._db, host="127.0.0.1", port=str(port))
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._server.close()
        for task in self._tasks:
            task.cancel()
        for writer in self._writers:
            writer.close()

    def freeze(self) -> None:
        for gate in self._open:
            gate.clear()
        self._open.clear()

    async def _accept(self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter) -> None:
        server_r, server_w = await asyncio.open_connection(str(self._target["host"]), int(str(self._target["port"])))
        self._writers += [client_w, server_w]
        gate = asyncio.Event()
        gate.set()
        self._open.append(gate)
        for reader, writer in ((client_r, server_w), (server_r, client_w)):
            task = asyncio.create_task(self._pipe(reader, writer, gate))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    @staticmethod
    async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, gate: asyncio.Event) -> None:
        with contextlib.suppress(ConnectionError):
            while data := await reader.read(65536):
                await gate.wait()
                writer.write(data)
                await writer.drain()
