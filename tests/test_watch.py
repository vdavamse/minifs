"""Watch: the vfs_notify trigger (migration 005) and minifs.vfs.watch."""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import re
from collections import deque
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import AsyncConnection, errors, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool

from minifs.db.migrate import load_migrations, migrate
from minifs.vfs.core import MAX_COMPONENTS, VFSError
from minifs.vfs.watch import RESCAN, ConnectionFactory, Event, Op, PathWatch, Subscription, Watcher
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
    A rescan still on its way when this subscribes (every subscription gets those) is skipped.
    """
    marker = uuid4()
    with watcher.subscribe(lambda e: e.id == marker) as events:
        payload = json.dumps({"op": "modify", "id": str(marker), "parent": str(uuid4())})
        await conn.execute("SELECT pg_notify(vfs_channel(), %s)", (payload,))
        while (event := await next_event(events)) is RESCAN:
            pass
        assert event.id == marker


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
        {"op": "modify", "id": str(f), "parent": str(d), "name": "f", "size": 5, "gen": 0},
        {"op": "modify", "id": str(f), "parent": str(d), "name": "f", "size": 2, "gen": 1},
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
        # #83: over 4 KiB, so each append is an extent of its own (10-byte appends would all merge into one),
        # leaving compaction five extents to merge.
        await append(conn, f, 5000)
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
        assert await next_event(events) == Event("modify", id=log, parent=logs, name="app.log", size=10, generation=0)


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
            ("modify", x, "x"),
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
    """A schema that predates migration 018 has no vfs_channel(): the watcher says so instead of listening on a
    channel nothing notifies, even when a later schema on the search_path has the function."""
    await conn.execute("CREATE SCHEMA old")

    async def connect_old() -> AsyncConnection[Any]:
        return await AsyncConnection.connect(db, autocommit=True, options="-c search_path=old,public")

    async with await connect_old() as c:
        await migrate(c, [m for m in load_migrations() if m.version < 18])
    with pytest.raises(RuntimeError, match="no vfs_channel.. in schema 'old'.*migration 018"):
        await Watcher(connect_old).start()


async def test_the_schema_is_where_vfs_nodes_resolves(conn: AsyncConnection, db: str) -> None:
    """The minifs schema need not be first on the search_path: the watcher takes the one its statements use."""
    await conn.execute("CREATE SCHEMA app")

    async def connect_app() -> AsyncConnection[Any]:
        return await AsyncConnection.connect(db, autocommit=True, options="-c search_path=app,public")

    async with Watcher(connect_app) as watcher:
        assert watcher.channel == await channel(conn)
    await conn.execute("CREATE SCHEMA empty")

    async def connect_empty() -> AsyncConnection[Any]:
        return await AsyncConnection.connect(db, autocommit=True, options="-c search_path=empty")

    with pytest.raises(RuntimeError, match="no vfs_nodes table"):
        await Watcher(connect_empty).start()


@pytest.mark.parametrize("how", ["probe", "reconnect"])
async def test_a_watcher_follows_a_changed_channel(conn: AsyncConnection, db: str, how: str) -> None:
    """The name can change under a running watcher. The keepalive probe notices and reconnects, and a reconnect
    for any other reason reads the name again: either way the subscriber gets a rescan and then the events on the
    new channel."""
    keepalive = 0.2 if how == "probe" else 3600  # no probe in the reconnect case
    async with Watcher(factory(db), keepalive=keepalive, retry_min=0.01) as watcher:
        events = watcher.subscribe(lambda e: e.op == "create")  # and rescans; not the catch-up marker
        await catch_up(watcher, conn)
        await conn.execute("UPDATE vfs_channel_name SET channel = %s", (name := f"vfs_{uuid4().hex}",))
        if how == "reconnect":
            await terminate(conn, watcher.backend_pid)
        assert await next_event(events) == RESCAN
        assert watcher.channel == name
        f = await create(conn, None, "f")
        assert (await next_event(events)).id == f


async def test_the_trigger_functions_refuse_another_table(conn: AsyncConnection, listener: AsyncConnection) -> None:
    """They are SECURITY DEFINER, so they must act for vfs_nodes alone, whatever their ACL says: on another table
    (here the owner's own, so EXECUTE is not what stops it) they raise before reading the row, and notify nothing."""
    await conn.execute("CREATE TEMP TABLE other (id uuid, parent_id uuid, name text, kind text)")
    await conn.execute("CREATE TRIGGER a AFTER INSERT ON other FOR EACH ROW EXECUTE FUNCTION vfs_notify()")
    await conn.execute("CREATE TRIGGER b AFTER TRUNCATE ON other EXECUTE FUNCTION vfs_notify_truncate()")
    for statement in ("INSERT INTO other (id) VALUES (gen_random_uuid())", "TRUNCATE other"):
        with pytest.raises(errors.RaiseException, match="the trigger function of vfs_nodes alone"):
            await conn.execute(statement)
    f = await create(conn, None, "f")
    assert [p["id"] for p in await payloads(listener, 1)] == [str(f)]


async def test_a_function_in_the_schema_cannot_stand_in_for_a_builtin(conn: AsyncConnection) -> None:
    """The trigger functions run as the owner, so they call pg_catalog's functions and operators by name: a
    closer-matching json_build_object or <> in the schema (which a role with CREATE on it could add) is not what
    they run. Nor does the table take a name that pg_notify would refuse, which would fail every write."""
    await conn.execute(
        "CREATE FUNCTION json_build_object(text, text, text, uuid, text, uuid, text, text, text, text) RETURNS json"
        " LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'hijacked as %', current_user; END $$"
    )
    await conn.execute(
        "CREATE FUNCTION evil(oid, regclass) RETURNS boolean"
        " LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'hijacked as %', current_user; END $$"
    )
    await conn.execute("CREATE OPERATOR <> (LEFTARG = oid, RIGHTARG = regclass, FUNCTION = evil)")
    d = await mkdir_p(conn, "d")
    f = await create(conn, d, "f")
    await rename(conn, f, None, "g")
    await unlink(conn, f)
    await conn.execute("TRUNCATE vfs_nodes CASCADE")
    for bad in ("", "x" * 64):
        with pytest.raises(errors.CheckViolation):
            await conn.execute("UPDATE vfs_channel_name SET channel = %s", (bad,))


async def test_an_empty_channel_table_fails_at_start(conn: AsyncConnection, db: str) -> None:
    await conn.execute("DELETE FROM vfs_channel_name")
    with pytest.raises(RuntimeError, match="no channel in public.vfs_channel_name"):
        await Watcher(factory(db)).start()


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


async def test_only_a_role_granted_the_function_learns_the_channel(db: str) -> None:
    """The channel's name is what keeps other roles out, so it must not leak: the table is the owner's, the name
    isn't in vfs_channel()'s source, and EXECUTE on the function is not PUBLIC's (USAGE on "public" is). A role with
    table grants alone still writes, and its writes notify (the trigger runs as the owner), but it can't learn the
    name and a watcher run as it says what to grant. With the grant, it gets the name."""
    name_ = f"minifs_watch_{uuid4().hex[:8]}"
    role = sql.Identifier(name_)

    async def connect_as_role() -> AsyncConnection[Any]:
        c = await AsyncConnection.connect(db, autocommit=True)
        await c.execute(sql.SQL("SET ROLE {}").format(role))
        return c

    async with await AsyncConnection.connect(db, autocommit=True) as admin:
        name = await channel(admin)
        await admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(role))
        try:
            await admin.execute(
                sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON vfs_nodes, vfs_extents TO {}").format(role)
            )
            await admin.execute(sql.SQL("SET ROLE {}").format(role))
            for statement in ("SELECT vfs_channel()", "SELECT channel FROM vfs_channel_name"):
                with pytest.raises(errors.InsufficientPrivilege):
                    await admin.execute(statement)
            cur = await admin.execute("SELECT count(*) FROM pg_proc WHERE prosrc LIKE '%%' || %s || '%%'", (name,))
            assert await cur.fetchone() == (0,)
            with pytest.raises(RuntimeError, match=r"GRANT EXECUTE ON FUNCTION \"public\".vfs_channel\(\)"):
                await Watcher(connect_as_role).start()
            # Nor may it put the trigger functions, which run as the owner, on a table of its own: that would
            # forge events without the name, and run the role's own cast to json as the owner.
            await admin.execute(
                "CREATE TEMP TABLE forged (id uuid, parent_id uuid, name text, kind text, size bigint, generation int)"
            )
            for statement in (
                "CREATE TRIGGER t AFTER INSERT ON forged FOR EACH ROW EXECUTE FUNCTION public.vfs_notify()",
                "CREATE TRIGGER t AFTER TRUNCATE ON forged EXECUTE FUNCTION public.vfs_notify_truncate()",
            ):
                with pytest.raises(errors.InsufficientPrivilege):
                    await admin.execute(statement)
            async with await AsyncConnection.connect(db, autocommit=True) as listener:
                await listener.execute(sql.SQL("LISTEN {}").format(sql.Identifier(name)))
                f = await create(admin, None, "f")
                notify = await anext(listener.notifies(timeout=10))
                assert json.loads(notify.payload)["id"] == str(f)

            await admin.execute("RESET ROLE")
            await admin.execute(sql.SQL("GRANT EXECUTE ON FUNCTION vfs_channel() TO {}").format(role))
            async with Watcher(connect_as_role) as watcher:
                assert watcher.channel == name
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
        assert await next_event(events) == RESCAN  # its check opens a connection: no short window around it
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


async def test_a_reconnect_rescan_is_not_deferred(conn: AsyncConnection, db: str) -> None:
    """Inside the interval too, the rescan after a reconnect is queued at once: the consumer learns of the gap
    before it sees a later event."""
    async with Watcher(factory(db), rescan_interval=30, retry_min=0.01) as watcher:
        events = watcher.subscribe()
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        assert await next_event(events) == RESCAN
        await terminate(conn, watcher.backend_pid)
        assert await next_event(events) == RESCAN  # a deferred one would outlast next_event's bound


async def test_a_failed_predicate_rescan_is_not_deferred(db: str) -> None:
    """The rescan for a subscription whose predicate raised is queued at once, inside the interval too."""

    def broken(event: Event) -> bool:
        raise RuntimeError("the predicate is broken")

    async with Watcher(factory(db), rescan_interval=30, retry_min=0.01) as watcher:
        events = watcher.subscribe(broken)
        watcher._broadcast(RESCAN)  # pyright: ignore[reportPrivateUsage]
        assert await next_event(events) == RESCAN
        watcher._broadcast(Event("modify", id=uuid4()))  # pyright: ignore[reportPrivateUsage]
        assert await next_event(events) == RESCAN


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


async def test_a_fence_reaches_only_its_watch(
    conn: AsyncConnection, watcher: Watcher, caplog: pytest.LogCaptureFixture
) -> None:
    """A fenced watch's fence is no event: no other subscription gets it and it isn't logged as malformed; nor
    is another process's fence (a payload with the prefix no watch here waits for). Once it arrives the watch
    delivers what follows, and closing forgets it."""
    caplog.set_level("WARNING", logger="minifs.vfs.watch")
    await conn.execute("SELECT pg_notify(vfs_channel(), %s)", ("minifs-fence:not-ours",))
    with watcher.subscribe() as everything:
        async with watcher._watch("/", fence=True) as w:  # pyright: ignore[reportPrivateUsage]
            await catch_up(watcher, conn)
            assert w._fenced  # pyright: ignore[reportPrivateUsage]
            assert watcher._fences == {}  # pyright: ignore[reportPrivateUsage]
            await conn.execute("SELECT pg_notify(vfs_channel(), %s)", (json.dumps({"op": "rescan"}),))
            assert await next_event(w) is RESCAN
        everything.close()
        assert {e.op for e in await drain(everything)} <= {"modify", "rescan"}  # catch_up's marker, the rescan
    assert not [r for r in caplog.records if "malformed" in r.getMessage()]


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


def test_channel_argument_is_validated(db: str) -> None:
    """A given channel must be one LISTEN and pg_notify take: LISTEN cuts a longer name silently, and the watcher
    would then drop every notification."""
    for bad in ("", "x" * 64, "é" * 32):  # bytes, not characters: 32 of these are 64 bytes
        with pytest.raises(ValueError):
            Watcher(factory(db), channel=bad)
    Watcher(factory(db), channel="x" * 63)  # the longest name it takes; not started, so nothing listens on it


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


# The dispatch index


def _random_event(rng: random.Random, pool: list[UUID]) -> Event:
    """An event with fields drawn from ``pool`` (plus None), as forged or root-level payloads can have."""
    ids: list[UUID | None] = [*pool, None]
    op: Op = rng.choice(("create", "modify", "move", "delete"))
    return Event(
        op=op,
        id=rng.choice(ids),
        parent=rng.choice(ids),
        name="".join(rng.choice("abcdefgh") for _ in range(rng.randint(1, 4))),
        kind=rng.choice(("file", "dir")),
        old_parent=rng.choice(ids) if op == "move" else None,
        old_name="old" if op == "move" else None,
    )


def _mix(watcher: Watcher, pool: list[UUID], seed: int) -> tuple[list[Subscription], list[tuple[Any, ...]]]:
    """Subscriptions built in a fixed order from ``seed``, so two watchers' lists correspond pairwise: 14
    PathWatches of random shapes (file or dir, the root among them, with or without ``ancestors``, with or without
    a fence) that resolve, reach their fence and close at random event indexes (their plans), plus a
    ``subscribe()`` whose predicate raises every 7th call and a plain one. No database traffic."""
    rng = random.Random(seed)
    subs: list[Subscription] = []
    plans: list[tuple[Any, ...]] = []
    for _ in range(14):
        ancestors, fence = rng.random() < 0.5, rng.random() < 0.5
        watch = PathWatch(watcher, ["s0", "s1", "s2"], 10_000, 0, ancestors=ancestors, fence=fence)
        watcher._add(watch)  # pyright: ignore[reportPrivateUsage]
        chain = tuple(rng.sample(pool, rng.choice((0, 1, 2, 3))))
        node = chain[-1] if chain else None  # an empty chain is the root
        kind = rng.choice(("dir", "file")) if chain else "dir"
        resolve_at, fence_at, close_at = rng.randrange(300), rng.randrange(600), rng.randrange(900)
        plans.append((watch, node, kind, chain, ancestors, resolve_at, max(resolve_at, fence_at), close_at))
        subs.append(watch)
    calls = 0

    def flaky(event: Event) -> bool:
        nonlocal calls
        calls += 1
        if calls % 7 == 0:
            raise ValueError("a predicate that fails")
        return event.op != "modify"

    subs.append(watcher.subscribe(flaky))
    subs.append(watcher.subscribe())
    return subs, plans


def _resolve(
    watcher: Watcher, watch: PathWatch, node: UUID | None, kind: str, chain: tuple[UUID, ...], up: bool
) -> None:
    """What ``PathWatch._open`` does once the lookup returns, without the database: resolve, index, filter the
    buffer, and judge it as before the fence. Keep it in step with ``_open`` (the end-to-end test and the view
    watch tests go through the real one)."""
    watch.node, watch.kind, watch.chain = node, kind, chain
    if up:
        watch.ancestors = frozenset(chain[:-1])
    watch._resolved = True  # pyright: ignore[reportPrivateUsage]
    watcher._index(watch)  # pyright: ignore[reportPrivateUsage]
    watch._events = deque(e for e in watch._events if watch.matches(e))  # pyright: ignore[reportPrivateUsage]
    if not watch._fenced:  # pyright: ignore[reportPrivateUsage]
        buffered, watch._events = watch._events, deque()  # pyright: ignore[reportPrivateUsage]
        for event in buffered:
            watch._before_fence(event)  # pyright: ignore[reportPrivateUsage]


async def test_broadcast_reaches_the_same_subscribers_as_offering_to_all(
    db: str, caplog: pytest.LogCaptureFixture
) -> None:
    """The index only narrows who ``_offer`` is called on: over seeded streams of random events, with watches
    resolving, reaching their fence and closing mid-stream and a predicate that fails, every subscription
    receives exactly the events, in the order, it would receive were every event offered to every subscription
    as before; and once all are closed the indexes are empty. Neither watcher is started (``db`` only feeds the
    factories), and ``rescan_interval=0`` keeps timers out of it."""
    caplog.set_level("CRITICAL", logger="minifs.vfs.watch")  # the failing predicate's tracebacks
    received = 0
    for seed in range(20):
        pool = [uuid4() for _ in range(5)]
        indexed, naive = Watcher(factory(db), rescan_interval=0), Watcher(factory(db), rescan_interval=0)
        subs_a, plans_a = _mix(indexed, pool, seed)
        subs_b, plans_b = _mix(naive, pool, seed)
        rng = random.Random(seed + 1000)
        for t in range(1000):
            event = RESCAN if rng.random() < 0.03 else _random_event(rng, pool)
            for watcher, plans in ((indexed, plans_a), (naive, plans_b)):
                for watch, node, kind, chain, up, resolve_at, fence_at, close_at in plans:
                    if t == resolve_at:
                        _resolve(watcher, watch, node, kind, chain, up)
                    if t == fence_at and not watch._fenced:  # pyright: ignore[reportPrivateUsage]
                        watch._fence_reached()  # pyright: ignore[reportPrivateUsage]
                    if t == close_at:
                        watch.close()
            indexed._broadcast(event, now=True)  # pyright: ignore[reportPrivateUsage]
            for subscription in list(naive._subscriptions):  # pyright: ignore[reportPrivateUsage]
                try:  # the old loop, inline: the same offer, and the same rescue of a failed predicate
                    subscription._offer(event, now=True)  # pyright: ignore[reportPrivateUsage]
                except Exception:
                    subscription._offer(RESCAN, now=True)  # pyright: ignore[reportPrivateUsage]
        for a, b in zip(subs_a, subs_b, strict=True):
            assert list(a._events) == list(b._events), seed  # pyright: ignore[reportPrivateUsage]
            received += len(a._events)  # pyright: ignore[reportPrivateUsage]
        for subscription in subs_a:
            subscription.close()
        assert not indexed._subscriptions and not indexed._unindexed  # pyright: ignore[reportPrivateUsage]
        assert not indexed._by_id and not indexed._by_ancestor and not indexed._by_parent  # pyright: ignore[reportPrivateUsage]
    assert received > 1000  # not vacuous


async def test_broadcast_skips_unrelated_watches(db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """With a thousand indexed file watches, a modify of one node is offered only to the watches the index
    selects: ``matches`` runs a handful of times, not once per watch."""
    watcher = Watcher(factory(db), rescan_interval=0)
    maxsize = watcher._maxsize  # pyright: ignore[reportPrivateUsage]
    interval = watcher._rescan_interval  # pyright: ignore[reportPrivateUsage]
    node = uuid4()
    watches: list[PathWatch] = []
    for i in range(1001):
        watch = PathWatch(watcher, ["f"], maxsize, interval)
        watcher._add(watch)  # pyright: ignore[reportPrivateUsage]
        watched = node if i == 0 else uuid4()
        watch.node, watch.kind, watch.chain = watched, "file", (watched,)
        watch._resolved = True  # pyright: ignore[reportPrivateUsage]
        watcher._index(watch)  # pyright: ignore[reportPrivateUsage]
        watches.append(watch)
    calls = 0
    matches = PathWatch.matches

    def counting(self: PathWatch, event: Event) -> bool:
        nonlocal calls
        calls += 1
        return matches(self, event)

    monkeypatch.setattr(PathWatch, "matches", counting)
    event = Event("modify", id=node, parent=None, name="f")
    watcher._broadcast(event)  # pyright: ignore[reportPrivateUsage]
    assert list(watches[0]._events) == [event]  # pyright: ignore[reportPrivateUsage]
    # The target's own matches; nowhere near the ~1,000 an offer to all would run.
    assert calls <= 3


async def test_closed_watches_leave_the_index(conn: AsyncConnection, db: str, watcher: Watcher) -> None:
    """Indexed, fenced, and never resolved (a lookup that raised): closing a watch removes it from every
    structure, with no empty sets left behind, and ``aclose`` clears all of them."""
    d = await mkdir_p(conn, "d")
    assert d is not None
    await create(conn, d, "f")
    await catch_up(watcher, conn)
    async with (
        watcher.watch("/d"),
        watcher._watch("/d/f", ancestors=True, fence=True),  # pyright: ignore[reportPrivateUsage]
    ):
        assert watcher._by_id and watcher._by_ancestor and watcher._by_parent  # pyright: ignore[reportPrivateUsage]
        assert not watcher._unindexed  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(VFSError):  # a lookup that raises: the watch is closed never having been indexed
        async with watcher.watch("/missing"):
            pass
    assert watcher._subscriptions == set()  # pyright: ignore[reportPrivateUsage]
    assert watcher._unindexed == set()  # pyright: ignore[reportPrivateUsage]
    assert watcher._by_id == {} and watcher._by_ancestor == {} and watcher._by_parent == {}  # pyright: ignore[reportPrivateUsage]

    other = Watcher(factory(db))
    await other.start()
    other.subscribe()
    async with other.watch("/d"):
        await other.aclose()  # with subscriptions open; exiting the block closes the watch on the cleared index
    assert other._subscriptions == set()  # pyright: ignore[reportPrivateUsage]
    assert other._unindexed == set()  # pyright: ignore[reportPrivateUsage]
    assert other._by_id == {} and other._by_ancestor == {} and other._by_parent == {}  # pyright: ignore[reportPrivateUsage]


async def test_an_indexed_watch_still_sees_its_events_end_to_end(conn: AsyncConnection, watcher: Watcher) -> None:
    """Through the real pipeline: a directory watch sees a child's create, a file watch its own modify, and a
    watch with ``ancestors`` the move of a directory above it."""
    p = await mkdir_p(conn, "p")
    d = await mkdir_p(conn, "p", "d")
    assert p is not None and d is not None
    f = await create(conn, d, "f")
    await catch_up(watcher, conn)
    async with (
        watcher.watch("/p/d") as dir_events,
        watcher.watch("/p/d/f") as file_events,
        watcher._watch("/p/d/f", ancestors=True) as heir_events,  # pyright: ignore[reportPrivateUsage]
    ):
        x = await create(conn, d, "x")
        assert await next_event(dir_events) == Event("create", id=x, parent=d, name="x", kind="file")
        await append(conn, f, 10)
        modified = Event("modify", id=f, parent=d, name="f", size=10, generation=0)
        assert await next_event(file_events) == modified
        assert await next_event(dir_events) == modified  # a directory sees its child's modify too
        assert await next_event(heir_events) == modified
        assert await rename(conn, p, None, "p2")  # the watched path changes; the watches follow their nodes
        moved = Event("move", id=p, parent=None, name="p2", kind="dir", old_parent=None, old_name="p")
        assert await next_event(heir_events) == moved


async def test_watch_refuses_a_control_character_escaped(watcher: Watcher) -> None:
    """Like core.split for a caller's path (#92): EINVAL before any lookup, the path escaped in the error (#98)."""
    with pytest.raises(VFSError) as info:
        async with watcher.watch("/a\nINFO forged"):
            pass
    assert info.value.errno == "EINVAL"
    assert "\n" not in str(info.value)
    assert info.value.path == "/a\\u000aINFO forged"


async def test_watch_refuses_an_over_long_path(watcher: Watcher) -> None:
    """#89: ENAMETOOLONG before any lookup, as core.split does, not vfs_lookup's raise as a database error."""
    with pytest.raises(VFSError) as info:
        async with watcher.watch("/" + "/".join(["d"] * (MAX_COMPONENTS + 1))):
            pass
    assert info.value.errno == "ENAMETOOLONG" and info.value.db_error is None
