"""The directory-entry cache (minifs.vfs.cache): unit behaviour, and freshness through a real Watcher."""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs.db.pool import Connector, open_pool
from minifs.vfs.cache import CacheStats, DirEntryCache
from minifs.vfs.client import DirCache, VFSClient
from minifs.vfs.core import VFSError
from minifs.vfs.search import SearchRefused
from minifs.vfs.watch import RESCAN, Event, Watcher
from tests.test_tree import wait_until_blocked
from tests.test_watch import catch_up, factory, terminate

Pool = AsyncConnectionPool[AsyncConnection[Any]]


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def until(condition: Callable[[], bool]) -> None:
    for _ in range(500):
        if condition():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition never held")


# Unit


def test_implements_the_client_protocol() -> None:
    cache: DirCache = DirEntryCache()
    assert cache.lookup(None, "x") is None


def test_store_lookup_evict() -> None:
    cache = DirEntryCache()
    d, f = uuid4(), uuid4()
    cache.store(None, "d", d, "dir")
    cache.store(d, "f", f, "file")
    assert cache.lookup(None, "d") == (d, "dir")
    assert cache.lookup(d, "f") == (f, "file")
    assert cache.lookup(d, "g") is None  # a miss stores nothing
    assert len(cache) == 2
    cache.evict(f)  # by id, through the reverse map
    assert cache.lookup(d, "f") is None
    cache.evict(f)  # already gone
    cache.evict(uuid4())  # never cached
    assert cache.lookup(None, "d") == (d, "dir")
    assert cache.stats() == CacheStats(lookups=5, hits=3, misses=2, expired=0, stores=2, evictions=1, clears=0, size=1)


def test_a_node_has_one_name() -> None:
    cache = DirEntryCache()
    d, f, g = uuid4(), uuid4(), uuid4()
    cache.store(d, "a", f, "file")
    cache.store(d, "b", f, "file")  # f was moved: its old name goes
    assert cache.lookup(d, "a") is None
    assert cache.lookup(d, "b") == (f, "file")
    cache.store(d, "b", g, "file")  # another node took the name: f is no longer cached anywhere
    assert cache.lookup(d, "b") == (g, "file")
    cache.evict(f)
    assert cache.lookup(d, "b") == (g, "file")
    assert len(cache) == 1 and cache.stats().evictions == 0
    cache.evict(g)
    assert len(cache) == 0


def test_ttl_counts_from_the_store() -> None:
    clock = Clock()
    cache = DirEntryCache(ttl=2.0, clock=clock)
    f = uuid4()
    cache.store(None, "f", f, "file")
    clock.now += 1.5
    assert cache.lookup(None, "f") == (f, "file")  # a hit doesn't extend the TTL
    clock.now += 0.5
    assert cache.lookup(None, "f") is None
    assert len(cache) == 0
    stats = cache.stats()
    assert (stats.hits, stats.misses, stats.expired) == (1, 1, 1)
    cache.store(None, "f", f, "file")  # storing again does
    clock.now += 1.9
    assert cache.lookup(None, "f") == (f, "file")


def test_stores_sweep_expired_entries() -> None:
    """Entries nobody looks up again (those below a removed directory) don't outlive the TTL by much."""
    clock = Clock()
    cache = DirEntryCache(ttl=2.0, clock=clock)
    old = [uuid4() for _ in range(100)]
    for i, node in enumerate(old):
        cache.store(None, f"old{i}", node, "file")
    kept = uuid4()
    cache.store(None, "kept", kept, "file")
    clock.now += 1.0
    refreshed = uuid4()
    cache.store(None, "refreshed", refreshed, "file")
    cache.store(None, "kept", kept, "file")
    clock.now += 1.5
    cache.store(None, "new", uuid4(), "file")
    assert len(cache) == 3  # refreshed, kept, new
    assert cache.lookup(None, "refreshed") == (refreshed, "file")


def test_max_entries_drops_the_oldest() -> None:
    cache = DirEntryCache(max_entries=3)
    nodes = [uuid4() for _ in range(5)]
    for i, node in enumerate(nodes):
        cache.store(None, str(i), node, "file")
    assert len(cache) == 3
    assert [cache.lookup(None, str(i)) is not None for i in range(5)] == [False, False, True, True, True]
    cache.evict(nodes[0])  # its reverse entry went with it
    assert cache.stats().evictions == 0


def test_clear_and_events() -> None:
    cache = DirEntryCache()
    d, f = uuid4(), uuid4()
    cache.store(None, "d", d, "dir")
    cache.store(d, "f", f, "file")
    cache._apply(Event("move", id=f, parent=d, name="g", old_parent=d, old_name="f"))  # pyright: ignore[reportPrivateUsage]
    assert cache.lookup(d, "f") is None
    cache._apply(RESCAN)  # pyright: ignore[reportPrivateUsage]
    assert len(cache) == 0 and cache.stats().clears == 1


def test_a_late_move_event_keeps_the_new_name() -> None:
    cache = DirEntryCache()
    d, f = uuid4(), uuid4()
    cache.store(d, "new", f, "file")  # a listing after the move, handled before its event
    cache._apply(Event("move", id=f, parent=d, name="new", old_parent=d, old_name="old"))  # pyright: ignore[reportPrivateUsage]
    assert cache.lookup(d, "new") == (f, "file")
    cache._apply(Event("move", id=f, parent=d, name="newer", old_parent=d, old_name="new"))  # pyright: ignore[reportPrivateUsage]
    assert cache.lookup(d, "new") is None


def test_a_stale_listing_stored_after_its_event_expires() -> None:
    """The case notifications can't cover: the TTL bounds it."""
    clock = Clock()
    cache = DirEntryCache(ttl=2.0, clock=clock)
    d, f = uuid4(), uuid4()
    cache._apply(Event("delete", id=f, parent=d, name="f", kind="file"))  # pyright: ignore[reportPrivateUsage]
    cache.store(d, "f", f, "file")  # from a listing whose snapshot predates the delete
    assert cache.lookup(d, "f") == (f, "file")
    clock.now += 2.0
    assert cache.lookup(d, "f") is None


def test_hit_rate() -> None:
    cache = DirEntryCache()
    assert cache.stats().hit_rate == 0.0
    cache.store(None, "a", uuid4(), "file")
    for _ in range(3):
        cache.lookup(None, "a")
    cache.lookup(None, "b")
    assert cache.stats().hit_rate == 0.75


def test_bad_arguments() -> None:
    with pytest.raises(ValueError, match="ttl"):
        DirEntryCache(ttl=0)
    with pytest.raises(ValueError, match="max_entries"):
        DirEntryCache(max_entries=0)
    for ttl in (float("nan"), float("inf")):
        with pytest.raises(ValueError, match="ttl"):
            DirEntryCache(ttl=ttl)


async def test_follow_needs_a_started_watcher(db: str) -> None:
    with pytest.raises(RuntimeError, match="started"):
        async with DirEntryCache().follow(Watcher(factory(db))):
            pass


# Through a Watcher and a client


@pytest.fixture
async def pool(db: str) -> AsyncIterator[Pool]:
    async with open_pool(Connector(db), max_size=3) as p:
        yield p


def cached_ids(cache: DirEntryCache) -> set[UUID]:
    return set(cache._names)  # pyright: ignore[reportPrivateUsage]


async def test_rename_and_delete_by_another_client_evict(pool: Pool, db: str, conn: AsyncConnection) -> None:
    async with Watcher(factory(db)) as watcher, DirEntryCache(ttl=60).follow(watcher) as cache:
        vfs, other = VFSClient(pool, cache=cache), VFSClient(pool)
        d = await vfs.mkdir("/d")
        a = await vfs.create("/d/a")
        b = await vfs.create("/d/b")
        await catch_up(watcher, conn)
        assert cached_ids(cache) == {d, a.id, b.id}
        await other.rename("/d/a", "/d/c")
        await until(lambda: a.id not in cached_ids(cache))
        await other.unlink("/d/b")
        await until(lambda: b.id not in cached_ids(cache))
        assert cached_ids(cache) == {d}
        assert cache.stats().evictions == 2
        assert [e.name for e in await vfs.readdir("/d")] == ["c"]


async def test_a_missing_name_is_never_cached(pool: Pool, db: str) -> None:
    async with Watcher(factory(db)) as watcher, DirEntryCache(ttl=60).follow(watcher) as cache:
        vfs, other = VFSClient(pool, cache=cache), VFSClient(pool)
        await vfs.mkdir("/d")
        with pytest.raises(VFSError) as error:
            await vfs.stat("/d/new")
        assert error.value.errno == "ENOENT"
        created = await other.create("/d/new", b"x")
        assert (await vfs.stat("/d/new")).id == created.id  # at once, no notification needed


async def test_killing_the_listen_backend_clears_the_cache(pool: Pool, db: str, conn: AsyncConnection) -> None:
    async with Watcher(factory(db), retry_min=0.01) as watcher, DirEntryCache(ttl=60).follow(watcher) as cache:
        vfs = VFSClient(pool, cache=cache)
        await vfs.mkdir("/d")
        await vfs.create("/d/f")
        assert len(cache) == 2
        clears = cache.stats().clears
        await terminate(conn, watcher.backend_pid)
        await until(lambda: cache.stats().clears == clears + 1)
        assert len(cache) == 0
        assert (await vfs.stat("/d/f")).name == "f"  # resolved on the server again


async def test_follow_clears_stale_entries_and_stops(db: str, caplog: pytest.LogCaptureFixture) -> None:
    cache = DirEntryCache()
    cache.store(None, "stale", uuid4(), "file")
    async with Watcher(factory(db)) as watcher:
        with caplog.at_level(logging.WARNING, "minifs.vfs.cache"):
            async with cache.follow(watcher):
                assert len(cache) == 0
                watcher._broadcast(Event("delete", id=uuid4()))  # pyright: ignore[reportPrivateUsage]  # still queued
        assert "TTL only" not in caplog.text
        assert not watcher._subscriptions  # pyright: ignore[reportPrivateUsage]


async def test_watcher_closing_falls_back_to_the_ttl(db: str, caplog: pytest.LogCaptureFixture) -> None:
    cache = DirEntryCache()
    watcher = Watcher(factory(db))
    await watcher.start()
    with caplog.at_level(logging.WARNING, "minifs.vfs.cache"):
        async with cache.follow(watcher):
            await watcher.aclose()
            await until(lambda: "TTL only" in caplog.text)
            cache.store(None, "f", uuid4(), "file")  # still a working cache
            assert len(cache) == 1


async def test_hit_rate_through_the_client(pool: Pool, db: str) -> None:
    async with Watcher(factory(db)) as watcher, DirEntryCache(ttl=60).follow(watcher) as cache:
        vfs = VFSClient(pool, cache=cache)
        await vfs.mkdir("/d")
        await vfs.create("/d/f", b"abc")
        for _ in range(4):
            assert (await vfs.pread("/d/f")).content == b"abc"
        stats = cache.stats()
        assert stats.hits == stats.lookups == 9  # /d for the create, then two links per read
        assert stats.hit_rate == 1.0


# Filled from every lookup (#46)


def walk_stats(cache: DirEntryCache) -> tuple[int, int, int]:
    stats = cache.stats()
    return stats.lookups, stats.hits, stats.stores


async def test_a_cold_stat_stores_every_component(pool: Pool) -> None:
    other = VFSClient(pool)
    f = await other.create("/a/b/c/f", b"f", parents=True)
    g = await other.create("/a/b/c/g", b"g")
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    assert (await vfs.stat("/a/b/c/f")).id == f.id
    assert walk_stats(cache) == (1, 0, 4)  # one miss at /a, then the walk stored a, b, c and f
    assert (await vfs.stat("/a/b/c/f")).id == f.id
    assert walk_stats(cache) == (1 + 4, 4, 4)  # every component hit
    assert (await vfs.stat("/a/b/c/g")).id == g.id
    assert walk_stats(cache) == (5 + 4, 4 + 3, 4 + 4)  # a, b and c hit, g missed; the walk refreshed all four
    stats = cache.stats()
    assert stats.size == 5 and stats.misses == 2


async def test_enoent_stores_the_resolved_prefix(pool: Pool) -> None:
    other = VFSClient(pool)
    b = await other.mkdir("/a/b", parents=True)
    a = (await other.stat("/a")).id
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    with pytest.raises(VFSError) as error:
        await vfs.stat("/a/b/missing")
    assert error.value.errno == "ENOENT"
    assert cached_ids(cache) == {a, b}  # /a and /a/b, nothing for missing
    assert cache.lookup(None, "a") == (a, "dir") and cache.lookup(a, "b") == (b, "dir")
    created = await other.create("/a/b/missing", b"x")
    assert (await vfs.stat("/a/b/missing")).id == created.id  # at once: missing names are never cached


async def test_enotdir_stores_the_file(pool: Pool) -> None:
    f = await VFSClient(pool).create("/d/f", parents=True)
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    with pytest.raises(VFSError) as error:
        await vfs.stat("/d/f/x")
    assert error.value.errno == "ENOTDIR"
    d = (await vfs.stat("/d")).id
    assert cache.lookup(d, "f") == (f.id, "file")


@pytest.mark.parametrize("follow", [True, False], ids=["follow", "no-notify"])
async def test_a_rename_after_the_store_still_resolves(
    pool: Pool, db: str, conn: AsyncConnection, follow: bool
) -> None:
    other = VFSClient(pool)
    old = await other.create("/a/b/f", b"old", parents=True)
    a, old_b = (await other.stat("/a")).id, (await other.stat("/a/b")).id
    assert old_b is not None
    async with Watcher(factory(db)) as watcher:
        cache = DirEntryCache(ttl=60)
        async with contextlib.AsyncExitStack() as stack:
            if follow:
                await stack.enter_async_context(cache.follow(watcher))
            vfs = VFSClient(pool, cache=cache)
            assert (await vfs.stat("/a/b/f")).id == old.id  # stores a, b and f
            assert len(cache) == 3
            # Another client moves b away and puts a new b, with a new f, in its place.
            await other.rename("/a/b", "/a/moved")
            await other.mkdir("/a/b")
            new = await other.create("/a/b/f", b"new")
            await catch_up(watcher, conn)  # with follow, the move's notification has evicted b by now
            # The race the notification can't cover: a walk whose snapshot predates the move, stored after it.
            cache.store(a, "b", old_b, "dir")
            assert (await vfs.pread("/a/b/f")).content == b"new"  # the stale chain is caught in the statement
            await vfs.settle()
            new_b = (await other.stat("/a/b")).id
            assert new_b is not None and cache.lookup(a, "b") == (new_b, "dir")  # the fallback's walk replaced it
            assert cache.lookup(new_b, "f") == (new.id, "file")
            assert (await vfs.stat("/a/b/f")).id == new.id
            assert (await vfs.stat("/a/moved/f")).id == old.id
            assert (await vfs.pread("/a/b/f")).content == b"new"


async def test_a_cached_chain_stores_nothing(pool: Pool) -> None:
    await VFSClient(pool).create("/d/f", parents=True)
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    await vfs.stat("/d/f")
    stores = cache.stats().stores
    for _ in range(3):
        await vfs.stat("/d/f")
        await vfs.pread("/d/f")
    assert cache.stats().stores == stores


async def test_mutations_learn_their_paths(pool: Pool) -> None:
    other = VFSClient(pool)
    await other.mkdir("/src/x", parents=True)
    await other.mkdir("/dst/y", parents=True)
    f = await other.create("/src/x/f")
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    await vfs.rename("/src/x/f", "/dst/y/g")  # both sides walked; the moved node is kept under its new name only
    src, dst = (await other.stat("/src/x")).id, (await other.stat("/dst/y")).id
    assert cached_ids(cache) == {(await other.stat("/src")).id, src, (await other.stat("/dst")).id, dst, f.id}
    assert cache.lookup(src, "f") is None
    assert cache.lookup(dst, "g") == (f.id, "file")
    created = await vfs.create("/dst/y/h")  # its parent hits every component
    assert cache.lookup(dst, "h") == (created.id, "file")
    await vfs.unlink("/dst/y/g")
    assert f.id not in cached_ids(cache)


async def test_glob_and_grep_learn_their_root(pool: Pool) -> None:
    f = await VFSClient(pool).create("/w/src/a.py", b"x = 1\n", parents=True)
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    assert [e.id for e in await vfs.glob("*.py", "/w/src")] == [f.id]
    assert len(cache) == 2  # w and src
    assert [h.path for h in (await vfs.grep("x = 1", "/w/src/a.py")).hits] == ["/w/src/a.py"]
    assert f.id in cached_ids(cache)
    other = await VFSClient(pool).mkdir("/w/other")
    with pytest.raises(VFSError):
        await vfs.glob("*.py", "/w/other/missing")  # the error path learns the prefix too
    assert other in cached_ids(cache)
    cache.clear()
    with pytest.raises(SearchRefused):
        await vfs.grep("x = 1", "/w/src", max_files=0)  # raised inside the snapshot, after the lookup
    assert len(cache) == 2  # w and src


async def test_a_node_gone_after_its_lookup_is_not_kept(pool: Pool, db: str, conn: AsyncConnection) -> None:
    f = await VFSClient(pool).create("/d/f", b"f", parents=True)
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    async with await AsyncConnection.connect(db) as remover:
        async with remover.transaction():
            await remover.execute("DELETE FROM vfs_nodes WHERE id = %s", (f.id,))  # not committed: still found
            unlink = asyncio.create_task(vfs.unlink("/d/f"))  # walks to f, then waits on its row lock
            await wait_until_blocked(conn, (await _backend(pool, unlink)))
        with pytest.raises(VFSError) as error:
            await unlink  # the call found f gone after the lookup found it
    assert error.value.errno == "ENOENT"
    assert f.id not in cached_ids(cache) and (await VFSClient(pool).stat("/d")).id in cached_ids(cache)


async def _backend(pool: Pool, task: asyncio.Task[None]) -> int:
    """The pid of the one backend of ``pool`` running a query other than this one, once ``task`` has sent it."""
    for _ in range(200):
        async with pool.connection() as probe:
            cur = await probe.execute(
                "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"
                " AND state = 'active' AND query LIKE '%%vfs_unlink%%'"
            )
            row = await cur.fetchone()
        if row is not None:
            return row[0]
        assert not task.done(), task
        await asyncio.sleep(0.02)
    raise AssertionError("the unlink never started")


async def test_each_operation_is_one_statement(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    other = VFSClient(pool)
    await other.create("/a/b/c/f", b"f", parents=True)
    statements: list[str] = []
    execute = AsyncConnection.execute

    async def counting(self: AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        if str(query).strip():  # the pool's check at checkout is an empty query
            statements.append(str(query))
        return await execute(self, query, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", counting)
    vfs = VFSClient(pool, cache=DirEntryCache(ttl=60))
    operations = [
        vfs.stat("/a/b/c/f"),  # cold: the walk comes back in the same row
        vfs.stat("/a/b/c/f"),  # warm
        vfs.pread("/a/b/c/f"),
        vfs.create("/a/b/c/g", b"g"),
        vfs.rename("/a/b/c/g", "/a/b/h"),
        vfs.unlink("/a/b/h"),
        vfs.readdir("/a/b"),
    ]
    for operation in operations:
        statements.clear()
        await operation
        await vfs.settle()
        assert len(statements) == 1, statements
    statements.clear()
    with pytest.raises(VFSError):
        await vfs.stat("/a/b/missing")
    assert len(statements) == 1


# More walks (#55)


def capture(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Any]]:
    """Every statement sent from now on, with its parameters."""
    statements: list[tuple[str, Any]] = []
    execute = AsyncConnection.execute

    async def capturing(self: AsyncConnection[Any], query: Any, params: Any = None, *args: Any, **kwargs: Any) -> Any:
        if str(query).strip():  # the pool's check at checkout is an empty query
            statements.append((str(query), params))
        return await execute(self, query, params, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", capturing)
    return statements


async def test_a_cold_create_with_parents_caches_every_directory(pool: Pool) -> None:
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    f = await vfs.create("/w/a/b/f", b"f", parents=True)
    w, a, b = [await VFSClient(pool).stat(path) for path in ("/w", "/w/a", "/w/a/b")]
    assert cache.lookup(None, "w") == (w.id, "dir") and cache.lookup(w.id, "a") == (a.id, "dir")
    assert cache.lookup(a.id, "b") == (b.id, "dir") and cache.lookup(b.id, "f") == (f.id, "file")
    lookups, hits, stores = walk_stats(cache)
    assert (await vfs.stat("/w/a/b/f")).id == f.id
    assert walk_stats(cache) == (lookups + 4, hits + 4, stores)  # every component hit
    y = await vfs.mkdir_p("/w/x/y")  # w found, x and y made: all three stored
    x = (await VFSClient(pool).stat("/w/x")).id
    assert cache.lookup(w.id, "x") == (x, "dir") and y is not None and cache.lookup(x, "y") == (y, "dir")
    cache.clear()
    with pytest.raises(VFSError) as error:
        await vfs.mkdir_p("/w/a/b/f/z")
    assert error.value.errno == "ENOTDIR"
    assert cached_ids(cache) == {w.id, a.id, b.id}  # the directories before the file, all as directories
    assert cache.lookup(a.id, "b") == (b.id, "dir")


async def test_a_partly_cached_path_walks_below_its_prefix(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    other = VFSClient(pool)
    await other.create("/a/b/c/f", b"f", parents=True)
    g = await other.create("/a/b/c/g", b"g")
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    await vfs.stat("/a/b/c/f")  # stores a, b, c and f
    ids = [(await other.stat(path)).id for path in ("/a", "/a/b", "/a/b/c")]
    statements = capture(monkeypatch)
    lookups, hits, stores = walk_stats(cache)
    assert (await vfs.stat("/a/b/c/g")).id == g.id
    # One statement, whose lookup starts below c: the server checks a, b and c, then looks up g alone.
    assert len(statements) == 1 and "vfs_lookup(%s, %s::uuid[])" in statements[0][0]
    assert statements[0][1][:2] == [["a", "b", "c", "g"], ids]
    assert walk_stats(cache) == (lookups + 4, hits + 3, stores + 4)  # g missed; the walk stored all four
    assert cache.lookup(ids[2], "g") == (g.id, "file")


async def test_a_stale_prefix_is_evicted_without_a_query(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    other = VFSClient(pool)
    await other.create("/a/b/f", b"f", parents=True)
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    await vfs.stat("/a/b/f")
    a, b = (await other.stat("/a")).id, (await other.stat("/a/b")).id
    await other.rename("/a/b", "/moved")  # no notifications here: the cache still has a/b
    statements = capture(monkeypatch)
    with pytest.raises(VFSError) as error:
        await vfs.stat("/a/b/g")  # the prefix a, b no longer holds: walked from the root, ENOENT at b
    assert error.value.errno == "ENOENT" and error.value.path == "/a/b/g"
    assert len(statements) == 1
    assert cache.lookup(a, "b") is None and cache.lookup(None, "a") == (a, "dir")
    assert b is not None and cache.lookup(b, "f") is not None  # keyed by b's id, which still holds
    assert (await vfs.stat("/moved/f")).path == "/moved/f"


async def test_a_rename_destination_hits_next_time(pool: Pool) -> None:
    other = VFSClient(pool)
    await other.mkdir("/dst")
    await other.create("/src/f", b"f", parents=True)
    d = await other.mkdir("/src/d")
    await other.create("/src/d/inner", b"i")
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    await vfs.stat("/src/d/inner")
    await vfs.rename("/src/f", "/dst/g")
    lookups, hits, stores = walk_stats(cache)
    assert (await vfs.stat("/dst/g")).kind == "file"
    assert walk_stats(cache) == (lookups + 2, hits + 2, stores)
    await vfs.rename("/src/d", "/dst/e")  # a cached chain (kind from the statement) moving a directory
    lookups, hits, stores = walk_stats(cache)
    assert (await vfs.stat("/dst/e/inner")).path == "/dst/e/inner"
    assert walk_stats(cache) == (lookups + 3, hits + 3, stores)  # its entries were kept: keyed by its id
    assert cache.lookup((await other.stat("/dst")).id, "e") == (d, "dir")


async def test_a_rename_evicts_a_stale_destination_link(pool: Pool) -> None:
    other = VFSClient(pool)
    await other.create("/x/f", b"f", parents=True)
    old_c = await other.mkdir("/a/b/c", parents=True)
    old_b = (await other.stat("/a/b")).id
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    await vfs.stat("/a/b/c")
    # No notifications here: the cache still has a/b/c. b and c both move, and new ones take their names.
    await other.rename("/a/b", "/a/moved")
    await other.rename("/a/moved/c", "/c")
    new_c = await other.mkdir("/a/b/c", parents=True)
    await vfs.rename("/x/f", "/a/b/c/f")  # the destination's cached chain no longer holds
    # The fallback's walk replaced (a, b) and stored (new b, c). (old b, c) -> old c is a stale link it can't reach,
    # under a directory that still exists: only checking the destination's links evicts it.
    await vfs.settle()
    assert old_b is not None and cache.lookup(old_b, "c") is None and old_c not in cached_ids(cache)
    assert (await vfs.stat("/a/b/c/f")).path == "/a/b/c/f"
    assert new_c in cached_ids(cache)


async def test_a_stale_chain_to_the_same_node_is_replaced(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    other = VFSClient(pool)
    f = await other.create("/a/b/f", b"f", parents=True)
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    await vfs.stat("/a/b/f")
    # b is replaced, and f moved into the new b: the cached chain is stale but still ends at f.
    await other.rename("/a/b", "/a/c")
    await other.mkdir("/a/b")
    await other.rename("/a/c/f", "/a/b/f")
    assert (await vfs.stat("/a/b/f")).id == f.id  # the fallback walk, which stores the chain it walked
    statements = capture(monkeypatch)
    lookups, hits, stores = walk_stats(cache)
    assert (await vfs.stat("/a/b/f")).id == f.id
    assert walk_stats(cache) == (lookups + 3, hits + 3, stores)  # all cached, and every link holds
    assert len(statements) == 1 and "unnest(%s::uuid[], %s::uuid[], %s::text[])" in statements[0][0]  # the chain
    new_b = (await other.stat("/a/b")).id
    assert new_b is not None and cache.lookup(new_b, "f") == (f.id, "file")


async def test_pread_many_learns_its_walks(pool: Pool) -> None:
    other = VFSClient(pool)
    agents = await other.create("/w/AGENTS.md", b"rules", parents=True)
    await other.create("/w/sub/f", b"f", parents=True)
    w = (await other.stat("/w")).id
    cache = DirEntryCache(ttl=60)
    vfs = VFSClient(pool, cache=cache)
    reads = [("/w/AGENTS.md", 0, 100), ("/w/CLAUDE.md", 0, 100), ("/w/sub/f/x", 0, 100)]
    results = await vfs.pread_many(reads, groups=[0, 0, 1])
    assert [r.errno if isinstance(r, VFSError) else r.content for r in results] == [b"rules", "ENOENT", "ENOTDIR"]
    f = (await other.stat("/w/sub/f")).id
    assert cache.lookup(None, "w") == (w, "dir") and cache.lookup(w, "AGENTS.md") == (agents.id, "file")
    sub = cache.lookup(w, "sub")
    assert sub is not None and cache.lookup(sub[0], "f") == (f, "file")  # up to the file, as a file
    assert cache.lookup(w, "CLAUDE.md") is None
    lookups, hits, stores = walk_stats(cache)
    assert (await vfs.pread("/w/AGENTS.md")).content == b"rules"
    assert walk_stats(cache) == (lookups + 2, hits + 2, stores)
