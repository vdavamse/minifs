"""The directory-entry cache (minifs.vfs.cache): unit behaviour, and freshness through a real Watcher."""

import asyncio
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
from minifs.vfs.watch import RESCAN, Event, Watcher
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
