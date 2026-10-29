"""open_vfs (minifs.vfs.service): the pools, and with a cache or ``watch``, a Watcher the cache follows."""

import asyncio
from typing import Any

import pytest

from minifs.db.pool import Connector
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.service import open_vfs


class Counting(Connector):
    """A connector that counts the connections opened with it: the pools' and the watcher's alike build each
    connection's parameters with ``kwargs``."""

    opened = 0

    async def kwargs(self) -> dict[str, Any]:
        self.opened += 1
        return await super().kwargs()


async def test_without_a_cache_opens_only_the_pools(db: str) -> None:
    async with open_vfs(Connector(db), min_size=0) as vfs:
        assert vfs.client.cache is None and vfs.cache is None and vfs.watcher is None
        assert vfs.client.pool is vfs.pools.write and vfs.client.search_pool is vfs.pools.search
        await vfs.client.create("/a.txt", b"a\n")
        assert (await vfs.client.stat("/a.txt")).size == 2


async def test_the_cache_follows_another_process_s_renames(db: str) -> None:
    cache = DirEntryCache(ttl=60)
    async with open_vfs(Connector(db), cache=cache) as vfs:
        assert vfs.client.cache is cache and vfs.cache is cache
        assert vfs.watcher is not None and vfs.watcher.started
        await vfs.client.create("/d/a.txt", b"a\n", parents=True)
        await vfs.client.stat("/d/a.txt")
        assert cache.stats().clears == 1  # entering follow()
        evictions = cache.stats().evictions
        async with open_vfs(Connector(db), min_size=0) as other:  # another process, without a cache
            await other.client.rename("/d/a.txt", "/d/b.txt")
        async with asyncio.timeout(10):
            while cache.stats().evictions == evictions:  # the move's NOTIFY evicts the entry
                await asyncio.sleep(0.01)
        assert (await vfs.client.stat("/d/b.txt")).size == 2


async def test_watch_without_a_cache_opens_the_watcher(db: str) -> None:
    async with open_vfs(Connector(db), watch=True, min_size=0) as vfs:
        assert vfs.cache is None and vfs.watcher is not None
        await vfs.client.create("/w.txt", b"w\n")
        async with vfs.watcher.watch("/w.txt") as watch:
            await vfs.client.append("/w.txt", b"x\n")
            async with asyncio.timeout(10):
                while (await watch.get()).op != "modify":  # the create's event may come first
                    pass


async def test_listen_serves_the_watcher_and_connector_the_pools(db: str) -> None:
    main, listen = Counting(db), Counting(db)
    async with open_vfs(main, cache=DirEntryCache(), listen=listen, min_size=0) as vfs:
        await vfs.client.create("/a.txt", b"a\n")  # the write pool
        assert [e.path for e in await vfs.client.glob("*.txt")] == ["/a.txt"]  # the search pool
        assert listen.opened == 1
        assert main.opened >= 2


async def test_listen_needs_something_to_listen() -> None:
    with pytest.raises(ValueError, match="listen= needs a cache or watch=True"):
        async with open_vfs(Connector(""), listen=Connector("")):
            pass


async def test_the_write_timeouts_reach_the_client(db: str) -> None:
    async with open_vfs(Connector(db), min_size=0, lock_timeout=1.5, write_timeout=2.5) as vfs:
        assert (vfs.client.lock_timeout, vfs.client.write_timeout) == (1.5, 2.5)
