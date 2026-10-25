"""open_vfs (minifs.vfs.service): the pools, and with a cache, a Watcher it follows."""

import asyncio
from typing import Any

from psycopg import AsyncConnection

from minifs.db.pool import Connector
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.service import open_vfs


async def test_without_a_cache_opens_only_the_pools(db: str) -> None:
    async with open_vfs(Connector(db), min_size=0) as client:
        assert client.cache is None
        assert client.search_pool is not client.pool
        await client.create("/a.txt", b"a\n")
        assert (await client.stat("/a.txt")).size == 2


async def test_the_cache_follows_another_process_s_renames(db: str) -> None:
    cache = DirEntryCache(ttl=60)
    async with open_vfs(Connector(db), cache=cache) as client:
        assert client.cache is cache
        await client.create("/d/a.txt", b"a\n", parents=True)
        await client.stat("/d/a.txt")
        assert cache.stats().clears == 1  # entering follow()
        evictions = cache.stats().evictions
        async with open_vfs(Connector(db), min_size=0) as other:  # another process, without a cache
            await other.rename("/d/a.txt", "/d/b.txt")
        async with asyncio.timeout(10):
            while cache.stats().evictions == evictions:  # the move's NOTIFY evicts the entry
                await asyncio.sleep(0.01)
        assert (await client.stat("/d/b.txt")).size == 2


class Counting(Connector):
    """A connector that counts the connections it opens."""

    opened = 0

    async def __call__(self) -> AsyncConnection[Any]:
        self.opened += 1
        return await super().__call__()


async def test_listen_takes_its_own_connector(db: str) -> None:
    """The Watcher connects through ``listen``; the pools don't."""
    listen = Counting(db)
    async with open_vfs(Connector(db), cache=DirEntryCache(), listen=listen, min_size=0):
        assert listen.opened == 1
