"""Everything one process needs to use the VFS, opened and closed in order: ``open_vfs``.

    cache = DirEntryCache()
    async with open_vfs(Connector(dsn), cache=cache) as vfs:
        ...  # one VFS per process; a VFSView over vfs.client per agent run
    log.info("directory cache: %s", cache.stats())

With a ``cache``, the process's ``Watcher`` (its one LISTEN connection) keeps it fresh from NOTIFY (docs/design.md),
and the same watcher serves ``vfs.watcher.watch(path)``. Without a cache or ``watch``, as for a one-shot
script, nothing but the pools is opened.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from dataclasses import dataclass

from minifs.db.pool import Connector, Pools
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.client import VFSClient
from minifs.vfs.watch import Watcher


@dataclass(frozen=True)
class VFS:
    """What ``open_vfs`` opened: the client, the pools it runs on, and the watcher and cache if any."""

    client: VFSClient
    pools: Pools
    watcher: Watcher | None
    cache: DirEntryCache | None


@contextlib.asynccontextmanager
async def open_vfs(
    connector: Connector,
    *,
    cache: DirEntryCache | None = None,
    watch: bool = False,
    listen: Connector | None = None,
    write_size: int = 5,
    search_size: int = 2,
    min_size: int = 1,
) -> AsyncGenerator[VFS]:
    """A ``VFSClient`` over the write and search pools (see ``Pools``), with ``cache`` if given. A cache, or
    ``watch``, opens the process's ``Watcher`` on ``listen`` (default: ``connector``), which the cache follows until
    the block exits.

    ``listen`` is for a deployment whose ``connector`` goes through a transaction-mode pooler, which drops LISTEN: give
    it a direct connection. ValueError if nothing would listen on it. The watcher keeps that connection open, and its
    keepalive probe keeps a Lakebase compute awake; ``min_size=0`` lets idle pools hold no connections. On exit, the
    client's background cache maintenance is awaited, then the watcher and the pools close.
    """
    if listen is not None and cache is None and not watch:
        raise ValueError("listen= needs a cache or watch=True: nothing would listen")
    async with contextlib.AsyncExitStack() as stack:
        pools = await stack.enter_async_context(
            Pools(connector, write_size=write_size, search_size=search_size, min_size=min_size)
        )
        watcher = None
        if cache is not None or watch:
            watcher = await stack.enter_async_context(Watcher(listen or connector, pool=pools.write))
        if cache is not None:
            assert watcher is not None
            await stack.enter_async_context(cache.follow(watcher))
        client = VFSClient(pools.write, search_pool=pools.search, cache=cache)
        try:
            yield VFS(client, pools, watcher, cache)
        finally:
            await client.settle()
