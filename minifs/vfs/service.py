"""Everything one process needs to use the VFS, opened and closed in order: ``open_vfs``.

    cache = DirEntryCache()
    async with open_vfs(Connector(dsn), cache=cache) as client:
        ...  # one client per process; a VFSView per agent run
    log.info("directory cache: %s", cache.stats())

With a ``cache``, the process's ``Watcher`` (its one LISTEN connection) keeps it fresh from NOTIFY (README:
Caching). Without one, as for a one-shot script, nothing but the pools is opened.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from typing import Any

from minifs.db.pool import Connector, Pools
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.client import VFSClient
from minifs.vfs.watch import Watcher


@contextlib.asynccontextmanager
async def open_vfs(
    connector: Connector,
    *,
    cache: DirEntryCache | None = None,
    listen: Connector | None = None,
    **pool_args: Any,
) -> AsyncGenerator[VFSClient]:
    """A ``VFSClient`` over the write and search pools (``Pools(connector, **pool_args)``), with ``cache`` if given,
    which follows a ``Watcher`` on ``listen`` (default: ``connector``) until the block exits.

    ``listen`` is for a deployment whose ``connector`` goes through a transaction-mode pooler, which drops LISTEN: give
    it a direct connection. A following cache keeps that connection open, and its keepalive probe keeps a Lakebase
    compute awake. On exit, the client's background cache maintenance is awaited, then the watcher and the pools close.
    """
    async with contextlib.AsyncExitStack() as stack:
        pools = await stack.enter_async_context(Pools(connector, **pool_args))
        if cache is not None:
            watcher = await stack.enter_async_context(Watcher(listen or connector, pool=pools.write))
            await stack.enter_async_context(cache.follow(watcher))
        client = VFSClient(pools.write, search_pool=pools.search, cache=cache)
        try:
            yield client
        finally:
            await client.settle()
