"""Connections and pools for managed Postgres (Databricks Lakebase): fresh credentials, idle reconnects, retries.

Lakebase authenticates with OAuth tokens that expire after an hour, and scales an idle compute to zero, which
closes every connection. So nothing here holds a fixed password or trusts an idle connection:

- A ``Connector`` builds each connection's parameters when it opens: a static conninfo plus, optionally, a
  password from a callable that fetches a fresh token. Calling the connector opens a connection, so it is the
  ``ConnectionFactory`` that a LISTEN connection (``minifs.vfs.watch``) reconnects with.
- ``open_pool`` builds a psycopg pool on a connector that checks each connection when it is handed out and
  replaces a dead one, so a connection closed while idle never reaches the caller.
- ``Pools`` holds the two pools a process needs: one for writes and short reads, one for searches, so a heavy
  search can't take every connection.
- ``retry_serializable`` reruns a transaction that failed with a serialization failure or a deadlock.

No state outlives a transaction: no session advisory locks, no session settings, and no server-side prepared
statements (``Connector`` turns off psycopg's automatic preparation).
"""

from __future__ import annotations

import asyncio
import inspect
import os
import random
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any, Self

import psycopg
from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import AsyncConnectionPool

ConnectionFactory = Callable[[], Awaitable[AsyncConnection[Any]]]
"""Returns a new connection, with fresh credentials. The caller closes the connections it gets."""

PasswordSource = Callable[[], str | Awaitable[str]]
"""Returns the password for a new connection, for example a freshly fetched OAuth token."""

# Serialization failure (REPEATABLE READ or SERIALIZABLE) and deadlock: the transaction can simply run again.
RETRY_SQLSTATES = frozenset({"40001", "40P01"})


class Connector:
    """Opens connections to one database: ``await connector()`` returns a new ``AsyncConnection``.

    ``conninfo`` is a libpq connection string (empty: the ``PG*`` environment variables), and ``params`` are
    extra connection parameters (``user=...``, ``sslmode=...``). ``password``, if given, is called for every new
    connection, so short-lived tokens are fetched as needed; it may be sync or async. A static DSN with its
    password inside works without it. ``connect_timeout`` defaults to 30 seconds, so a connection attempt to a
    compute that doesn't wake up fails instead of hanging (the pool replaces it with its own timeout), unless the
    conninfo or ``PGCONNECT_TIMEOUT`` sets one. ``prepare_threshold`` defaults to None: psycopg doesn't prepare
    statements on the server.
    """

    def __init__(self, conninfo: str = "", *, password: PasswordSource | None = None, **params: Any) -> None:
        self.conninfo = conninfo
        self.password = password
        self.params = params
        if "connect_timeout" not in conninfo_to_dict(conninfo) and "PGCONNECT_TIMEOUT" not in os.environ:
            self.params.setdefault("connect_timeout", 30)
        # No server-side prepared statements: they are session state, lost on reconnect and broken by a
        # transaction-mode pooler.
        self.params.setdefault("prepare_threshold", None)

    async def kwargs(self) -> dict[str, Any]:
        """The connection parameters for a new connection, with a fresh password if there is a source."""
        kwargs = dict(self.params)
        if self.password is not None:
            password = self.password()
            kwargs["password"] = await password if inspect.isawaitable(password) else password
        return kwargs

    async def __call__(self) -> AsyncConnection[Any]:
        return await AsyncConnection.connect(self.conninfo, **await self.kwargs())


def open_pool(
    connector: Connector, *, min_size: int = 1, max_size: int = 5, name: str | None = None, **pool_args: Any
) -> AsyncConnectionPool[AsyncConnection[Any]]:
    """A psycopg pool whose connections come from ``connector``. It isn't open yet: ``await pool.open()``.

    Each connection is checked (an empty query) when it is handed out, and replaced if dead, which costs a round
    trip per checkout and hides connections closed by scale-to-zero. Connections above ``min_size`` are closed
    after 5 minutes idle; ``min_size=0`` lets an idle process hold none, so its compute can scale to zero.
    ``pool_args`` go to ``AsyncConnectionPool``.
    """
    pool_args.setdefault("max_idle", 5 * 60.0)
    return AsyncConnectionPool(
        connector.conninfo,
        kwargs=connector.kwargs,
        min_size=min_size,
        max_size=max_size,
        name=name,
        open=False,
        check=AsyncConnectionPool.check_connection,
        **pool_args,
    )


class Pools:
    """The write pool and the search pool of one process, both on ``connector``. An async context manager.

    ``write`` serves every operation except search; ``search`` serves the scans (Grep, Glob), so a burst of
    searches can't starve the writers. ``min_size`` and ``pool_args`` apply to both (see ``open_pool``).
    """

    def __init__(
        self, connector: Connector, *, write_size: int = 5, search_size: int = 2, min_size: int = 1, **pool_args: Any
    ) -> None:
        self.write = open_pool(connector, min_size=min_size, max_size=write_size, name="minifs-write", **pool_args)
        self.search = open_pool(connector, min_size=min_size, max_size=search_size, name="minifs-search", **pool_args)

    async def open(self, wait: bool = True, timeout: float = 30.0) -> None:
        """Opens both pools. With ``wait``, waits until each has ``min_size`` connections, so bad credentials fail
        here (with ``PoolTimeout`` after ``timeout`` seconds; the pool logs the connection error)."""
        await self.write.open(wait=wait, timeout=timeout)
        try:
            await self.search.open(wait=wait, timeout=timeout)
        except BaseException:
            await self.write.close()
            raise

    async def close(self) -> None:
        await asyncio.gather(self.write.close(), self.search.close())

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.close()


async def retry_serializable[T](
    attempt: Callable[[], Awaitable[T]], *, attempts: int = 5, base_delay: float = 0.01
) -> T:
    """Runs ``attempt`` (one whole transaction) and reruns it on a serialization failure or a deadlock.

    Waits a jittered, doubling delay between attempts and reraises after ``attempts`` failures. ``attempt`` must
    open its own transaction each time, since the failed one is rolled back.
    """
    if attempts < 1:
        raise ValueError(f"attempts must be at least 1, not {attempts}")
    for n in range(attempts):
        try:
            return await attempt()
        except psycopg.Error as e:
            if e.sqlstate not in RETRY_SQLSTATES or n == attempts - 1:
                raise
        await asyncio.sleep(base_delay * 2**n * (0.5 + random.random()))
    raise AssertionError("unreachable")
