"""Commit latency of appends with and without the `modify` NOTIFY (README: Operations, NOTIFY volume).

A transaction that ran NOTIFY takes a database-wide lock at commit, held until its notifications are queued, so
every append that notifies commits through one lock. This measures whether that lock shows: append latency and
throughput with the update trigger (`vfs_nodes_notify_update`, which emits `modify`) enabled and disabled, for one
hot file and for as many files as appenders, at several concurrencies, with a LISTEN connection draining the
channel. Appends to one file serialize on its inode lock anyway; appends to different files only meet at the
NOTIFY lock (and the WAL flush).

    uv run python benchmarks/notify_latency.py                 # postgres:17 in testcontainers
    uv run python benchmarks/notify_latency.py --dsn "$DSN"    # an existing, migrated, scratch database

It disables and re-enables the trigger, so point --dsn only at a scratch database.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import statistics
import time
from collections.abc import Iterator
from typing import Any

from psycopg import AsyncConnection

from minifs.db.migrate import migrate
from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import VFSClient

LINE = b"2026-10-25T12:00:00Z INFO request served in 12 ms by worker 7, cache hit, 200 OK\n"


async def run(dsn: str, concurrency: int, files: int, appends: int, notify: bool) -> dict[str, float]:
    """Each of ``concurrency`` tasks appends ``appends`` lines to one of ``files`` files; the latencies of every
    append, and the throughput."""
    async with await AsyncConnection.connect(dsn, autocommit=True) as admin:
        state = "ENABLE" if notify else "DISABLE"
        await admin.execute(f"ALTER TABLE vfs_nodes {state} TRIGGER vfs_nodes_notify_update")
    async with open_pool(Connector(dsn), min_size=concurrency, max_size=concurrency) as pool:
        vfs = VFSClient(pool)
        tag = f"{concurrency}-{files}-{notify}-{time.monotonic_ns()}"
        paths = [f"/bench/{tag}/{i}.log" for i in range(files)]
        for path in paths:
            await vfs.create(path, b"", parents=True)
        latencies: list[float] = []
        received = 0

        async def listen(conn: AsyncConnection[Any]) -> None:
            nonlocal received
            async for _ in conn.notifies():
                received += 1

        async def appender(i: int) -> None:
            path = paths[i % files]
            for _ in range(appends):
                start = time.perf_counter()
                await vfs.append(path, LINE)
                latencies.append(time.perf_counter() - start)

        async with await AsyncConnection.connect(dsn, autocommit=True) as listener:
            await listener.execute("LISTEN vfs")
            drain = asyncio.create_task(listen(listener))
            start = time.perf_counter()
            await asyncio.gather(*(appender(i) for i in range(concurrency)))
            elapsed = time.perf_counter() - start
            await asyncio.sleep(0.2)
            drain.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drain
    latencies.sort()
    return {
        "p50_ms": statistics.median(latencies) * 1000,
        "p99_ms": latencies[int(len(latencies) * 0.99) - 1] * 1000,
        "appends_per_s": len(latencies) / elapsed,
        "notifies": received,
    }


@contextlib.contextmanager
def database(dsn: str | None, image: str) -> Iterator[str]:
    if dsn is not None:
        yield dsn
        return
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(image, driver=None) as container:
        url = container.get_connection_url()

        async def setup() -> None:
            async with await AsyncConnection.connect(url, autocommit=True) as conn:
                await migrate(conn)

        asyncio.run(setup())
        yield url


async def main(dsn: str, appends: int, concurrencies: list[int]) -> None:
    try:
        await run(dsn, 4, 4, 50, True)  # warm up
        print(f"{'layout':<8} {'tasks':>5} {'notify':>6} {'p50 ms':>8} {'p99 ms':>8} {'appends/s':>10} {'notifies':>9}")
        for concurrency in concurrencies:
            for layout, files in (("hot", 1), ("spread", concurrency)):
                for notify in (True, False):
                    r = await run(dsn, concurrency, files, appends, notify)
                    print(
                        f"{layout:<8} {concurrency:>5} {'on' if notify else 'off':>6} {r['p50_ms']:>8.2f}"
                        f" {r['p99_ms']:>8.2f} {r['appends_per_s']:>10.0f} {r['notifies']:>9.0f}"
                    )
    finally:  # even when interrupted: a scratch database, but don't leave it without modify events
        async with await AsyncConnection.connect(dsn, autocommit=True) as admin:
            await admin.execute("ALTER TABLE vfs_nodes ENABLE TRIGGER vfs_nodes_notify_update")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dsn", help="a migrated scratch database (default: start postgres in testcontainers)")
    parser.add_argument("--image", default="postgres:17")
    parser.add_argument("--appends", type=int, default=300, help="appends per task")
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8, 32])
    args = parser.parse_args()
    with database(args.dsn, args.image) as dsn:
        asyncio.run(main(dsn, args.appends, args.concurrency))
