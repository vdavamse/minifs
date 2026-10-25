"""Routine compaction (minifs.vfs.compaction): the scan, the threshold, and running it from several processes."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import VFSClient
from minifs.vfs.compaction import _LOCK_CLASS, CompactionPass, Compactor  # pyright: ignore[reportPrivateUsage]
from minifs.vfs.service import open_vfs
from tests.otel import histogram_sum, total

Pool = AsyncConnectionPool[AsyncConnection[Any]]


@pytest.fixture
async def pool(db: str) -> AsyncIterator[Pool]:
    async with open_pool(Connector(db), max_size=4) as pool:
        yield pool


async def _log(vfs: VFSClient, path: str, lines: int) -> UUID:
    """A file of ``lines`` appends, so as many extents."""
    node = (await vfs.create(path, b"")).id
    async with vfs.pool.connection() as conn:
        await conn.execute(
            "SELECT count(a.*) FROM generate_series(1, %s) i, vfs_append(%s, convert_to(i || E'\\n', 'UTF8')) a",
            (lines, node),
        )
    return node


async def _extents(pool: Pool, node: UUID) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM vfs_extents WHERE node_id = %s", (node,))
        row = await cur.fetchone()
    assert row is not None
    return row[0]


def _content(lines: int) -> bytes:
    return b"".join(b"%d\n" % i for i in range(1, lines + 1))


async def test_files_over_the_threshold_are_compacted(pool: Pool) -> None:
    vfs = VFSClient(pool)
    big = await _log(vfs, "/big.log", 1001)
    small = await _log(vfs, "/small.log", 1000)
    before = await vfs.stat("/big.log")
    files, removed = total("minifs.compaction.files"), total("minifs.compaction.extents_removed")
    counted = histogram_sum("minifs.file.extents")

    done = await Compactor(pool).run_once()

    assert done.scanned == 2 and done.compacted == 1 and done.skipped == 0
    assert await _extents(pool, big) == 1 and done.removed == 1000
    assert await _extents(pool, small) == 1000
    after = await vfs.stat("/big.log")
    assert (after.size, after.generation, after.mtime) == (before.size, before.generation, before.mtime)
    assert (await vfs.pread("/big.log", 0, 10**6)).content == _content(1001)
    assert (await vfs.pread_lines("/big.log", 1000, 2)).content == b"1000\n1001\n"
    assert total("minifs.compaction.files") == files + 1
    assert total("minifs.compaction.extents_removed") == removed + 1000
    assert histogram_sum("minifs.file.extents") == counted + 2001


async def test_later_scans_read_only_the_files_modified_since(pool: Pool) -> None:
    vfs = VFSClient(pool)
    await _log(vfs, "/a.log", 3)
    await _log(vfs, "/b.log", 3)
    job = Compactor(pool, threshold=2, margin=0)
    assert (await job.run_once(full=True)).scanned == 2
    assert (await job.run_once()).scanned == 0
    await vfs.append("/a.log", b"x\n")  # compacted to 1 extent, now 2: not over the threshold
    assert await job.run_once() == CompactionPass(scanned=1, compacted=0, skipped=0, removed=0)
    await vfs.append("/a.log", b"y\n")
    assert await job.run_once() == CompactionPass(scanned=1, compacted=1, skipped=0, removed=2)


async def test_a_file_another_process_is_compacting_is_skipped_and_retried(pool: Pool, db: str) -> None:
    vfs = VFSClient(pool)
    node = await _log(vfs, "/hot.log", 5)
    job = Compactor(pool, threshold=2, margin=0)  # so the retry comes from the skipped list, not a rescan
    async with await AsyncConnection.connect(db) as other, other.transaction():
        await other.execute("SELECT pg_advisory_xact_lock(%s, hashtext(%s))", (_LOCK_CLASS, str(node)))
        assert await job.run_once(full=True) == CompactionPass(scanned=1, compacted=0, skipped=1, removed=0)
    assert await job.run_once() == CompactionPass(scanned=0, compacted=1, skipped=0, removed=4)


async def test_a_file_a_writer_holds_is_skipped_after_the_lock_timeout(pool: Pool, db: str) -> None:
    vfs = VFSClient(pool)
    node = await _log(vfs, "/held.log", 5)
    job = Compactor(pool, threshold=2, lock_timeout=0.1)
    async with await AsyncConnection.connect(db) as writer, writer.transaction():
        await writer.execute("SELECT * FROM vfs_lock_file(%s)", (node,))
        assert (await job.run_once()).skipped == 1
    assert (await job.run_once()).compacted == 1
    assert await _extents(pool, node) == 1


async def test_jobs_in_several_processes_compact_each_file_once(pool: Pool, db: str) -> None:
    vfs = VFSClient(pool)
    nodes = [await _log(vfs, f"/f{i}.log", 50) for i in range(4)]
    async with open_pool(Connector(db), max_size=2) as other:
        passes = await asyncio.gather(
            Compactor(pool, threshold=10).run_once(), Compactor(other, threshold=10).run_once()
        )
    assert sum(p.compacted for p in passes) == 4  # each file by exactly one of them
    for i, node in enumerate(nodes):
        assert await _extents(pool, node) == 1
        assert (await vfs.pread(f"/f{i}.log", 0, 10**6)).content == _content(50)


async def test_appends_during_compaction_stay_gap_free(pool: Pool) -> None:
    vfs = VFSClient(pool)
    await _log(vfs, "/live.log", 200)
    job = Compactor(pool, threshold=10, margin=60)

    async def appender() -> None:
        for i in range(201, 301):
            await vfs.append("/live.log", b"%d\n" % i)

    await asyncio.gather(appender(), job.run_once(), job.run_once())
    await job.run_once()
    assert (await vfs.pread("/live.log", 0, 10**6)).content == _content(300)


async def test_the_first_pass_looks_back_margin_seconds_unless_full(pool: Pool) -> None:
    vfs = VFSClient(pool)
    old = await _log(vfs, "/old.log", 5)
    async with pool.connection() as conn:
        await conn.execute("UPDATE vfs_nodes SET mtime = now() - interval '1 day' WHERE id = %s", (old,))
    counted = total("minifs.file.extents")
    assert (await Compactor(pool, threshold=2).run_once()).scanned == 0
    job = Compactor(pool, threshold=2)
    assert await job.run_once(full=True) == CompactionPass(scanned=1, compacted=1, skipped=0, removed=4)
    assert total("minifs.file.extents") == counted + 1


async def test_a_file_is_counted_once_per_modification(pool: Pool) -> None:
    vfs = VFSClient(pool)
    await _log(vfs, "/once.log", 3)
    job = Compactor(pool)  # the default margin rescans it
    counted = total("minifs.file.extents")
    assert (await job.run_once()).scanned == 1
    assert (await job.run_once()).scanned == 1
    assert total("minifs.file.extents") == counted + 1
    await vfs.append("/once.log", b"x\n")
    await job.run_once()
    assert total("minifs.file.extents") == counted + 2


async def test_a_deleted_file_is_nothing_to_do(pool: Pool) -> None:
    vfs = VFSClient(pool)
    node = await _log(vfs, "/gone.log", 5)
    job = Compactor(pool, threshold=2)
    assert await job._compact(node) == 4  # pyright: ignore[reportPrivateUsage]
    await vfs.unlink("/gone.log")
    assert await job._compact(node) == 0  # pyright: ignore[reportPrivateUsage]


async def test_an_idle_pool_skips_the_pass(pool: Pool) -> None:
    job = Compactor(pool, interval=0.05)
    job._requests = pool.get_stats().get("requests_num", 0)  # pyright: ignore[reportPrivateUsage]
    assert not job._busy()  # pyright: ignore[reportPrivateUsage]
    await VFSClient(pool).create("/x", b"x\n")
    assert job._busy()  # pyright: ignore[reportPrivateUsage]


async def test_open_vfs_runs_the_job_unless_told_not_to(db: str) -> None:
    async with open_vfs(Connector(db), min_size=0, compact_interval=0.01) as vfs:
        assert vfs.compactor is not None and vfs.compactor.interval == 0.01
        node = await _log(vfs.client, "/svc.log", 1001)
        async with asyncio.timeout(20):
            while await _extents(vfs.pools.write, node) > 1:
                await asyncio.sleep(0.05)
    async with open_vfs(Connector(db), min_size=0, compact=False) as vfs:
        assert vfs.compactor is None


def test_settings_are_validated(pool: Pool) -> None:
    for bad in ({"threshold": 0}, {"target": 0}, {"interval": 0}, {"lock_timeout": -1}):
        with pytest.raises(ValueError):
            Compactor(pool, **bad)
