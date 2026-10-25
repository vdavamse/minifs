"""Routine compaction (README: Operations): a periodic job that merges the extents of files that have more than
``threshold`` (1,000) of them, as appends pile them up, with ``vfs_compact``.

``Compactor(pool).run()`` is the job, which ``open_vfs`` starts by default; ``run_once()`` is one pass:

1. **Scan**: count the current extents of every file modified since the previous scan (on the first, in the last
   ``margin`` seconds), recording each count in ``minifs.file.extents`` once per modification. A file only gains
   extents when it is written, so the files that need compacting are among those. ``mtime`` has no index (it
   would cost appends their HOT updates), so a scan reads ``vfs_nodes`` whole and the extents of the files
   modified. An append's ``mtime`` is the clock before it commits, so a scan looks ``margin`` seconds further back
   than the previous one started, to catch appends that committed after it. ``run_once(full=True)`` scans every
   file, reading every extent's key: run it by hand, for files that grew before any job ran.
2. **Compact** the files over the threshold, most extents first, each in its own short transaction: under a
   transaction-level advisory lock taken with ``pg_try_advisory_xact_lock``, so processes running the job at the
   same time skip a file one of them is compacting, and with a ``lock_timeout``, so a file that a long write holds
   is skipped rather than waited for. ``vfs_compact`` holds the file's inode lock while it merges, which appends to
   that file wait for. Skipped files are retried on the next pass.

Several processes can run the job against one database: each scans on its own, and the advisory lock keeps two of
them off one file. Nothing outlives a transaction (Lakebase: no session state). A pass is skipped while the pool
saw no other use since the previous one, so an idle process doesn't keep a Lakebase compute from scaling to zero.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, NoReturn
from uuid import UUID

import psycopg
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs import metrics
from minifs.vfs.budget import set_timeout

THRESHOLD = 1000
"""Files with more extents than this are compacted."""

INTERVAL = 300.0
"""Seconds between passes."""

_LOCK_CLASS = 0x6D66_6370  # "mfcp": the first key of the job's advisory locks; the second is the file's hash
_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CompactionPass:
    """What one pass did: files ``scanned`` (their extents counted), ``compacted`` (some extents merged),
    ``skipped`` (locked by another process or a writer, or failed: retried next pass), and the extents
    ``removed``."""

    scanned: int
    compacted: int
    skipped: int
    removed: int


class Compactor:
    """The compaction job over ``pool`` (the write pool). ``threshold`` is the extent count above which a file is
    compacted, ``target`` the extent size ``vfs_compact`` merges to, ``interval`` the seconds between passes of
    ``run``. ``timeout`` bounds each statement (the scan, one file's compaction), ``lock_timeout`` how long a file's
    compaction waits for its inode lock, and ``margin`` how far before the previous scan a scan looks back."""

    def __init__(
        self,
        pool: AsyncConnectionPool[AsyncConnection[Any]],
        *,
        threshold: int = THRESHOLD,
        target: int = 65536,
        interval: float = INTERVAL,
        timeout: float = 30.0,
        lock_timeout: float = 1.0,
        margin: float = 600.0,
    ) -> None:
        if threshold < 1 or not 0 < target <= 64 * 1024 * 1024 or interval <= 0 or timeout <= 0 or lock_timeout <= 0:
            raise ValueError("threshold, target, interval and timeouts must be positive (target at most 64 MB)")
        self.pool = pool
        self.threshold = threshold
        self.target = target
        self.interval = interval
        self.timeout = timeout
        self.lock_timeout = lock_timeout
        self.margin = timedelta(seconds=margin)
        self._since: datetime | None = None  # None: the first scan reads the last `margin` seconds
        self._recorded: dict[UUID, datetime] = {}  # the mtime each file's extents were last recorded at
        self._pending: set[UUID] = set()  # skipped files, retried next pass
        self._requests: int | None = None  # the pool's connection requests after the previous pass

    async def run(self) -> NoReturn:
        """Runs a pass every ``interval`` seconds, the first after a random part of it (so processes started
        together don't scan together), until cancelled. A failed pass is logged and the next one runs as usual."""
        await asyncio.sleep(random.uniform(0, self.interval))
        while True:
            if self._busy():
                try:
                    await self.run_once()
                except Exception:
                    _log.warning("compaction pass failed", exc_info=True)
                self._requests = self._request_count()
            await asyncio.sleep(self.interval)

    async def run_once(self, *, full: bool = False) -> CompactionPass:
        """One pass: scan, then compact the files over the threshold (see the module docstring). ``full``: scan
        every file, not only those modified since the previous pass."""
        started, counts = await self._scan(full)
        candidates = {node: extents for node, extents in counts.items() if extents > self.threshold}
        for node in self._pending - candidates.keys():
            candidates[node] = counts.get(node, self.threshold + 1)  # skipped last time, not modified since
        self._pending = set()
        compacted = removed = 0
        for node in sorted(candidates, key=lambda n: -candidates[n]):
            done = await self._compact(node)
            if done is None:
                self._pending.add(node)
                continue
            if done:
                compacted += 1
                removed += done
                metrics.COMPACTION_FILES.add(1)
                metrics.COMPACTION_REMOVED.add(done)
        self._since = started - self.margin
        return CompactionPass(len(counts), compacted, len(self._pending), removed)

    async def _scan(self, full: bool) -> tuple[datetime, dict[UUID, int]]:
        """The scan's start (the server's clock) and the current extent count of every file modified since
        ``_since`` (all of them if ``full``). Records each file's count once per ``mtime``, so the overlap between
        scans doesn't count a file twice (each process running the job records its own)."""
        async with self.pool.connection() as conn, conn.transaction():
            await set_timeout(conn, self.timeout)
            cur = await conn.execute("SELECT now()")
            row = await cur.fetchone()
            assert row is not None
            since = None if full else self._since or row[0] - self.margin
            cur = await conn.execute(
                "SELECT n.id, n.mtime, count(*)::int FROM vfs_nodes n"
                " JOIN vfs_extents e ON e.node_id = n.id AND e.generation = n.generation"
                " WHERE n.kind = 'file' AND (%(since)s::timestamptz IS NULL OR n.mtime >= %(since)s)"
                " GROUP BY n.id",
                {"since": since},
            )
            rows = await cur.fetchall()
        recorded: dict[UUID, datetime] = {}
        for node, mtime, extents in rows:
            if self._recorded.get(node) != mtime:
                metrics.FILE_EXTENTS.record(extents)
            recorded[node] = mtime
        self._recorded = recorded
        return row[0], {node: extents for node, _, extents in rows}

    async def _compact(self, node: UUID) -> int | None:
        """Compacts ``node`` and returns the extents removed (0 if the file is gone), or None if it was skipped:
        another process holds its advisory lock, a writer held its inode lock past ``lock_timeout``, or it failed."""
        lock_ms = f"{max(1, round(self.lock_timeout * 1000))}ms"
        try:
            async with self.pool.connection() as conn, conn.transaction():
                cur = await conn.execute(
                    "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s::text)),"
                    " set_config('statement_timeout', %s, true), set_config('lock_timeout', %s, true)",
                    (_LOCK_CLASS, str(node), f"{max(1, round(self.timeout * 1000))}ms", lock_ms),
                )
                row = await cur.fetchone()
                assert row is not None
                if not row[0]:
                    return None  # another process is compacting it
                cur = await conn.execute("SELECT removed FROM vfs_compact(%s, %s)", (node, self.target))
                row = await cur.fetchone()
        except psycopg.errors.LockNotAvailable:
            _log.debug("compaction of %s skipped: a writer holds it", node)
            return None
        except psycopg.Error:
            _log.warning("compaction of %s failed", node, exc_info=True)
            return None
        assert row is not None
        return row[0] or 0  # NULL with an err (the file is gone): nothing to do

    def _busy(self) -> bool:
        """Whether the pool served anything but this job since its previous pass (always, before the first)."""
        return self._requests is None or self._request_count() != self._requests

    def _request_count(self) -> int:
        return self.pool.get_stats().get("requests_num", 0)
