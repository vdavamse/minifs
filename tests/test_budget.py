"""Statement timeouts on every VFSClient path, and latency budgets (minifs.vfs.budget)."""

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs.db.pool import Connector, open_pool
from minifs.vfs.budget import budget, remaining
from minifs.vfs.client import VFSClient
from minifs.vfs.core import VFSError
from tests.otel import total

Pool = AsyncConnectionPool[AsyncConnection[Any]]


@pytest.fixture
async def pool(db: str) -> AsyncIterator[Pool]:
    async with open_pool(Connector(db), max_size=2) as pool:
        yield pool


async def _setting(vfs: VFSClient) -> int:
    """The statement_timeout, in milliseconds, of a statement the client runs."""
    query = "SELECT setting::int FROM pg_settings WHERE name = 'statement_timeout'"
    rows = await vfs._run("/", query, [])  # pyright: ignore[reportPrivateUsage]
    return rows[0][0]


async def test_every_statement_has_the_client_timeout(pool: Pool) -> None:
    assert await _setting(VFSClient(pool)) == 30_000
    assert await _setting(VFSClient(pool, timeout=1.5)) == 1500
    assert await _setting(VFSClient(pool, timeout=None)) == 0  # the server's default: none
    with budget(2.0):
        assert 0 < await _setting(VFSClient(pool)) <= 2000


async def test_a_statement_costs_three_round_trips(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    """BEGIN carries the timeout, so an operation is BEGIN, its statement and COMMIT, as with no timeout."""
    vfs = VFSClient(pool)
    await vfs.create("/f", b"x\n")
    sent: list[str] = []
    execute = AsyncConnection.execute

    async def recording(self: AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        if str(query).strip():  # not the pool's check
            sent.append(str(query))
        return await execute(self, query, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", recording)
    await vfs.stat("/f")
    assert len(sent) == 3 and sent[0] == "BEGIN; SET LOCAL statement_timeout = 30000" and sent[2] == "COMMIT", sent
    sent.clear()
    await vfs.glob("*")
    assert sent[0] == "BEGIN ISOLATION LEVEL REPEATABLE READ; SET LOCAL statement_timeout = 30000", sent
    sent.clear()
    with pytest.raises(VFSError):
        await vfs.create("/f")  # EEXIST, returned: still committed, nothing failed on the server
    assert sent[-1] == "COMMIT"


async def test_a_failed_statement_rolls_back_and_the_connection_is_reusable(pool: Pool) -> None:
    vfs = VFSClient(pool)
    with pytest.raises(psycopg.Error):
        await vfs._run("/", "SELECT 1/0", [])  # pyright: ignore[reportPrivateUsage]
    async with pool.connection() as conn:
        assert not conn.autocommit
    assert (await vfs.create("/after", b"ok\n")).size == 3


async def test_a_statement_past_its_timeout_is_etimedout(pool: Pool, db: str) -> None:
    vfs = VFSClient(pool, timeout=0.2)
    node = (await vfs.create("/log", b"a\n")).id
    before = total("minifs.timeouts", {"minifs.reason": "statement"})
    async with await AsyncConnection.connect(db) as holder, holder.transaction():
        await holder.execute("SELECT * FROM vfs_lock_file(%s)", (node,))  # an append now waits for the lock
        start = time.monotonic()
        with pytest.raises(VFSError) as error:
            await vfs.append("/log", b"b\n")
        assert time.monotonic() - start < 5
    assert error.value.errno == "ETIMEDOUT" and error.value.path == "/log"
    assert isinstance(error.value.db_error, psycopg.errors.QueryCanceled)
    assert total("minifs.timeouts", {"minifs.reason": "statement"}) == before + 1
    assert (await vfs.pread("/log")).content == b"a\n"  # nothing was appended


async def test_a_budget_bounds_the_statements_inside_it(pool: Pool, db: str) -> None:
    vfs = VFSClient(pool)  # 30 s a statement, but the budget is less
    node = (await vfs.create("/log", b"a\n")).id
    async with await AsyncConnection.connect(db) as holder, holder.transaction():
        await holder.execute("SELECT * FROM vfs_lock_file(%s)", (node,))
        start = time.monotonic()
        with budget(0.3), pytest.raises(VFSError) as error:
            await vfs.append("/log", b"b\n")
        assert time.monotonic() - start < 5
    assert error.value.errno == "ETIMEDOUT"


async def test_a_spent_budget_fails_before_the_statement(pool: Pool) -> None:
    vfs = VFSClient(pool)
    await vfs.create("/f", b"x\n")
    before = total("minifs.timeouts", {"minifs.reason": "budget"})
    with budget(0.01):
        await asyncio.sleep(0.02)
        for call in (vfs.stat("/f"), vfs.glob("*"), vfs.grep("x"), vfs.pread_lines("/f")):
            with pytest.raises(VFSError) as error:
                await call
            assert error.value.errno == "ETIMEDOUT", call
    assert total("minifs.timeouts", {"minifs.reason": "budget"}) == before + 4


async def test_budgets_nest_and_never_extend(pool: Pool) -> None:
    assert remaining(30.0, "/") == 30.0 and remaining(None, "/") is None
    with budget(1.0):
        with budget(60.0):
            left = remaining(None, "/")
            assert left is not None and left <= 1.0
        with budget(None):
            left = remaining(30.0, "/")
            assert left is not None and left <= 1.0
    for bad in (0, -1.0, 10**10):
        with pytest.raises(ValueError), budget(bad):
            pass


async def test_waiting_for_a_connection_counts_against_the_budget(db: str) -> None:
    async with open_pool(Connector(db), min_size=1, max_size=1) as pool:
        vfs = VFSClient(pool)
        before = total("minifs.timeouts", {"minifs.reason": "pool"})
        async with pool.connection():  # the only connection
            with budget(0.2), pytest.raises(VFSError) as error:
                await vfs.stat("/missing")
        assert error.value.errno == "ETIMEDOUT"
        assert total("minifs.timeouts", {"minifs.reason": "pool"}) == before + 1


def test_the_client_timeout_is_validated() -> None:
    for bad in (0, -1.0, 10**10):
        with pytest.raises(ValueError):
            VFSClient(object(), timeout=bad)  # pyright: ignore[reportArgumentType]
