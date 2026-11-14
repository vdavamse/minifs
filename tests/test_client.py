"""VFSClient over a real database, and the pools of minifs.db.pool: credentials, reconnects, retries."""

import argparse
import asyncio
import contextlib
import json
import pickle
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterator
from itertools import islice
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from minifs import cli
from minifs.db.errors import OUTCOMES, SQLSTATE_BY_ERRNO
from minifs.db.pool import Connector, Pools, open_pool, retry_serializable
from minifs.vfs.client import (
    DEFAULT_LOCK_TIMEOUT,
    DEFAULT_WRITE_TIMEOUT,
    MAX_TIMEOUT,
    VFSClient,
    _gated,  # pyright: ignore[reportPrivateUsage]
)
from minifs.vfs.core import FileVersion, Kind, ReadResult, VFSError
from minifs.vfs.glob import GlobError
from minifs.vfs.search import SearchRefused
from tests.test_tree import holding_the_lock, wait_until_blocked

Pool = AsyncConnectionPool[AsyncConnection[Any]]


@pytest.fixture
async def pool(db: str) -> AsyncIterator[Pool]:
    async with open_pool(Connector(db), max_size=3) as p:
        yield p


@pytest.fixture
def vfs(pool: Pool) -> VFSClient:
    return VFSClient(pool)


async def raises(errno: str, call: Awaitable[object], path: str | None = None) -> VFSError:
    with pytest.raises(VFSError) as info:
        await call
    assert info.value.errno == errno, info.value
    if path is not None:
        assert info.value.path == path
    return info.value


# Operations


async def test_create_stat_read(vfs: VFSClient) -> None:
    await vfs.mkdir("/docs")
    version = await vfs.create("/docs//./a.md", b"hello")
    assert (version.generation, version.size) == (0, 5)
    entry = await vfs.stat("/docs/a.md")
    assert (entry.path, entry.id, entry.kind, entry.size, entry.generation) == ("/docs/a.md", version.id, "file", 5, 0)
    assert entry.mtime is not None
    result = await vfs.pread("/docs/a.md")
    assert (result.content, result.offset, result.next_offset, result.size, result.eof) == (b"hello", 0, 5, 5, True)
    assert result.id == version.id
    assert entry.version == version
    root = await vfs.stat("/..")
    assert (root.path, root.id, root.kind) == ("/", None, "dir")


async def test_readdir(vfs: VFSClient) -> None:
    await vfs.mkdir_p("/w/sub")
    await vfs.create("/w/b", b"xy")
    await vfs.create("/w/A")
    entries = await vfs.readdir("/w/")
    expected = [("/w/A", "file", 0), ("/w/b", "file", 2), ("/w/sub", "dir", 0)]
    assert [(e.path, e.kind, e.size) for e in entries] == expected
    assert [e.path for e in await vfs.readdir("/")] == ["/w"]
    # Pages: the entries after a name, at most ``limit``.
    assert [e.path for e in await vfs.readdir("/w", limit=2)] == ["/w/A", "/w/b"]
    assert [e.path for e in await vfs.readdir("/w", after="A", limit=1)] == ["/w/b"]
    assert [e.path for e in await vfs.readdir("/w", after="b")] == ["/w/sub"]
    assert await vfs.readdir("/w", after="sub", limit=5) == []
    assert await vfs.readdir("/w", limit=0) == []
    assert await vfs.readdir("/", after="w") == []
    await raises("EINVAL", vfs.readdir("/w", limit=-1), "/w")
    await raises("EINVAL", vfs.readdir("/w", limit=2**31), "/w")
    await raises("EINVAL", vfs.readdir("/w", after="a\0"), "/w")
    await raises("ENOENT", vfs.readdir("/missing", after="a", limit=1), "/missing")


async def test_pread_ranges(vfs: VFSClient) -> None:
    await vfs.create("/f", b"0123")
    assert await vfs.append("/f", b"4567") == 4
    assert await vfs.append("/f", b"89") == 8
    r = await vfs.pread("/f", 3, 4)
    assert (r.content, r.offset, r.next_offset, r.eof) == (b"3456", 3, 7, False)
    r = await vfs.pread("/f", -3)
    assert (r.content, r.offset, r.eof) == (b"789", 7, True)
    r = await vfs.pread("/f", 50)
    assert (r.content, r.offset, r.next_offset, r.size, r.eof) == (b"", 10, 10, 10, True)
    r = await vfs.pread("/f", 2, 0)
    assert (r.content, r.offset, r.eof) == (b"", 2, False)
    await raises("EINVAL", vfs.pread("/f", 0, -1))
    await raises("EINVAL", vfs.pread("/f", 0, 2**31))
    await raises("EINVAL", vfs.pread("/f", 2**63))
    assert (await vfs.pread("/f", -(2**63))).offset == 0


async def test_pread_file_reads_every_page_on_one_snapshot(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every page is read on one connection, in one read-only REPEATABLE READ transaction: a file overwritten
    while the read is in flight still comes back whole, as the version the read started on, with no ESTALE."""
    vfs, other = VFSClient(pool), VFSClient(pool)
    data = b"0123456789" * 4  # 40 bytes: 10 pages of 4 bytes
    version = await vfs.create("/f", data)
    statements: list[tuple[str, int]] = []  # (the statement's text, the id of the connection that ran it)
    execute = AsyncConnection.execute
    overwritten = False

    async def counting(self: AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal overwritten
        text = str(query)
        if "vfs_pread" in text or "SET TRANSACTION" in text:
            statements.append((text, id(self)))
        result = await execute(self, query, *args, **kwargs)
        if "vfs_pread" in text and not overwritten:
            overwritten = True
            await other.write("/f", b"abcdefghij" * 4)  # during the read, on the other client's connection
        return result

    monkeypatch.setattr(AsyncConnection, "execute", counting)
    result = await vfs.pread_file("/f", 4)
    monkeypatch.undo()
    assert result.content == data  # the old content, not the overwrite's and no mix of the two
    assert (result.id, result.generation, result.size, result.eof) == (version.id, version.generation, 40, True)
    assert (await vfs.stat("/f")).generation == version.generation + 1  # the overwrite committed during the read
    assert len([s for s in statements if "vfs_pread" in s[0]]) == -(-len(data) // 4)  # ceil(size / page)
    assert len([s for s in statements if "SET TRANSACTION" in s[0]]) == 1
    assert len({conn for _, conn in statements}) == 1  # every statement on the same connection


async def test_pread_file_stops_after_a_page_over_max_size(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """A file over ``max_size`` is refused on its first page's size: the rest isn't read."""
    await vfs.create("/f", b"x" * 100)
    statements: list[str] = []
    execute = AsyncConnection.execute

    async def counting(self: AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        if "vfs_pread" in str(query):
            statements.append(str(query))
        return await execute(self, query, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", counting)
    result = await vfs.pread_file("/f", 10, max_size=50)
    assert (result.content, result.eof, result.size) == (b"x" * 10, False, 100)
    assert len(statements) == 1
    statements.clear()
    whole = await vfs.pread_file("/f", 10, max_size=100)
    assert (whole.content, whole.eof, whole.size) == (b"x" * 100, True, 100)
    assert len(statements) == 10


async def test_pread_file_outcomes_and_einval(pool: Pool) -> None:
    """The outcomes ``pread`` reports, and a fully cached stale path evicted, as ``pread_page`` checks it."""
    cache = DictCache()
    vfs, other = VFSClient(pool, cache=cache), VFSClient(pool)
    f = await vfs.create("/d/f", b"data", parents=True)
    await vfs.stat("/d/f")  # the whole path is cached
    assert any(v[0] == f.id for v in cache.entries.values())
    await other.unlink("/d/f")  # another client, invisible to this cache
    await raises("ENOENT", vfs.pread_file("/d/f", 4), "/d/f")
    assert all(v[0] != f.id for v in cache.entries.values())  # the stale link was evicted
    await raises("ENOENT", vfs.pread_file("/missing", 4), "/missing")
    await vfs.mkdir("/dir")
    await raises("EISDIR", vfs.pread_file("/dir", 4), "/dir")
    await raises("EISDIR", vfs.pread_file("/", 4), "/")
    await vfs.create("/f", b"data")
    await raises("EINVAL", vfs.pread_file("/f", 0), "/f")
    await raises("EINVAL", vfs.pread_file("/f", 2**31), "/f")
    await raises("EINVAL", vfs.pread_file("/f", 4, max_size=-1), "/f")
    await vfs.create("/empty", b"")
    result = await vfs.pread_file("/empty", 4)
    assert (result.content, result.eof, result.size) == (b"", True, 0)


async def test_pread_file_first_page_plan_stays_cheap(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """The gated first page's statement — the lookup part, which resolves the path around the call — stays under
    ``jit_above_cost`` and sorts nothing. An EXPLAIN of the whole call sees only a Function Scan of constant cost
    (plpgsql isn't inlined), so the body's own queries are checked in ``test_pread_file_page_queries_use_the_index``."""
    await vfs.create("/a/b/f", b"0123456789" * 4, parents=True)
    calls: list[tuple[str, Any]] = []
    execute = AsyncConnection.execute

    async def capturing(self: AsyncConnection[Any], query: Any, params: Any = None, *args: Any, **kwargs: Any) -> Any:
        if "vfs_pread" in str(query) and "vfs_gate" in str(query):
            calls.append((str(query), params))
        return await execute(self, query, params, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", capturing)
    await vfs.pread_file("/a/b/f", 4)
    monkeypatch.undo()
    assert len(calls) == 1, calls  # the gated first page alone; the later pages are plain function calls

    def walk(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield node
        for child in node.get("Plans", []):
            yield from walk(child)

    async with vfs.pool.connection() as conn:
        for query, params in calls:
            cur = await conn.execute(cast(Any, "EXPLAIN (FORMAT JSON) " + query), params)
            row = await cur.fetchone()
            assert row is not None
            plan = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            cost = float(plan[0]["Plan"]["Total Cost"])
            assert cost < 100_000, f"estimated cost {cost} passes jit_above_cost, of: {query[:70]}"
            for node in walk(plan[0]["Plan"]):
                assert node.get("Node Type") != "Sort", f"a Sort in the plan, of: {query[:70]}"


async def test_pread_file_page_queries_use_the_index(vfs: VFSClient) -> None:
    """Inside ``vfs_pread``, one index serves both the filter and the order of each of the body's two queries — the
    descent to the extent that contains the offset, and the forward range scan — so a page over a file of many
    extents neither sorts nor scans the table, and both plans stay under ``jit_above_cost``. An EXPLAIN of the whole
    call can't show that (plpgsql isn't inlined into its plan), so these EXPLAIN the body's queries as the planner
    sees them."""
    version = await vfs.create("/f", b"line 0\n")
    for i in range(1, 8):
        await vfs.append("/f", f"line {i}\n".encode())  # every append is one more extent
    async with vfs.pool.connection() as conn:
        cur = await conn.execute("SELECT generation, size FROM vfs_nodes WHERE id = %s", [version.id])
        node_row = await cur.fetchone()
        assert node_row is not None
        generation, size = node_row
        offset, limit = size // 2, 16
        # The two queries of ``vfs_pread``'s body; the SQL text must match migration 009's ``vfs_pread``.
        # Step 1: the extent containing the offset.
        step1 = (
            f"SELECT e.byte_offset FROM vfs_extents e"
            f" WHERE e.node_id = '{version.id}'::uuid AND e.generation = {generation} AND e.byte_offset <= {offset}"
            f" ORDER BY e.byte_offset DESC LIMIT 1"
        )
        cur = await conn.execute(cast(Any, step1))
        start_row = await cur.fetchone()
        assert start_row is not None
        start = start_row[0]
        # Step 2: the range scan forward from there.
        step2 = (
            f"SELECT {size}, {generation}, greatest(e.byte_offset, {offset}),"
            f"       substring(e.data"
            f"                 FROM (greatest({offset} - e.byte_offset, 0) + 1)::int"
            f"                 FOR (least(e.byte_offset + length(e.data), {offset} + {limit})"
            f"                      - greatest(e.byte_offset, {offset}))::int),"
            f"       NULL::text, NULL::text"
            f"  FROM vfs_extents e"
            f" WHERE e.node_id = '{version.id}'::uuid AND e.generation = {generation}"
            f"   AND e.byte_offset >= {start}"
            f"   AND e.byte_offset < {offset} + {limit}"
            f"   AND e.byte_offset + length(e.data) > {offset}"
            f"   AND {limit} > 0"
            f" ORDER BY e.byte_offset"
        )

        def walk(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
            yield node
            for child in node.get("Plans", []):
                yield from walk(child)

        async with conn.transaction():
            # The table is tiny, so without this the planner may seq-scan it and sort, whatever indexes exist.
            await conn.execute("SET LOCAL enable_seqscan = off")
            for query in (step1, step2):
                cur = await conn.execute(cast(Any, "EXPLAIN (FORMAT JSON) " + query))
                row = await cur.fetchone()
                assert row is not None
                plan = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                cost = float(plan[0]["Plan"]["Total Cost"])
                assert cost < 100_000, f"estimated cost {cost} passes jit_above_cost, of: {query[:70]}"
                nodes = list(islice(walk(plan[0]["Plan"]), 100))  # a plan tree is a handful of nodes
                assert not [n for n in nodes if n.get("Node Type") == "Sort"], f"a Sort in the plan, of: {query[:70]}"
                assert any(
                    "Index" in str(n.get("Node Type", "")) and n.get("Relation Name") == "vfs_extents" for n in nodes
                ), f"no index on vfs_extents serves the plan, of: {query[:70]}"


async def test_pread_file_holds_one_connection(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """A read of many pages checks one connection out of the pool, once, and holds it for the whole read (the
    page-per-pread reads before #124 checked one out per page)."""
    await vfs.create("/f", b"0123456789" * 10)  # 100 bytes: 25 pages of 4 bytes
    in_flight = peak = checkouts = 0
    connection = vfs.pool.connection

    @contextlib.asynccontextmanager
    async def counted(*args: Any, **kwargs: Any) -> AsyncGenerator[AsyncConnection[Any]]:
        nonlocal in_flight, peak, checkouts
        checkouts += 1
        in_flight += 1
        peak = max(peak, in_flight)
        try:
            async with connection(*args, **kwargs) as conn:
                yield conn
        finally:
            in_flight -= 1

    monkeypatch.setattr(vfs.pool, "connection", counted)
    result = await vfs.pread_file("/f", 4)
    assert result.content == b"0123456789" * 10
    assert (checkouts, peak) == (1, 1)


async def test_pread_file_idle_between_pages_is_etimedout(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """A gap between two pages that outlives the server's ``idle_in_transaction_session_timeout`` (25P03) is
    ETIMEDOUT: the server closes the connection, the pool discards it, and a later read of the same file succeeds."""
    await vfs.create("/f", b"0123456789")  # 10 bytes: 3 pages of 4
    pages = 0
    execute = AsyncConnection.execute

    async def idling(self: AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal pages
        if "vfs_pread" in str(query):
            pages += 1
            if pages == 2:  # just before the 2nd page: bound the idle time, then idle past it
                await execute(self, "SET LOCAL idle_in_transaction_session_timeout = '100ms'")
                await asyncio.sleep(0.5)
        return await execute(self, query, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", idling)
    error = await raises("ETIMEDOUT", vfs.pread_file("/f", 4), "/f")
    monkeypatch.undo()
    assert error.db_error is not None and error.db_error.sqlstate == "25P03", error.db_error
    assert pages == 2
    assert (await vfs.pread_file("/f", 4)).content == b"0123456789"  # the pool replaced the killed connection


async def test_pread_file_retries_a_serialization_failure_after_more_ran(
    vfs: VFSClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A serialization failure (40001) on a later page retries the whole read, from the first page
    (``retry_serializable``): the pages ``more`` had fetched are dropped with the failed attempt, so the retried
    read returns the file whole, with no duplicated bytes."""
    data = b"0123456789" * 4  # 40 bytes: 10 pages of 4
    await vfs.create("/f", data)
    preads = 0
    execute = AsyncConnection.execute

    async def failing(self: AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal preads
        if "vfs_pread" in str(query):
            preads += 1
            if preads == 3:
                raise psycopg.errors.SerializationFailure("could not serialize access due to concurrent update")
        return await execute(self, query, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", failing)
    result = await vfs.pread_file("/f", 4)
    assert result.content == data  # the retried read's pages alone, not the failed attempt's first two again
    assert (result.eof, result.size) == (True, 40)
    assert preads == 3 + 10  # the failed attempt's 3 statements, then the retried read's 10


async def test_a_snapshot_refuses_a_write(vfs: VFSClient) -> None:
    """A ``snapshot`` transaction is READ ONLY, so ``_run`` refuses to run a write in one."""
    with pytest.raises(TypeError, match="read-only"):
        await vfs._run("/x", "SELECT 1", [], snapshot=True, write=True)  # pyright: ignore[reportPrivateUsage]


async def test_pread_lines(vfs: VFSClient) -> None:
    version = await vfs.create("/f", b"one\ntwo\n")
    await vfs.append("/f", b"three\nfour")
    r = await vfs.pread_lines("/f", 2, 2)
    assert (r.content, r.first_line, r.offset, r.next_offset, r.lines, r.eof) == (b"two\nthree\n", 2, 4, 14, 4, False)
    assert (r.id, r.size, r.generation) == (version.id, 18, 0)
    r = await vfs.pread_lines("/f")
    assert (r.content, r.eof) == (b"one\ntwo\nthree\nfour", True)
    r = await vfs.pread_lines("/f", 1, 10, 6)
    assert (r.content, r.next_offset, r.eof) == (b"one\ntw", 6, False)
    r = await vfs.pread_lines("/f", 9)
    assert (r.content, r.first_line, r.offset, r.lines, r.eof) == (b"", 9, 18, 4, True)
    await raises("EINVAL", vfs.pread_lines("/f", 0))
    await raises("EINVAL", vfs.pread_lines("/f", 1, -1))
    await raises("EINVAL", vfs.pread_lines("/f", 1, 1, 2**31))
    await raises("EISDIR", vfs.pread_lines("/"))
    await vfs.mkdir("/d")
    await raises("EISDIR", vfs.pread_lines("/d"), "/d")
    await raises("ENOENT", vfs.pread_lines("/missing"), "/missing")


async def test_pread_lines_with_a_line_cap(vfs: VFSClient) -> None:
    await vfs.create("/f", b"one\nthree\r\nfour")
    r = await vfs.pread_lines("/f", max_line_bytes=3)
    assert (r.content, r.next_offset, r.cut, r.eof) == (b"one\nthr\nfou\n", 15, frozenset({2, 3}), True)
    r = await vfs.pread_lines("/f", 1, 10, 6, max_line_bytes=3)
    assert (r.content, r.next_offset, r.cut, r.eof) == (b"one\nth", 6, frozenset({2}), False)
    assert (await vfs.pread_lines("/f")).cut == frozenset()
    await raises("EINVAL", vfs.pread_lines("/f", max_line_bytes=-1))
    await raises("EINVAL", vfs.pread_lines("/f", max_line_bytes=2**31))


async def test_pread_many(vfs: VFSClient) -> None:
    await vfs.create("/a/f", b"hello", parents=True)
    await vfs.create("/a/g", b"world")
    f, g, missing, through, directory, root, tail = await vfs.pread_many(
        [
            ("/a/f", 0, 100),
            ("/a//g/", 1, 2),
            ("/a/x", 0, 1),
            ("/a/f/x", 0, 1),
            ("/a", 0, 1),
            ("/", 0, 1),
            ("/a/f", -2, 0),
        ]
    )
    assert isinstance(f, ReadResult) and (f.content, f.offset, f.size, f.eof) == (b"hello", 0, 5, True)
    assert isinstance(g, ReadResult) and (g.content, g.offset, g.next_offset, g.eof) == (b"or", 1, 3, False)
    assert isinstance(tail, ReadResult) and (tail.content, tail.offset, tail.id) == (b"", 3, f.id)
    for error, errno, path in [
        (missing, "ENOENT", "/a/x"),
        (through, "ENOTDIR", "/a/f/x"),
        (directory, "EISDIR", "/a"),
        (root, "EISDIR", "/"),
    ]:
        assert isinstance(error, VFSError) and (error.errno, error.path) == (errno, path)
    assert await vfs.pread_many([]) == []
    # Groups: only up to the first file of each group is read; a directory, or a missing path, doesn't count.
    await vfs.mkdir("/a/d")
    reads = [("/a/x", 0, 9), ("/a/f", 0, 9), ("/a/g", 0, 9), ("/a/d", 0, 9), ("/a/g", 0, 9), ("/a/f", 0, 9)]
    found = await vfs.pread_many(reads, groups=[0, 0, 0, 1, 1, 2])
    assert [r.errno if isinstance(r, VFSError) else r.content for r in found] == [
        "ENOENT",
        b"hello",
        b"",
        "EISDIR",
        b"world",
        b"hello",
    ]
    with pytest.raises(ValueError):
        await vfs.pread_many(reads, groups=[0])
    await raises("EINVAL", vfs.pread_many([("/a/f", 0, -1)]))
    await raises("EINVAL", vfs.pread_many([("/a/f", 2**63, 1)]))


async def test_pread_page(vfs: VFSClient) -> None:
    await vfs.create("/a/f", b"one\ntwo\nthree\n", parents=True)
    await vfs.create("/a/AGENTS.md", b"agents")
    await vfs.create("/a/CLAUDE.md", b"claude")
    page = await vfs.pread_page(
        "/a//f/",
        4096,
        2,
        2,
        1000,
        also=[("/a/AGENTS.md", 0, 100), ("/a/CLAUDE.md", 0, 100), ("/a/x", 0, 100)],
        groups=[0, 0, 1],
    )
    assert page.sample == await vfs.pread("/a/f", 0, 4096)
    assert page.lines == await vfs.pread_lines("/a/f", 2, 2, 1000)
    assert (page.lines.id, page.lines.generation) == (page.sample.id, page.sample.generation)
    agents, claude, missing = page.also
    assert isinstance(agents, ReadResult) and agents.content == b"agents"
    assert isinstance(claude, ReadResult) and claude.content == b""  # behind AGENTS.md: read with limit 0
    assert isinstance(missing, VFSError) and (missing.errno, missing.path) == ("ENOENT", "/a/x")
    # No also reads: the file's sample and page alone.
    page = await vfs.pread_page("/a/f", 3, 1, 1, 1000)
    assert (page.sample.content, page.lines.content, page.also) == (b"one", b"one\n", [])


async def test_pread_page_outcomes_raise(vfs: VFSClient) -> None:
    """The file's outcome raises, as pread's would; the other reads' stay in place (``test_pread_page``)."""
    await vfs.create("/a/f", b"one\ntwo\n", parents=True)
    await vfs.mkdir("/a/d")
    await raises("ENOENT", vfs.pread_page("/a/x", 4096, 1, 10, 1000), "/a/x")
    await raises("EISDIR", vfs.pread_page("/a/d", 4096, 1, 10, 1000), "/a/d")
    await raises("EISDIR", vfs.pread_page("/", 4096, 1, 10, 1000), "/")
    await raises("ENOTDIR", vfs.pread_page("/a/f/x", 4096, 1, 10, 1000), "/a/f/x")
    await raises("EINVAL", vfs.pread_page("/a/f", -1, 1, 10, 1000), "/a/f")
    await raises("EINVAL", vfs.pread_page("/a/f", 4096, 0, 10, 1000), "/a/f")
    await raises("EINVAL", vfs.pread_page("/a/f", 4096, 1, -1, 1000), "/a/f")
    await raises("EINVAL", vfs.pread_page("/a/f", 4096, 1, 10, 1000, also=[("/a/f", 0, -1)]), "/a/f")
    with pytest.raises(ValueError):
        await vfs.pread_page("/a/f", 4096, 1, 10, 1000, also=[("/a/f", 0, 1), ("/a/d", 0, 1)], groups=[0])


async def test_pread_page_of_a_file_of_several_extents(vfs: VFSClient) -> None:
    """The page isn't repeated or cut per extent row: the sample and the batch equal the separate calls'."""
    await vfs.create("/f", b"one\ntwo\n")
    for i in range(8):
        await vfs.append("/f", b"line %d\n" % i)
    page = await vfs.pread_page("/f", 4096, 1, 20, 1000)
    assert page.sample == await vfs.pread("/f", 0, 4096)
    assert page.lines == await vfs.pread_lines("/f", 1, 20, 1000)
    assert (page.sample.eof, page.lines.eof) == (True, True)


async def test_pread_page_is_one_statement(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    await vfs.create("/a/f", b"one\n", parents=True)
    await vfs.create("/a/AGENTS.md", b"rules")
    statements: list[str] = []
    execute = AsyncConnection.execute

    async def counting(self: AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        if "vfs_" in str(query):
            statements.append(str(query))
        return await execute(self, query, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", counting)
    await vfs.pread_page("/a/f", 4096, 1, 10, 1000, also=[("/a/AGENTS.md", 0, 100), ("/a/x", 0, 1)])
    assert len(statements) == 1, statements


async def test_pread_page_plan_stays_cheap(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """The plan's estimate stays under ``jit_above_cost``: with OFFSET 0 the planner put ``vfs_pread_lines`` (no
    ROWS) at 1000 rows, and every Read JIT-compiled. LIMIT 1 keeps the one-time filter, so the function still
    runs once per statement, not once per extent row of the file. The cost grows by about 350 per instruction
    candidate (``vfs_pread``'s own 1000-row estimate), so it passes ``jit_above_cost`` only at about 140
    directories deep."""
    await vfs.create("/a/b/f", b"one\ntwo\n", parents=True)
    for i in range(8):
        await vfs.append("/a/b/f", b"line %d\n" % i)  # several extents: read 1 has several rows
    await vfs.create("/a/AGENTS.md", b"rules")
    calls: list[tuple[str, Any]] = []
    execute = AsyncConnection.execute

    async def capturing(self: AsyncConnection[Any], query: Any, params: Any = None, *args: Any, **kwargs: Any) -> Any:
        if "vfs_pread_lines" in str(query):
            calls.append((str(query), params))
        return await execute(self, query, params, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", capturing)
    await vfs.pread_page(
        "/a/b/f",
        4096,
        1,
        1000,
        100000,
        also=[
            ("/a/b/AGENTS.md", 0, 100),
            ("/a/b/CLAUDE.md", 0, 100),
            ("/a/AGENTS.md", 0, 100),
            ("/a/CLAUDE.md", 0, 100),
            ("/x/AGENTS.md", 0, 1),
            ("/x/CLAUDE.md", 0, 1),
        ],
        groups=[0, 0, 1, 1, 2, 2],
    )
    monkeypatch.undo()
    assert len(calls) == 1, calls
    query, params = calls[0]
    async with vfs.pool.connection() as conn:
        cur = await conn.execute(cast(Any, "EXPLAIN (FORMAT JSON) " + query), params)
        row = await cur.fetchone()
        assert row is not None
        plan = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        cost = float(plan[0]["Plan"]["Total Cost"])
        assert cost < 100_000, f"estimated cost {cost} passes jit_above_cost: every Read would JIT-compile"
        async with conn.transaction():
            # Refused for a non-superuser, which fails the test rather than skip it silently.
            await conn.execute("SET LOCAL track_functions = 'pl'")
            cur = await conn.execute(cast(Any, query), params)
            await cur.fetchall()
            cur = await conn.execute("SELECT calls FROM pg_stat_xact_user_functions WHERE funcname = 'vfs_pread_lines'")
            row = await cur.fetchone()
        assert row is not None, "vfs_pread_lines was not tracked: SET track_functions must have been refused"
        assert row[0] == 1, f"vfs_pread_lines ran {row[0]} times, not once"


async def test_pread_page_sorts_the_page_once(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """The page rides the window's sort once, not once per extent row of the file: ``l`` is attached after the
    window that numbers read 1's rows, so a file built by many small appends doesn't spill the ~208 KB page to
    temp-disk sort once per extent. Under ``work_mem = '4MB'`` the old plan (``l`` below the window) spilled."""
    f = await vfs.create("/a/f", b"", parents=True)
    for i in range(400):
        await vfs.append("/a/f", b"line %04d\n" % i)  # an extent per append: 400 of them in the 4 KB sample
    await vfs.append("/a/f", b"".join(b"x" * 99 + b"\n" for _ in range(2000)))  # and a 200 KB tail
    async with vfs.pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM vfs_extents WHERE node_id = %s AND byte_offset < 4096", [f.id])
        row = await cur.fetchone()
    assert row is not None and row[0] > 300, row  # the layout the test needs: appends don't coalesce
    calls: list[tuple[str, Any]] = []
    execute = AsyncConnection.execute

    async def capturing(self: AsyncConnection[Any], query: Any, params: Any = None, *args: Any, **kwargs: Any) -> Any:
        if "vfs_pread_lines" in str(query):
            calls.append((str(query), params))
        return await execute(self, query, params, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", capturing)
    page = await vfs.pread_page("/a/f", 4096, 1, 2400, 400000)
    monkeypatch.undo()
    assert page.lines.lines == 2400 and len(page.lines.content) == page.lines.size  # the whole file came back
    assert len(calls) == 1, calls
    query, params = calls[0]

    def walk(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
        yield node
        for child in node.get("Plans", []):
            yield from walk(child)

    async with vfs.pool.connection() as conn, conn.transaction():
        await conn.execute("SET LOCAL work_mem = '4MB'")
        cur = await conn.execute(cast(Any, "EXPLAIN (ANALYZE, FORMAT JSON) " + query), params)
        row = await cur.fetchone()
        assert row is not None
        plan = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        for node in walk(plan[0]["Plan"]):
            assert node.get("Sort Space Type") != "Disk", (
                f"{node.get('Node Type')} spilled {node.get('Sort Space Used')} kB of sort to temp disk"
            )


async def test_pread_page_group_ids_never_reach_the_files_group(vfs: VFSClient) -> None:
    """A caller's group id is remapped to a dense id from 1: -1 shifted by one would land in the file's group 0
    and shadow the also read, and bigint's max shifted by one would overflow."""
    await vfs.create("/a/f", b"one\ntwo\n", parents=True)
    await vfs.create("/a/AGENTS.md", b"rules")
    for groups in ([-1], [2**63 - 1]):
        page = await vfs.pread_page("/a/f", 4096, 1, 10, 1000, also=[("/a/AGENTS.md", 0, 100)], groups=groups)
        result = page.also[0]
        assert isinstance(result, ReadResult), result
        assert result.content == b"rules", result


async def test_pread_page_evicts_a_stale_cached_path(pool: Pool) -> None:
    """A file found gone evicts its cached links, as ``pread``'s ``_outcome`` does (the ``DictCache`` pattern of
    ``test_stale_cached_node_is_enoent``): the statement resolves without the cache, so nothing else drops them."""
    cache = DictCache()
    vfs, other = VFSClient(pool, cache=cache), VFSClient(pool)
    f = await vfs.create("/d/f", b"data", parents=True)
    await vfs.stat("/d/f")  # the whole path is cached
    assert any(v[0] == f.id for v in cache.entries.values())
    await other.unlink("/d/f")  # another client, invisible to this cache
    await raises("ENOENT", vfs.pread_page("/d/f", 4096, 1, 10, 1000), "/d/f")
    assert all(v[0] != f.id for v in cache.entries.values())  # the stale link was evicted


async def test_write_and_compare_and_swap(vfs: VFSClient) -> None:
    created = await vfs.create("/n.md", b"v0")
    written = await vfs.write("/n.md", b"v1!")
    assert written == FileVersion(created.id, 1, 3)
    again = await vfs.write("/n.md", b"v2", expect=written)
    assert again == FileVersion(created.id, 2, 2)
    await raises("ESTALE", vfs.write("/n.md", b"lost", expect=written), "/n.md")
    # The expected node must still be at the path: renamed away, the write is stale.
    await vfs.rename("/n.md", "/m.md")
    await raises("ESTALE", vfs.write("/n.md", b"x", expect=again))
    assert (await vfs.write("/m.md", b"ok", expect=again)).generation == 3
    assert (await vfs.pread("/m.md")).content == b"ok"


async def test_write_compare_and_swap_in_subdirectory(vfs: VFSClient) -> None:
    version = await vfs.create("/a/b/c.txt", b"1", parents=True)
    assert (await vfs.write("/a/b/c.txt", b"2", expect=version)).generation == 1
    await vfs.mkdir("/z")
    await vfs.rename("/a/b/c.txt", "/z/c.txt")
    await raises("ESTALE", vfs.write("/z/c.txt", b"3", expect=version))


async def test_create_with_parents(vfs: VFSClient) -> None:
    await vfs.create("/x/y/z.txt", b"data", parents=True)
    assert (await vfs.stat("/x/y")).kind == "dir"
    await vfs.mkdir("/x/q/r", parents=True)
    await raises("EEXIST", vfs.mkdir("/x/q/r", parents=True), "/x/q/r")
    assert await vfs.mkdir_p("/x/q/r") == (await vfs.stat("/x/q/r")).id
    assert await vfs.mkdir_p("/") is None
    await raises("ENOENT", vfs.create("/nope/f"), "/nope/f")
    await vfs.create("/file")
    await raises("ENOTDIR", vfs.create("/file/x", b"y", parents=True), "/file/x")
    await raises("ENOTDIR", vfs.mkdir("/file/x", parents=True), "/file/x")
    await raises("ENOTDIR", vfs.create("/file/x/y", parents=True), "/file/x/y")
    await raises("EEXIST", vfs.create("/x/y/z.txt", parents=True), "/x/y/z.txt")


async def test_rename_and_claim(vfs: VFSClient) -> None:
    await vfs.mkdir_p("/tasks/new")
    await vfs.mkdir_p("/tasks/cur")
    await vfs.create("/tasks/new/t1")
    results = await asyncio.gather(
        *(vfs.claim("/tasks/new/t1", f"/tasks/cur/t1-{i}") for i in range(4)), return_exceptions=True
    )
    # One claim moves it; the others lose, whether they find the source already gone or lose the compare-and-swap.
    assert sorted(results, key=str) == [False, False, False, True]
    assert [e.name for e in await vfs.readdir("/tasks/cur")][0].startswith("t1-")
    await raises("EINVAL", vfs.claim("/tasks", "/t"))
    await raises("ENOENT", vfs.claim("/tasks/gone/t1", "/tasks/cur/x"), "/tasks/gone")
    # A source that is gone is a lost race only if the destination could have taken it.
    await raises("ENOENT", vfs.claim("/tasks/new/t1", "/nope/t1"), "/nope/t1")
    await raises("ENAMETOOLONG", vfs.claim("/tasks/new/t1", "/tasks/cur/" + "x" * 256))
    await vfs.create("/tasks/file")
    await raises("ENOTDIR", vfs.claim("/tasks/new/t1", "/tasks/file/t1"), "/tasks/file/t1")
    await vfs.rename("/tasks", "/jobs")
    assert [e.path for e in await vfs.readdir("/")] == ["/jobs"]
    await raises("EEXIST", vfs.rename("/jobs/new", "/jobs/cur"), "/jobs/cur")
    await raises("EINVAL", vfs.rename("/", "/x"))
    await raises("EEXIST", vfs.rename("/jobs", "/"))


async def test_unlink_and_compact(vfs: VFSClient) -> None:
    await vfs.create("/log")
    for i in range(10):
        await vfs.append("/log", b"line %d\n" % i)
    assert await vfs.compact("/log") == 9
    assert (await vfs.pread("/log", 0, 7)).content == b"line 0\n"
    await vfs.unlink("/log")
    await raises("ENOENT", vfs.stat("/log"), "/log")
    await raises("EINVAL", vfs.unlink("/"))
    await vfs.create("/g", b"x")
    await raises("EINVAL", vfs.compact("/g", 0))
    await raises("EINVAL", vfs.compact("/g", 2**31))


async def test_root_is_a_directory(vfs: VFSClient) -> None:
    for call in (vfs.pread("/"), vfs.append("/", b"x"), vfs.write("/", b"x"), vfs.compact("/")):
        await raises("EISDIR", call, "/")
    await raises("EEXIST", vfs.create("/"))


async def test_rename_errors_name_the_path_they_concern(vfs: VFSClient) -> None:
    await vfs.create("/a.txt")
    await vfs.create("/n.md")
    await raises("ENOENT", vfs.rename("/a.txt", "/missing/b"), "/missing/b")
    await raises("ENOTDIR", vfs.rename("/a.txt", "/n.md/b"), "/n.md/b")
    await raises("ENOENT", vfs.rename("/nope", "/b"), "/nope")
    await raises("ENOTDIR", vfs.rename("/n.md/x", "/b"), "/n.md/x")


# Every errno the SQL layer reports comes back as a VFSError with that errno and the normalised path: the outcomes
# it returns (migration 009) with no database error, the faults it raises with theirs.


async def _enoent(vfs: VFSClient) -> Awaitable[object]:
    return vfs.stat("/missing")


async def _eexist(vfs: VFSClient) -> Awaitable[object]:
    await vfs.create("/f")
    return vfs.create("/f")


async def _enotdir(vfs: VFSClient) -> Awaitable[object]:
    await vfs.create("/f")
    return vfs.stat("/f/x")


async def _eisdir(vfs: VFSClient) -> Awaitable[object]:
    await vfs.mkdir("/d")
    return vfs.append("/d", b"x")


async def _einval(vfs: VFSClient) -> Awaitable[object]:
    await vfs.mkdir_p("/d/sub")
    return vfs.rename("/d", "/d/sub/d")


async def _enametoolong(vfs: VFSClient) -> Awaitable[object]:
    return vfs.create("/" + "a" * 256)


async def _enotempty(vfs: VFSClient) -> Awaitable[object]:
    await vfs.create("/d/f", parents=True)
    return vfs.unlink("/d")


async def _estale(vfs: VFSClient) -> Awaitable[object]:
    version = await vfs.create("/f", b"1")
    await vfs.append("/f", b"2")
    return vfs.write("/f", b"3", expect=version)


ERRNO_CASES: dict[str, tuple[Callable[[VFSClient], Awaitable[Awaitable[object]]], str]] = {
    "ENOENT": (_enoent, "/missing"),
    "EEXIST": (_eexist, "/f"),
    "ENOTDIR": (_enotdir, "/f/x"),
    "EISDIR": (_eisdir, "/d"),
    "EINVAL": (_einval, "/d"),
    "ENAMETOOLONG": (_enametoolong, "/" + "a" * 256),
    "ENOTEMPTY": (_enotempty, "/d"),
    "ESTALE": (_estale, "/f"),
}


def test_errno_cases_cover_the_sql_layer() -> None:
    assert set(ERRNO_CASES) == set(SQLSTATE_BY_ERRNO)


@pytest.mark.parametrize("errno", sorted(ERRNO_CASES))
async def test_errno_round_trip(vfs: VFSClient, errno: str) -> None:
    setup, path = ERRNO_CASES[errno]
    error = await raises(errno, await setup(vfs), path)
    if errno in OUTCOMES:
        assert error.db_error is None
        assert error.__cause__ is None and error.__context__ is None
    else:
        assert isinstance(error.db_error, psycopg.Error)
        assert error.db_error.sqlstate == SQLSTATE_BY_ERRNO[errno]
        assert error.__cause__ is None and error.__suppress_context__  # no DETAIL in tracebacks
        assert error.at("/x").db_error is error.db_error  # re-reported for another path, with its database error
    copy = pickle.loads(pickle.dumps(error))
    assert (copy.errno, copy.path, copy.detail, str(copy)) == (error.errno, error.path, error.detail, str(error))
    assert error.detail  # the server's DETAIL is kept for logs


async def test_other_database_errors_propagate(db: str) -> None:
    async with open_pool(Connector(db)) as p:
        async with p.connection() as conn:
            await conn.execute("DROP FUNCTION vfs_unlink(uuid)")
        await VFSClient(p).create("/f")
        with pytest.raises(psycopg.errors.UndefinedFunction):
            await VFSClient(p).unlink("/f")


# The directory-entry cache hook


class DictCache:
    def __init__(self) -> None:
        self.entries: dict[tuple[UUID | None, str], tuple[UUID, Kind]] = {}
        self.lookups = 0

    def lookup(self, parent: UUID | None, name: str) -> tuple[UUID, Kind] | None:
        self.lookups += 1
        return self.entries.get((parent, name))

    def store(self, parent: UUID | None, name: str, node: UUID, kind: Kind) -> None:
        self.entries[(parent, name)] = (node, kind)

    def evict(self, node: UUID) -> None:
        self.entries = {k: v for k, v in self.entries.items() if v[0] != node}


async def test_cache_hook(pool: Pool) -> None:
    cache = DictCache()
    vfs = VFSClient(pool, cache=cache)
    d = await vfs.mkdir("/d")
    f = await vfs.create("/d/f", b"abc")
    assert cache.entries == {(None, "d"): (d, "dir"), (d, "f"): (f.id, "file")}
    # Fully cached paths are passed by id; the results are the same.
    assert (await vfs.stat("/d/f")).id == f.id
    assert (await vfs.pread("/d/f")).content == b"abc"
    assert (await vfs.write("/d/f", b"xyz", expect=f)).generation == 1
    await vfs.rename("/d/f", "/d/g")
    assert (d, "f") not in cache.entries
    assert [e.name for e in await vfs.readdir("/d")] == ["g"]
    assert (d, "g") in cache.entries
    await vfs.unlink("/d/g")
    assert cache.entries == {(None, "d"): (d, "dir")}


async def test_stale_cache_never_reaches_the_wrong_node(pool: Pool) -> None:
    cache = DictCache()
    vfs, other = VFSClient(pool, cache=cache), VFSClient(pool)
    await vfs.mkdir("/d")
    x = await vfs.create("/d/x", b"x")
    await other.rename("/d/x", "/d/y")  # invisible to the cache
    await raises("ENOENT", vfs.write("/d/x", b"CLOBBER"), "/d/x")
    assert (await other.pread("/d/y")).content == b"x"
    assert all(v[0] != x.id for v in cache.entries.values())  # the failed chain was evicted
    # Recreated at the old path: the cache resolves to the new node, not the old one.
    await other.create("/d/x", b"new")
    cache.store((await vfs.stat("/d")).id, "x", x.id, "file")  # stale again
    assert (await vfs.pread("/d/x")).content == b"new"
    # A moved ancestor invalidates the whole chain.
    await vfs.readdir("/d")
    await other.rename("/d", "/e")
    await other.mkdir("/d")
    await other.create("/d/x", b"other")
    assert (await vfs.pread("/d/x")).content == b"other"
    # The stale chain is gone: the fallback's walk replaced it, and nothing cached points at the moved directory
    # under its old name.
    await vfs.settle()
    d = (await other.stat("/d")).id
    assert all(node == d for (_, name), (node, _) in cache.entries.items() if name == "d")
    assert (await vfs.claim("/d/x", "/e/z")) is True
    assert (await other.pread("/e/z")).content == b"other"


async def test_eviction_without_a_fully_cached_path(pool: Pool) -> None:
    cache = DictCache()
    vfs = VFSClient(pool, cache=cache)
    d = await vfs.mkdir("/d")
    f = await vfs.create("/d/f")
    await vfs.rename("/d", "/e")  # d is cached under its new name only; its entries are keyed by its id, so kept
    assert cache.entries == {(d, "f"): (f.id, "file"), (None, "e"): (d, "dir")}
    cache.entries.pop((None, "e"))
    await vfs.unlink("/e/f")  # resolved on the server, still evicted; the walk taught the cache /e
    assert cache.entries == {(None, "e"): (d, "dir")}


async def test_uncached_readdir_fills_the_cache(pool: Pool) -> None:
    await VFSClient(pool).create("/a/b/c", parents=True)
    cache = DictCache()
    vfs = VFSClient(pool, cache=cache)
    await vfs.readdir("/")
    await vfs.readdir("/a")
    await vfs.readdir("/a/b")
    assert [name for _, name in cache.entries] == ["a", "b", "c"]
    assert (await vfs.stat("/a/b/c")).kind == "file"


async def test_a_listing_returns_the_walk_once(pool: Pool) -> None:
    for name in "abc":
        await VFSClient(pool).create(f"/d/{name}", parents=True)
    vfs = VFSClient(pool, cache=DictCache())
    ref = vfs._ref(("d",))  # pyright: ignore[reportPrivateUsage]
    async with pool.connection() as conn:
        cur = await conn.execute(_gated([ref], "vfs_readdir({0})"), ref.params)  # pyright: ignore[reportPrivateUsage, reportArgumentType]
        rows = await cur.fetchall()
    assert len(rows) == 3 and rows[0][0] is not None
    assert all(row[:3] == (None, None, None) for row in rows[1:])  # chain, kind and err only on the first row


async def test_stale_cached_node_is_enoent(pool: Pool) -> None:
    cache = DictCache()
    vfs = VFSClient(pool, cache=cache)
    await vfs.create("/f")
    await VFSClient(pool).unlink("/f")  # another client, invisible to this cache
    await raises("ENOENT", vfs.stat("/f"), "/f")


# Pools


def _without_password(conninfo: str) -> tuple[str, str]:
    params = conninfo_to_dict(conninfo)
    password = str(params.pop("password"))
    return make_conninfo("", **{k: str(v) for k, v in params.items() if v is not None}), password


@pytest.mark.parametrize("is_async", [False, True])
async def test_password_fetched_per_connection(db: str, is_async: bool) -> None:
    conninfo, password = _without_password(db)
    calls = 0

    def sync_token() -> str:
        nonlocal calls
        calls += 1
        return password

    async def async_token() -> str:
        return sync_token()

    connector = Connector(conninfo, password=async_token if is_async else sync_token)
    async with Pools(connector, write_size=2, search_size=1) as pools:
        assert calls >= 2  # each pool opened a connection
        async with pools.write.connection() as a, pools.write.connection() as b:
            assert a is not b
        assert calls >= 3
        await VFSClient(pools.write, search_pool=pools.search).create("/f")
    async with await connector() as conn:
        assert (await (await conn.execute("SELECT 1")).fetchone()) == (1,)


async def test_wrong_password_fails_on_open(db: str) -> None:
    conninfo, _ = _without_password(db)
    pools = Pools(Connector(conninfo, password=lambda: "wrong"))
    with pytest.raises(PoolTimeout):
        await pools.open(timeout=2)
    await pools.close()


async def test_reconnects_after_the_server_closes_idle_connections(db: str, conn: AsyncConnection) -> None:
    """Scale-to-zero closes every connection; the pool's checkout check replaces them transparently."""
    async with open_pool(Connector(db), min_size=2, max_size=2) as p:
        await p.wait()
        vfs = VFSClient(p)
        await vfs.create("/f", b"x")
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname = current_database() AND pid <> pg_backend_pid()"
        )
        await asyncio.sleep(0.2)
        for _ in range(4):
            assert (await vfs.pread("/f")).content == b"x"
        await vfs.append("/f", b"y")
        assert (await vfs.stat("/f")).size == 2


async def test_retry_serializable() -> None:
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise psycopg.errors.SerializationFailure("conflict")
        if attempts == 2:
            raise psycopg.errors.DeadlockDetected("deadlock")
        return "done"

    assert await retry_serializable(flaky, base_delay=0) == "done"
    assert attempts == 3

    async def always() -> None:
        nonlocal attempts
        attempts += 1
        raise psycopg.errors.SerializationFailure("conflict")

    attempts = 0
    with pytest.raises(psycopg.errors.SerializationFailure):
        await retry_serializable(always, attempts=4, base_delay=0)
    assert attempts == 4

    async def other() -> None:
        nonlocal attempts
        attempts += 1
        raise psycopg.errors.UniqueViolation("dup")

    attempts = 0
    with pytest.raises(psycopg.errors.UniqueViolation):
        await retry_serializable(other, base_delay=0)
    assert attempts == 1


async def test_retry_real_repeatable_read_conflict(db: str, pool: Pool) -> None:
    """A REPEATABLE READ writer whose snapshot predates a concurrent append gets 40001 and succeeds on retry."""
    vfs = VFSClient(pool)
    await vfs.create("/f")
    started = asyncio.Event()
    proceed = asyncio.Event()
    attempts = 0

    async def writer() -> int:
        nonlocal attempts
        attempts += 1
        async with await AsyncConnection.connect(db) as c:
            await c.set_isolation_level(psycopg.IsolationLevel.REPEATABLE_READ)
            async with c.transaction():
                await c.execute("SELECT size FROM vfs_nodes")  # takes the snapshot
                if attempts == 1:
                    started.set()
                    await proceed.wait()
                cur = await c.execute(
                    "SELECT a.byte_offset FROM vfs_nodes n, vfs_append(n.id, 'w') a WHERE n.name = 'f'"
                )
                row = await cur.fetchone()
                assert row is not None
                return row[0]

    task = asyncio.create_task(retry_serializable(writer, base_delay=0))
    await started.wait()
    await vfs.append("/f", b"a")
    proceed.set()
    assert await task == 1
    assert attempts == 2
    assert (await vfs.pread("/f")).content == b"aw"


async def test_no_session_state_survives(pool: Pool) -> None:
    """No server-side prepared statements, even after psycopg's auto-prepare threshold."""
    vfs = VFSClient(pool)
    await vfs.create("/f", b"x")
    for _ in range(10):
        await vfs.stat("/f")
    async with pool.connection() as c:
        cur = await c.execute("SELECT count(*) FROM pg_prepared_statements")
        assert await cur.fetchone() == (0,)


def test_connector_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PGCONNECT_TIMEOUT", raising=False)
    assert Connector("").params == {"connect_timeout": 30, "prepare_threshold": None}
    assert "connect_timeout" not in Connector("connect_timeout=5").params
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "5")
    assert "connect_timeout" not in Connector("").params
    assert Connector("", prepare_threshold=5).params["prepare_threshold"] == 5


async def test_retry_needs_an_attempt() -> None:
    async def never() -> None:
        raise AssertionError

    with pytest.raises(ValueError):
        await retry_serializable(never, attempts=0)


async def test_lost_claim_keeps_valid_cache_entries(pool: Pool) -> None:
    cache = DictCache()
    vfs, other = VFSClient(pool, cache=cache), VFSClient(pool)
    await vfs.mkdir_p("/tasks/new")
    await vfs.mkdir_p("/tasks/cur")
    for path in ("/", "/tasks", "/tasks/new"):
        await vfs.readdir(path)
    await vfs.create("/tasks/new/t1")
    t1 = (await vfs.stat("/tasks/new/t1")).id
    await other.claim("/tasks/new/t1", "/tasks/cur/t1-other")
    assert await vfs.claim("/tasks/new/t1", "/tasks/cur/t1-mine") is False
    # Only the stale link went; tasks, new and cur stay cached.
    assert sorted(name for _, name in cache.entries) == ["cur", "new", "tasks"]
    assert all(v[0] != t1 for v in cache.entries.values())
    # A mistake in the destination doesn't evict the source's valid chain.
    await vfs.create("/tasks/new/t2")
    await raises("ENOENT", vfs.rename("/tasks/new/t2", "/missing/t2"), "/missing/t2")
    assert "t2" in {name for _, name in cache.entries}


async def test_rewrapped_errors_keep_the_detail(vfs: VFSClient) -> None:
    await vfs.create("/a")
    await vfs.create("/b")
    error = await raises("EEXIST", vfs.rename("/a", "/b"), "/b")
    assert error.db_error is None and error.detail
    error = await raises("ENOTDIR", vfs.create("/a/x", parents=True), "/a/x")
    assert error.db_error is None and error.detail


async def test_no_await_after_commit(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cache maintenance after a committed write runs in the background, so a timeout can't hide the commit."""
    cache = DictCache()
    vfs, other = VFSClient(pool, cache=cache), VFSClient(pool)
    old = await vfs.create("/f")
    await other.unlink("/f")
    await other.create("/f")
    assert (None, "f") in cache.entries  # stale
    release = asyncio.Event()
    original = VFSClient.__dict__["_evict_stale"]

    async def slow_evict(self: VFSClient, refs: Any, started: Any = None) -> None:
        await release.wait()  # as if the pool were saturated
        await original(self, refs, started)

    monkeypatch.setattr(VFSClient, "_evict_stale", slow_evict)
    assert await asyncio.wait_for(vfs.append("/f", b"data"), 2) == 0  # returns without waiting for the eviction
    release.set()
    await vfs.settle()
    assert all(v[0] != old.id for v in cache.entries.values())


async def test_glob(pool: Pool) -> None:
    vfs = VFSClient(pool, search_pool=pool)
    await vfs.create("/w/src/a.py", b"1", parents=True)
    await vfs.create("/w/src/b.txt", parents=True)
    await vfs.create("/w/.hidden/c.py", parents=True)
    await vfs.create("/other/d.py", parents=True)
    assert [e.path for e in await vfs.glob("**/*.py", "/w")] == ["/w/src/a.py"]
    assert sorted(e.path for e in await vfs.glob("*.py", "/w/", hidden=True)) == ["/w/.hidden/c.py", "/w/src/a.py"]
    assert len(await vfs.glob("*.py")) == 2
    entry = (await vfs.glob("a.py", "/w"))[0]
    assert (entry.kind, entry.size, entry.id) == ("file", 1, (await vfs.stat("/w/src/a.py")).id)
    await raises("ENOENT", vfs.glob("*", "/nope"), "/nope")
    await raises("ENOTDIR", vfs.glob("*", "/w/src/a.py"), "/w/src/a.py")
    with pytest.raises(GlobError):
        await vfs.glob("src/", "/w")
    with pytest.raises(GlobError):  # the pattern is checked first
        await vfs.glob("src/", "/nope")
    await raises("EINVAL", vfs.glob("*", limit=-1))
    await raises("EINVAL", vfs.glob("*", limit=2**31))
    assert len(await vfs.glob("*.py", limit=1)) == 1
    for max_nodes in (-1, 2**31):
        await raises("EINVAL", vfs.glob("*", "/w", max_nodes=max_nodes), "/w")
    for timeout in (0, -1, 1e10):
        await raises("EINVAL", vfs.glob("*", "/w", timeout=timeout), "/w")
    # A cached root is used, and its stale links evicted.
    cache = DictCache()
    cached = VFSClient(pool, cache=cache)
    await cached.readdir("/")
    await cached.readdir("/w")
    await vfs.rename("/w", "/w.old")
    await vfs.create("/w/new.py", parents=True)
    assert [e.path for e in await cached.glob("*.py", "/w")] == ["/w/new.py"]


async def until(condition: Callable[[], bool], what: str) -> None:
    for _ in range(500):
        if condition():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out: {what}")


async def test_concurrent_stale_hits_share_one_eviction(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    cache = DictCache()
    vfs, other = VFSClient(pool, cache=cache), VFSClient(pool)
    old = await vfs.create("/f")
    await other.unlink("/f")
    new = await other.create("/f")
    calls = waiting = 0
    release = asyncio.Event()
    evict_stale = VFSClient.__dict__["_evict_stale"]
    await_eviction = VFSClient.__dict__["_await_eviction"]

    async def counted(self: VFSClient, refs: Any, started: Any = None) -> None:
        nonlocal calls
        calls += 1
        await release.wait()  # as if waiting for a connection: the eviction can still be shared
        await evict_stale(self, refs, started)

    async def counted_wait(self: VFSClient, refs: Any) -> None:
        nonlocal waiting
        waiting += 1
        await await_eviction(self, refs)

    monkeypatch.setattr(VFSClient, "_evict_stale", counted)
    monkeypatch.setattr(VFSClient, "_await_eviction", counted_wait)
    # Every read finds the cached link stale; one query evicts it for all of them.
    results = await asyncio.gather(*(vfs.stat("/f") for _ in range(5)))
    assert {r.id for r in results} == {new.id}
    assert calls == 1
    release.set()
    await vfs.settle()
    assert all(v[0] != old.id for v in cache.entries.values())

    # Once that eviction is done, a new stale link gets a new one, which the error paths share and wait for.
    release.clear()
    cache.store(None, "f", old.id, "file")
    await other.unlink("/f")
    stats = [asyncio.create_task(vfs.stat("/f")) for _ in range(3)]
    await until(lambda: waiting == 3, "the three reads never reached the eviction")
    assert calls == 2
    stats[0].cancel()  # a caller giving up doesn't cancel the eviction the others wait for
    release.set()
    errors = await asyncio.gather(*stats, return_exceptions=True)
    assert isinstance(errors[0], asyncio.CancelledError)
    assert all(isinstance(e, VFSError) and e.errno == "ENOENT" for e in errors[1:])
    assert calls == 2
    assert all(v[0] != old.id for v in cache.entries.values())


async def test_a_later_stale_hit_does_not_join_a_started_eviction(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    cache = DictCache()
    vfs = VFSClient(pool, cache=cache)
    cache.store(None, "x", uuid4(), "file")
    ref = vfs._ref(("x",))  # pyright: ignore[reportPrivateUsage]
    sent, release = asyncio.Event(), asyncio.Event()
    evict_stale = VFSClient.__dict__["_evict_stale"]

    async def paused(self: VFSClient, refs: Any, started: Any = None) -> None:
        def on_start() -> None:
            started()
            sent.set()

        await evict_stale(self, refs, on_start)
        await release.wait()

    monkeypatch.setattr(VFSClient, "_evict_stale", paused)
    first = vfs._evict_stale_task([ref])  # pyright: ignore[reportPrivateUsage]
    assert vfs._evict_stale_task([ref]) is first  # pyright: ignore[reportPrivateUsage]
    await asyncio.wait_for(sent.wait(), 10)
    assert first is not None and not first.done()
    second = vfs._evict_stale_task([ref])  # pyright: ignore[reportPrivateUsage]  # its query may predate this hit
    assert second is not None and second is not first
    release.set()
    await vfs.settle()


@pytest.mark.parametrize("outcome", ["cancelled", "failed"])
async def test_error_path_callers_keep_their_error_whatever_the_eviction_does(
    pool: Pool, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    cache = DictCache()
    vfs = VFSClient(pool, cache=cache)
    old = await vfs.create("/f")
    await VFSClient(pool).unlink("/f")
    release = asyncio.Event()
    waiting = 0
    await_eviction = VFSClient.__dict__["_await_eviction"]

    async def broken(self: VFSClient, refs: Any, started: Any = None) -> None:
        await release.wait()
        raise RuntimeError("evict failed")

    async def counted_wait(self: VFSClient, refs: Any) -> None:
        nonlocal waiting
        waiting += 1
        await await_eviction(self, refs)  # reaches asyncio.wait() without suspending before it

    monkeypatch.setattr(VFSClient, "_evict_stale", broken)
    monkeypatch.setattr(VFSClient, "_await_eviction", counted_wait)
    stats = [asyncio.create_task(vfs.stat("/f")) for _ in range(2)]
    try:
        await until(lambda: waiting == 2, "both reads never reached the eviction")
        (shared,) = vfs._evicting.values()  # pyright: ignore[reportPrivateUsage]
        assert len(vfs._background) == 1  # pyright: ignore[reportPrivateUsage]  # both wait for the one task
        if outcome == "cancelled":
            shared.cancel()
        else:
            release.set()
        errors = await asyncio.wait_for(asyncio.gather(*stats, return_exceptions=True), 10)
    finally:
        release.set()  # never leave an eviction hanging
    assert all(isinstance(e, VFSError) and e.errno == "ENOENT" for e in errors), errors
    await vfs.settle()
    assert not vfs._evicting  # pyright: ignore[reportPrivateUsage]
    assert any(v[0] == old.id for v in cache.entries.values())  # nothing evicted it


# Returned outcomes (migration 009): no server errors on the expected paths


class _NoTransaction:
    """A connection whose ``transaction()`` does nothing, so every statement runs in the caller's transaction."""

    def __init__(self, conn: AsyncConnection[Any]) -> None:
        self._conn = conn

    def transaction(self) -> contextlib.nullcontext[None]:
        return contextlib.nullcontext()

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        return await self._conn.execute(*args, **kwargs)


class _OneTransaction:
    """A stand-in pool that hands out one connection, inside one open transaction: had the server raised an error
    for any call, that transaction would be aborted, and every later statement would fail."""

    def __init__(self, conn: AsyncConnection[Any]) -> None:
        self._conn = _NoTransaction(conn)

    @contextlib.asynccontextmanager
    async def connection(self, timeout: float | None = None) -> AsyncGenerator[_NoTransaction]:
        yield self._conn


async def test_outcomes_raise_nothing_on_the_server(conn: AsyncConnection, tmp_path: Path) -> None:
    vfs = VFSClient(cast(Any, _OneTransaction(conn)))
    errors: list[VFSError] = []

    async def outcome(errno: str, call: Awaitable[object]) -> None:
        errors.append(await raises(errno, call))

    async with conn.transaction():
        await vfs.create("/d/f", b"v1", parents=True)
        await vfs.mkdir_p("/tasks/new")
        await vfs.mkdir_p("/tasks/cur")
        await vfs.create("/tasks/new/t1")
        # A missing file, whatever reads or writes it.
        for call in (
            vfs.stat("/missing"),
            vfs.pread("/missing"),
            vfs.pread_lines("/missing"),
            vfs.readdir("/missing"),
            vfs.append("/missing", b"x"),
            vfs.write("/missing", b"x"),
            vfs.unlink("/missing"),
            vfs.rename("/missing", "/x"),
            vfs.create("/missing/x"),
        ):
            await outcome("ENOENT", call)
        await outcome("ENOTDIR", vfs.stat("/d/f/x"))
        await outcome("ENOTDIR", vfs.create("/d/f/x", parents=True))
        for call in (
            vfs.pread("/d"),
            vfs.pread_lines("/d"),
            vfs.append("/d", b"x"),
            vfs.write("/d", b"x"),
            vfs.compact("/d"),
        ):
            await outcome("EISDIR", call)
        await outcome("EEXIST", vfs.create("/d/f"))
        await outcome("EEXIST", vfs.rename("/tasks/new/t1", "/d/f"))
        version = await vfs.stat("/d/f")
        assert version.version is not None
        await vfs.append("/d/f", b"+")
        await outcome("ESTALE", vfs.write("/d/f", b"lost", expect=version.version))  # a compare-and-swap conflict
        await outcome("ENOTEMPTY", vfs.unlink("/d"))  # rmdir of a non-empty directory
        assert await vfs.claim("/tasks/new/t1", "/tasks/cur/t1")
        assert not await vfs.claim("/tasks/new/t1", "/tasks/cur/t1-again")  # a lost claim race: the source is gone
        # put of a new file and of an existing one.
        source = tmp_path / "data"
        source.write_bytes(b"put")
        for path in ("/new/file", "/d/f"):
            await cli._put(vfs, argparse.Namespace(path=path, source=str(source), append=False, parents=True))  # pyright: ignore[reportPrivateUsage]
        assert all(e.db_error is None and e.detail for e in errors), errors
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS  # not INERROR
        cur = await conn.execute("SELECT 1")
        assert await cur.fetchone() == (1,)
    vfs = VFSClient(cast(Any, _OneTransaction(conn)))
    assert (await vfs.pread("/new/file")).content == b"put"
    assert (await vfs.pread("/d/f")).content == b"put"


async def test_a_lost_compare_and_swap_claim_raises_nothing(conn: AsyncConnection) -> None:
    vfs = VFSClient(cast(Any, _OneTransaction(conn)))
    await vfs.mkdir_p("/tasks/new")
    await vfs.mkdir_p("/tasks/cur")
    await vfs.create("/tasks/new/t1")
    t1 = (await vfs.stat("/tasks/new/t1")).id
    async with conn.transaction():
        # The race lost inside vfs_rename: the node moved after the lookup found it in tasks/new.
        cur = await conn.execute(
            "SELECT err, err_detail FROM vfs_rename(%s, (SELECT id FROM vfs_lookup(ARRAY['tasks', 'cur'])), 'x',"
            " (SELECT id FROM vfs_lookup(ARRAY['tasks', 'cur'])))",
            (t1,),
        )
        row = await cur.fetchone()
        assert row is not None and row[0] == "ESTALE" and row[1]
        cur = await conn.execute("SELECT 1")
        assert await cur.fetchone() == (1,)


async def test_a_glob_wider_than_the_node_cap_is_refused(pool: Pool) -> None:
    """#85: the walk under /w keeps src, a.py and b.py. Over the cap the glob is refused whole, and the search
    connection is returned in a usable state."""
    vfs = VFSClient(pool, search_pool=pool)
    await vfs.create("/w/src/a.py", parents=True)
    await vfs.create("/w/src/b.py")
    assert len(await vfs.glob("**", "/w", max_nodes=3)) == 2
    for limit in (None, 1):
        with pytest.raises(SearchRefused) as info:
            await vfs.glob("**", "/w", max_nodes=2, limit=limit)
        assert info.value.reason == "nodes"
        assert str(info.value) == "Glob scope is over 2 files and directories; narrow the path or pattern"
    assert len(await vfs.glob("**", "/w", max_nodes=None)) == 2
    assert len(await vfs.glob("**", "/w")) == 2  # the default cap is far above this
    await raises("ENOENT", vfs.glob("**", "/missing", max_nodes=0), "/missing")  # the path comes first


async def test_a_glob_runs_no_longer_than_its_timeout(pool: Pool, db: str) -> None:
    """#85: a glob that can't finish (here it waits for a lock on the tree) is cancelled after ``timeout`` and
    reported as a refused search, as a grep is."""
    vfs = VFSClient(pool, search_pool=pool)
    await vfs.create("/w/a.py", parents=True)
    async with await AsyncConnection.connect(db) as locker:
        await locker.execute("LOCK TABLE vfs_nodes IN ACCESS EXCLUSIVE MODE")
        for path in ("/", "/w"):  # the walk, and the lookup of the path before it
            with pytest.raises(SearchRefused) as info:
                await asyncio.wait_for(vfs.glob("*.py", path, timeout=0.2), 10)
            assert info.value.reason == "timeout"
            assert str(info.value) == "Search timed out; narrow the pattern or path"
        await locker.rollback()
    assert [e.path for e in await vfs.glob("*.py", "/w")] == ["/w/a.py"]  # the connection is fine afterwards


async def test_grep_and_glob_of_a_missing_path_return_the_error(vfs: VFSClient) -> None:
    await vfs.create("/f")
    for call, errno in [
        (vfs.grep("x", "/missing"), "ENOENT"),
        (vfs.glob("*", "/missing"), "ENOENT"),
        (vfs.glob("*", "/f"), "ENOTDIR"),
        (vfs.grep("x", "/f/x"), "ENOTDIR"),
    ]:
        error = await raises(errno, call)
        assert error.db_error is None


@pytest.mark.parametrize("cached", [False, True])
async def test_a_failed_lookup_never_reaches_the_root(pool: Pool, cached: bool) -> None:
    # A NULL directory id is the root: an operation called with a failed lookup's id would act there.
    cache = DictCache()
    vfs = VFSClient(pool, cache=cache if cached else None)
    await vfs.mkdir_p("/gone")
    a = await vfs.create("/a", b"root a")
    await vfs.readdir("/")  # caches /gone
    await VFSClient(pool).unlink("/gone")  # so its cached link is stale
    for parent in ("/nope", "/gone"):
        await raises("ENOENT", vfs.create(f"{parent}/f"), f"{parent}/f")
        await raises("ENOENT", vfs.mkdir(f"{parent}/d"), f"{parent}/d")
        await raises("ENOENT", vfs.rename("/a", f"{parent}/a"), f"{parent}/a")
        await raises("ENOENT", vfs.readdir(parent), parent)
        # expect_parent NULL would also mean the root, where /a is: the compare-and-swap must not pass.
        await raises("ENOENT", vfs.write(f"{parent}/a", b"CLOBBER", expect=a), f"{parent}/a")
        await raises("ENOENT", vfs.claim(f"{parent}/a", "/b"), parent)
    assert [e.path for e in await vfs.readdir("/")] == ["/a"]
    assert (await vfs.pread("/a")).content == b"root a"


async def test_rename_racing_a_create_reports_the_destination(vfs: VFSClient, db: str) -> None:
    # The create commits between vfs_rename's EEXIST check and its update: the unique constraint raises.
    await vfs.create("/a")
    await vfs.mkdir("/d")
    async with await AsyncConnection.connect(db) as other:
        await other.execute("SELECT * FROM vfs_create((SELECT id FROM vfs_lookup(ARRAY['d'])), 'b', 'file')")
        task = asyncio.create_task(vfs.rename("/a", "/d/b"))
        for _ in range(200):
            cur = await other.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()"
            )
            if (await cur.fetchone()) == (1,):
                break
            await asyncio.sleep(0.02)
        await other.commit()
    error = await raises("EEXIST", task, "/d/b")
    assert error.db_error is not None  # raised, by the constraint


async def test_a_claim_lost_inside_vfs_rename_raises_nothing(conn: AsyncConnection, db: str) -> None:
    # The lookup finds the task in tasks/new, then vfs_rename waits for a concurrent claim's lock and finds it moved:
    # ESTALE, returned, and the claim is False with the caller's transaction intact.
    vfs = VFSClient(cast(Any, _OneTransaction(conn)))
    await vfs.mkdir_p("/tasks/new")
    await vfs.mkdir_p("/tasks/cur")
    await vfs.create("/tasks/new/t1")
    async with (
        await AsyncConnection.connect(db) as other,
        await AsyncConnection.connect(db, autocommit=True) as watcher,
    ):
        await other.execute(
            "SELECT * FROM vfs_rename((SELECT id FROM vfs_lookup(ARRAY['tasks', 'new', 't1'])),"
            " (SELECT id FROM vfs_lookup(ARRAY['tasks', 'cur'])), 't1-other',"
            " (SELECT id FROM vfs_lookup(ARRAY['tasks', 'new'])))"
        )  # uncommitted: holds t1 FOR UPDATE
        async with conn.transaction():
            task = asyncio.create_task(vfs.claim("/tasks/new/t1", "/tasks/cur/t1-mine"))
            # From an autocommit connection: a transaction keeps its first pg_stat_activity snapshot.
            await wait_until_blocked(watcher, conn.info.backend_pid)
            await other.commit()
            assert await task is False
            assert conn.info.transaction_status == psycopg.pq.TransactionStatus.INTRANS
    assert [e.name for e in await vfs.readdir("/tasks/cur")] == ["t1-other"]


# Write timeouts (#84): a write waits for a lock no longer than lock_timeout and runs no longer than write_timeout,
# so one long write of a hot file can't hold every other writer, and with them the pool, behind it.


Writer = Callable[[VFSClient, FileVersion], Awaitable[object]]
WRITERS: dict[str, tuple[str, str, Writer]] = {
    # name: (the path locked by another writer, the path the error names, the write)
    "append": ("/f", "/f", lambda vfs, _: vfs.append("/f", b"more\n")),
    "write": ("/f", "/f", lambda vfs, _: vfs.write("/f", b"v2\n")),
    "write-expect": ("/f", "/f", lambda vfs, v: vfs.write("/f", b"v2\n", expect=v)),
    "compact": ("/f", "/f", lambda vfs, _: vfs.compact("/f")),
    "unlink": ("/f", "/f", lambda vfs, _: vfs.unlink("/f")),
    "rename": ("/f", "/f", lambda vfs, _: vfs.rename("/f", "/g")),
    "claim": ("/d/f", "/d/f", lambda vfs, _: vfs.claim("/d/f", "/d/g")),
    "create": ("/d", "/d/x", lambda vfs, _: vfs.create("/d/x")),  # the directory's lock
    "mkdir_p": ("/d", "/d/x/y", lambda vfs, _: vfs.mkdir_p("/d/x/y")),
}


@pytest.mark.parametrize("name", sorted(WRITERS))
async def test_a_write_waits_for_a_lock_no_longer_than_lock_timeout(pool: Pool, db: str, name: str) -> None:
    locked, path, write = WRITERS[name]
    vfs = VFSClient(pool, lock_timeout=0.1)
    await vfs.mkdir("/d")
    version = await vfs.create("/f", b"v1\n")
    await vfs.create("/d/f", b"v1\n")
    async with holding_the_lock(db, locked):
        error = await raises("EAGAIN", asyncio.wait_for(write(vfs, version), 10), path)
        assert isinstance(error.db_error, psycopg.errors.LockNotAvailable)
        # A read takes no lock.
        assert (await vfs.pread("/f")).content == b"v1\n"
    await write(vfs, version)  # the lock is free again


async def test_a_long_write_is_cancelled_after_write_timeout(pool: Pool) -> None:
    vfs = VFSClient(pool, write_timeout=0.001)
    data = (b"x" * 79 + b"\n") * 20_000  # 1.6 MB: tens of milliseconds to cut into extents
    error = await raises("ETIMEDOUT", vfs.create("/f", data), "/f")
    assert isinstance(error.db_error, psycopg.errors.QueryCanceled)
    await raises("ENOENT", vfs.stat("/f"))  # rolled back
    assert (await VFSClient(pool).create("/f", data)).size == len(data)  # the default leaves room for it


async def test_a_lock_wait_counts_against_write_timeout(pool: Pool, db: str) -> None:
    # A lock_timeout longer than write_timeout leaves statement_timeout to end the wait.
    vfs = VFSClient(pool, lock_timeout=10, write_timeout=0.1)
    await vfs.create("/f", b"v1\n")
    async with holding_the_lock(db, "/f"):
        error = await raises("ETIMEDOUT", asyncio.wait_for(vfs.append("/f", b"more\n"), 10), "/f")
        assert isinstance(error.db_error, psycopg.errors.QueryCanceled)
    assert await vfs.append("/f", b"more\n") == 3


async def test_reads_run_without_the_write_timeouts(pool: Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    # Reads take no locks and have their own caps: one statement each, with no SET LOCAL before it.
    await VFSClient(pool).create("/f", b"v1\n")
    vfs = VFSClient(pool, lock_timeout=0.001, write_timeout=0.001)
    statements = capture(monkeypatch)
    for read in (vfs.pread("/f"), vfs.stat("/f"), vfs.readdir("/")):
        statements.clear()
        await read
        assert len(statements) == 1 and "lock_timeout" not in statements[0], statements


async def test_the_timeouts_do_not_outlive_the_transaction(db: str) -> None:
    # SET LOCAL, not SET: behind a transaction-mode pooler, the next transaction on the connection is someone else's.
    # One connection, so every statement here runs on it, and its pid shows the pool didn't replace it. A session
    # SET would only outlive a transaction that commits (a rollback takes it back too), hence the second write.
    data = (b"x" * 79 + b"\n") * 20_000
    async with open_pool(Connector(db), min_size=1, max_size=1) as one:
        async with one.connection() as conn:
            pid = conn.info.backend_pid
        vfs = VFSClient(one, lock_timeout=0.001, write_timeout=0.001)
        await raises("ETIMEDOUT", vfs.create("/f", data), "/f")
        await VFSClient(one, lock_timeout=0.25, write_timeout=0.5).create("/g", b"x\n")  # commits
        async with one.connection() as conn:
            assert conn.info.backend_pid == pid
            cur = await conn.execute("SELECT current_setting('lock_timeout'), current_setting('statement_timeout')")
            assert await cur.fetchone() == ("0", "0")  # not ("250ms", "500ms")
        assert (await VFSClient(one).create("/f", data)).size == len(data)
        assert (await vfs.pread("/f", 0, len(data))).content == data


def capture(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every statement sent from now on (the pool's check at checkout, an empty query, left out)."""
    statements: list[str] = []
    execute = AsyncConnection.execute

    async def counting(self: AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        if str(query).strip():
            statements.append(str(query))
        return await execute(self, query, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", counting)
    return statements


async def test_the_timeouts_are_not_retried(pool: Pool, db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Retrying would hold a connection again, which is what the timeout frees: one attempt, then the error.
    vfs = VFSClient(pool, lock_timeout=0.1)
    await vfs.create("/f", b"v1\n")
    async with holding_the_lock(db, "/f"):
        statements = capture(monkeypatch)
        await raises("EAGAIN", asyncio.wait_for(vfs.append("/f", b"more\n"), 10), "/f")
    assert len(statements) == 2 and "lock_timeout" in statements[0] and "vfs_append" in statements[1], statements


NO_POOL = cast(Pool, None)  # for a client that is only constructed


def test_the_defaults() -> None:
    vfs = VFSClient(NO_POOL)
    assert (vfs.lock_timeout, vfs.write_timeout) == (DEFAULT_LOCK_TIMEOUT, DEFAULT_WRITE_TIMEOUT) == (5.0, 30.0)


@pytest.mark.parametrize(
    "kwargs",
    [{"lock_timeout": 0}, {"write_timeout": -1}, {"lock_timeout": MAX_TIMEOUT + 1}, {"write_timeout": True}],
    ids=repr,
)
def test_the_timeouts_are_checked(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="must be over 0"):
        VFSClient(NO_POOL, **kwargs)
