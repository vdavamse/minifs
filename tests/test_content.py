"""File content in vfs_extents: vfs_append, vfs_write, vfs_pread, vfs_compact and vfs_create with data."""

import asyncio
import random
from datetime import datetime
from uuid import UUID

import psycopg
import pytest
from psycopg import AsyncConnection

from tests.test_tree import create, mkdir_p, raises_errno, wait_until_blocked


async def _one(conn: AsyncConnection, query: str, params: tuple[object, ...] = ()) -> tuple[object, ...]:
    cur = await conn.execute(query.encode(), params)
    row = await cur.fetchone()
    assert row is not None
    return row


async def create_file(conn: AsyncConnection, name: str, data: bytes | None = None) -> UUID:
    row = await _one(conn, "SELECT id FROM vfs_create(NULL, %s, 'file', %s)", (name, data))
    assert isinstance(row[0], UUID)
    return row[0]


async def append(conn: AsyncConnection, node: UUID, data: bytes) -> int:
    row = await _one(conn, "SELECT vfs_append(%s, %s)", (node, data))
    assert isinstance(row[0], int)
    return row[0]


async def write(conn: AsyncConnection, node: UUID, data: bytes, *expect: object) -> tuple[int, int]:
    params = ", ".join(["%s"] * (2 + len(expect)))
    row = await _one(conn, f"SELECT generation, size FROM vfs_write({params})", (node, data, *expect))
    assert isinstance(row[0], int) and isinstance(row[1], int)
    return row[0], row[1]


async def pread(conn: AsyncConnection, node: UUID, offset: int, limit: int) -> tuple[bytes, int, int]:
    """The bytes read, joined, with the file's size and generation."""
    cur = await conn.execute(
        "SELECT size, generation, byte_offset, data FROM vfs_pread(%s, %s, %s)", (node, offset, limit)
    )
    rows = await cur.fetchall()
    assert rows
    sizes = {(size, gen) for size, gen, _, _ in rows}
    assert len(sizes) == 1, "every row carries the same size and generation"
    size, gen = sizes.pop()
    chunks = [(off, bytes(data)) for _, _, off, data in rows if data is not None]
    # Contiguous chunks in offset order.
    for (off, data), (next_off, _) in zip(chunks, chunks[1:], strict=False):
        assert off + len(data) == next_off
    return b"".join(data for _, data in chunks), size, gen


async def stat(conn: AsyncConnection, node: UUID) -> tuple[int, int, datetime]:
    row = await _one(conn, "SELECT size, generation, mtime FROM vfs_nodes WHERE id = %s", (node,))
    assert isinstance(row[0], int) and isinstance(row[1], int) and isinstance(row[2], datetime)
    return row[0], row[1], row[2]


async def extents(conn: AsyncConnection, node: UUID) -> list[tuple[int, int, int]]:
    """(generation, byte_offset, length) of every extent of ``node``, in key order."""
    cur = await conn.execute(
        "SELECT generation, byte_offset, length(data) FROM vfs_extents WHERE node_id = %s ORDER BY 1, 2", (node,)
    )
    return [(g, o, n) for g, o, n in await cur.fetchall()]


async def compact(conn: AsyncConnection, node: UUID, target: int | None = None) -> int:
    if target is None:
        row = await _one(conn, "SELECT vfs_compact(%s)", (node,))
    else:
        row = await _one(conn, "SELECT vfs_compact(%s, %s)", (node, target))
    assert isinstance(row[0], int)
    return row[0]


def expected(content: bytes, offset: int, limit: int) -> bytes:
    start = max(len(content) + offset, 0) if offset < 0 else min(offset, len(content))
    return content[start : start + limit]


async def read_all(conn: AsyncConnection, node: UUID, chunk: int) -> tuple[bytes, int]:
    """Reads ``node`` in ``chunk``-byte preads until one returns nothing; returns the bytes and the last size."""
    parts: list[bytes] = []
    offset = 0
    while True:
        data, size, _ = await pread(conn, node, offset, chunk)
        if not data:
            return b"".join(parts), size
        parts.append(data)
        offset += len(data)


# Schema


async def test_extent_storage_is_external(conn: AsyncConnection) -> None:
    row = await _one(
        conn, "SELECT attstorage FROM pg_attribute WHERE attrelid = 'vfs_extents'::regclass AND attname = 'data'"
    )
    assert row[0] == "e"


async def test_extent_on_a_directory_is_rejected(conn: AsyncConnection) -> None:
    d = await mkdir_p(conn, "d")
    with raises_errno("EISDIR"):
        await conn.execute(
            "INSERT INTO vfs_extents (node_id, generation, byte_offset, data) VALUES (%s, 0, 0, 'x')", (d,)
        )
    with raises_errno("EISDIR"):
        await conn.execute(
            "INSERT INTO vfs_extents (node_id, generation, byte_offset, data, node_kind) VALUES (%s, 0, 0, 'x', 'dir')",
            (d,),
        )


async def test_extents_go_with_their_file(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f", b"hello")
    await append(conn, f, b" world")
    await conn.execute("SELECT vfs_unlink(%s)", (f,))
    row = await _one(conn, "SELECT count(*) FROM vfs_extents")
    assert row[0] == 0


# Errors


@pytest.mark.parametrize(
    "call",
    [
        "SELECT vfs_append(%s, 'x')",
        "SELECT vfs_write(%s, 'x')",
        "SELECT * FROM vfs_pread(%s, 0, 10)",
        "SELECT vfs_compact(%s)",
    ],
)
async def test_content_operations_on_a_directory_or_missing_node(conn: AsyncConnection, call: str) -> None:
    d = await mkdir_p(conn, "d")
    with raises_errno("EISDIR"):
        await conn.execute(call.encode(), (d,))
    with raises_errno("ENOENT"):
        await conn.execute(call.encode(), (UUID(int=1),))


@pytest.mark.parametrize(
    "call",
    [
        "SELECT vfs_append(%s, NULL)",
        "SELECT vfs_write(%s, NULL)",
        "SELECT * FROM vfs_pread(%s, NULL, 10)",
        "SELECT * FROM vfs_pread(%s, 0, NULL)",
        "SELECT * FROM vfs_pread(%s, 0, -1)",
        "SELECT vfs_compact(%s, 0)",
        "SELECT vfs_compact(%s, NULL)",
        "SELECT vfs_compact(%s, 64 * 1024 * 1024 + 1)",
        "SELECT vfs_write(%s, 'x', 0)",  # an incomplete compare-and-swap
        "SELECT vfs_write(%s, 'x', NULL, NULL, gen_random_uuid())",
    ],
)
async def test_invalid_arguments(conn: AsyncConnection, call: str) -> None:
    f = await create_file(conn, "f")
    with raises_errno("EINVAL"):
        await conn.execute(call.encode(), (f,))


async def test_directory_errors_do_not_wait_for_a_lock(conn: AsyncConnection, db: str) -> None:
    d = await mkdir_p(conn, "d")
    assert d is not None
    async with await AsyncConnection.connect(db) as other:
        await other.execute("SELECT FROM vfs_nodes WHERE id = %s FOR UPDATE", (d,))
        await conn.execute("SET statement_timeout = '5s'")
        with raises_errno("EISDIR"):
            await append(conn, d, b"x")
        await other.rollback()


# vfs_create with data


async def test_create_with_data(conn: AsyncConnection) -> None:
    row = await _one(conn, "SELECT id, generation, size FROM vfs_create(NULL, 'f', 'file', 'hello')")
    assert row[1:] == (0, 5)
    assert isinstance(row[0], UUID)
    assert await pread(conn, row[0], 0, 100) == (b"hello", 5, 0)
    assert await extents(conn, row[0]) == [(0, 0, 5)]


async def test_create_with_empty_or_no_data_has_no_extents(conn: AsyncConnection) -> None:
    for name, data in [("a", None), ("b", b"")]:
        f = await create_file(conn, name, data)
        assert await extents(conn, f) == []
        assert await pread(conn, f, 0, 10) == (b"", 0, 0)


async def test_create_directory_with_data_is_einval(conn: AsyncConnection) -> None:
    with raises_errno("EINVAL"):
        await conn.execute("SELECT vfs_create(NULL, 'd', 'dir', 'x')")
    assert await create(conn, None, "d", "dir")  # without data, and with three arguments, as before


# vfs_append


async def test_append_returns_offsets_and_keeps_generation(conn: AsyncConnection) -> None:
    f = await create_file(conn, "log")
    _, _, mtime0 = await stat(conn, f)
    assert await append(conn, f, b"abc") == 0
    assert await append(conn, f, b"de") == 3
    size, gen, mtime = await stat(conn, f)
    assert (size, gen) == (5, 0)
    assert mtime > mtime0
    assert await extents(conn, f) == [(0, 0, 3), (0, 3, 2)]
    assert await pread(conn, f, 0, 100) == (b"abcde", 5, 0)


async def test_empty_append_changes_nothing(conn: AsyncConnection) -> None:
    f = await create_file(conn, "log", b"abc")
    before = await stat(conn, f)
    assert await append(conn, f, b"") == 3
    assert await stat(conn, f) == before
    assert await append(conn, f, b"d") == 3
    assert await extents(conn, f) == [(0, 0, 3), (0, 3, 1)]


async def test_append_may_split_a_utf8_character(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    text = "naïve ☃".encode()
    for i in range(len(text)):
        await append(conn, f, text[i : i + 1])
    data, _, _ = await pread(conn, f, 0, 100)
    assert data.decode() == "naïve ☃"


async def test_concurrent_appender_waits_and_lands_after_the_first_commit(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log")
    async with await AsyncConnection.connect(db) as first, await AsyncConnection.connect(db) as second:
        assert await append(first, f, b"first") == 0  # transaction open, inode locked
        task = asyncio.create_task(append(second, f, b"second"))
        await wait_until_blocked(conn, second.info.backend_pid)
        await first.commit()
        assert await task == 5
        await second.commit()
    assert await pread(conn, f, 0, 100) == (b"firstsecond", 11, 0)


async def test_rolled_back_append_leaves_no_gap(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log")
    async with await AsyncConnection.connect(db) as first, await AsyncConnection.connect(db) as second:
        assert await append(first, f, b"lost") == 0
        task = asyncio.create_task(append(second, f, b"kept"))
        await wait_until_blocked(conn, second.info.backend_pid)
        await first.rollback()
        assert await task == 0
        await second.commit()
    assert await pread(conn, f, 0, 100) == (b"kept", 4, 0)


async def test_many_concurrent_appenders_are_gap_free(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log")

    async def appender(worker: int) -> list[tuple[int, bytes]]:
        done: list[tuple[int, bytes]] = []
        async with await AsyncConnection.connect(db, autocommit=True) as c:
            for i in range(25):
                data = f"<{worker}:{i}:{'x' * random.randrange(10)}>".encode()
                done.append((await append(c, f, data), data))
        return done

    results = await asyncio.gather(*(appender(w) for w in range(4)))
    landed = sorted(item for result in results for item in result)
    content, size, _ = await pread(conn, f, 0, 1_000_000)
    offset = 0
    for at, data in landed:
        assert at == offset
        assert content[at : at + len(data)] == data
        offset += len(data)
    assert offset == size == len(content)
    # Each appender's own appends land in its own order.
    for result in results:
        assert [at for at, _ in result] == sorted(at for at, _ in result)


async def test_append_waiting_on_an_unlink_gets_enoent(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log", b"x")
    async with await AsyncConnection.connect(db) as remover, await AsyncConnection.connect(db) as appender:
        await remover.execute("SELECT vfs_unlink(%s)", (f,))
        task = asyncio.create_task(append(appender, f, b"y"))
        await wait_until_blocked(conn, appender.info.backend_pid)
        await remover.commit()
        with raises_errno("ENOENT"):
            await task


async def test_append_waiting_on_an_overwrite_lands_in_the_new_generation(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log", b"old content")
    async with await AsyncConnection.connect(db) as writer, await AsyncConnection.connect(db) as appender:
        assert await write(writer, f, b"new") == (1, 3)
        task = asyncio.create_task(append(appender, f, b"+"))
        await wait_until_blocked(conn, appender.info.backend_pid)
        await writer.commit()
        assert await task == 3
        await appender.commit()
    assert await pread(conn, f, 0, 100) == (b"new+", 4, 1)
    assert await extents(conn, f) == [(1, 0, 3), (1, 3, 1)]


async def test_repeatable_read_append_after_a_concurrent_append_must_retry(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log")
    async with await AsyncConnection.connect(db) as late:
        await late.set_isolation_level(psycopg.IsolationLevel.REPEATABLE_READ)
        await late.execute("SELECT 1")  # takes the snapshot
        await append(conn, f, b"first")
        with pytest.raises(psycopg.errors.SerializationFailure):
            await append(late, f, b"late")
        await late.rollback()
        assert await append(late, f, b"late") == 5  # the retry
        await late.commit()


async def test_mtime_never_goes_back(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log")
    async with await AsyncConnection.connect(db) as early:
        await early.execute("SELECT 1")  # its transaction, and now(), start here
        await asyncio.sleep(0.05)
        await append(conn, f, b"a")
        _, _, mtime = await stat(conn, f)
        await append(early, f, b"b")
        await early.commit()
    assert (await stat(conn, f))[2] >= mtime


# vfs_write


async def test_write_replaces_content_with_a_new_generation(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f", b"old content")
    await append(conn, f, b" and more")
    _, _, mtime0 = await stat(conn, f)
    assert await write(conn, f, b"new") == (1, 3)
    size, gen, mtime = await stat(conn, f)
    assert (size, gen) == (3, 1)
    assert mtime > mtime0
    assert await extents(conn, f) == [(1, 0, 3)]
    assert await pread(conn, f, 0, 100) == (b"new", 3, 1)
    assert await append(conn, f, b"!") == 3
    assert await pread(conn, f, 0, 100) == (b"new!", 4, 1)
    assert await write(conn, f, b"") == (2, 0)
    assert await extents(conn, f) == []
    assert await pread(conn, f, 0, 100) == (b"", 0, 2)


async def test_write_compare_and_swap(conn: AsyncConnection) -> None:
    d = await mkdir_p(conn, "d")
    f = await create(conn, d, "f")
    assert await write(conn, f, b"v1", 0, 0, d, "f") == (1, 2)
    for stale in [(0, 2, d, "f"), (1, 3, d, "f"), (1, 2, None, "f"), (1, 2, d, "g")]:
        with raises_errno("ESTALE"):
            await write(conn, f, b"v2", *stale)
    assert await pread(conn, f, 0, 100) == (b"v1", 2, 1)
    assert await write(conn, f, b"v2", 1, 2, d, "f") == (2, 2)
    # A root entry is expected with a NULL parent.
    r = await create_file(conn, "r")
    assert await write(conn, r, b"x", 0, 0, None, "r") == (1, 1)


async def test_reader_snapshot_survives_an_overwrite(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "f", b"old")
    async with await AsyncConnection.connect(db) as reader:
        await reader.set_isolation_level(psycopg.IsolationLevel.REPEATABLE_READ)
        assert await pread(reader, f, 0, 100) == (b"old", 3, 0)
        await write(conn, f, b"brand new")
        assert await pread(reader, f, 0, 100) == (b"old", 3, 0)
        await reader.commit()
        assert await pread(reader, f, 0, 100) == (b"brand new", 9, 1)


# vfs_pread


async def test_pread_edge_offsets(conn: AsyncConnection) -> None:
    content = b"0123456789abcdefghij"
    f = await create_file(conn, "f")
    for i in range(0, len(content), 7):  # extents of 7, 7 and 6 bytes
        await append(conn, f, content[i : i + 7])
    size = len(content)
    for offset in [0, 1, 6, 7, 8, size - 1, size, size + 1, 10**15, -1, -7, -size, -(size + 1), -(10**15)]:
        for limit in [0, 1, 3, 7, 8, 14, size, size + 5, 2**31 - 1]:
            assert await pread(conn, f, offset, limit) == (expected(content, offset, limit), size, 0), (offset, limit)


async def test_pread_returns_size_and_generation_when_nothing_is_read(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    cur = await conn.execute("SELECT * FROM vfs_pread(%s, 0, 10)", (f,))
    assert await cur.fetchall() == [(0, 0, 0, None)]
    await write(conn, f, b"abc")
    # byte_offset is where the read started: past EOF and negative offsets come back resolved.
    for offset, start in [(3, 3), (99, 3), (-1, 2), (-99, 0)]:
        cur = await conn.execute("SELECT * FROM vfs_pread(%s, %s, 0)", (f, offset))
        assert await cur.fetchall() == [(3, 1, start, None)]


async def test_pread_of_one_extent_larger_than_the_limit(conn: AsyncConnection) -> None:
    content = bytes(range(256)) * 400  # 100 KB, one extent, TOASTed
    f = await create_file(conn, "f", content)
    cur = await conn.execute("SELECT byte_offset, length(data) FROM vfs_pread(%s, 50000, 16000)", (f,))
    assert await cur.fetchall() == [(50000, 16000)]
    assert (await pread(conn, f, 50000, 16000))[0] == content[50000:66000]
    assert (await pread(conn, f, -100, 16000))[0] == content[-100:]


async def test_pread_spanning_extents(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    for part in [b"aaaa", b"bbbb", b"cccc", b"dddd"]:
        await append(conn, f, part)
    cur = await conn.execute("SELECT byte_offset, data FROM vfs_pread(%s, 2, 11)", (f,))
    assert [(o, bytes(d)) for o, d in await cur.fetchall()] == [(2, b"aa"), (4, b"bbbb"), (8, b"cccc"), (12, b"d")]


@pytest.mark.parametrize("seed", range(5))
async def test_pread_chunks_join_to_the_content(conn: AsyncConnection, seed: int) -> None:
    rng = random.Random(seed)
    f = await create_file(conn, "f")
    content = b""
    for _ in range(rng.randrange(1, 40)):
        part = rng.randbytes(rng.choice([1, 2, 5, 50, 500, 5000]))
        await append(conn, f, part)
        content += part
    for _ in range(30):
        offset = rng.randrange(-len(content) - 10, len(content) + 10)
        limit = rng.randrange(0, len(content) + 10)
        assert (await pread(conn, f, offset, limit))[0] == expected(content, offset, limit)
    for chunk in [*rng.sample(range(1, 2000), 3), len(content)]:
        assert await read_all(conn, f, chunk) == (content, len(content))


async def test_pread_chunks_while_another_session_appends(conn: AsyncConnection, db: str) -> None:
    rng = random.Random(42)
    parts = [rng.randbytes(rng.randrange(1, 300)) for _ in range(200)]
    f = await create_file(conn, "f")
    stop = asyncio.Event()

    async def appender() -> None:
        async with await AsyncConnection.connect(db, autocommit=True) as c:
            for part in parts:
                await append(c, f, part)
            stop.set()

    task = asyncio.create_task(appender())
    full = b"".join(parts)
    reads = 0
    while not stop.is_set() or reads == 0:
        data, size = await read_all(conn, f, rng.choice([13, 256, 1000]))
        assert len(data) >= size  # never short of what a snapshot reported
        assert data == full[: len(data)]
        tail, tail_size, _ = await pread(conn, f, -50, 50)
        assert tail == full[max(tail_size - 50, 0) : tail_size]
        reads += 1
    await task
    assert await read_all(conn, f, 333) == (full, len(full))


async def _plan_of_pread(conn: AsyncConnection, node: UUID, offset: int, limit: int) -> str:
    """The EXPLAIN ANALYZE output, with buffers, of the query inside vfs_pread (auto_explain, superuser only)."""
    notices: list[str] = []
    conn.add_notice_handler(lambda d: notices.append(d.message_primary or ""))
    for setting in [
        "LOAD 'auto_explain'",
        "SET auto_explain.log_min_duration = 0",
        "SET auto_explain.log_analyze = on",
        "SET auto_explain.log_buffers = on",
        "SET auto_explain.log_nested_statements = on",
        "SET client_min_messages = log",
    ]:
        await conn.execute(setting.encode())
    await conn.execute("SELECT * FROM vfs_pread(%s, %s, %s)", (node, offset, limit))
    await conn.execute("SET client_min_messages = notice")
    plans = [n for n in notices if "vfs_extents" in n and "WITH n AS" in n]
    assert len(plans) == 1, notices
    return plans[0]


async def test_pread_near_the_end_of_many_extents_uses_two_index_steps(conn: AsyncConnection) -> None:
    f = await create_file(conn, "log")
    await conn.execute(
        "SELECT vfs_append(%s, convert_to(format('line %%s\n', i), 'UTF8')) FROM generate_series(1, 12000) i", (f,)
    )
    await conn.execute("ANALYZE vfs_extents")
    size, _, _ = await stat(conn, f)
    data, _, _ = await pread(conn, f, size - 30, 20)
    content, _ = await read_all(conn, f, 1_000_000)
    assert data == content[-30:-10]

    plan = await _plan_of_pread(conn, f, size - 30, 20)
    # Step 1: a backward descent to the extent containing the offset.
    assert "Scan Backward using vfs_extents_pkey" in plan, plan
    # Step 2: a forward scan bounded on both sides by the index condition.
    conds = [line for line in plan.splitlines() if "Index Cond:" in line and "byte_offset >= " in line]
    assert len(conds) == 1 and "byte_offset < " in conds[0], plan
    # A few index and heap pages, not the thousands a scan from offset 0 would touch.
    hits = [int(part.split("=")[1].split()[0]) for part in plan.split() if part.startswith("hit=")]
    assert hits and max(hits) < 30, plan


# vfs_compact


async def test_compact_keeps_bytes_size_generation_and_mtime(conn: AsyncConnection) -> None:
    rng = random.Random(7)
    f = await create_file(conn, "f")
    await write(conn, f, b"")  # generation 1
    content = b""
    for _ in range(300):
        part = rng.randbytes(rng.choice([1, 10, 100, 3000]))
        await append(conn, f, part)
        content += part
    before = await stat(conn, f)
    xmin = await _one(conn, "SELECT xmin::text FROM vfs_nodes WHERE id = %s", (f,))
    removed = await compact(conn, f, 4096)
    after = await extents(conn, f)
    assert removed == 300 - len(after) > 0
    assert await stat(conn, f) == before
    assert await _one(conn, "SELECT xmin::text FROM vfs_nodes WHERE id = %s", (f,)) == xmin  # not updated
    assert await read_all(conn, f, 1000) == (content, len(content))
    # The pieces tile the file in the same generation, and none exceeds the target (no extent here does).
    assert all(gen == 1 for gen, _, _ in after)
    assert [o for _, o, _ in after] == [sum(n for _, _, n in after[:i]) for i in range(len(after))]
    assert all(n <= 4096 for _, _, n in after)
    # Idempotent.
    assert await compact(conn, f, 4096) == 0


async def test_compact_with_the_default_target(conn: AsyncConnection) -> None:
    f = await create_file(conn, "log")
    await conn.execute("SELECT vfs_append(%s, decode(repeat('41', 100), 'hex')) FROM generate_series(1, 2000)", (f,))
    assert len(await extents(conn, f)) == 2000
    await compact(conn, f)
    assert [n for _, _, n in await extents(conn, f)] == [65500, 65500, 65500, 3500]
    assert (await pread(conn, f, 0, 300_000))[0] == b"A" * 200_000


async def test_compact_leaves_a_large_extent_whole(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    for part in [b"a", b"b", b"x" * 100, b"c", b"d", b"e"]:
        await append(conn, f, part)
    assert await compact(conn, f, 10) == 3
    assert [n for _, _, n in await extents(conn, f)] == [2, 100, 3]


async def test_compact_starts_pieces_only_at_short_extents(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    for part in [b"x" * 6, b"a", b"b", b"y" * 6, b"c"]:  # with target 10, extents of 5 bytes or more are full
        await append(conn, f, part)
    xs = await _one(conn, "SELECT ctid::text FROM vfs_extents WHERE node_id = %s AND byte_offset = 0", (f,))
    assert await compact(conn, f, 10) == 3  # a + b + yyyyyy + c; xxxxxx starts no piece
    assert [n for _, _, n in await extents(conn, f)] == [6, 9]
    assert await _one(conn, "SELECT ctid::text FROM vfs_extents WHERE node_id = %s AND byte_offset = 0", (f,)) == xs


async def test_empty_extents_are_rejected(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    with raises_errno("EINVAL"):
        await conn.execute(
            "INSERT INTO vfs_extents (node_id, generation, byte_offset, data) VALUES (%s, 0, 0, '')", (f,)
        )


async def test_compact_refuses_extents_that_do_not_tile(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    await conn.execute(
        "INSERT INTO vfs_extents (node_id, generation, byte_offset, data) VALUES (%s, 0, 0, 'ab'), (%s, 0, 5, 'cd')",
        (f, f),
    )
    with pytest.raises(psycopg.errors.DataCorrupted):
        await compact(conn, f, 10)


async def test_compact_of_an_empty_file(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    assert await compact(conn, f) == 0


async def test_appends_wait_for_compaction_and_stay_gap_free(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log")
    for i in range(50):
        await append(conn, f, f"{i};".encode())
    content, size = await read_all(conn, f, 1000)
    async with await AsyncConnection.connect(db) as compactor, await AsyncConnection.connect(db) as appender:
        assert await compact(compactor, f) == 49  # transaction open, inode locked
        task = asyncio.create_task(append(appender, f, b"tail"))
        await wait_until_blocked(conn, appender.info.backend_pid)
        # Readers don't wait, and see the old extents until the compaction commits.
        assert await read_all(conn, f, 7) == (content, size)
        await compactor.commit()
        assert await task == size
        await appender.commit()
    assert await read_all(conn, f, 7) == (content + b"tail", size + 4)
    assert [n for _, _, n in await extents(conn, f)] == [size, 4]


async def test_concurrent_appends_and_compactions(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log")
    parts = [f"[{i}]".encode() for i in range(300)]

    async def appender() -> list[int]:
        async with await AsyncConnection.connect(db, autocommit=True) as c:
            return [await append(c, f, part) for part in parts]

    async def compactor() -> None:
        async with await AsyncConnection.connect(db, autocommit=True) as c:
            for _ in range(20):
                await compact(c, f, 64)
                await asyncio.sleep(0.005)

    offsets, _ = await asyncio.gather(appender(), compactor())
    full = b"".join(parts)
    assert offsets == [sum(len(p) for p in parts[:i]) for i in range(len(parts))]
    assert await read_all(conn, f, 50) == (full, len(full))
