"""Line-aware extents (migration 007): vfs_cut, line_start, line_offsets, vfs_nodes.lines and vfs_pread_lines."""

import asyncio
import random
from uuid import UUID

import pytest
from psycopg import AsyncConnection

from minifs.db.migrate import load_migrations, migrate
from tests.test_content import (
    append,
    compact,
    create_file,
    expected,
    max_buffer_hits,
    plans_of,
    pread,
    read_all,
    write,
)
from tests.test_tree import raises_errno

TARGET = 65536  # vfs_cut's default


async def one(conn: AsyncConnection, query: str, params: tuple[object, ...] = ()) -> tuple[object, ...]:
    cur = await conn.execute(query.encode(), params)
    row = await cur.fetchone()
    assert row is not None
    return row


def split_lines(content: bytes) -> list[bytes]:
    """The lines of ``content`` with their ``\\n``, split on ``\\n`` only; a trailing ``\\n`` adds no empty line."""
    lines = content.split(b"\n")
    tail = lines.pop()
    return [line + b"\n" for line in lines] + ([tail] if tail else [])


def expected_lines(content: bytes, first: int, max_lines: int, max_bytes: int) -> tuple[int, bytes]:
    """What vfs_pread_lines returns for ``content``: the byte offset of line ``first`` and the bytes."""
    lines = split_lines(content)
    if first > len(lines):
        return len(content), b""
    start = sum(len(line) for line in lines[: first - 1])
    return start, b"".join(lines[first - 1 : first - 1 + max_lines])[:max_bytes]


async def extent_rows(conn: AsyncConnection, node: UUID) -> list[tuple[int, int, int, bytes, list[int]]]:
    """(generation, byte_offset, line_start, data, line_offsets) of every extent of ``node``, in key order."""
    cur = await conn.execute(
        "SELECT generation, byte_offset, line_start, data, line_offsets FROM vfs_extents WHERE node_id = %s"
        " ORDER BY 1, 2",
        (node,),
    )
    return [(g, o, ls, bytes(d), list(lo)) for g, o, ls, d, lo in await cur.fetchall()]


async def check_layout(conn: AsyncConnection, node: UUID, content: bytes) -> None:
    """The extents of ``node`` hold ``content``, line-aligned, and its inode has the right size and line count."""
    size, generation, lines = await one(conn, "SELECT size, generation, lines FROM vfs_nodes WHERE id = %s", (node,))
    assert (size, lines) == (len(content), len(split_lines(content)))
    rows = await extent_rows(conn, node)
    assert all(gen == generation for gen, *_ in rows), "only the current generation has extents"
    offset = newlines = 0
    for i, (_, byte_offset, line_start, data, line_offsets) in enumerate(rows):
        assert byte_offset == offset, "the extents tile [0, size)"
        assert data == content[offset : offset + len(data)]
        assert i == len(rows) - 1 or data.endswith(b"\n"), "only the last extent may end mid-line"
        assert line_start == newlines, "line_start counts the \\n before the extent"
        newlines += data.count(b"\n")
        assert line_offsets == [0] + [j + 1 for j, b in enumerate(data[:-1]) if b == 0x0A]
        assert len(data) <= TARGET or len(line_offsets) == 1, "only a single line makes an oversized extent"
        offset += len(data)
    assert offset == len(content)


async def pread_lines_many(
    conn: AsyncConnection, node: UUID, queries: list[tuple[int, int, int]]
) -> list[tuple[int, int, int, int, int, bytes]]:
    """vfs_pread_lines for each ``(first_line, max_lines, max_bytes)``, in one statement (so one snapshot)."""
    firsts, maxes, caps = zip(*queries, strict=True)
    cur = await conn.execute(
        "SELECT r.size, r.generation, r.lines, r.first_line, r.byte_offset, r.data"
        " FROM unnest(%s::bigint[], %s::int[], %s::int[]) WITH ORDINALITY AS q(f, m, b, i),"
        " vfs_pread_lines(%s, q.f, q.m, q.b) r ORDER BY q.i",
        (list(firsts), list(maxes), list(caps), node),
    )
    return [(s, g, n, fl, o, bytes(d)) for s, g, n, fl, o, d in await cur.fetchall()]


async def check_reads(conn: AsyncConnection, node: UUID, content: bytes, rng: random.Random) -> None:
    """vfs_pread_lines from every line, with a sample of limits, and some vfs_preads, against ``content``."""
    lines = split_lines(content)
    total = len(lines)
    starts = [0]
    for line in lines:
        starts.append(starts[-1] + len(line))
    firsts = range(1, total + 3)
    limits = [0, 1, 2, 3, 7, total + 1, 2**31 - 1]
    caps = [0, 1, 5, 64, 1000, 2**31 - 1]
    queries = [(first, rng.choice(limits), rng.choice(caps)) for first in firsts]
    queries += [(1, 2**31 - 1, 2**31 - 1), (max(total, 1), 1, 2**31 - 1), (2**62, 1, 1)]
    for (first, max_lines, max_bytes), row in zip(queries, await pread_lines_many(conn, node, queries), strict=True):
        size, _, n, first_line, byte_offset, data = row
        assert (size, n, first_line) == (len(content), total, first)
        if first > total:
            assert (byte_offset, data) == (len(content), b""), first
            continue
        end = starts[min(first - 1 + max_lines, total)]
        want = content[starts[first - 1] : end][:max_bytes]
        assert (byte_offset, data) == (starts[first - 1], want), (first, max_lines, max_bytes)
    for _ in range(5):
        offset = rng.randrange(-len(content) - 2, len(content) + 2)
        limit = rng.randrange(0, len(content) + 2)
        assert (await pread(conn, node, offset, limit))[0] == expected(content, offset, limit)


# vfs_cut


@pytest.mark.parametrize(
    ("data", "target", "pieces"),
    [
        (b"", 10, []),
        (b"\n", 10, [(0, 0, b"\n", [0])]),
        (b"a", 10, [(0, 0, b"a", [0])]),
        (b"a\nb\n", 10, [(0, 0, b"a\nb\n", [0, 2])]),
        (b"aa\nbbbb\ncc\nd", 6, [(0, 0, b"aa\n", [0]), (3, 1, b"bbbb\n", [0]), (8, 2, b"cc\nd", [0, 3])]),
        (b"aa\nbb\n", 6, [(0, 0, b"aa\nbb\n", [0, 3])]),  # exactly the target: one piece
        (b"aa\nbb\nc", 6, [(0, 0, b"aa\nbb\n", [0, 3]), (6, 2, b"c", [0])]),
        (b"x" * 20 + b"\ny\n", 6, [(0, 0, b"x" * 20 + b"\n", [0]), (21, 1, b"y\n", [0])]),  # a long line is whole
        (b"y\n" + b"x" * 20, 6, [(0, 0, b"y\n", [0]), (2, 1, b"x" * 20, [0])]),
        (b"\n\n\n", 2, [(0, 0, b"\n\n", [0, 1]), (2, 2, b"\n", [0])]),
        # Bytes that encode(..., 'escape') escapes: NUL, high-bit bytes, backslashes.
        (b"\\\x00\xc3\xa9\n\xff\\\n", 100, [(0, 0, b"\\\x00\xc3\xa9\n\xff\\\n", [0, 5])]),
    ],
)
async def test_cut(
    conn: AsyncConnection, data: bytes, target: int, pieces: list[tuple[int, int, bytes, list[int]]]
) -> None:
    cur = await conn.execute("SELECT * FROM vfs_cut(%s, %s)", (data, target))
    assert [(o, n, bytes(d), list(lo)) for o, n, d, lo in await cur.fetchall()] == pieces


async def test_cut_of_null_is_nothing(conn: AsyncConnection) -> None:
    cur = await conn.execute("SELECT count(*) FROM vfs_cut(NULL)")
    assert await cur.fetchone() == (0,)


@pytest.mark.parametrize("target", ["NULL", "0", "-1"])
async def test_cut_rejects_a_bad_target(conn: AsyncConnection, target: str) -> None:
    with raises_errno("EINVAL"):
        await conn.execute(f"SELECT * FROM vfs_cut('a', {target})".encode())


async def test_cut_finds_lines_across_its_encoding_windows(conn: AsyncConnection) -> None:
    # Over 1 MB, so it is encoded in windows: lines cross window boundaries, one line spans a whole window, and
    # bytes the escape encoding expands (high-bit, NUL, backslash) fill a window.
    rng = random.Random(5)
    data = b"".join(rng.choice([b"\n", b"ab\\", b"\xff\x00" * 50, b"x" * 3000]) for _ in range(2000))
    data += b"L" * 1_500_000 + b"\n" + b"\xff" * 1_200_000 + b"\ntail"
    pieces: list[bytes] = []
    for line in split_lines(data):
        if pieces and len(pieces[-1]) + len(line) <= TARGET:
            pieces[-1] += line
        else:
            pieces.append(line)
    cur = await conn.execute("SELECT rel_line, data, line_offsets FROM vfs_cut(%s)", (data,))
    rows = [(n, bytes(d), list(lo)) for n, d, lo in await cur.fetchall()]
    assert [d for _, d, _ in rows] == pieces
    assert [n for n, _, _ in rows] == [sum(p.count(b"\n") for p in pieces[:i]) for i in range(len(pieces))]
    assert all(lo == [0] + [j + 1 for j, b in enumerate(d[:-1]) if b == 0x0A] for _, d, lo in rows)


@pytest.mark.parametrize("seed", range(3))
async def test_cut_matches_a_greedy_split(conn: AsyncConnection, seed: int) -> None:
    rng = random.Random(seed)
    data = b"".join(rng.choice([b"\n", b"ab", b"\xe2\x98\x83", b"x" * 50, rng.randbytes(20)]) for _ in range(300))
    target = rng.choice([1, 16, 100, 1000])
    pieces: list[bytes] = []
    for line in split_lines(data):
        if pieces and len(pieces[-1]) + len(line) <= target:
            pieces[-1] += line
        else:
            pieces.append(line)
    cur = await conn.execute("SELECT rel_offset, data FROM vfs_cut(%s, %s)", (data, target))
    rows = [(o, bytes(d)) for o, d in await cur.fetchall()]
    assert [d for _, d in rows] == pieces
    assert [o for o, _ in rows] == [sum(len(p) for p in pieces[:i]) for i in range(len(pieces))]


# The layout, and reads, after any sequence of writes


def _random_append(rng: random.Random) -> bytes:
    kind = rng.choice(["line", "lines", "partial", "utf8", "empty", "newline", "long", "no newline", "bytes"])
    match kind:
        case "line":
            return b"x" * rng.randrange(0, 80) + b"\n"
        case "lines":
            return b"".join(b"y" * rng.randrange(0, 30) + b"\n" for _ in range(rng.randrange(1, 20)))
        case "partial":
            return b"\n".join(b"z" * rng.randrange(0, 20) for _ in range(rng.randrange(1, 4)))
        case "utf8":  # a snowman cut anywhere, perhaps mid-character
            text = ("☃ naïve\n" * rng.randrange(1, 4)).encode()
            return text[: rng.randrange(1, len(text) + 1)]
        case "empty":
            return b""
        case "newline":
            return b"\n"
        case "long":  # longer than an extent's target
            return b"L" * rng.randrange(TARGET - 10, 2 * TARGET) + rng.choice([b"", b"\n"])
        case "bytes":  # NUL, backslashes, high-bit bytes and newlines anywhere
            return rng.randbytes(rng.randrange(1, 300))
        case _:
            return b"n" * rng.randrange(1, 200)


@pytest.mark.parametrize("seed", range(6))
async def test_layout_and_reads_after_random_writes(conn: AsyncConnection, seed: int) -> None:
    rng = random.Random(seed)
    f = await create_file(conn, "f", rng.choice([None, b"", b"first\nline", b"one\n"]))
    content = (await read_all(conn, f, 1_000_000))[0]
    await check_layout(conn, f, content)
    for _ in range(25):
        op = rng.choices(["append", "write", "compact"], weights=[8, 1, 2])[0]
        if op == "append":
            data = _random_append(rng)
            assert await append(conn, f, data) == len(content)
            content += data
        elif op == "write":
            content = rng.choice(
                [
                    b"",
                    b"rewritten",
                    b"".join(f"line {i:06} {'p' * 20}\n".encode() for i in range(rng.randrange(1, 4000))),  # to 132 KB
                ]
            )
            await write(conn, f, content)
        else:
            await compact(conn, f, rng.choice([16, 1000, TARGET]))
        await check_layout(conn, f, content)
        await check_reads(conn, f, content, rng)


async def test_create_and_write_cut_large_content(conn: AsyncConnection) -> None:
    content = b"".join(f"{i:07}\n".encode() for i in range(30_000))  # 240 KB of 8-byte lines
    f = await create_file(conn, "f", content)
    await check_layout(conn, f, content)
    assert [len(d) for _, _, _, d, _ in await extent_rows(conn, f)] == [65536, 65536, 65536, 43392]
    content = content[:100_000] + b"no newline"
    await write(conn, f, content)
    await check_layout(conn, f, content)


async def test_create_of_a_taken_name_with_data_changes_nothing(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f", b"old\n")
    with raises_errno("EEXIST"):
        await create_file(conn, "f", b"new\nlines\n")
    await check_layout(conn, f, b"old\n")
    row = await one(conn, "SELECT count(*) FROM vfs_extents")
    assert row == (1,)


async def test_append_to_a_partial_line_rewrites_only_the_last_extent(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    for part in [b"a\n", b"b\n", b"c"]:
        await append(conn, f, part)
    cur = await conn.execute("SELECT byte_offset, ctid::text FROM vfs_extents WHERE node_id = %s ORDER BY 1", (f,))
    before = await cur.fetchall()
    assert await append(conn, f, b"d\ne") == 5
    cur = await conn.execute("SELECT byte_offset, ctid::text FROM vfs_extents WHERE node_id = %s ORDER BY 1", (f,))
    after = await cur.fetchall()
    assert after[:2] == before[:2]  # untouched
    assert after[2] != before[2]
    await check_layout(conn, f, b"a\nb\ncd\ne")


# vfs_pread_lines


async def test_pread_lines_examples(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f", b"ab\ncd\n\nlast")
    cur = await conn.execute("SELECT * FROM vfs_pread_lines(%s, 2, 2, 100)", (f,))
    assert await cur.fetchall() == [(11, 0, 4, 2, 3, b"cd\n\n")]
    cur = await conn.execute("SELECT * FROM vfs_pread_lines(%s, 4, 10, 100)", (f,))
    assert await cur.fetchall() == [(11, 0, 4, 4, 7, b"last")]
    cur = await conn.execute("SELECT * FROM vfs_pread_lines(%s, 1, 10, 4)", (f,))  # the byte cap cuts a line
    assert await cur.fetchall() == [(11, 0, 4, 1, 0, b"ab\nc")]
    # Past the end: no bytes, and the line count for the message.
    cur = await conn.execute("SELECT * FROM vfs_pread_lines(%s, 5, 10, 100)", (f,))
    assert await cur.fetchall() == [(11, 0, 4, 5, 11, b"")]


async def test_pread_lines_of_an_empty_file(conn: AsyncConnection) -> None:
    f = await create_file(conn, "f")
    cur = await conn.execute("SELECT * FROM vfs_pread_lines(%s, 1, 10, 100)", (f,))
    assert await cur.fetchall() == [(0, 0, 0, 1, 0, b"")]


async def test_pread_lines_across_many_extents(conn: AsyncConnection) -> None:
    rng = random.Random(3)
    f = await create_file(conn, "f")
    content = b""
    for i in range(400):
        part = f"{i}:{'w' * rng.randrange(0, 40)}".encode() + rng.choice([b"\n", b"\n\n", b"", b"\nmore"])
        await append(conn, f, part)
        content += part
    await check_layout(conn, f, content)
    await check_reads(conn, f, content, rng)
    await compact(conn, f, 256)
    await check_reads(conn, f, content, rng)


async def test_pread_lines_while_another_session_appends(conn: AsyncConnection, db: str) -> None:
    rng = random.Random(11)
    parts = [f"{i}{'.' * rng.randrange(0, 30)}".encode() + rng.choice([b"\n", b""]) for i in range(200)]
    full = b"".join(parts)
    f = await create_file(conn, "f")
    stop = asyncio.Event()

    async def appender() -> None:
        async with await AsyncConnection.connect(db, autocommit=True) as c:
            for part in parts:
                await append(c, f, part)
            stop.set()

    task = asyncio.create_task(appender())
    reads = partial = 0
    while not stop.is_set() or reads == 0:
        queries = [(rng.randrange(1, 300), rng.randrange(0, 50), rng.choice([10, 500, 2**31 - 1])) for _ in range(5)]
        rows = await pread_lines_many(conn, f, queries)
        for (first, max_lines, max_bytes), (size, _, lines, _, byte_offset, data) in zip(queries, rows, strict=True):
            seen = full[:size]  # every row describes one snapshot of the file
            assert lines == len(split_lines(seen))
            assert (byte_offset, data) == expected_lines(seen, first, max_lines, max_bytes)
        assert len({row[0] for row in rows}) == 1
        partial += 0 < rows[0][0] < len(full)
        reads += 1
    await task
    assert partial, "no read ran while the appends did"


async def test_line_reads_of_many_extents_use_index_steps(conn: AsyncConnection) -> None:
    f = await create_file(conn, "log")
    await conn.execute(
        "SELECT vfs_append(%s, convert_to(format('line %%s\n', i), 'UTF8')) FROM generate_series(1, 20000) i", (f,)
    )
    await conn.execute("ANALYZE vfs_extents")
    for first in [1, 10_000, 19_990]:
        cur = await conn.execute("SELECT data FROM vfs_pread_lines(%s, %s, 5, 1000)", (f, first))
        assert await cur.fetchall() == [("".join(f"line {i}\n" for i in range(first, first + 5)).encode(),)]
        plan = await plans_of(conn, "SELECT * FROM vfs_pread_lines(%s, %s, 5, 1000)", (f, first))
        # Steps 1 and 2: backward descents of the line index.
        assert plan.count("Scan Backward using vfs_extents_line_key") == 2, plan
        # Step 3: a forward scan of the primary key bounded on both sides.
        conds = [line for line in plan.splitlines() if "Index Cond:" in line and "byte_offset >= " in line]
        assert len(conds) == 1 and "byte_offset < " in conds[0], plan
        assert max_buffer_hits(plan) < 30, plan


# Concurrency


async def test_concurrent_appenders_stay_gap_free_and_line_aligned(conn: AsyncConnection, db: str) -> None:
    f = await create_file(conn, "log")

    async def appender(worker: int) -> list[tuple[int, bytes]]:
        rng = random.Random(worker)
        done: list[tuple[int, bytes]] = []
        async with await AsyncConnection.connect(db, autocommit=True) as c:
            for i in range(25):
                data = f"<{worker}:{i}>".encode() + rng.choice([b"\n", b"", b"\npartial", b"\n\n"])
                done.append((await append(c, f, data), data))
        return done

    results = await asyncio.gather(*(appender(w) for w in range(4)))
    landed = sorted(item for result in results for item in result)
    content, size = await read_all(conn, f, 1_000_000)
    offset = 0
    for at, data in landed:
        assert at == offset
        assert content[at : at + len(data)] == data
        offset += len(data)
    assert offset == size == len(content)
    await check_layout(conn, f, content)


# Migration


async def test_migration_cuts_existing_content(empty_db: str) -> None:
    migrations = load_migrations()
    before = [m for m in migrations if m.version < 7]
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        await migrate(conn, before)
        # Content written the 004 way: one extent per append, ending anywhere.
        a = await create_file(conn, "a")
        for part in [b"ab", b"c\nd", b"", b"e\n", b"\n", b"tail"]:
            await append(conn, a, part)
        b = await create_file(conn, "b", b"old")
        await write(conn, b, b"x\n" * 40_000)  # generation 1, one extent of 80 KB
        empty = await create_file(conn, "empty")
        d = await one(conn, "SELECT id FROM vfs_create(NULL, 'd', 'dir')")
        await migrate(conn, migrations)
        await check_layout(conn, a, b"abc\nde\n\ntail")
        await check_layout(conn, b, b"x\n" * 40_000)
        assert len(await extent_rows(conn, b)) == 2
        await check_layout(conn, empty, b"")
        assert await one(conn, "SELECT lines FROM vfs_nodes WHERE id = %s", (d[0],)) == (0,)
