"""vfs_nodes and the tree operations: vfs_resolve, vfs_readdir, vfs_create, vfs_mkdir_p, vfs_rename, vfs_unlink."""

import asyncio
import time
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from uuid import UUID

import psycopg
import pytest
from psycopg import AsyncConnection

from minifs.db.errors import SQLSTATE_BY_ERRNO, errno_name

Kind = str


@contextmanager
def raises_errno(name: str) -> Generator[None]:
    with pytest.raises(psycopg.Error) as info:
        yield
    assert errno_name(info.value) == name, info.value


async def _one(conn: AsyncConnection, query: str, params: tuple[object, ...] = ()) -> tuple[object, ...]:
    cur = await conn.execute(query.encode(), params)
    row = await cur.fetchone()
    assert row is not None
    return row


async def create(conn: AsyncConnection, parent: UUID | None, name: str, kind: Kind = "file") -> UUID:
    row = await _one(conn, "SELECT id FROM vfs_create(%s, %s, %s)", (parent, name, kind))
    assert isinstance(row[0], UUID)
    return row[0]


async def resolve(conn: AsyncConnection, *segments: str) -> tuple[UUID | None, Kind]:
    row = await _one(conn, "SELECT id, kind FROM vfs_resolve(%s)", (list(segments),))
    assert row[0] is None or isinstance(row[0], UUID)
    assert isinstance(row[1], str)
    return row[0], row[1]


async def readdir(conn: AsyncConnection, directory: UUID | None) -> list[tuple[str, Kind]]:
    cur = await conn.execute("SELECT name, kind FROM vfs_readdir(%s)", (directory,))
    return [(name, kind) for name, kind in await cur.fetchall()]


async def mkdir_p(conn: AsyncConnection, *segments: str) -> UUID | None:
    row = await _one(conn, "SELECT vfs_mkdir_p(%s)", (list(segments),))
    assert row[0] is None or isinstance(row[0], UUID)
    return row[0]


async def rename(
    conn: AsyncConnection, node: UUID, new_parent: UUID | None, new_name: str, expect_parent: UUID | None = None
) -> bool:
    row = await _one(conn, "SELECT vfs_rename(%s, %s, %s, %s)", (node, new_parent, new_name, expect_parent))
    assert isinstance(row[0], bool)
    return row[0]


async def unlink(conn: AsyncConnection, node: UUID) -> None:
    await conn.execute("SELECT vfs_unlink(%s)", (node,))


async def wait_until_blocked(conn: AsyncConnection, pid: int) -> None:
    """Waits until backend ``pid`` is waiting on a lock."""
    for _ in range(200):
        row = await _one(conn, "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (pid,))
        if row[0] == "Lock":
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"backend {pid} never blocked on a lock")


# uuidv7


async def test_uuidv7_ids(conn: AsyncConnection, pg_version: int) -> None:
    before = time.time()
    node = await create(conn, None, "f")
    after = time.time()
    assert node.version == 7
    millis = node.int >> 80
    assert before - 1 <= millis / 1000 <= after + 1
    # Built in from PostgreSQL 18; defined by migration 002 before that.
    row = await _one(conn, "SELECT to_regprocedure('public.uuidv7()') IS NOT NULL")
    assert row[0] == (pg_version < 180000)


async def test_uuidv7_sql_fallback_is_valid_v7(conn: AsyncConnection, pg_version: int) -> None:
    if pg_version >= 180000:
        pytest.skip("the built-in uuidv7() is used on PostgreSQL 18")
    cur = await conn.execute("SELECT uuidv7() FROM generate_series(1, 1000)")
    ids: list[UUID] = [row[0] for row in await cur.fetchall()]
    assert len(set(ids)) == 1000
    assert all(i.version == 7 and i.variant == uuid.RFC_4122 for i in ids)


# Schema


async def test_duplicate_root_names_are_rejected(conn: AsyncConnection) -> None:
    await create(conn, None, "docs", "dir")
    with raises_errno("EEXIST"):
        await create(conn, None, "docs", "dir")
    with raises_errno("EEXIST"):
        await conn.execute("INSERT INTO vfs_nodes (parent_id, name, kind) VALUES (NULL, 'docs', 'file')")


async def test_raw_insert_under_a_file_violates_the_foreign_key(conn: AsyncConnection) -> None:
    file = await create(conn, None, "f")
    with raises_errno("ENOTDIR"):
        await conn.execute("INSERT INTO vfs_nodes (parent_id, name, kind) VALUES (%s, 'x', 'file')", (file,))


async def test_raw_move_under_a_file_violates_the_foreign_key(conn: AsyncConnection) -> None:
    file = await create(conn, None, "f")
    other = await create(conn, None, "g")
    with raises_errno("ENOTDIR"):
        await conn.execute("UPDATE vfs_nodes SET parent_id = %s WHERE id = %s", (file, other))


async def test_raw_delete_of_non_empty_directory_is_restricted(conn: AsyncConnection) -> None:
    directory = await create(conn, None, "d", "dir")
    await create(conn, directory, "f")
    # PostgreSQL 18 reports RESTRICT as restrict_violation (23001), earlier versions as foreign_key_violation.
    with raises_errno("ENOTEMPTY"):
        await conn.execute("DELETE FROM vfs_nodes WHERE id = %s", (directory,))


async def test_kind_cannot_change(conn: AsyncConnection) -> None:
    file = await create(conn, None, "f")
    with raises_errno("EINVAL"):
        await conn.execute("UPDATE vfs_nodes SET kind = 'dir' WHERE id = %s", (file,))


@pytest.mark.parametrize("name", ["", ".", "..", "a/b", "x" * 256])
async def test_invalid_names_are_rejected(conn: AsyncConnection, name: str) -> None:
    with raises_errno("ENAMETOOLONG" if len(name) > 255 else "EINVAL"):
        await create(conn, None, name)
    with raises_errno("EINVAL"):
        await conn.execute("INSERT INTO vfs_nodes (name, kind) VALUES (%s, 'file')", (name,))


async def test_longest_name_is_255_bytes(conn: AsyncConnection) -> None:
    await create(conn, None, "é" * 127 + "x")  # 255 bytes
    with raises_errno("ENAMETOOLONG"):
        await create(conn, None, "é" * 128)  # 256 bytes


async def _fill_first_page(conn: AsyncConnection, table: str) -> str:
    """Inserts files into ``table`` until its first page is full; returns the ctid of a row on that page."""
    for i in range(1000):
        await conn.execute(f"INSERT INTO {table} (name, kind) VALUES ('file-{i:04d}', 'file')".encode())
        row = await _one(conn, f"SELECT max((ctid::text::point)[0]) FROM {table}")
        if row[0] == 1:
            break
    row = await _one(conn, f"SELECT id FROM {table} WHERE (ctid::text::point)[0] = 0 LIMIT 1")
    return str(row[0])


async def _page_after_updates(conn: AsyncConnection, table: str, node: str, updates: int) -> set[float]:
    """Updates ``node``'s size and mtime the way an append does, each in its own transaction; returns the pages
    its row version was on after each update."""
    pages: set[float] = set()
    for size in range(1, updates + 1):
        await conn.execute(f"UPDATE {table} SET size = %s, mtime = now() WHERE id = %s".encode(), (size, node))
        row = await _one(conn, f"SELECT (ctid::text::point)[0] FROM {table} WHERE id = %s", (node,))
        assert isinstance(row[0], float)
        pages.add(row[0])
    return pages


async def test_append_style_updates_are_hot(conn: AsyncConnection) -> None:
    # A HOT update keeps the new row version on the same page (no index changes). With fillfactor 80 a full page
    # still has room, and pruning reclaims the dead versions between transactions.
    row = await _one(conn, "SELECT reloptions FROM pg_class WHERE oid = 'vfs_nodes'::regclass")
    assert row[0] == ["fillfactor=80"]
    node = await _fill_first_page(conn, "vfs_nodes")
    assert await _page_after_updates(conn, "vfs_nodes", node, 50) == {0.0}

    # The control: the same table at the default fillfactor 100 moves the first update off the full page.
    await conn.execute("CREATE TABLE nodes_ff100 (LIKE vfs_nodes INCLUDING DEFAULTS INCLUDING INDEXES)")
    node = await _fill_first_page(conn, "nodes_ff100")
    assert 0.0 not in await _page_after_updates(conn, "nodes_ff100", node, 1)


# vfs_resolve


async def test_resolve_returns_id_and_kind(conn: AsyncConnection) -> None:
    docs = await create(conn, None, "docs", "dir")
    guide = await create(conn, docs, "guide.md")
    assert await resolve(conn) == (None, "dir")
    assert await resolve(conn, "docs") == (docs, "dir")
    assert await resolve(conn, "docs", "guide.md") == (guide, "file")


async def test_resolve_errors(conn: AsyncConnection) -> None:
    docs = await create(conn, None, "docs", "dir")
    await create(conn, docs, "guide.md")
    with raises_errno("ENOENT"):
        await resolve(conn, "nope")
    with raises_errno("ENOENT"):
        await resolve(conn, "docs", "nope", "deeper")
    with raises_errno("ENOTDIR"):
        await resolve(conn, "docs", "guide.md", "x")


async def test_resolve_does_not_confuse_root_entries_with_children(conn: AsyncConnection) -> None:
    docs = await create(conn, None, "docs", "dir")
    await create(conn, docs, "inner", "dir")
    with raises_errno("ENOENT"):
        await resolve(conn, "inner")


# vfs_readdir


async def test_readdir_lists_entries_in_byte_order(conn: AsyncConnection) -> None:
    directory = await create(conn, None, "d", "dir")
    for name in ["é", "a", "B", "_"]:
        await create(conn, directory, name)
    await create(conn, directory, "sub", "dir")
    assert await readdir(conn, directory) == [
        ("B", "file"),
        ("_", "file"),
        ("a", "file"),
        ("sub", "dir"),
        ("é", "file"),
    ]
    assert await readdir(conn, None) == [("d", "dir")]


async def test_readdir_returns_size_and_mtime(conn: AsyncConnection) -> None:
    directory = await create(conn, None, "d", "dir")
    file = await create(conn, directory, "f")
    cur = await conn.execute("SELECT id, name, kind, size, mtime FROM vfs_readdir(%s)", (directory,))
    rows = await cur.fetchall()
    assert len(rows) == 1
    assert rows[0][:4] == (file, "f", "file", 0)
    assert isinstance(rows[0][4], datetime)


async def test_readdir_errors(conn: AsyncConnection) -> None:
    file = await create(conn, None, "f")
    with raises_errno("ENOTDIR"):
        await readdir(conn, file)
    with raises_errno("ENOENT"):
        await readdir(conn, uuid.uuid4())


# vfs_create


async def test_create_returns_id_generation_and_size(conn: AsyncConnection) -> None:
    row = await _one(conn, "SELECT id, generation, size FROM vfs_create(NULL, 'f', 'file')")
    assert row[1:] == (0, 0)
    assert await resolve(conn, "f") == (row[0], "file")


async def test_create_errors(conn: AsyncConnection) -> None:
    file = await create(conn, None, "f")
    with raises_errno("EEXIST"):
        await create(conn, None, "f", "dir")
    with raises_errno("ENOTDIR"):
        await create(conn, file, "x")
    with raises_errno("ENOENT"):
        await create(conn, uuid.uuid4(), "x")
    with raises_errno("EINVAL"):
        await create(conn, None, "x", "symlink")


async def test_directory_mtime_is_not_touched_by_entries(conn: AsyncConnection) -> None:
    directory = await create(conn, None, "d", "dir")
    before = await _one(conn, "SELECT mtime, size FROM vfs_nodes WHERE id = %s", (directory,))
    file = await create(conn, directory, "f")
    await rename(conn, file, directory, "g")
    await unlink(conn, file)
    assert await _one(conn, "SELECT mtime, size FROM vfs_nodes WHERE id = %s", (directory,)) == before


# vfs_mkdir_p


async def test_mkdir_p_creates_missing_directories(conn: AsyncConnection) -> None:
    a = await mkdir_p(conn, "a")
    c = await mkdir_p(conn, "a", "b", "c")
    assert c is not None
    assert await resolve(conn, "a", "b", "c") == (c, "dir")
    assert await resolve(conn, "a") == (a, "dir")
    assert await mkdir_p(conn, "a", "b", "c") == c
    assert await mkdir_p(conn) is None


async def test_mkdir_p_errors(conn: AsyncConnection) -> None:
    a = await mkdir_p(conn, "a")
    await create(conn, a, "f")
    with raises_errno("ENOTDIR"):
        await mkdir_p(conn, "a", "f", "x")
    with raises_errno("EEXIST"):
        await mkdir_p(conn, "a", "f")
    with raises_errno("EINVAL"):
        await mkdir_p(conn, "a", "..", "x")


async def test_concurrent_mkdir_p_agree(conn: AsyncConnection, db: str) -> None:
    async with await AsyncConnection.connect(db) as first:
        created = await mkdir_p(first, "tasks", "new")  # uncommitted
        # The second one's insert of "tasks" waits on the first's; after the commit it finds the rows instead.
        second = asyncio.create_task(mkdir_p(conn, "tasks", "new"))
        await wait_until_blocked(first, conn.info.backend_pid)
        await first.commit()
    assert await second == created


async def test_many_concurrent_mkdir_p_agree(db: str) -> None:
    async def run() -> UUID | None:
        async with await AsyncConnection.connect(db, autocommit=True) as conn:
            return await mkdir_p(conn, "tasks", "new")

    results = await asyncio.gather(*(run() for _ in range(6)))
    assert len(set(results)) == 1 and results[0] is not None


# vfs_rename


async def test_rename_moves_and_renames(conn: AsyncConnection) -> None:
    src = await mkdir_p(conn, "src")
    dst = await mkdir_p(conn, "dst")
    file = await create(conn, src, "a")
    before = await _one(conn, "SELECT ctime, mtime FROM vfs_nodes WHERE id = %s", (file,))
    assert await rename(conn, file, dst, "b")
    assert await resolve(conn, "dst", "b") == (file, "file")
    assert await readdir(conn, src) == []
    after = await _one(conn, "SELECT ctime, mtime FROM vfs_nodes WHERE id = %s", (file,))
    assert after[1] == before[1]  # content unchanged
    assert after[0] != before[0]  # status changed (now() of a later transaction)
    assert await rename(conn, file, None, "top")
    assert await resolve(conn, "top") == (file, "file")
    assert await rename(conn, file, None, "top")  # no-op


async def test_rename_errors(conn: AsyncConnection) -> None:
    d = await mkdir_p(conn, "d")
    file = await create(conn, d, "a")
    await create(conn, d, "b")
    with raises_errno("EEXIST"):
        await rename(conn, file, d, "b")
    with raises_errno("ENOTDIR"):
        await rename(conn, file, file, "x")
    with raises_errno("ENOENT"):
        await rename(conn, uuid.uuid4(), d, "x")
    with raises_errno("ENOENT"):
        await rename(conn, file, uuid.uuid4(), "x")
    with raises_errno("EINVAL"):
        await rename(conn, file, d, "x/y")


async def test_rename_into_own_subtree_is_einval(conn: AsyncConnection) -> None:
    a = await mkdir_p(conn, "a")
    c = await mkdir_p(conn, "a", "b", "c")
    assert a is not None and c is not None
    with raises_errno("EINVAL"):
        await rename(conn, a, c, "a")  # mv /a /a/b/c/a
    with raises_errno("EINVAL"):
        await rename(conn, a, a, "a")
    assert await rename(conn, c, None, "c")  # moving a subdirectory up is fine


async def test_rename_compare_and_swap(conn: AsyncConnection) -> None:
    new = await mkdir_p(conn, "tasks", "new")
    cur = await mkdir_p(conn, "tasks", "cur")
    task = await create(conn, new, "t1")
    assert await rename(conn, task, cur, "t1", expect_parent=new)
    assert not await rename(conn, task, cur, "t1", expect_parent=new)
    assert await resolve(conn, "tasks", "cur", "t1") == (task, "file")


async def test_racing_claims_have_one_winner(db: str) -> None:
    async with (
        await AsyncConnection.connect(db, autocommit=True) as setup,
        await AsyncConnection.connect(db, autocommit=True) as first,
        await AsyncConnection.connect(db, autocommit=True) as second,
    ):
        new = await mkdir_p(setup, "tasks", "new")
        cur = await mkdir_p(setup, "tasks", "cur")
        task = await create(setup, new, "t1")
        async with first.transaction():
            assert await rename(first, task, cur, "t1-first", expect_parent=new)
            loser = asyncio.create_task(rename(second, task, cur, "t1-second", expect_parent=new))
            await wait_until_blocked(setup, second.info.backend_pid)
        assert not await loser
        assert await readdir(setup, cur) == [("t1-first", "file")]


async def test_concurrent_directory_moves_cannot_make_a_cycle(db: str) -> None:
    async with (
        await AsyncConnection.connect(db, autocommit=True) as setup,
        await AsyncConnection.connect(db, autocommit=True) as first,
        await AsyncConnection.connect(db, autocommit=True) as second,
    ):
        a = await mkdir_p(setup, "a")
        b = await mkdir_p(setup, "b")
        assert a is not None and b is not None
        async with first.transaction():
            assert await rename(first, a, b, "a")  # mv /a /b/a
            other = asyncio.create_task(rename(second, b, a, "b"))  # mv /b /a/b
            await wait_until_blocked(setup, second.info.backend_pid)
        with raises_errno("EINVAL"):
            await other
        assert await resolve(setup, "b", "a") == (a, "dir")


async def test_repeatable_read_moves_cannot_make_a_cycle(db: str) -> None:
    # A REPEATABLE READ snapshot taken before a concurrent move must not pass the cycle check on stale parents.
    async with (
        await AsyncConnection.connect(db, autocommit=True) as setup,
        await AsyncConnection.connect(db, autocommit=True) as mover,
        await AsyncConnection.connect(db) as stale,
    ):
        a = await mkdir_p(setup, "a")
        y = await mkdir_p(setup, "a", "y")
        b = await mkdir_p(setup, "b")
        x = await mkdir_p(setup, "b", "x")
        assert a is not None and b is not None
        await stale.set_isolation_level(psycopg.IsolationLevel.REPEATABLE_READ)
        await stale.execute("SELECT 1")  # takes the snapshot
        assert await rename(mover, a, x, "a")  # mv /a /b/x/a, committed after the snapshot
        with pytest.raises(psycopg.errors.SerializationFailure):
            await rename(stale, b, y, "b")  # mv /b /a/y/b would close the cycle a -> x -> b -> y -> a
        await stale.rollback()
        assert await resolve(setup, "b", "x", "a", "y") == (y, "dir")


async def test_open_transaction_in_one_directory_does_not_block_other_moves(db: str) -> None:
    async with (
        await AsyncConnection.connect(db, autocommit=True) as setup,
        await AsyncConnection.connect(db) as writer,
        await AsyncConnection.connect(db, autocommit=True) as blocked,
        await AsyncConnection.connect(db, autocommit=True) as mover,
    ):
        d = await mkdir_p(setup, "d")
        e = await mkdir_p(setup, "e")
        assert d is not None and e is not None
        await create(writer, d, "x")  # left uncommitted: holds FOR KEY SHARE on d
        moving_d = asyncio.create_task(rename(blocked, d, None, "d2"))  # waits for the writer
        await wait_until_blocked(setup, blocked.info.backend_pid)
        await mover.execute("SET statement_timeout = '5s'")
        assert await rename(mover, e, None, "e2")  # an unrelated directory move doesn't queue behind d's
        await writer.rollback()
        assert await moving_d


async def test_rename_racing_a_create_of_the_same_name(conn: AsyncConnection, db: str) -> None:
    d = await mkdir_p(conn, "d")
    file = await create(conn, d, "a")
    async with await AsyncConnection.connect(db) as other:
        await create(other, d, "b")  # uncommitted
        task = asyncio.create_task(rename(conn, file, d, "b"))
        await wait_until_blocked(other, conn.info.backend_pid)
        await other.commit()
    with raises_errno("EEXIST"):
        await task


# vfs_unlink


async def test_unlink(conn: AsyncConnection) -> None:
    d = await mkdir_p(conn, "d")
    assert d is not None
    file = await create(conn, d, "f")
    with raises_errno("ENOTEMPTY"):
        await unlink(conn, d)
    await unlink(conn, file)
    await unlink(conn, d)
    assert await readdir(conn, None) == []
    with raises_errno("ENOENT"):
        await unlink(conn, d)


async def test_unlink_waits_for_a_concurrent_create(conn: AsyncConnection, db: str) -> None:
    d = await mkdir_p(conn, "d")
    assert d is not None
    async with await AsyncConnection.connect(db) as other:
        await create(other, d, "f")  # uncommitted, holds FOR KEY SHARE on d
        task = asyncio.create_task(unlink(conn, d))
        await wait_until_blocked(other, conn.info.backend_pid)
        await other.commit()
    with raises_errno("ENOTEMPTY"):
        await task


# Errors


@pytest.mark.parametrize("name", sorted(SQLSTATE_BY_ERRNO))
async def test_every_errno_round_trips(conn: AsyncConnection, name: str) -> None:
    with pytest.raises(psycopg.Error) as info:
        await conn.execute("SELECT vfs_raise(%s, 'detail')", (name,))
    assert errno_name(info.value) == name
    assert info.value.sqlstate == SQLSTATE_BY_ERRNO[name]
    assert info.value.diag.message_primary == name
    assert info.value.diag.message_detail == "detail"


async def test_unknown_errno_is_a_plain_error(conn: AsyncConnection) -> None:
    with pytest.raises(psycopg.Error) as info:
        await conn.execute("SELECT vfs_raise('EWHAT', 'detail')")
    assert errno_name(info.value) is None
