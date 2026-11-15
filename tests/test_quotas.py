"""Write limits (migration 023): the per-directory quotas, what every writer charges, and the 64 MiB file cap."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs.db.errors import errno_name
from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import VFSClient
from minifs.vfs.core import MAX_FILE_SIZE, Quota, VFSError
from tests.test_client import raises
from tests.test_tree import Returned, check, create, mkdir_p, raises_db_errno, rename, wait_until_blocked

Pool = AsyncConnectionPool[AsyncConnection[Any]]


@pytest.fixture
async def pool(db: str) -> AsyncIterator[Pool]:
    async with open_pool(Connector(db), max_size=3) as p:
        yield p


@pytest.fixture
def vfs(pool: Pool) -> VFSClient:
    return VFSClient(pool)


async def _one(conn: AsyncConnection, query: str, params: tuple[object, ...] = ()) -> tuple[object, ...]:
    cur = await conn.execute(query.encode(), params)
    row = await cur.fetchone()
    assert row is not None
    return row


async def create_data(conn: AsyncConnection, parent: UUID | None, name: str, data: bytes) -> UUID:
    """vfs_create of a file with content, as tests.test_tree's ``create`` but for data."""
    row = await _one(conn, "SELECT id, err, err_detail FROM vfs_create(%s, %s, 'file', %s)", (parent, name, data))
    check(row[1], row[2])
    assert isinstance(row[0], UUID)
    return row[0]


async def create_hex(conn: AsyncConnection, name: str, size: int) -> UUID:
    """A file of ``size`` bytes of 'x', with its content built in SQL (never as Python bytes)."""
    row = await _one(
        conn,
        "SELECT id, err, err_detail FROM vfs_create(NULL, %s, 'file', decode(repeat('78', %s), 'hex'))",
        (name, size),
    )
    check(row[1], row[2])
    assert isinstance(row[0], UUID)
    return row[0]


async def quota_of_child(conn: AsyncConnection, parent: UUID, name: str) -> tuple[object, ...]:
    """(max_bytes, max_nodes, used_bytes, used_nodes) of the quota of the directory ``parent/name``."""
    return await _one(
        conn,
        "SELECT q.max_bytes, q.max_nodes, q.used_bytes, q.used_nodes FROM vfs_quotas q"
        " JOIN vfs_nodes n ON n.id = q.root WHERE n.parent_id = %s AND n.name = %s",
        (parent, name),
    )


# Accounting: every writer charges its change once, at its end


async def test_charges_of_every_operation(vfs: VFSClient) -> None:
    await vfs.mkdir("/q")
    assert await vfs.set_quota("/q", max_bytes=100, max_nodes=5) == Quota(100, 5, 0, 0)
    await vfs.create("/q/f", b"x" * 30)
    assert await vfs.quota("/q") == Quota(100, 5, 30, 1)
    await vfs.mkdir("/q/d")
    assert await vfs.quota("/q") == Quota(100, 5, 30, 2)
    await vfs.mkdir_p("/q/a/b/c")  # three directories
    assert await vfs.quota("/q") == Quota(100, 5, 30, 5)
    assert await vfs.append("/q/f", b"y" * 20) == 30
    assert await vfs.quota("/q") == Quota(100, 5, 50, 5)
    await vfs.write("/q/f", b"z" * 70)  # grows by 20
    assert await vfs.quota("/q") == Quota(100, 5, 70, 5)
    await vfs.write("/q/f", b"z" * 10)  # shrinks by 60
    assert await vfs.quota("/q") == Quota(100, 5, 10, 5)
    await vfs.unlink("/q/f")
    assert await vfs.quota("/q") == Quota(100, 5, 0, 4)
    await vfs.unlink("/q/d")  # an empty directory: just the node
    assert await vfs.quota("/q") == Quota(100, 5, 0, 3)
    # A rename within the one quota root charges nothing.
    await vfs.rename("/q/a", "/q/aa")
    assert await vfs.quota("/q") == Quota(100, 5, 0, 3)
    # Out of the quota and back: the subtree's bytes and nodes leave, then return.
    await vfs.create("/q/aa/g", b"g" * 25)
    assert await vfs.quota("/q") == Quota(100, 5, 25, 4)
    await vfs.rename("/q/aa", "/out")
    assert await vfs.quota("/q") == Quota(100, 5, 0, 0)
    await vfs.rename("/out", "/q/back")
    assert await vfs.quota("/q") == Quota(100, 5, 25, 4)
    # Compaction changes no size, so it charges nothing.
    await vfs.create("/q/log")
    for i in range(3):
        await vfs.append("/q/log", f"line {i}\n".encode())
    assert await vfs.quota("/q") == Quota(100, 5, 46, 5)
    assert await vfs.compact("/q/log") == 2
    assert await vfs.quota("/q") == Quota(100, 5, 46, 5)


async def test_every_quota_root_up_the_chain_is_charged(vfs: VFSClient) -> None:
    await vfs.mkdir_p("/t1/t2/t3")
    for path in ("/t1", "/t1/t2", "/t1/t2/t3"):
        await vfs.set_quota(path, max_bytes=100, max_nodes=10)
    await vfs.create("/t1/t2/t3/f", b"x" * 10)
    await vfs.append("/t1/t2/t3/f", b"y" * 15)
    assert await vfs.quota("/t1") == Quota(100, 10, 25, 3)  # t2, t3 and f below it
    assert await vfs.quota("/t1/t2") == Quota(100, 10, 25, 2)  # t3 and f
    assert await vfs.quota("/t1/t2/t3") == Quota(100, 10, 25, 1)  # f alone


# EDQUOT


async def test_edquot_leaves_nothing_changed(vfs: VFSClient) -> None:
    await vfs.mkdir("/q")
    await vfs.set_quota("/q", max_bytes=100, max_nodes=5)
    # A create over max_bytes: no node, usage unchanged.
    await raises("EDQUOT", vfs.create("/q/big", b"x" * 101), "/q/big")
    await raises("ENOENT", vfs.stat("/q/big"))
    assert await vfs.quota("/q") == Quota(100, 5, 0, 0)
    # An append over: the file keeps its content and size.
    await vfs.create("/q/f", b"a" * 60)
    await raises("EDQUOT", vfs.append("/q/f", b"b" * 41), "/q/f")
    result = await vfs.pread("/q/f", 0, 200)
    assert (result.content, result.size) == (b"a" * 60, 60)
    assert await vfs.quota("/q") == Quota(100, 5, 60, 1)
    # A write growing over: the old content stays, at its generation.
    await raises("EDQUOT", vfs.write("/q/f", b"c" * 101), "/q/f")
    assert (await vfs.pread("/q/f", 0, 200)).content == b"a" * 60
    assert (await vfs.stat("/q/f")).generation == 0
    # A mkdir_p over max_nodes: none of its directories exist.
    await raises("EDQUOT", vfs.mkdir_p("/q/x/y/z/w/v"), "/q/x/y/z/w/v")
    await raises("ENOENT", vfs.stat("/q/x"))
    assert await vfs.quota("/q") == Quota(100, 5, 60, 1)
    # A rename into a quota that can't hold the subtree: it didn't move.
    await vfs.create("/src/g", b"s" * 50, parents=True)
    await raises("EDQUOT", vfs.rename("/src/g", "/q/g"), "/src/g")
    assert (await vfs.stat("/src/g")).size == 50
    await raises("ENOENT", vfs.stat("/q/g"))
    assert await vfs.quota("/q") == Quota(100, 5, 60, 1)


async def test_edquot_rolls_back_the_parents_a_create_made(vfs: VFSClient) -> None:
    """A create with parents runs mkdir_p and the create in one statement and one transaction: an EDQUOT on the
    file's bytes rolls the directories it created back too."""
    await vfs.mkdir("/q")
    await vfs.set_quota("/q", max_bytes=10)
    await raises("EDQUOT", vfs.create("/q/a/b/f", b"x" * 11, parents=True), "/q/a/b/f")
    await raises("ENOENT", vfs.stat("/q/a"))
    assert await vfs.quota("/q") == Quota(10, None, 0, 0)


async def test_a_cross_quota_rename_over_max_nodes_moves_nothing(vfs: VFSClient) -> None:
    """The moved subtree's nodes join the quota roots above the destination: a directory of 2 nodes (itself and
    one child) doesn't fit a quota of 2 nodes with 1 used, and nothing moves."""
    await vfs.mkdir("/q")
    await vfs.set_quota("/q", max_nodes=2)
    await vfs.create("/q/f")
    await vfs.mkdir_p("/src/d/e")
    assert await vfs.quota("/q") == Quota(None, 2, 0, 1)
    await raises("EDQUOT", vfs.rename("/src/d", "/q/d"), "/src/d")
    assert await vfs.quota("/q") == Quota(None, 2, 0, 1)
    assert (await vfs.stat("/src/d/e")).is_dir  # the subtree is untouched
    await raises("ENOENT", vfs.stat("/q/d"))


async def test_a_shrinking_write_is_never_refused(vfs: VFSClient) -> None:
    """A decrease is never refused, even when the usage is over a lowered max; only increases are."""
    await vfs.mkdir("/q")
    await vfs.create("/q/f", b"a" * 60)
    await vfs.set_quota("/q", max_bytes=100)
    # Lowered below the usage: the usage is kept.
    assert await vfs.set_quota("/q", max_bytes=10) == Quota(10, None, 60, 1)
    await vfs.write("/q/f", b"b" * 5)  # the shrink goes through
    assert await vfs.quota("/q") == Quota(10, None, 5, 1)
    await raises("EDQUOT", vfs.write("/q/f", b"c" * 50), "/q/f")  # growing is refused from then on
    assert (await vfs.pread("/q/f", 0, 10)).content == b"b" * 5


# Nesting


async def test_quotas_nest(vfs: VFSClient) -> None:
    """A quota inside a quota: both are charged, the outer one's usage includes the inner one's, and the
    tighter inner one refuses what the outer would still allow."""
    await vfs.mkdir_p("/outer/inner")
    await vfs.set_quota("/outer", max_bytes=100, max_nodes=10)
    await vfs.set_quota("/outer/inner", max_bytes=30, max_nodes=3)
    assert await vfs.quota("/outer") == Quota(100, 10, 0, 1)  # the inner directory itself
    await vfs.create("/outer/inner/f", b"x" * 20)
    assert await vfs.quota("/outer/inner") == Quota(30, 3, 20, 1)
    assert await vfs.quota("/outer") == Quota(100, 10, 20, 2)
    # 20 more bytes fit the outer quota but not the inner one: the inner refuses, with nothing changed.
    inner = (await vfs.stat("/outer/inner")).id
    error = await raises("EDQUOT", vfs.create("/outer/inner/g", b"y" * 20), "/outer/inner/g")
    assert str(inner) in error.detail  # the inner quota's root is the one named
    assert await vfs.quota("/outer/inner") == Quota(30, 3, 20, 1)
    assert await vfs.quota("/outer") == Quota(100, 10, 20, 2)
    await vfs.create("/outer/h", b"y" * 20)  # the same bytes fit outside the inner quota
    assert await vfs.quota("/outer") == Quota(100, 10, 40, 3)


async def test_moving_a_quota_root_keeps_its_own_quota(vfs: VFSClient) -> None:
    """A quota on a moved directory moves with it, usage intact; the quotas above the old place lose the
    subtree and the ones above the new place gain it."""
    await vfs.mkdir_p("/a/q")
    await vfs.mkdir("/b")
    await vfs.set_quota("/a", max_bytes=1000, max_nodes=100)
    await vfs.set_quota("/b", max_bytes=1000, max_nodes=100)
    await vfs.set_quota("/a/q", max_bytes=100, max_nodes=5)
    await vfs.create("/a/q/f", b"x" * 30)
    assert await vfs.quota("/a/q") == Quota(100, 5, 30, 1)
    assert await vfs.quota("/a") == Quota(1000, 100, 30, 2)
    assert await vfs.quota("/b") == Quota(1000, 100, 0, 0)
    await vfs.rename("/a/q", "/b/q")
    assert await vfs.quota("/b/q") == Quota(100, 5, 30, 1)
    assert await vfs.quota("/a") == Quota(1000, 100, 0, 0)
    assert await vfs.quota("/b") == Quota(1000, 100, 30, 2)


# Child quotas


async def test_child_quotas(vfs: VFSClient, conn: AsyncConnection) -> None:
    await vfs.mkdir_p("/workspaces/w1")
    await vfs.mkdir("/workspaces/w2")
    await vfs.create("/workspaces/w1/f", b"x" * 12)
    await vfs.set_quota("/workspaces/w2", max_bytes=7, max_nodes=1)
    # Every existing child directory without a quota of its own gets one now, with recounted usage;
    # a child that already has its own quota keeps it.
    assert await vfs.set_child_quota("/workspaces", max_bytes=100, max_nodes=5) == 1
    assert await vfs.quota("/workspaces/w1") == Quota(100, 5, 12, 1)
    assert await vfs.quota("/workspaces/w2") == Quota(7, 1, 0, 0)
    # A directory created later gets the quota at its creation: by mkdir, by mkdir_p, by a create with parents.
    await vfs.mkdir("/workspaces/w3")
    assert await vfs.quota("/workspaces/w3") == Quota(100, 5, 0, 0)
    await vfs.mkdir_p("/workspaces/w/a/b")
    assert await vfs.quota("/workspaces/w") == Quota(100, 5, 0, 2)  # a and b below it
    assert await vfs.quota("/workspaces/w/a") is None  # the default covers the children of /workspaces alone
    await vfs.create("/workspaces/p/f", b"y" * 4, parents=True)
    assert await vfs.quota("/workspaces/p") == Quota(100, 5, 4, 1)
    # A file created in /workspaces gets no quota: the rows are w1, w2, w3, w and p.
    await vfs.create("/workspaces/loose.txt", b"z")
    assert await _one(conn, "SELECT count(*) FROM vfs_quotas") == (5,)
    # Both None stops it: a later child gets none, and the quotas already given stay.
    assert await vfs.set_child_quota("/workspaces") == 0
    await vfs.mkdir("/workspaces/w9")
    assert await vfs.quota("/workspaces/w9") is None
    assert await vfs.quota("/workspaces/w1") == Quota(100, 5, 12, 1)
    assert await _one(conn, "SELECT count(*) FROM vfs_quotas") == (5,)


async def test_mkdir_p_over_an_inherited_max_nodes_is_edquot(vfs: VFSClient, conn: AsyncConnection) -> None:
    """Every directory a mkdir_p creates inherits the child quota at its creation and counts the ones created
    below it: 3 directories below an inherited max_nodes of 2 is EDQUOT, and since the whole call is one
    transaction, nothing it created stays — not even the directory that got the quota."""
    await vfs.mkdir("/workspaces")
    assert await vfs.set_child_quota("/workspaces", max_nodes=2) == 0
    await raises("EDQUOT", vfs.mkdir_p("/workspaces/w/a/b/c"), "/workspaces/w/a/b/c")
    await raises("ENOENT", vfs.stat("/workspaces/w"))
    assert await _one(conn, "SELECT count(*) FROM vfs_quotas") == (0,)  # the inherited row rolled back too
    assert await _one(conn, "SELECT count(*) FROM vfs_quota_defaults") == (1,)  # the defaults row is committed


# vfs_set_quota


async def test_set_quota_recounts_after_drift(vfs: VFSClient, conn: AsyncConnection) -> None:
    await vfs.mkdir("/q")
    await vfs.set_quota("/q", max_bytes=100, max_nodes=5)
    await vfs.create("/q/f", b"x" * 30)
    q = (await vfs.stat("/q")).id
    # Raw SQL isn't charged: the usage drifts.
    await conn.execute("UPDATE vfs_quotas SET used_bytes = 999 WHERE root = %s", (q,))
    assert await vfs.quota("/q") == Quota(100, 5, 999, 1)
    # Setting the quota again recounts it under the row lock.
    assert await vfs.set_quota("/q", max_bytes=100, max_nodes=5) == Quota(100, 5, 30, 1)
    assert await vfs.quota("/q") == Quota(100, 5, 30, 1)


async def test_set_quota_removes_and_reports_errors(vfs: VFSClient, conn: AsyncConnection) -> None:
    await vfs.mkdir("/q")
    assert await vfs.quota("/q") is None  # no quota yet
    await vfs.set_quota("/q", max_bytes=100)
    assert await vfs.set_quota("/q") is None  # both None removes it
    assert await vfs.quota("/q") is None
    assert await _one(conn, "SELECT count(*) FROM vfs_quotas") == (0,)
    await raises("ENOENT", vfs.quota("/missing"), "/missing")
    await raises("ENOENT", vfs.set_quota("/missing", max_bytes=1), "/missing")
    await raises("ENOENT", vfs.set_child_quota("/missing", max_bytes=1), "/missing")
    await vfs.create("/f")
    await raises("ENOTDIR", vfs.quota("/f"), "/f")
    await raises("ENOTDIR", vfs.set_quota("/f", max_bytes=1), "/f")
    await raises("EINVAL", vfs.quota("/"), "/")  # the root can't have a quota
    await raises("EINVAL", vfs.set_quota("/", max_bytes=1), "/")
    await raises("EINVAL", vfs.set_quota("/q", max_bytes=-1), "/q")
    await raises("EINVAL", vfs.set_quota("/q", max_nodes=-1), "/q")
    await raises("EINVAL", vfs.set_child_quota("/q", max_bytes=-1), "/q")


async def test_the_quota_row_goes_with_its_directory(vfs: VFSClient, conn: AsyncConnection) -> None:
    await vfs.mkdir("/q")
    await vfs.set_quota("/q", max_bytes=100)
    assert await _one(conn, "SELECT count(*) FROM vfs_quotas") == (1,)
    await vfs.unlink("/q")
    assert await _one(conn, "SELECT count(*) FROM vfs_quotas") == (0,)


async def test_unlink_removes_the_child_quota_defaults(vfs: VFSClient, conn: AsyncConnection) -> None:
    """vfs_unlink of a directory removes its vfs_quota_defaults row too, not just its vfs_quotas row."""
    await vfs.mkdir("/p")
    assert await vfs.set_child_quota("/p", max_bytes=100) == 0
    assert await _one(conn, "SELECT count(*) FROM vfs_quota_defaults") == (1,)
    await vfs.unlink("/p")
    assert await _one(conn, "SELECT count(*) FROM vfs_quota_defaults") == (0,)


# Concurrency


async def test_two_creates_under_one_quota_take_turns(conn: AsyncConnection, db: str) -> None:
    """The second create waits for the quota row lock the first one's charge took, and after its commit sees
    its bytes: of two 60-byte creates under a 100-byte quota, exactly one fits."""
    q = await mkdir_p(conn, "q")
    await _one(conn, "SELECT used_bytes FROM vfs_set_quota(%s, 100, NULL)", (q,))
    async with await AsyncConnection.connect(db) as first:
        await create_data(first, q, "a", b"x" * 60)  # uncommitted: its charge holds the quota row FOR UPDATE
        second = asyncio.create_task(create_data(conn, q, "b", b"y" * 60))
        await wait_until_blocked(first, conn.info.backend_pid)
        await first.commit()
    with raises_db_errno("EDQUOT"):
        await second
    assert await _one(conn, "SELECT used_bytes, used_nodes FROM vfs_quotas WHERE root = %s", (q,)) == (60, 1)
    assert await _one(conn, "SELECT count(*) FROM vfs_nodes WHERE parent_id = %s", (q,)) == (1,)


async def test_a_create_without_a_quota_never_waits_on_one(conn: AsyncConnection, db: str) -> None:
    """Without any quota row a charge locks nothing, so a create doesn't wait for another transaction's
    uncommitted create in the same directory (the twin of the test above, minus the quota)."""
    nq = await mkdir_p(conn, "nq")
    async with await AsyncConnection.connect(db) as first:
        await create_data(first, nq, "a", b"x" * 60)  # uncommitted
        second = asyncio.create_task(create_data(conn, nq, "b", b"y" * 60))
        assert await asyncio.wait_for(second, timeout=30) is not None  # it didn't wait for the first
        await first.rollback()
    assert await _one(conn, "SELECT count(*) FROM vfs_nodes WHERE parent_id = %s", (nq,)) == (1,)


async def give_pending(conn: AsyncConnection, parent: UUID) -> list[bool]:
    """What VFSClient.set_child_quota does after vfs_set_child_quota: each pending child, one statement each."""
    cur = await conn.execute("SELECT c FROM vfs_quota_pending_children(%s) c", (parent,))
    return [
        bool((await _one(conn, "SELECT vfs_give_child_quota(%s, %s)", (child, parent)))[0])
        for (child,) in await cur.fetchall()
    ]


async def test_a_directory_created_while_set_child_quota_runs_gets_the_quota(conn: AsyncConnection, db: str) -> None:
    """vfs_set_child_quota locks the parent FOR UPDATE (023): a mkdir_p that starts while the set is in flight
    waits for it and then sees the defaults row, and one already in flight commits first, so the pending children
    listed after the set include it. Either way the new directory gets the quota."""
    workspaces = await mkdir_p(conn, "workspaces")
    assert workspaces is not None
    async with await AsyncConnection.connect(db) as first:
        cur = await first.execute("SELECT err FROM vfs_set_child_quota(%s, 100, 5)", (workspaces,))
        assert await cur.fetchone() == (None,)  # uncommitted
        made = asyncio.create_task(mkdir_p(conn, "workspaces", "new"))
        await wait_until_blocked(first, conn.info.backend_pid)  # on the parent's FOR UPDATE
        await first.commit()
    assert await made is not None
    assert await quota_of_child(conn, workspaces, "new") == (100, 5, 0, 0)  # inherited at its creation
    assert await give_pending(conn, workspaces) == []

    # The other order: the mkdir_p is in flight (its FOR KEY SHARE on the parent holds), so the set waits for
    # it to commit; the directory is then a pending child and gets the quota.
    projects = await mkdir_p(conn, "projects")
    assert projects is not None
    async with await AsyncConnection.connect(db) as second:
        assert await mkdir_p(second, "projects", "new") is not None  # uncommitted
        setting = asyncio.create_task(_one(conn, "SELECT err FROM vfs_set_child_quota(%s, 100, 5)", (projects,)))
        await wait_until_blocked(second, conn.info.backend_pid)  # on the mkdir_p's lock of the parent
        await second.commit()
    assert await setting == (None,)
    assert await give_pending(conn, projects) == [True]
    assert await quota_of_child(conn, projects, "new") == (100, 5, 0, 0)


async def test_giving_child_quotas_blocks_no_write_elsewhere(conn: AsyncConnection, db: str) -> None:
    """#83 review round 2: a child's quota is given and recounted in a transaction of its own, with no lock on
    the parent, so while one is in flight a mkdir_p into another workspace, into the same one, and a new
    workspace (which inherits the committed child quota) all go through without waiting."""
    workspaces = await mkdir_p(conn, "workspaces")
    ws1 = await mkdir_p(conn, "workspaces", "ws1")
    assert workspaces is not None and ws1 is not None
    assert await mkdir_p(conn, "workspaces", "ws2") is not None
    assert await _one(conn, "SELECT err FROM vfs_set_child_quota(%s, 100, 5)", (workspaces,)) == (None,)
    async with await AsyncConnection.connect(db) as giver:
        cur = await giver.execute("SELECT vfs_give_child_quota(%s, %s)", (ws1, workspaces))
        assert await cur.fetchone() == (True,)  # uncommitted, ws1's quota row locked
        for path in (("workspaces", "ws2", "a"), ("workspaces", "ws1", "b"), ("workspaces", "ws3")):
            assert await asyncio.wait_for(mkdir_p(conn, *path), timeout=10) is not None, path
        await giver.commit()
    assert await quota_of_child(conn, workspaces, "ws3") == (100, 5, 0, 0)
    # ws1's first recount ran before b was made and b's charge didn't see the uncommitted row: the documented
    # window of a new row, now one child wide. A recount fixes it.
    assert await quota_of_child(conn, workspaces, "ws1") == (100, 5, 0, 0)
    assert await give_pending(conn, workspaces) == [True]  # ws2


async def test_two_child_quota_sets_on_one_parent_take_turns(conn: AsyncConnection, db: str) -> None:
    """The parent's FOR UPDATE is the set's first lock, so a second set waits for the first instead of the two
    deadlocking on an upgrade from FOR KEY SHARE; the later one wins."""
    parent = await mkdir_p(conn, "p")
    assert parent is not None
    async with (
        await AsyncConnection.connect(db) as first,
        await AsyncConnection.connect(db) as second,
    ):
        await first.execute("SELECT err FROM vfs_set_child_quota(%s, 100, 5)", (parent,))
        later = asyncio.create_task(_one(second, "SELECT err FROM vfs_set_child_quota(%s, 7, 1)", (parent,)))
        await wait_until_blocked(first, second.info.backend_pid)
        await first.commit()
        assert await asyncio.wait_for(later, timeout=10) == (None,)
        await second.commit()
    assert await _one(conn, "SELECT max_bytes, max_nodes FROM vfs_quota_defaults WHERE parent = %s", (parent,)) == (
        7,
        1,
    )


async def test_a_child_moved_away_or_given_its_own_quota_is_skipped(conn: AsyncConnection, db: str) -> None:
    """vfs_give_child_quota re-reads the child under its lock: a child a rename in flight moves out of the parent
    gets nothing, nor does one that has a quota of its own by then (it isn't overwritten)."""
    workspaces = await mkdir_p(conn, "workspaces")
    moved = await mkdir_p(conn, "workspaces", "moved")
    own = await mkdir_p(conn, "workspaces", "own")
    other = await mkdir_p(conn, "other")
    assert workspaces is not None and moved is not None and own is not None and other is not None
    assert await _one(conn, "SELECT err FROM vfs_set_child_quota(%s, 100, 5)", (workspaces,)) == (None,)
    async with await AsyncConnection.connect(db) as renamer:
        assert await rename(renamer, moved, other, "moved")  # uncommitted
        giving = asyncio.create_task(_one(conn, "SELECT vfs_give_child_quota(%s, %s)", (moved, workspaces)))
        await wait_until_blocked(renamer, conn.info.backend_pid)  # on the rename's lock of the child
        await renamer.commit()
    assert await giving == (False,)
    assert await _one(conn, "SELECT count(*) FROM vfs_quotas WHERE root = %s", (moved,)) == (0,)
    await _one(conn, "SELECT used_bytes FROM vfs_set_quota(%s, 7, NULL)", (own,))
    assert await _one(conn, "SELECT vfs_give_child_quota(%s, %s)", (own, workspaces)) == (False,)
    assert await _one(conn, "SELECT max_bytes, max_nodes FROM vfs_quotas WHERE root = %s", (own,)) == (7, None)
    # Once the child quota is stopped, nothing is given either.
    fresh = await mkdir_p(conn, "workspaces", "fresh")
    assert fresh is not None
    await conn.execute("DELETE FROM vfs_quotas WHERE root = %s", (fresh,))
    assert await _one(conn, "SELECT err FROM vfs_set_child_quota(%s, NULL, NULL)", (workspaces,)) == (None,)
    assert await _one(conn, "SELECT vfs_give_child_quota(%s, %s)", (fresh, workspaces)) == (False,)


async def test_set_child_quota_carries_on_past_a_child_that_runs_out_of_time(
    pool: Pool, vfs: VFSClient, conn: AsyncConnection, db: str
) -> None:
    """#83 review round 3: a child whose give runs out of time (here EAGAIN, behind a lock another connection
    holds) is skipped, the children after it still get their quota, and the error names that child's path and
    every child left without one; calling again finishes."""
    for name in ("a", "b", "c"):
        await vfs.mkdir_p(f"/workspaces/{name}")
    b = await _one(conn, "SELECT n.id FROM vfs_nodes n WHERE n.name = 'b'")
    async with await AsyncConnection.connect(db) as holder:
        await holder.execute("SELECT FROM vfs_nodes WHERE id = %s FOR UPDATE", b)
        with pytest.raises(VFSError) as info:
            await VFSClient(pool, lock_timeout=0.2).set_child_quota("/workspaces", max_bytes=100, max_nodes=5)
        await holder.rollback()
    assert (info.value.errno, info.value.path) == ("EAGAIN", "/workspaces/b")
    assert "2 child directories got the quota, 1 didn't (/workspaces/b)" in info.value.message
    assert await vfs.quota("/workspaces/a") == Quota(100, 5, 0, 0)
    assert await vfs.quota("/workspaces/b") is None
    assert await vfs.quota("/workspaces/c") == Quota(100, 5, 0, 0)
    assert await vfs.set_child_quota("/workspaces", max_bytes=100, max_nodes=5) == 1


async def test_a_legacy_over_deep_node_can_still_be_removed_and_moved_up(conn: AsyncConnection) -> None:
    """With a quota anywhere, an unlink and a rename's source walk their ancestors only up to the cap (#83
    review round 2): a node of a legacy chain deeper than vfs_max_components() can still be removed, and moved up
    (022's repair). A root the walk misses keeps the usage given back, which only overstates it. A charge that
    adds usage there is still ENAMETOOLONG."""
    q = await mkdir_p(conn, "q")
    assert q is not None
    await _one(conn, "SELECT used_bytes FROM vfs_set_quota(%s, NULL, 5000)", (q,))
    await conn.execute(
        """
        DO $$
        DECLARE v_parent uuid := (SELECT id FROM vfs_nodes WHERE parent_id IS NULL AND name = 'q'); v_id uuid;
        BEGIN
            FOR i IN 1..1100 LOOP
                INSERT INTO vfs_nodes (parent_id, name, kind) VALUES (v_parent, 'd', 'dir') RETURNING id INTO v_id;
                v_parent := v_id;
            END LOOP;
            INSERT INTO vfs_nodes (parent_id, name, kind) VALUES (v_parent, 'leaf', 'dir');
        END $$"""
    )
    leaf, deepest = await _one(conn, "SELECT id, parent_id FROM vfs_nodes WHERE name = 'leaf'")
    assert isinstance(leaf, UUID) and isinstance(deepest, UUID)
    with raises_db_errno("ENAMETOOLONG"):  # adding usage needs every root
        await create(conn, deepest, "f", "dir")
    gone = await _one(
        conn, "INSERT INTO vfs_nodes (parent_id, name, kind) VALUES (%s, 'gone', 'dir') RETURNING id", (deepest,)
    )
    assert await _one(conn, "SELECT err FROM vfs_unlink(%s)", gone) == (None,)  # removed: the walk stops at the cap
    assert await rename(conn, leaf, deepest, "renamed")  # same parent: walks nothing, refused for nothing (022)
    # A shrinking write gives usage back, so it walks partially too (a raw file: creating one there is refused).
    f = await _one(
        conn,
        "INSERT INTO vfs_nodes (parent_id, name, kind, size) VALUES (%s, 'f', 'file', 10) RETURNING id",
        (deepest,),
    )
    assert await _one(conn, "SELECT err FROM vfs_write(%s, 'x')", f) == (None,)
    assert await rename(conn, leaf, q, "up")  # moved up: the source walk stops at the cap
    assert await _one(conn, "SELECT used_nodes FROM vfs_quotas WHERE root = %s", (q,)) == (1,)  # added at q
    sub = await mkdir_p(conn, "q", "up", "x")
    assert sub is not None
    with raises_db_errno("ENAMETOOLONG"):  # a move down past the cap: the destination needs every root
        await rename(conn, sub, deepest, "x")


async def test_a_rename_racing_a_create_of_its_destination_does_not_deadlock(conn: AsyncConnection, db: str) -> None:
    """A cross-quota rename charges the destination's quota rows only after its UPDATE (023): quota rows are
    every writer's last lock. A third connection holds q's quota row, so each statement stops at its charge:
    the rename has already taken the name (q, 'x') by then, and a create of (q, 'x') waits on that name, not
    holding q's row. Released, the rename commits and the create gets EEXIST. Had the rename charged first, it
    would wait on q's row without the name, the create would insert (q, 'x') and wait on q's row too, and the
    two would deadlock once the row was released (DeadlockDetected fails this test)."""
    q = await mkdir_p(conn, "q")
    src = await mkdir_p(conn, "src")
    assert q is not None and src is not None
    f = await create_data(conn, src, "f", b"x" * 10)
    await _one(conn, "SELECT used_bytes FROM vfs_set_quota(%s, 100, NULL)", (q,))
    async with (
        await AsyncConnection.connect(db) as holder,
        await AsyncConnection.connect(db) as renamer,
        await AsyncConnection.connect(db) as creator,
    ):
        await holder.execute("SELECT 1 FROM vfs_quotas WHERE root = %s FOR UPDATE", (q,))
        renamed = asyncio.create_task(rename(renamer, f, q, "x"))
        await wait_until_blocked(holder, renamer.info.backend_pid)  # at its charge, the name already taken
        created = asyncio.create_task(create_data(creator, q, "x", b"y" * 5))
        await wait_until_blocked(holder, creator.info.backend_pid)  # on the rename's uncommitted name
        await holder.commit()
        assert await asyncio.wait_for(renamed, 10) is True
        await renamer.commit()
        with pytest.raises((Returned, psycopg.Error)) as info:
            await asyncio.wait_for(created, 10)
        await creator.rollback()
    error = info.value
    assert not isinstance(error, psycopg.errors.DeadlockDetected), "the create deadlocked with the rename"
    if isinstance(error, Returned):
        assert error.errno == "EEXIST"
    else:
        assert errno_name(error) == "EEXIST", error  # the unique violation of the committed (q, 'x')
    # The rename won: f is q/x, and q is charged its 10 bytes and 1 node, not the create's.
    assert await _one(conn, "SELECT parent_id, name FROM vfs_nodes WHERE id = %s", (f,)) == (q, "x")
    assert await _one(conn, "SELECT used_bytes, used_nodes FROM vfs_quotas WHERE root = %s", (q,)) == (10, 1)


# The 64 MiB file cap (EFBIG), at the SQL level; the over-cap content is built in SQL, not in Python


async def test_max_file_size_matches_the_client(conn: AsyncConnection) -> None:
    assert await _one(conn, "SELECT vfs_max_file_size()") == (MAX_FILE_SIZE,)


async def test_create_over_the_file_cap_is_efbig(conn: AsyncConnection) -> None:
    with raises_db_errno("EFBIG"):
        await create_hex(conn, "f", MAX_FILE_SIZE + 1)
    assert await _one(conn, "SELECT count(*) FROM vfs_nodes") == (0,)


async def test_write_over_the_file_cap_is_efbig(conn: AsyncConnection) -> None:
    f = await create_data(conn, None, "f", b"old")
    with raises_db_errno("EFBIG"):
        await conn.execute("SELECT vfs_write(%s, convert_to(repeat('x', %s), 'UTF8'))", (f, MAX_FILE_SIZE + 1))
    assert await _one(conn, "SELECT size, generation FROM vfs_nodes WHERE id = %s", (f,)) == (3, 0)


async def test_a_file_at_the_cap_refuses_one_more_byte(conn: AsyncConnection) -> None:
    f = await create_hex(conn, "f", MAX_FILE_SIZE)  # exactly the cap is fine
    assert await _one(conn, "SELECT size FROM vfs_nodes WHERE id = %s", (f,)) == (MAX_FILE_SIZE,)
    with raises_db_errno("EFBIG"):
        await conn.execute("SELECT vfs_append(%s, %s)", (f, b"x"))
    assert await _one(conn, "SELECT size FROM vfs_nodes WHERE id = %s", (f,)) == (MAX_FILE_SIZE,)
