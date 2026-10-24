"""VFSClient over a real database, and the pools of minifs.db.pool: credentials, reconnects, retries."""

import asyncio
import pickle
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from minifs.db.errors import SQLSTATE_BY_ERRNO
from minifs.db.pool import Connector, Pools, open_pool, retry_serializable
from minifs.vfs.client import VFSClient
from minifs.vfs.core import FileVersion, Kind, VFSError
from minifs.vfs.glob import GlobError

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


# Every errno the SQL layer raises comes back as a VFSError with that errno and the normalised path.


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
    assert isinstance(error.db_error, psycopg.Error)
    assert error.db_error.sqlstate == SQLSTATE_BY_ERRNO[errno]
    assert error.__cause__ is None and error.__suppress_context__  # no DETAIL in tracebacks
    if errno in ("EEXIST", "ENOTDIR"):  # re-reported for another path, still with its database error
        assert error.at("/x").db_error is error.db_error
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
    # The stale chain is gone; nothing cached points at the moved directory under its old name.
    await vfs.settle()
    assert all(name != "d" for _, name in cache.entries)
    assert (await vfs.claim("/d/x", "/e/z")) is True
    assert (await other.pread("/e/z")).content == b"other"


async def test_eviction_without_a_fully_cached_path(pool: Pool) -> None:
    cache = DictCache()
    vfs = VFSClient(pool, cache=cache)
    d = await vfs.mkdir("/d")
    f = await vfs.create("/d/f")
    await vfs.rename("/d", "/e")  # evicts d only
    assert cache.entries == {(d, "f"): (f.id, "file")}
    await vfs.unlink("/e/f")  # resolved on the server, still evicted
    assert cache.entries == {}


async def test_uncached_readdir_fills_the_cache(pool: Pool) -> None:
    await VFSClient(pool).create("/a/b/c", parents=True)
    cache = DictCache()
    vfs = VFSClient(pool, cache=cache)
    await vfs.readdir("/")
    await vfs.readdir("/a")
    await vfs.readdir("/a/b")
    assert [name for _, name in cache.entries] == ["a", "b", "c"]
    assert (await vfs.stat("/a/b/c")).kind == "file"


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
                cur = await c.execute("SELECT vfs_append(id, 'w') FROM vfs_nodes WHERE name = 'f'")
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


async def test_rewrapped_errors_keep_the_database_error(vfs: VFSClient) -> None:
    await vfs.create("/a")
    await vfs.create("/b")
    error = await raises("EEXIST", vfs.rename("/a", "/b"), "/b")
    assert error.db_error is not None and error.detail
    error = await raises("ENOTDIR", vfs.create("/a/x", parents=True), "/a/x")
    assert error.db_error is not None


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
