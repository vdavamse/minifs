"""VFSView (the chroot) and the write policy, over a real database."""

import asyncio
import itertools
import re
from collections.abc import AsyncIterator, Awaitable, Sequence
from functools import cache
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import VFSClient
from minifs.vfs.core import Entry, VFSError
from minifs.vfs.glob import GlobError, compile_excludes, compile_glob
from minifs.vfs.policy import DEFAULT_POLICY, Policy, Rule
from minifs.vfs.search import SearchRefused
from minifs.vfs.view import VFSView, _below, workspace_mounts  # pyright: ignore[reportPrivateUsage]
from tests.test_tree import wait_until_blocked

Pool = AsyncConnectionPool[AsyncConnection[Any]]
W1, W2 = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"


@pytest.fixture
async def vfs(db: str) -> AsyncIterator[VFSClient]:
    async with open_pool(Connector(db), max_size=3) as pool:
        client = VFSClient(pool)
        await client.create(f"/workspaces/{W1}/notes.md", b"mine", parents=True)
        await client.create(f"/workspaces/{W1}/log.md", b"log\n")
        await client.create(f"/workspaces/{W2}/secret.md", b"theirs", parents=True)
        await client.create("/docs/guide.md", b"guide", parents=True)
        yield client


@pytest.fixture
def policy() -> Policy:
    return Policy(DEFAULT_POLICY.rules)  # fresh denial counters


def view(vfs: VFSClient, policy: Policy, actor: str = "agent") -> VFSView:
    return VFSView(vfs, workspace_mounts(W1), policy, actor)


async def raises(errno: str, call: Awaitable[object], path: str | None = None) -> VFSError:
    with pytest.raises(VFSError) as info:
        await call
    assert info.value.errno == errno, info.value
    if path is not None:
        assert info.value.path == path
    return info.value


# Mapping


async def test_paths_map_into_the_workspace(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    assert (await agent.pread("/workspace/notes.md")).content == b"mine"
    entry = await agent.stat("/workspace//./notes.md")
    assert (entry.path, entry.size) == ("/workspace/notes.md", 4)
    assert [e.path for e in await agent.readdir("/workspace")] == ["/workspace/log.md", "/workspace/notes.md"]
    await agent.create("/workspace/sub/new.md", b"x", parents=True)
    await agent.write("/workspace/sub/new.md", b"xy", expect=(await agent.stat("/workspace/sub/new.md")).version)
    assert (await vfs.pread(f"/workspaces/{W1}/sub/new.md")).content == b"xy"
    assert await agent.claim("/workspace/sub/new.md", "/workspace/claimed.md")
    assert not await agent.claim("/workspace/sub/new.md", "/workspace/claimed2.md")
    await agent.rename("/workspace/claimed.md", "/workspace/sub/back.md")
    await agent.unlink("/workspace/sub/back.md")
    assert await agent.mkdir_p("/workspace/a/b") is not None
    await agent.mkdir("/workspace/c")
    assert (await vfs.stat(f"/workspaces/{W1}/a/b")).is_dir


@pytest.mark.parametrize(
    "path",
    [
        f"/workspace/../workspaces/{W2}/secret.md",
        f"/workspace/../../../workspaces/{W2}/secret.md",
        f"/docs/../workspaces/{W2}/secret.md",
        f"/workspaces/{W2}/secret.md",
        f"/workspaces/{W1}/notes.md",  # even its own workspace, by its real path
        "/workspaces",
        "/nowhere",
    ],
)
async def test_escapes_are_enoent(vfs: VFSClient, policy: Policy, path: str) -> None:
    agent = view(vfs, policy)
    for call in (
        agent.stat(path),
        agent.pread(path),
        agent.readdir(path),
        agent.glob("**", path),
        agent.create(path, b"x"),
        agent.write(path, b"x"),
        agent.append(path, b"x"),
        agent.unlink(path),
        agent.rename("/workspace/notes.md", path),
        agent.rename(path, "/workspace/stolen.md"),
    ):
        error = await raises("ENOENT", call)
        assert (error.detail, error.db_error) == ("", None)
    assert (await vfs.pread(f"/workspaces/{W2}/secret.md")).content == b"theirs"


async def test_no_symlinks(db: str, vfs: VFSClient) -> None:
    # A node is a file or a directory and a name one path component, so no node leads anywhere else.
    async with await psycopg.AsyncConnection.connect(db, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute("INSERT INTO vfs_nodes (name, kind) VALUES ('link', 'symlink')")
        for name in ("..", ".", "a/b"):
            with pytest.raises(psycopg.Error):
                await conn.execute("SELECT vfs_create(NULL, %s, 'file')", [name])
        kinds = await (await conn.execute("SELECT DISTINCT kind FROM vfs_nodes ORDER BY 1")).fetchall()
        assert kinds == [("dir",), ("file",)]


async def test_errors_carry_only_visible_paths(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    error = await raises("ENOENT", agent.pread("/workspace/missing.md"), "/workspace/missing.md")
    assert (error.detail, error.db_error, error.__cause__, error.__context__) == ("", None, None, None)
    error = await raises("EEXIST", agent.create("/workspace/notes.md"), "/workspace/notes.md")
    assert error.detail == "" and error.db_error is None
    error = await raises("ENOTDIR", agent.readdir("/workspace/notes.md/x"), "/workspace/notes.md/x")
    assert W1 not in repr(error) + str(error) + error.message
    await raises("EISDIR", agent.pread("/workspace"), "/workspace")


async def test_errors_of_a_move_name_the_right_visible_path(vfs: VFSClient) -> None:
    both = VFSView(vfs, workspace_mounts(W1), Policy({"mover": [Rule("/**", "read", "write")]}), "mover")
    await raises("EEXIST", both.rename("/workspace/notes.md", "/docs/guide.md"), "/docs/guide.md")
    await raises("ENOENT", both.rename("/workspace/missing.md", "/docs/x.md"), "/workspace/missing.md")
    await raises("ENOENT", both.rename("/workspace/notes.md", "/docs/no/x.md"), "/docs/no/x.md")
    await both.rename("/workspace/notes.md", "/docs/notes.md")  # across mounts
    assert (await vfs.pread("/docs/notes.md")).content == b"mine"


async def test_virtual_directories(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    root = await agent.stat("/")
    assert (root.path, root.id, root.kind) == ("/", None, "dir")
    assert [e.path for e in await agent.readdir("/..")] == ["/docs", "/workspace"]
    await raises("EBUSY", agent.unlink("/workspace"), "/workspace")
    await raises("EBUSY", agent.rename("/workspace", "/workspace2"))
    await raises("EBUSY", agent.rename("/workspace/notes.md", "/workspace"), "/workspace")
    await raises("EISDIR", agent.pread("/"))
    await raises("ENOENT", agent.create("/top.md", b"x"), "/top.md")

    nested = VFSView(vfs, {"/a/b/ws": f"/workspaces/{W1}"}, Policy({"agent": [Rule("/**", "read", "write")]}), "agent")
    assert [(e.path, e.id) for e in await nested.readdir("/")] == [("/a", None)]
    assert [e.path for e in await nested.readdir("/a/b")] == ["/a/b/ws"]
    assert await nested.mkdir_p("/a") is None


async def test_a_missing_mount_root(vfs: VFSClient, policy: Policy) -> None:
    fresh = VFSView(vfs, workspace_mounts("33333333-3333-3333-3333-333333333333"), policy, "agent")
    assert [e.path for e in await fresh.readdir("/")] == ["/docs"]
    await raises("ENOENT", fresh.stat("/workspace"), "/workspace")
    await raises("ENOENT", fresh.create("/workspace/a.md"), "/workspace/a.md")
    await raises("EISDIR", fresh.create("/workspace", b"oops"), "/workspace")  # it would wedge the workspace
    await raises("EISDIR", fresh.create("/workspace", b"oops", parents=True), "/workspace")
    await fresh.create("/workspace/a.md", b"x", parents=True)  # bootstraps the workspace
    assert [e.path for e in await fresh.readdir("/workspace")] == ["/workspace/a.md"]


def test_bad_configuration(vfs: VFSClient, policy: Policy) -> None:
    with pytest.raises(ValueError, match="inside mount"):
        VFSView(vfs, {"/w": "/a", "/w/x": "/b"}, policy, "agent")
    with pytest.raises(ValueError, match="reaches into"):
        VFSView(vfs, {"/rw": "/data", "/ro": "/data/ro"}, policy, "agent")
    with pytest.raises(ValueError, match="reaches into"):
        VFSView(vfs, {"/a": "/data", "/b": "/data"}, policy, "agent")
    for bad in ("", "..", f"{W1}/.."):
        with pytest.raises(ValueError):
            workspace_mounts(bad)
    with pytest.raises(ValueError, match="no rules"):
        VFSView(vfs, workspace_mounts(W1), policy, "intruder")


# The policy


async def test_agent_policy(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    assert (await agent.pread("/docs/guide.md")).content == b"guide"
    for call in (
        agent.create("/docs/new.md", b"x"),
        agent.write("/docs/guide.md", b"x"),
        agent.append("/docs/guide.md", b"x"),
        agent.unlink("/docs/guide.md"),
        agent.mkdir("/docs/sub"),
        agent.mkdir_p("/docs/sub"),
        agent.rename("/workspace/notes.md", "/docs/notes.md"),
        agent.rename("/docs/guide.md", "/workspace/guide.md"),
        agent.write("/workspace/log.md", b"rewritten"),
        agent.unlink("/workspace/log.md"),
        agent.rename("/workspace/log.md", "/workspace/old.md"),
    ):
        error = await raises("EACCES", call)
        assert error.path.startswith(("/docs", "/workspace/log.md"))
    assert await agent.append("/workspace/log.md", b"entry\n") == 4
    await agent.append("/workspace/notes.md", b"!")  # write implies append
    assert (await vfs.pread("/docs/guide.md")).content == b"guide"
    assert policy.denials == {("agent", "write"): 10, ("agent", "append"): 1}
    assert await agent.mkdir_p("/") is None  # a virtual directory exists already: nothing to deny


async def test_ingestion_policy(vfs: VFSClient, policy: Policy) -> None:
    ingestion = view(vfs, policy, "ingestion")
    await ingestion.create("/docs/new.md", b"x")
    await ingestion.write("/docs/guide.md", b"v2")
    await ingestion.append("/docs/guide.md", b"!")
    await raises("EACCES", ingestion.create("/workspace/x.md", b"x"), "/workspace/x.md")
    await raises("EACCES", ingestion.append("/workspace/log.md", b"x"))
    await raises("EACCES", ingestion.pread("/workspace/notes.md"))
    assert (await ingestion.pread("/docs/guide.md")).content == b"v2!"
    assert [e.path for e in await ingestion.readdir("/docs")] == ["/docs/guide.md", "/docs/new.md"]
    assert [e.path for e in await ingestion.readdir("/")] == ["/docs"]
    assert policy.denials == {("ingestion", "write"): 1, ("ingestion", "append"): 1, ("ingestion", "read"): 1}


async def test_operator_policy(vfs: VFSClient, policy: Policy) -> None:
    operator = view(vfs, policy, "operator")
    await operator.write("/workspace/notes.md", b"fixed")
    assert (await operator.pread("/workspace/log.md")).content == b"log\n"
    await raises("EACCES", operator.append("/workspace/log.md", b"x"))
    await raises("EACCES", operator.write("/docs/guide.md", b"x"))


async def test_a_move_carries_its_subtree(vfs: VFSClient) -> None:
    rules = [Rule("/workspace/keep/f.md", "read"), Rule("/workspace/**", "read", "write")]
    agent = view(vfs, Policy({"agent": rules}))
    await vfs.create(f"/workspaces/{W1}/keep/f.md", b"kept", parents=True)
    await raises("EACCES", agent.rename("/workspace/keep", "/workspace/moved"), "/workspace/keep")
    # A file moves alone, and can't replace the directory (RENAME_NOREPLACE), so f.md is safe either way.
    await raises("EEXIST", agent.rename("/workspace/notes.md", "/workspace/keep"), "/workspace/keep")
    await raises("ENOTEMPTY", agent.unlink("/workspace/keep"), "/workspace/keep")  # an unlink takes nothing below
    await raises("EACCES", agent.unlink("/workspace/keep/f.md"), "/workspace/keep/f.md")
    await agent.rename("/workspace/notes.md", "/workspace/keep2.md")
    assert (await vfs.pread(f"/workspaces/{W1}/keep/f.md")).content == b"kept"


async def test_creating_parents_needs_write_on_them(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    await vfs.unlink(f"/workspaces/{W1}/log.md")
    for call in (
        agent.mkdir_p("/workspace/log.md/x"),
        agent.create("/workspace/log.md/x", b"junk", parents=True),
        agent.mkdir("/workspace/log.md/x", parents=True),
    ):
        await raises("EACCES", call, "/workspace/log.md")
    await raises("ENOENT", vfs.stat(f"/workspaces/{W1}/log.md"))
    await agent.create("/workspace/new/deep/x.md", b"x", parents=True)
    await raises("EACCES", agent.create("/workspace/log.md", b""), "/workspace/log.md")  # creating is a write


async def test_a_move_needs_read_on_its_source(vfs: VFSClient) -> None:
    drop = Policy({"agent": [Rule("/workspace/drop/**", "write"), Rule("/workspace/**", "read", "write")]})
    agent = view(vfs, drop)
    await vfs.create(f"/workspaces/{W1}/drop/secret.md", b"s", parents=True)
    await raises("EACCES", agent.pread("/workspace/drop/secret.md"))
    await raises("EACCES", agent.rename("/workspace/drop/secret.md", "/workspace/s.md"), "/workspace/drop/secret.md")
    await raises("EACCES", agent.claim("/workspace/drop/secret.md", "/workspace/s.md"))
    await agent.rename("/workspace/notes.md", "/workspace/drop/notes.md")  # into the drop box is fine


async def test_a_rule_below_a_directory_still_lets_its_files_move(vfs: VFSClient) -> None:
    env = Policy({"agent": [Rule("/workspace/**/.env", "read"), Rule("/workspace/**", "read", "write")]})
    agent = view(vfs, env)
    await vfs.create(f"/workspaces/{W1}/app/.env", b"KEY=1", parents=True)
    await vfs.create(f"/workspaces/{W1}/tasks/new/t.md", b"t", parents=True)
    await vfs.mkdir(f"/workspaces/{W1}/tasks/cur")
    await agent.rename("/workspace/notes.md", "/workspace/notes2.md")  # a file has nothing below it
    assert await agent.claim("/workspace/tasks/new/t.md", "/workspace/tasks/cur/t.md")
    assert not await agent.claim("/workspace/tasks/new/t.md", "/workspace/tasks/cur/t2.md")  # lost, not EACCES
    await agent.unlink("/workspace/notes2.md")
    await raises("EACCES", agent.unlink("/workspace/app/.env"), "/workspace/app/.env")
    await raises("EACCES", agent.rename("/workspace/app", "/workspace/app2"), "/workspace/app")  # a directory
    await raises("EACCES", agent.rename("/workspace/tasks", "/workspace/t"), "/workspace/tasks")  # conservative
    assert (await vfs.pread(f"/workspaces/{W1}/app/.env")).content == b"KEY=1"


async def test_a_move_pinned_to_a_node_refuses_another(vfs: VFSClient) -> None:
    # What the view relies on when only a file may move: the move takes the node it checked, or nothing.
    other = (await vfs.stat(f"/workspaces/{W1}/log.md")).id
    assert other is not None
    src, dst = f"/workspaces/{W1}/notes.md", f"/workspaces/{W1}/moved.md"
    await raises("ESTALE", vfs.rename(src, dst, expect=other), src)
    assert not await vfs.claim(src, dst, expect=other)
    assert (await vfs.pread(src)).content == b"mine"
    await vfs.rename(src, dst, expect=(await vfs.stat(src)).id)
    assert (await vfs.pread(dst)).content == b"mine"
    await raises("ESTALE", vfs.grep("mine", "/", expect=other), "/")  # the root is never the expected file
    await raises("ESTALE", vfs.grep("mine", dst, expect=other), dst)
    directory = (await vfs.stat(f"/workspaces/{W1}")).id
    await raises("ESTALE", vfs.grep("mine", f"/workspaces/{W1}", expect=directory), f"/workspaces/{W1}")  # not a file
    assert [h.path for h in (await vfs.grep("mine", dst, expect=(await vfs.stat(dst)).id)).hits] == [dst]
    assert not await vfs.claim(dst, f"/workspaces/{W1}/claimed.md", expect=uuid4())


async def test_a_pinned_move_does_not_follow_the_node_elsewhere(db: str) -> None:
    # Moved by someone else between this move's lookup and its lock, the node isn't at `src` any more: the pinned
    # move is ESTALE, where a plain one moves the node from wherever it went (vfs_rename moves a node by id).
    async with open_pool(Connector(db), max_size=1) as pool:
        client = VFSClient(pool)
        await client.create("/a/f.md", b"f", parents=True)
        await client.mkdir("/locked")
        pinned = (await client.stat("/a/f.md")).id
        async with pool.connection() as c:
            pid = (await (await c.execute("SELECT pg_backend_pid()")).fetchall())[0][0]
        async with (
            await AsyncConnection.connect(db) as other,
            await AsyncConnection.connect(db, autocommit=True) as watcher,
        ):
            await other.execute(
                "SELECT * FROM vfs_rename((SELECT id FROM vfs_lookup(ARRAY['a', 'f.md'])),"
                " (SELECT id FROM vfs_lookup(ARRAY['locked'])), 'f.md', NULL)"
            )  # uncommitted: holds f.md FOR UPDATE
            task = asyncio.create_task(client.rename("/a/f.md", "/b.md", expect=pinned))
            await wait_until_blocked(watcher, pid)
            await other.commit()
            await raises("ESTALE", task, "/a/f.md")
        assert (await client.pread("/locked/f.md")).content == b"f"


async def test_a_claim_across_mounts(vfs: VFSClient) -> None:
    both = view(vfs, Policy({"agent": [Rule("/**", "read", "write")]}))
    assert await both.claim("/workspace/notes.md", "/docs/notes.md")
    assert not await both.claim("/workspace/notes.md", "/docs/notes2.md")
    assert (await vfs.pread("/docs/notes.md")).content == b"mine"
    agent = view(vfs, Policy(DEFAULT_POLICY.rules))
    await vfs.create(f"/workspaces/{W1}/n.md", b"")
    await raises("EACCES", agent.claim("/workspace/n.md", "/docs/n.md"), "/docs/n.md")


async def test_glob_skips_mounts_it_cannot_read(vfs: VFSClient) -> None:
    hidden = view(vfs, Policy({"agent": [Rule("/workspace/**"), Rule("/**", "read")]}))
    assert [e.path for e in await hidden.glob("**", "/")] == ["/docs/guide.md"]
    assert not hidden.policy.allows_any("agent", "read", "/workspace")
    assert hidden.policy.allows_any("agent", "read", "/")
    assert Policy({"a": [Rule("/w/x.md", "read"), Rule("/w/**")]}).allows_any("a", "read", "/w")


async def test_glob_limit_counts_only_readable_files(vfs: VFSClient) -> None:
    hiding = view(vfs, Policy({"agent": [Rule("/workspace/log.md"), Rule("/**", "read")]}))
    await vfs.append(f"/workspaces/{W1}/log.md", b"newest")
    assert [e.path for e in await hiding.glob("*.md", "/workspace", limit=1)] == ["/workspace/notes.md"]
    assert [e.path for e in await hiding.glob("**/log.md", "/", limit=1)] == []


async def test_glob_looks_past_hidden_matches_a_bounded_distance(vfs: VFSClient) -> None:
    hiding = view(vfs, Policy({"agent": [Rule("/workspace/hid/**"), Rule("/**", "read")]}))
    for i in range(7):  # newer than notes.md and log.md
        await vfs.create(f"/workspaces/{W1}/hid/{i}.md", b"", parents=True)
    assert [e.path for e in await hiding.glob("**", "/workspace", limit=1)] == ["/workspace/log.md"]
    for i in range(7, 9):  # now 9 hidden matches come first, past the 8 the re-fetch looks at
        await vfs.create(f"/workspaces/{W1}/hid/{i}.md", b"")
    assert await hiding.glob("**", "/workspace", limit=1) == []


async def test_ingestion_globs_only_the_docs(vfs: VFSClient, policy: Policy) -> None:
    ingestion = view(vfs, policy, "ingestion")
    assert [e.path for e in await ingestion.glob("**", "/")] == ["/docs/guide.md"]
    await raises("EACCES", ingestion.glob("**", "/workspace"), "/workspace")


async def test_every_exclude_is_restated_even_after_one_covers_the_mount(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    for excludes in (["workspace", "**/w*/"], ["**/w*/", "workspace"]):
        # "workspace" covers that mount, but "**/w*/" can't be restated there: EINVAL whatever the order.
        await raises("EINVAL", agent.glob("*.md", "/", excludes=excludes), "/")
        await raises("EINVAL", agent.grep("x", "/", excludes=excludes), "/")


async def test_other_database_errors_are_eio(db: str) -> None:
    async with open_pool(Connector(db), max_size=1) as pool:
        client = VFSClient(pool)
        await client.create(f"/workspaces/{W1}/f.md", b"x", parents=True)
        agent = view(client, Policy(DEFAULT_POLICY.rules))
        await pool.close()
        error = await raises("EIO", agent.stat("/workspace/f.md"), "/workspace/f.md")
        assert (error.detail, error.db_error, error.__context__) == ("", None, None)


# Read by line, and grep


async def test_pread_lines(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    await vfs.write(f"/workspaces/{W1}/notes.md", b"one\ntwo\nthree\n")
    page = await agent.pread_lines("/workspace/notes.md", 2, max_lines=1)
    assert (page.content, page.first_line, page.lines) == (b"two\n", 2, 3)
    await raises("EISDIR", agent.pread_lines("/"), "/")
    await raises("ENOENT", agent.pread_lines("/workspace/none.md"), "/workspace/none.md")
    await raises("ENOENT", agent.pread_lines(f"/workspaces/{W2}/secret.md"))
    ingestion = view(vfs, policy, "ingestion")
    await raises("EACCES", ingestion.pread_lines("/workspace/notes.md"), "/workspace/notes.md")


async def test_grep_in_visible_paths(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    await vfs.create(f"/workspaces/{W1}/src/a.py", b"x = 1\nmine = 2\n", parents=True)
    hits = (await agent.grep("mine", "/workspace")).hits
    assert [(h.path, h.line) for h in hits] == [("/workspace/notes.md", 1), ("/workspace/src/a.py", 2)]
    assert [h.path for h in (await agent.grep("mine", "/workspace/src/a.py")).hits] == ["/workspace/src/a.py"]
    assert [h.path for h in (await agent.grep("mine", "/workspace", include="*.py")).hits] == ["/workspace/src/a.py"]
    await raises("ENOENT", agent.grep("theirs", f"/workspaces/{W2}"))
    await raises("ENOENT", agent.grep("x", "/workspace/none"), "/workspace/none")


async def test_grep_from_a_virtual_directory(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    await vfs.write("/docs/guide.md", b"needle\n")
    await vfs.write(f"/workspaces/{W1}/notes.md", b"needle\nneedle\n")
    result = await agent.grep("needle", "/")
    assert [(h.path, h.line) for h in result.hits] == [
        ("/docs/guide.md", 1),
        ("/workspace/notes.md", 1),
        ("/workspace/notes.md", 2),
    ]  # in path order, across mounts
    assert (result.truncated, result.files) == (False, 3)
    limited = await agent.grep("needle", "/", limit=2)
    assert ([h.path for h in limited.hits], limited.truncated) == (["/docs/guide.md", "/workspace/notes.md"], True)
    exact = await agent.grep("needle", "/", limit=1)
    assert (len(exact.hits), exact.truncated) == (1, True)  # the next mount still says there is more
    assert [h.path for h in (await agent.grep("needle", "/", include="workspace/*.md")).hits] == [
        "/workspace/notes.md",
        "/workspace/notes.md",
    ]
    assert (await agent.grep("needle", "/", excludes=["docs/"])).files == 2
    assert [h.path for h in (await view(vfs, policy, "ingestion").grep("needle", "/")).hits] == ["/docs/guide.md"]
    await raises("EINVAL", agent.grep("needle", "/", include="**/workspace/*.md"), "/")
    with pytest.raises(GlobError):
        await agent.grep("needle", "/", include="[")


async def test_grep_stops_at_its_limit(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    await vfs.write("/docs/guide.md", b"needle\n")
    for i in range(3):
        await vfs.create(f"/workspaces/{W1}/f{i}.md", b"")
    with pytest.raises(SearchRefused):
        await agent.grep("needle", "/workspace", max_files=2)
    result = await agent.grep("needle", "/", limit=1, max_files=2)  # /workspace is over the cap, but it's full
    assert ([h.path for h in result.hits], result.truncated, result.files) == (["/docs/guide.md"], True, 1)
    with pytest.raises(SearchRefused):
        await agent.grep("needle", "/", limit=2, max_files=2)
    await raises("EINVAL", agent.grep("needle", "/", max_files=-1), "/")
    await raises("EINVAL", agent.grep("needle", "/", timeout=0), "/")
    await raises("EINVAL", agent.grep("needle", "/", timeout=1e9), "/")


async def test_grep_skips_a_missing_mount_root(vfs: VFSClient, policy: Policy) -> None:
    agent = VFSView(vfs, {"/workspace": "/workspaces/none", "/docs": "/docs"}, policy, "agent")
    assert [h.path for h in (await agent.grep("guide", "/")).hits] == ["/docs/guide.md"]


async def test_grep_needs_read_on_everything_it_searches(vfs: VFSClient) -> None:
    env = Policy({"agent": [Rule("/workspace/**/.env"), Rule("/**", "read", "write")]})
    agent = view(vfs, env)
    await vfs.create(f"/workspaces/{W1}/app/.env", b"KEY=secret", parents=True)
    await raises("EACCES", agent.grep("secret", "/workspace"), "/workspace")
    await raises("EACCES", agent.grep("secret", "/"), "/workspace")
    await raises("EACCES", agent.grep("secret", "/workspace/app/.env"), "/workspace/app/.env")
    assert (await agent.grep("mine", "/workspace/notes.md")).files == 1  # a file needs read on itself only
    assert (await agent.grep("secret", "/docs")).hits == []
    assert env.denials == {("agent", "read"): 3}


async def test_a_file_checked_alone_is_pinned(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    # The policy lets only a file be moved or searched at these paths. If a directory takes the file's place
    # after the view's stat, the operation must not reach it.
    env = Policy({"agent": [Rule("/workspace/**/.env"), Rule("/**", "read", "write")]})
    agent = view(vfs, env)
    await vfs.create(f"/workspaces/{W1}/app/.env", b"KEY=secret", parents=True)
    real_stat = vfs.stat
    file_id = (await vfs.stat(f"/workspaces/{W1}/notes.md")).id

    async def stale_stat(path: str) -> Entry:  # what a stat just before the swap saw: a file
        entry = await real_stat(path)
        return Entry(entry.path, file_id, "file", 4, entry.mtime, entry.generation)

    monkeypatch.setattr(vfs, "stat", stale_stat)
    await raises("ESTALE", agent.rename("/workspace/app", "/workspace/app2"), "/workspace/app")
    assert not await agent.claim("/workspace/app", "/docs/app")
    await raises("ESTALE", agent.grep("secret", "/workspace/app"), "/workspace/app")
    assert (await real_stat(f"/workspaces/{W1}/app/.env")).size == 10


def test_rules() -> None:
    p = Policy(
        {
            "a": [Rule("/w/keep.md", "read"), Rule("/w/**", "read", "write"), Rule("/w/*/*.log"), Rule("/hidden/**")],
        }
    )
    assert p.allows("a", "write", "/w") and p.allows("a", "write", "/w/x/y.md")  # ** matches the directory too
    assert not p.allows("a", "write", "/w/keep.md")  # the first match wins
    assert p.allows("a", "write", "/w/x/y.log")  # shadowed by /w/**
    assert p.allows("a", "append", "/w/z")  # write implies append
    assert not p.allows("a", "read", "/hidden") and not p.allows("a", "read", "/hidden/a/b")
    assert p.allows("a", "read", "/other") and not p.allows("a", "write", "/other")  # the default
    assert not p.allows("a", "write", "/w/x/../keep.md")
    assert Rule("/**/x.md").matches("/x.md") and Rule("/a/*.md").matches("/a/b.md")
    assert not Rule("/a/*.md").matches("/a/b/c.md") and not Rule("/a").matches("/a/b")
    assert Rule("/a/**/b/**/c.md").matches("/a/b/c.md") and Rule("/a/**/b/**/c.md").matches("/a/x/b/y/z/c.md")
    assert p.allows("a", "write", "/w/x", subtree=True)
    assert not p.allows("a", "write", "/w", subtree=True)  # /w/keep.md is below
    assert not p.allows("a", "write", "/other", subtree=True)
    with pytest.raises(VFSError) as info:
        p.check("a", "write", "/w/keep.md")
    assert info.value.errno == "EACCES" and p.denials == {("a", "write"): 1}
    for bad in ("w/**", "/w/../x", "/w//x", "/w/"):
        with pytest.raises(ValueError):
            Rule(bad)
    for literal in (
        "/w/{a,b}/**",
        "/w/[^.]*",
        "/w/a\\*",
        "/w/[[:alpha:]]*",
        "/w/[!/]*",
        "/w/[secret",
        "/w/[]",
        "/w/[!]",
        "/w/[![:alpha:]]*",
    ):
        with pytest.raises(ValueError, match="fnmatch"):
            Rule(literal)
    for valid, path in (("/w/[]a]", "/w/]"), ("/w/[!]]", "/w/x"), ("/w/[!][]", "/w/a"), ("/w/[a-c]*", "/w/bx")):
        assert Rule(valid).matches(path)
    with pytest.raises(ValueError, match="unknown"):
        Rule("/w", "delete")  # pyright: ignore[reportArgumentType]


# Glob


async def test_glob_in_a_mount(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    assert [e.path for e in await agent.glob("*.md", "/workspace")] == ["/workspace/log.md", "/workspace/notes.md"]
    await raises("ENOENT", agent.glob("*.md", "/workspace/none"), "/workspace/none")


async def test_glob_below_a_virtual_directory(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    await vfs.create(f"/workspaces/{W1}/node_modules/m.md", b"", parents=True)
    await vfs.create(f"/workspaces/{W1}/src/a.py", b"", parents=True)

    async def paths(pattern: str, *excludes: str, limit: int | None = None) -> list[str]:
        return sorted(e.path for e in await agent.glob(pattern, "/", excludes=excludes, limit=limit))

    assert await paths("workspace/*.md") == ["/workspace/log.md", "/workspace/notes.md"]  # anchored, not nested
    assert await paths("*/notes.md") == ["/workspace/notes.md"]
    assert await paths("{docs,workspace}/*.md") == ["/docs/guide.md", "/workspace/log.md", "/workspace/notes.md"]
    assert await paths("/*.md") == []
    every_md = ["/docs/guide.md", "/workspace/log.md", "/workspace/node_modules/m.md", "/workspace/notes.md"]
    assert await paths("*.md") == every_md
    assert await paths("**/*.md") == every_md
    assert await paths("*.md", "**/node_modules/**") == ["/docs/guide.md", "/workspace/log.md", "/workspace/notes.md"]
    assert await paths("*.md", "docs/") == ["/workspace/log.md", "/workspace/node_modules/m.md", "/workspace/notes.md"]
    assert await paths("*.md", "workspace") == ["/docs/guide.md"]
    assert await paths("/docs/*.md") == ["/docs/guide.md"]
    assert await paths("workspace/src/*.py") == ["/workspace/src/a.py"]
    assert await paths("workspace/**/a.py") == ["/workspace/src/a.py"]
    assert await paths("workspaces/**") == []
    assert len(await paths("**/*.md", limit=2)) == 2
    mtimes = [e.mtime for e in await agent.glob("**", "/") if e.mtime is not None]
    assert len(mtimes) == 5 and mtimes == sorted(mtimes, reverse=True)
    await vfs.create(f"/workspaces/{W1}/src/node_modules/n.md", b"", parents=True)
    assert await paths("**/*.md", "workspace/node_modules/") == [
        "/docs/guide.md",
        "/workspace/log.md",
        "/workspace/notes.md",
        "/workspace/src/node_modules/n.md",
    ]
    assert await paths("*.md", "*.md") == []
    assert await paths("*.md", "w*") == ["/docs/guide.md"]
    for pattern in ("**/workspace/*.md", "**/w*/x.md", "{a/b,c}/x"):
        await raises("EINVAL", agent.glob(pattern, "/"), "/")
    await raises("EINVAL", agent.glob("*.md", "/", excludes=["**/d*/"]), "/")
    await raises("EINVAL", agent.glob("*.md", "/", limit=2**31), "/")


@pytest.mark.parametrize(
    ("pattern", "dirs", "expected"),
    [
        ("*.md", False, "*.md"),
        ("ws/*.md", False, "/*.md"),
        ("/ws/a/**", False, "/a/**"),
        ("other/*.md", False, None),
        ("ws", False, "ws"),  # a file named ws at any depth
        ("/ws", False, None),
        ("/*.md", False, None),
        ("ws/**", False, "/**"),
        ("*/x", False, "/x"),
        ("{ws,other}/x", False, "/x"),
        ("w[st]/x", False, "/x"),
        ("**/*.md", False, "**/*.md"),
        ("**/src/*.py", False, "**/src/*.py"),
        ("**/ws/*.md", False, "EINVAL"),
        ("**/w?/x", False, "EINVAL"),
        ("{a/b,c}/x", False, "EINVAL"),
        ("a//x", False, None),
        ("ws//x", False, None),  # a path has no empty segments
        ("ws/./x", False, None),
        ("ws\\/x.md", False, "EINVAL"),  # an escaped / is a separator: segments aren't where split() puts them
        ("[!/]*.md", False, "EINVAL"),
        ("{**}/x.md", False, "/x.md"),  # globset keeps a brace in its segment: {**} is *, not **
        ("{a,**}/x", False, "/x"),
        ("ws\\/", True, "EINVAL"),
        ("ws//", True, "EINVAL"),
        ("**//", True, "EINVAL"),  # globset reads it as **/, every directory
        ("node_modules", True, "node_modules"),
        ("*.tmp", True, "*.tmp"),
        ("w*", True, True),
        ("ws", True, True),
        ("ws/", True, True),
        ("ws/build/", True, "/build/"),
        ("**/node_modules/**", True, "**/node_modules/**"),
        ("**/*.tmp", True, "**/*.tmp"),
        ("**/*s", True, "EINVAL"),
        ("ws/**", True, "/**"),
        ("**", True, True),
    ],
)
def test_patterns_restated_below_a_mount_point(pattern: str, dirs: bool, expected: str | bool | None) -> None:
    if expected == "EINVAL":
        with pytest.raises(VFSError):
            _below(pattern, ("ws",), dirs=dirs, path="/")
    else:
        assert _below(pattern, ("ws",), dirs=dirs, path="/") == expected


# `_below` against globbing the mounts directly: a glob from a virtual directory, restated per mount, matches
# exactly what the pattern matches in the visible tree, or is EINVAL.

_SEGMENTS = (
    *("ws", "w*", "w?", "w[st]", "w[!x]", "[!/]*", "\\w\\s", "w\\*", "w\\[s]", "{ws,c}", "{w*,x}", "{,ws}"),
    *("{a,{b,ws}}", "{a/b,c}", "{a,**}", "{**}", "*{*}", "ws\\/b", "[/]", "*", "**", "**.md", "a**", "a"),
    *("*.md", "", ".", ".."),
)
_PREFIXES = (("ws",), ("a", "b"), ("w[s]",))
_NAMES = ("x.md", "ws", "a", "b", "w[s]", "{a,b}", "*", "\\")
_FILES = tuple(
    "/".join([*dirs, name])
    for depth in range(3)
    for dirs in itertools.product(("ws", "a", "b", "x.md"), repeat=depth)
    for name in _NAMES
)


def _patterns() -> list[str]:
    patterns = ["/".join(p) for n in (1, 2) for p in itertools.product(_SEGMENTS, repeat=n)]
    return list(dict.fromkeys(q for p in patterns for q in (p, "/" + p, p + "/")))


@cache
def _matches(regex: str, path: str) -> bool:
    return re.fullmatch(regex, path) is not None


def _excluded(regexes: Sequence[str], path: str) -> bool:
    """As vfs_glob: a file is skipped when its path matches, or a directory above it does, as "dir/"."""
    parts = path.split("/")
    dirs = ["/".join(parts[:k]) + "/" for k in range(1, len(parts))]
    return any(_matches(r, p) for r in regexes for p in (*dirs, path))


def _check_restated(pattern: str, prefix: tuple[str, ...], dirs: bool) -> bool:
    """Whether ``_below`` restates ``pattern`` exactly for the mount at ``prefix`` (False: it was EINVAL)."""
    try:
        restated = _below(pattern, prefix, dirs=dirs, path="/")
    except VFSError as e:
        assert e.errno == "EINVAL"
        return False
    mount = "/".join(prefix)
    where = f"{pattern!r} restated as {restated!r} below {mount}"
    if dirs:
        original = compile_excludes([pattern], prune=False).regexes
        if restated is True:  # the exclude takes a directory at or above the mount point
            assert any(_excluded(original, "/".join(prefix[:k]) + "/") for k in range(1, len(prefix) + 1)), where
            return True
        mine = compile_excludes([restated], prune=False).regexes if restated is not None else ()
        for f in _FILES:
            assert _excluded(original, f"{mount}/{f}") == _excluded(mine, f), f"{where}: {f}"
    else:
        assert restated is not True, where
        regex = compile_glob(pattern).regex
        mine = compile_glob(restated).regex if restated is not None else None
        for f in _FILES:
            assert _matches(regex, f"{mount}/{f}") == (mine is not None and _matches(mine, f)), f"{where}: {f}"
    return True


@pytest.mark.parametrize("dirs", [False, True], ids=["pattern", "exclude"])
def test_restated_patterns_match_what_globbing_the_mount_does(dirs: bool) -> None:
    checked = 0
    for pattern in _patterns():
        try:
            compile_excludes([pattern]) if dirs else compile_glob(pattern)
        except GlobError:
            continue  # the view reports a bad pattern before restating it
        checked += sum(_check_restated(pattern, prefix, dirs) for prefix in _PREFIXES)
    assert checked > 2000  # most patterns are restated, not refused


_MIRROR = "/mirror"
_MIRROR_FILES = ("x.md", "ws/x.md", "ws/c/x", "a/x.md", "a/ws/y.md", ".h/x.md", "c/x", "x", "w[s]/x.md", "{a,b}")


@pytest.mark.parametrize("hidden", [False, True])
async def test_glob_from_a_virtual_directory_matches_globbing_the_tree(vfs: VFSClient, hidden: bool) -> None:
    # The mounts, each a copy of the same tree, are also laid out under /mirror as the view shows them, so a glob
    # from /mirror is what the view's glob from / has to return.
    mounts = {"/ws": "/mirror/ws", "/docs": "/mirror/docs", "/a/b": "/mirror/a/b", "/.h": "/mirror/.h"}
    for visible in mounts:
        for f in _MIRROR_FILES:
            await vfs.create(f"{_MIRROR}{visible}/{f}", b"", parents=True)
    tester = VFSView(vfs, mounts, Policy({"tester": [Rule("/**", "read", "write")]}), "tester")
    cases = [(p, ()) for p in ("*.md", "**", "ws/**", "**/x.md", "*/x.md", "a/b/ws/*.md", "{ws,docs}/*", "x")]
    cases += [(p, ()) for p in ("w[s]/*", "w\\[s]/**", "{a,**}/x.md", "**/c/x", "a/*/c/x", "/ws/**/x", "a**/x")]
    cases += [("**", (e,)) for e in ("ws", "ws/", "**/ws/", "x.md", "*/c/", "a/b/", "a/*/ws/", "{ws,a}/x.md")]
    cases += [("**", ("**/x.md", "docs/**")), ("*.md", ("**/node_modules/**", "w*/a/"))]
    for directory in ("/", "/a"):
        for pattern, excludes in cases:
            try:
                seen = await tester.glob(pattern, directory, excludes=excludes, hidden=hidden)
            except VFSError as e:
                assert e.errno == "EINVAL", (pattern, excludes)
                continue
            real = await vfs.glob(pattern, _MIRROR + directory.rstrip("/"), excludes=excludes, hidden=hidden)
            expected = sorted(e.path.removeprefix(_MIRROR) for e in real)
            assert sorted(e.path for e in seen) == expected, (directory, pattern, excludes)
