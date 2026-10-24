"""VFSView (the chroot) and the write policy, over a real database."""

from collections.abc import AsyncIterator, Awaitable
from typing import Any

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import VFSClient
from minifs.vfs.core import VFSError
from minifs.vfs.policy import DEFAULT_POLICY, Policy, Rule
from minifs.vfs.view import VFSView, _below, workspace_mounts  # pyright: ignore[reportPrivateUsage]

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
    await raises("EACCES", ingestion.readdir("/docs"))  # the README grants ingestion write, not read
    assert await ingestion.readdir("/") == []
    assert policy.denials == {("ingestion", "write"): 1, ("ingestion", "append"): 1, ("ingestion", "read"): 2}


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
    await raises("EACCES", agent.rename("/workspace/notes.md", "/workspace/keep"), "/workspace/keep")
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


async def test_other_database_errors_are_eio(db: str) -> None:
    async with open_pool(Connector(db), max_size=1) as pool:
        client = VFSClient(pool)
        await client.create(f"/workspaces/{W1}/f.md", b"x", parents=True)
        agent = view(client, Policy(DEFAULT_POLICY.rules))
        await pool.close()
        error = await raises("EIO", agent.stat("/workspace/f.md"), "/workspace/f.md")
        assert (error.detail, error.db_error, error.__context__) == ("", None, None)


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
