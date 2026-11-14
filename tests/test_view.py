"""VFSView (the chroot) and the write policy, over a real database."""

import asyncio
import itertools
import logging
import pickle
import re
from collections.abc import AsyncIterator, Awaitable, Sequence
from functools import cache
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs.db.pool import Connector, open_pool
from minifs.vfs import glob as globbing
from minifs.vfs.client import VFSClient
from minifs.vfs.core import Entry, GlobResult, ReadResult, VFSError
from minifs.vfs.glob import GlobError, GlobMatch, compile_excludes, compile_glob
from minifs.vfs.policy import DEFAULT_POLICY, Policy, PolicyDenied, Rule
from minifs.vfs.search import GrepResult, SearchRefused
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


async def test_readdir_pages_are_slices_of_the_readable_listing(vfs: VFSClient) -> None:
    """``limit`` counts readable entries and ``after`` is a name, so a page says nothing about the entries the
    policy hides: the view reads on past them. A virtual directory pages its mount points the same way."""
    for name in ("a", "b", "c", "d", "e"):
        await vfs.create(f"/workspaces/{W1}/many/{name}", b"", parents=True)
    hidden = Policy({"agent": [Rule("/workspace/many/b"), Rule("/workspace/many/c"), Rule("/**", "read", "write")]})
    agent = view(vfs, hidden)

    async def names(after: str | None = None, limit: int | None = None) -> list[str]:
        return [e.name for e in await agent.readdir("/workspace/many", after=after, limit=limit)]

    assert await names() == ["a", "d", "e"]
    assert await names(limit=2) == ["a", "d"]  # not ["a"]: b and c don't use up the limit
    assert await names("a", 1) == ["d"]  # not []: nothing tells that b follows a
    assert await names("b", 5) == ["d", "e"]
    assert await names("e") == [] and await names(limit=0) == []
    assert [e.path for e in await agent.readdir("/", limit=1)] == ["/docs"]
    assert [e.path for e in await agent.readdir("/", after="docs")] == ["/workspace"]
    assert await agent.readdir("/", after="workspace", limit=3) == []
    await raises("EINVAL", agent.readdir("/", limit=-1), "/")
    await raises("EINVAL", agent.readdir("/workspace", limit=-1), "/workspace")


async def test_a_readdir_page_reads_past_a_long_hidden_run(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """The view asks for a batch at a time until it has ``limit`` readable entries or the listing ends."""
    monkeypatch.setattr("minifs.vfs.view._READDIR_BATCH", 2)
    for i in range(7):
        await vfs.create(f"/workspaces/{W1}/run/h{i}", b"", parents=True)
    await vfs.create(f"/workspaces/{W1}/run/z", b"")
    agent = view(vfs, Policy({"agent": [Rule("/workspace/run/h*"), Rule("/**", "read", "write")]}))
    asked: list[int | None] = []
    readdir = VFSClient.readdir

    async def spy(self: VFSClient, path: str, *, after: str | None = None, limit: int | None = None) -> list[Entry]:
        asked.append(limit)
        return await readdir(self, path, after=after, limit=limit)

    monkeypatch.setattr(VFSClient, "readdir", spy)
    assert [e.name for e in await agent.readdir("/workspace/run", limit=1)] == ["z"]
    assert asked == [1, 2, 4, 8]  # 1 + 2 + 4 hidden entries, then z
    monkeypatch.setattr("minifs.vfs.view._READDIR_MAX_BATCH", 2)
    asked.clear()
    assert [e.name for e in await agent.readdir("/workspace/run", limit=3)] == ["z"]
    assert asked == [3, 3, 3]  # never below what is still wanted, and no larger than the cap otherwise


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


@pytest.mark.parametrize(
    ("mounts", "match"),
    [
        # A prefix formatted by hand with an empty or climbing id: each would be normalised into other workspaces.
        ({"/workspace": "/workspaces/"}, "isn't normalised"),
        ({"/workspace": "/workspaces/x/../../docs"}, "isn't normalised"),
        ({"/workspace": f"/workspaces/{W1}/."}, "isn't normalised"),
        ({"/workspace": f"/workspaces//{W1}"}, "isn't normalised"),
        ({"/workspace/": f"/workspaces/{W1}"}, "visible prefix '/workspace/' isn't normalised"),
        ({"/a/../workspace": f"/workspaces/{W1}"}, "visible prefix '/a/../workspace' isn't normalised"),
        ({"/workspace": f"workspaces/{W1}"}, "real prefix 'workspaces/.*' isn't a valid absolute path"),
        ({"/workspace": ""}, "real prefix '' isn't a valid absolute path"),
        ({"/workspace": "/workspaces/a\n"}, r"prefix '/workspaces/a\\u000a' isn't a valid absolute path"),
        # The whole tree, the directory that holds every workspace, and less or more than one whole workspace.
        ({"/everything": "/"}, "every workspace"),
        ({"/workspace": "/workspaces"}, "one whole workspace"),
        ({"/workspace": f"/workspaces/{W2}/private"}, "one whole workspace"),  # an id containing '/'
        ({"/workspace": "/workspaces/none"}, "one whole workspace"),
        (
            {"/workspace": "/workspaces/AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"},
            "one whole workspace",
        ),  # not as workspace_mounts writes it
    ],
)
def test_hand_built_mounts_are_refused(vfs: VFSClient, policy: Policy, mounts: dict[str, str], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        VFSView(vfs, mounts, policy, "agent")  # each alone, as in the issue


def test_hand_built_mounts_in_normal_form_are_accepted(vfs: VFSClient, policy: Policy) -> None:
    VFSView(vfs, {"/workspace": f"/workspaces/{W1}", "/docs": "/docs"}, policy, "agent")
    VFSView(vfs, {"/w": "/data/w", "/x": "/other", "/ws": "/workspacesX/a"}, policy, "agent")


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


async def test_uncounted_reads_are_refused_but_not_counted(vfs: VFSClient, caplog: pytest.LogCaptureFixture) -> None:
    policy = Policy({"agent": [Rule("/workspace/secret.md"), Rule("/workspace/**", "read", "write")]})
    agent = view(vfs, policy)
    await vfs.create(f"/workspaces/{W1}/secret.md", b"s")
    with caplog.at_level(logging.INFO, logger="minifs.vfs.policy"):
        found = await agent.pread_many(
            [("/workspace/notes.md", 0, 10), ("/workspace/secret.md", 0, 10)], count_denials=False
        )
        await raises("EACCES", agent.pread_lines("/workspace/secret.md", count_denials=False))
        await raises("EACCES", agent.readdir("/workspace/secret.md", count_denials=False))
    assert isinstance(found[0], ReadResult) and found[0].content == b"mine"
    assert isinstance(found[1], VFSError) and (found[1].errno, found[1].path) == ("EACCES", "/workspace/secret.md")
    assert policy.denials == {} and not caplog.records
    await raises("EACCES", agent.pread_lines("/workspace/secret.md"))
    assert policy.denials == {("agent", "read"): 1}


def test_policy_matches_a_legacy_name_and_check_refuses_it(policy: Policy) -> None:
    """``allows`` filters paths the server listed, so a name from before migration 016 is matched, not an error
    that fails the whole listing; ``check``, for a caller's path, is EINVAL before EACCES."""
    assert policy.allows("agent", "read", "/workspace/a\nb") is True
    assert policy.allows_any("agent", "read", "/workspace/a\nb") is True
    with pytest.raises(VFSError) as info:
        policy.check("agent", "write", "/elsewhere/a\nb")
    assert info.value.errno == "EINVAL" and "\n" not in str(info.value)


def test_a_path_no_rule_matches_is_refused() -> None:
    """A path no rule matches is refused, unless the policy is built with ``read_unmatched=True``, which lets it be
    read (not written)."""
    p = Policy({"a": [Rule("/w/**", "read", "write")]})
    assert p.allows("a", "read", "/other") is False
    assert p.allows("a", "write", "/other") is False
    assert p.allows_any("a", "read", "/other") is False
    assert p.allows("a", "read", "/w/x") is True
    old = Policy(p.rules, read_unmatched=True)
    assert old.allows("a", "read", "/other") is True
    assert old.allows("a", "write", "/other") is False
    assert old.allows_any("a", "read", "/other") is True


async def test_a_mount_no_rule_covers_is_hidden(vfs: VFSClient, policy: Policy) -> None:
    """A mount the rules don't cover is refused, and so left out of the root's listing; ``read_unmatched=True``
    restores the old default, where every actor of the policy may read it."""
    mounts = {"/workspace": f"/workspaces/{W1}", "/shared": "/docs"}
    agent = VFSView(vfs, mounts, policy, "agent")
    assert [e.path for e in await agent.readdir("/")] == ["/workspace"]
    await raises("EACCES", agent.stat("/shared"), "/shared")
    await raises("EACCES", agent.pread("/shared/guide.md"), "/shared/guide.md")
    old = VFSView(vfs, mounts, Policy(policy.rules, read_unmatched=True), "agent")
    assert [e.path for e in await old.readdir("/")] == ["/shared", "/workspace"]
    assert (await old.pread("/shared/guide.md")).content == b"guide"


async def test_a_virtual_directory_is_readable_where_something_below_is(vfs: VFSClient) -> None:
    """A virtual directory (the root, or one above a mount point) may be read where something below it may be;
    where nothing below it is readable, the refusal is counted."""
    only_ws = VFSView(vfs, workspace_mounts(W1), Policy({"x": [Rule("/workspace/**", "read")]}), "x")
    assert (await only_ws.stat("/")).kind == "dir"
    assert [e.path for e in await only_ws.readdir("/")] == ["/workspace"]
    nothing = VFSView(vfs, workspace_mounts(W1), Policy({"x": [Rule("/workspace/**")]}), "x")
    await raises("EACCES", nothing.stat("/"), "/")
    await raises("EACCES", nothing.readdir("/"), "/")
    assert nothing.policy.denials == {("x", "read"): 2}


async def test_a_nested_mount_is_listed_under_a_deny_default(vfs: VFSClient) -> None:
    """A virtual directory below a nested mount point is listed where something below it may be read. A rule with no
    actions leaves nothing readable below it, so the virtual directories above the mount are refused. Under one virtual
    directory, a virtual child and a mount point are each decided on their own terms."""
    mounts = {"/a/b/ws": f"/workspaces/{W1}"}
    nested = VFSView(vfs, mounts, Policy({"x": [Rule("/a/b/ws/**", "read", "write")]}), "x")
    assert [e.path for e in await nested.readdir("/")] == ["/a"]
    assert [e.path for e in await nested.readdir("/a")] == ["/a/b"]
    assert [e.path for e in await nested.readdir("/a/b")] == ["/a/b/ws"]
    assert nested.policy.denials == {}
    hidden = VFSView(vfs, mounts, Policy({"x": [Rule("/a/b/ws/**")]}), "x")
    await raises("EACCES", hidden.readdir("/"), "/")
    await raises("EACCES", hidden.readdir("/a"), "/a")
    assert hidden.policy.denials == {("x", "read"): 2}
    mixed = VFSView(
        vfs, {"/a/pub/ws": f"/workspaces/{W1}", "/a/secret": "/docs"}, Policy({"x": [Rule("/a/pub/**", "read")]}), "x"
    )
    assert [e.path for e in await mixed.readdir("/a")] == ["/a/pub"]  # a virtual child shown, a mount point hidden


async def test_a_virtual_directory_is_decided_by_the_mounts_below_it(vfs: VFSClient) -> None:
    """A rule that matches no mount (a leading wildcard here) doesn't make a virtual directory above a mount readable:
    only what a mount below it may show counts."""
    v = VFSView(vfs, {"/a/b/ws": f"/workspaces/{W1}"}, Policy({"x": [Rule("/*/public/**", "read")]}), "x")
    await raises("EACCES", v.readdir("/"), "/")
    await raises("EACCES", v.stat("/a"), "/a")
    assert v.policy.denials == {("x", "read"): 2}


async def test_a_rule_naming_a_virtual_directory_decides_it(vfs: VFSClient) -> None:
    """A rule that names a virtual directory decides it, like any path: ``Rule("/a")`` with no actions hides ``/a``
    though a mount below it may be read, and a ``/**`` read rule makes the listing agree with ``stat``."""
    mounts = {"/a/b/ws": f"/workspaces/{W1}"}
    hide_a = VFSView(vfs, mounts, Policy({"x": [Rule("/a"), Rule("/a/b/ws/**", "read")]}), "x")
    await raises("EACCES", hide_a.stat("/a"), "/a")
    assert [e.path for e in await hide_a.readdir("/")] == []
    assert [e.path for e in await hide_a.readdir("/a/b")] == ["/a/b/ws"]  # /a/b is unmatched, a readable mount below
    fallback = VFSView(vfs, mounts, Policy({"x": [Rule("/a/b/ws/**"), Rule("/**", "read")]}), "x")
    assert [e.path for e in await fallback.readdir("/")] == ["/a"]
    assert (await fallback.stat("/a")).kind == "dir"
    assert [e.path for e in await fallback.readdir("/a/b")] == []  # the mount point is hidden by its rule
    hide_root = VFSView(vfs, mounts, Policy({"x": [Rule("/"), Rule("/a/b/ws/**", "read")]}), "x")
    await raises("EACCES", hide_root.readdir("/"), "/")


async def test_read_unmatched_reads_virtual_directories(vfs: VFSClient) -> None:
    """With ``read_unmatched``, a virtual directory no rule matches may be read even where the mount below it may
    not be."""
    old = VFSView(vfs, {"/a/b/ws": f"/workspaces/{W1}"}, Policy({"x": [Rule("/a/b/ws/**")]}, read_unmatched=True), "x")
    assert [e.path for e in await old.readdir("/")] == ["/a"]
    assert [e.path for e in await old.readdir("/a/b")] == []


async def test_a_virtual_listing_doesnt_stat_a_hidden_mount_point(
    vfs: VFSClient, policy: Policy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A virtual listing skips a mount point the actor may not read without stat-ing it."""
    agent = VFSView(vfs, {"/workspace": f"/workspaces/{W1}", "/shared": "/docs"}, policy, "agent")
    seen: list[str] = []
    real_stat = vfs.stat

    async def stat(path: str) -> Entry:
        seen.append(path)
        return await real_stat(path)

    monkeypatch.setattr(vfs, "stat", stat)
    assert [e.path for e in await agent.readdir("/")] == ["/workspace"]
    assert seen == [f"/workspaces/{W1}"]


async def test_glob_and_grep_from_the_root_skip_a_mount_no_rule_covers(vfs: VFSClient, policy: Policy) -> None:
    """A glob and a grep from the virtual root search only the mounts something in them may be read, and skipping a
    mount counts no denial."""
    agent = VFSView(vfs, {"/workspace": f"/workspaces/{W1}", "/shared": "/docs"}, policy, "agent")
    paths = [e.path for e in (await agent.glob("**", "/")).entries]
    assert "/workspace/notes.md" in paths
    assert not any(p.startswith("/shared") for p in paths)
    assert (await agent.grep("guide", "/")).hits == []
    assert [h.path for h in (await agent.grep("mine", "/")).hits] == ["/workspace/notes.md"]
    assert policy.denials == {}


async def test_a_rule_narrower_than_its_mount_leaves_the_mount_point_unreadable(vfs: VFSClient) -> None:
    """A rule that matches only below a mount point leaves the mount point itself unreadable: it is refused, left out
    of the root's listing, and a glob from the root still reaches what the rule covers."""
    await vfs.create(f"/workspaces/{W1}/src/a.md", b"x", parents=True)
    narrow = VFSView(vfs, workspace_mounts(W1), Policy({"x": [Rule("/workspace/src/**", "read", "write")]}), "x")
    await raises("EACCES", narrow.stat("/workspace"), "/workspace")
    assert [e.path for e in await narrow.readdir("/")] == []
    assert [e.path for e in await narrow.readdir("/workspace/src")] == ["/workspace/src/a.md"]
    assert [e.path for e in (await narrow.glob("**", "/")).entries] == ["/workspace/src/a.md"]
    assert [h.path for h in (await narrow.grep("x", "/")).hits] == ["/workspace/src/a.md"]


async def test_the_default_policy_still_reads_the_root(vfs: VFSClient, policy: Policy) -> None:
    """Each actor of ``DEFAULT_POLICY`` may read the root, and nothing is counted as denied."""
    for actor in ("agent", "ingestion", "operator"):
        await view(vfs, policy, actor).stat("/")
    assert policy.denials == {}


async def test_view_allows_maps_the_path_first(vfs: VFSClient, policy: Policy) -> None:
    """``VFSView.allows`` decides as the view's operations do: a path outside every mount is ENOENT, and a refusal
    it reports isn't counted."""
    agent = view(vfs, policy)
    assert agent.allows("read", "/workspace/notes.md") is True
    assert agent.allows("write", "/docs/guide.md") is False
    assert agent.allows("read", "/") is True
    with pytest.raises(VFSError) as info:
        agent.allows("read", "/elsewhere/x.md")
    assert info.value.errno == "ENOENT"
    assert policy.denials == {}


async def test_a_legacy_name_is_listed_escaped(vfs: VFSClient, db: str) -> None:
    """A name from before migration 016 (here: the constraint dropped and the node renamed in SQL) doesn't fail the
    listing or the search it is in, is matched by the policy as stored and reported with its control characters
    escaped; the raw path is refused when it comes back."""
    await vfs.create(f"/workspaces/{W1}/dir/f.md", b"needle\n", parents=True)
    async with await AsyncConnection.connect(db, autocommit=True) as conn:
        await conn.execute("ALTER TABLE vfs_nodes DROP CONSTRAINT vfs_nodes_name_check")
        await conn.execute("UPDATE vfs_nodes SET name = %s WHERE name = 'dir'", ("le\ngacy",))
        await conn.execute("UPDATE vfs_nodes SET name = %s WHERE name = 'log.md'", ("x\x1b[31m.md",))
    # Not readable as a whole subtree, so grep filters each file by its (legacy) path.
    rules = [Rule("/workspace/notes.md"), Rule("/workspace/le*/hidden.md"), Rule("/workspace/**", "read", "write")]
    agent = view(vfs, Policy({"agent": rules}))
    directory, file = r"/workspace/le\u000agacy", r"/workspace/x\u001b[31m.md"
    assert [e.path for e in await agent.readdir("/workspace")] == [directory, file]
    assert {e.path for e in (await agent.glob("**/*.md", "/workspace")).entries} == {directory + "/f.md", file}
    assert [(h.path, h.line) for h in (await agent.grep("needle", "/workspace")).hits] == [(directory + "/f.md", 1)]
    await raises("EINVAL", agent.readdir("/workspace/le\ngacy"))


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
    assert [e.path for e in (await hidden.glob("**", "/")).entries] == ["/docs/guide.md"]
    assert not hidden.policy.allows_any("agent", "read", "/workspace")
    assert hidden.policy.allows_any("agent", "read", "/")
    assert Policy({"a": [Rule("/w/x.md", "read"), Rule("/w/**")]}).allows_any("a", "read", "/w")


async def test_glob_bounds_each_walk(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """#85: ``max_nodes`` and ``timeout`` reach every walk the view makes: a mount's own, each mount's below
    ``/``, and the second, wider look past matches the policy hides (each call's arguments are recorded). A cap of
    0 is refused from a mount and from ``/``; bad bounds are EINVAL."""
    calls: list[dict[str, object]] = []
    original = vfs.glob

    async def recording(*args: Any, **kwargs: Any) -> list[Entry]:
        calls.append(kwargs)
        return await original(*args, **kwargs)

    monkeypatch.setattr(vfs, "glob", recording)
    agent = view(vfs, Policy(DEFAULT_POLICY.rules))
    hiding = view(vfs, Policy({"agent": [Rule("/workspace/hid/**"), Rule("/**", "read")]}))
    for i in range(3):  # newer than everything else, and hidden: the first look (limit + 1 = 2) sees only these
        await vfs.create(f"/workspaces/{W1}/hid/{i}.md", b"", parents=True)
    for glob, path, expected_calls in [
        (agent.glob, "/workspace", 1),
        (agent.glob, "/", 2),
        (hiding.glob, "/workspace", 2),
    ]:
        calls.clear()
        await glob("**", path, limit=1, max_nodes=12345, timeout=1.5)
        assert len(calls) == expected_calls, calls
        assert all((c["max_nodes"], c["timeout"]) == (12345, 1.5) for c in calls), calls
    everything = await agent.glob("**", "/")
    assert everything.entries
    for path in ("/workspace", "/"):
        with pytest.raises(SearchRefused) as info:
            await agent.glob("**", path, max_nodes=0)
        assert info.value.reason == "nodes"
        with pytest.raises(SearchRefused):
            await agent.glob("**", path, max_nodes=0, limit=1)
    assert await agent.glob("**", "/", max_nodes=None) == everything
    for max_nodes in (-1, 2**31):
        await raises("EINVAL", agent.glob("**", "/workspace", max_nodes=max_nodes), "/workspace")
    for timeout in (0, 1e10):
        await raises("EINVAL", agent.glob("**", "/workspace", timeout=timeout), "/workspace")


async def test_glob_limit_counts_only_readable_files(vfs: VFSClient) -> None:
    hiding = view(vfs, Policy({"agent": [Rule("/workspace/log.md"), Rule("/**", "read")]}))
    await vfs.append(f"/workspaces/{W1}/log.md", b"newest")
    assert [e.path for e in (await hiding.glob("*.md", "/workspace", limit=1)).entries] == ["/workspace/notes.md"]
    assert [e.path for e in (await hiding.glob("**/log.md", "/", limit=1)).entries] == []


async def test_glob_looks_past_hidden_matches_a_bounded_distance(vfs: VFSClient) -> None:
    hiding = view(vfs, Policy({"agent": [Rule("/workspace/hid/**"), Rule("/**", "read")]}))
    for i in range(7):  # newer than notes.md and log.md
        await vfs.create(f"/workspaces/{W1}/hid/{i}.md", b"", parents=True)
    result = await hiding.glob("**", "/workspace", limit=1)
    assert ([e.path for e in result.entries], result.truncated) == (["/workspace/log.md"], True)  # notes.md
    for i in range(7, 9):  # now 9 hidden matches come first and fill the 9 the re-fetch looks at
        await vfs.create(f"/workspaces/{W1}/hid/{i}.md", b"")
    assert await hiding.glob("**", "/workspace", limit=1) == GlobResult([], False)


async def test_glob_looks_past_hidden_matches_across_mounts(vfs: VFSClient) -> None:
    hiding = view(vfs, Policy({"agent": [Rule("/docs/hid/**"), Rule("/**", "read")]}))
    for i in range(7):  # newer than every readable file; the match past the limit, log.md, is in /workspace
        await vfs.create(f"/docs/hid/{i}.md", b"", parents=True)
    result = await hiding.glob("**", "/", limit=1)
    assert ([e.path for e in result.entries], result.truncated) == (["/docs/guide.md"], True)
    for i in range(7, 9):  # the window, applied after the merge, is all hidden
        await vfs.create(f"/docs/hid/{i}.md", b"")
    assert await hiding.glob("**", "/", limit=1) == GlobResult([], False)


async def test_glob_is_truncated_only_past_its_limit(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)

    async def glob(path: str, limit: int) -> tuple[list[str], bool]:
        result = await agent.glob("*.md", path, limit=limit)
        return [e.path for e in result.entries], result.truncated

    async def glob_from_root(pattern: str, limit: int) -> tuple[list[str], bool]:
        result = await agent.glob(pattern, "/", limit=limit)
        return [e.path for e in result.entries], result.truncated

    assert await glob("/workspace", 2) == (["/workspace/log.md", "/workspace/notes.md"], False)  # exactly limit
    assert await glob("/workspace", 1) == (["/workspace/log.md"], True)
    assert await glob("/workspace", 0) == ([], True)
    assert await glob("/docs", 1) == (["/docs/guide.md"], False)
    # From /, each mount is asked for limit + 1 and the merge tells: 3 files in all, newest first.
    assert await glob("/", 3) == (["/docs/guide.md", "/workspace/log.md", "/workspace/notes.md"], False)
    assert await glob("/", 4) == (["/docs/guide.md", "/workspace/log.md", "/workspace/notes.md"], False)
    assert await glob("/", 2) == (["/docs/guide.md", "/workspace/log.md"], True)
    assert await glob("/", 0) == ([], True)
    # Only one mount matches: its own limit + 1 tells.
    workspace = ["/workspace/log.md", "/workspace/notes.md"]
    assert await glob_from_root("workspace/*.md", 2) == (workspace, False)
    assert await glob_from_root("workspace/*.md", 1) == (workspace[:1], True)
    assert (await agent.glob("*.md", "/")).truncated is False  # no limit
    assert await agent.glob("*.txt", "/", limit=0) == GlobResult([], False)
    assert (await agent.glob("*.md", "/", limit=2**31 - 2)).truncated is False  # the largest: it asks for one more
    await raises("EINVAL", agent.glob("*.md", "/", limit=2**31 - 1), "/")


async def test_hidden_files_never_make_a_glob_truncated(vfs: VFSClient) -> None:
    rules = [Rule("/workspace/log.md"), Rule("/docs/secret.md"), Rule("/**", "read")]
    hiding = view(vfs, Policy({"agent": rules}))
    await vfs.create("/docs/secret.md", b"")  # newest, then guide.md, log.md, notes.md

    async def glob(path: str, limit: int) -> tuple[list[str], bool]:
        result = await hiding.glob("*.md", path, limit=limit)
        return [e.path for e in result.entries], result.truncated

    # The hidden files come before, between and after the readable ones, and neither count nor truncate.
    assert await glob("/", 2) == (["/docs/guide.md", "/workspace/notes.md"], False)
    assert await glob("/", 1) == (["/docs/guide.md"], True)
    assert await glob("/workspace", 1) == (["/workspace/notes.md"], False)
    assert await glob("/docs", 1) == (["/docs/guide.md"], False)
    assert await glob("/docs", 0) == ([], True)  # guide.md, past the secret.md that comes first
    await vfs.append(f"/workspaces/{W1}/log.md", b"newer")  # a hidden file past the last readable one
    await vfs.append(f"/workspaces/{W1}/notes.md", b"newest")
    assert await glob("/workspace", 1) == (["/workspace/notes.md"], False)
    assert await glob("/", 2) == (["/workspace/notes.md", "/docs/guide.md"], False)


async def test_ingestion_globs_only_the_docs(vfs: VFSClient, policy: Policy) -> None:
    ingestion = view(vfs, policy, "ingestion")
    assert [e.path for e in (await ingestion.glob("**", "/")).entries] == ["/docs/guide.md"]
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
    page = await agent.pread_lines("/workspace/notes.md", max_line_bytes=2)
    assert (page.content, page.cut) == (b"on\ntw\nth\n", frozenset({1, 2, 3}))


async def test_pread_many(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    await vfs.write(f"/workspaces/{W1}/notes.md", b"mine\n")
    reads = [
        ("/workspace/notes.md", 0, 100),
        ("/workspace/none.md", 0, 1),
        (f"/workspaces/{W2}/secret.md", 0, 1),
        ("/", 0, 1),
        ("/workspace", 0, 1),
    ]
    notes, *errors = await agent.pread_many(reads)
    assert isinstance(notes, ReadResult) and notes.content == b"mine\n"
    expected = [
        ("ENOENT", "/workspace/none.md"),
        ("ENOENT", f"/workspaces/{W2}/secret.md"),
        ("EISDIR", "/"),
        ("EISDIR", "/workspace"),
    ]
    for error, (errno, path) in zip(errors, expected, strict=True):
        assert isinstance(error, VFSError) and (error.errno, error.path, error.detail) == (errno, path, "")
    ingestion = view(vfs, policy, "ingestion")
    denied, guide = await ingestion.pread_many([("/workspace/notes.md", 0, 1), ("/docs/guide.md", 0, 100)])
    assert isinstance(denied, VFSError) and (denied.errno, denied.path) == ("EACCES", "/workspace/notes.md")
    assert isinstance(guide, ReadResult)
    with pytest.raises(ValueError):
        await agent.pread_many(reads, groups=[0])
    (only,) = await agent.pread_many([("/", 0, 1)])  # nothing to send
    assert isinstance(only, VFSError) and only.errno == "EISDIR"


async def test_pread_page(vfs: VFSClient, policy: Policy) -> None:
    ingestion = view(vfs, policy, "ingestion")
    await vfs.write("/docs/guide.md", b"one\ntwo\n")
    page = await ingestion.pread_page(
        "/docs/guide.md",
        4096,
        2,
        1,
        1000,
        also=[
            ("/workspace/notes.md", 0, 1),  # denied
            (f"/workspaces/{W2}/secret.md", 0, 1),  # outside every mount
            ("/", 0, 1),  # a virtual directory
            ("/docs/guide.md", 1, 3),  # readable
        ],
    )
    assert page.sample.content == b"one\ntwo\n"
    assert (page.lines.content, page.lines.first_line, page.lines.lines) == (b"two\n", 2, 2)
    denied, outside, virtual, guide = page.also
    expected = [
        ("EACCES", "/workspace/notes.md"),
        ("ENOENT", f"/workspaces/{W2}/secret.md"),
        ("EISDIR", "/"),
    ]
    for error, (errno, path) in zip((denied, outside, virtual), expected, strict=True):
        assert isinstance(error, VFSError) and (error.errno, error.path, error.detail) == (errno, path, "")
    assert isinstance(guide, ReadResult) and guide.content == b"ne\n"
    assert policy.denials == {}  # the also reads are speculative: their refusals aren't counted
    await raises("EACCES", ingestion.pread_page("/workspace/notes.md", 4096, 1, 10, 1000), "/workspace/notes.md")
    assert policy.denials == {("ingestion", "read"): 1}  # the file's refusal is
    agent = view(vfs, policy)
    await raises("EISDIR", agent.pread_page("/", 4096, 1, 10, 1000), "/")
    await raises("ENOENT", agent.pread_page(f"/workspaces/{W2}/secret.md", 4096, 1, 10, 1000))


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
    agent = VFSView(vfs, workspace_mounts("44444444-4444-4444-4444-444444444444"), policy, "agent")
    assert [h.path for h in (await agent.grep("guide", "/")).hits] == ["/docs/guide.md"]


async def test_grep_leaves_out_what_the_actor_may_not_read(vfs: VFSClient) -> None:
    env = Policy({"agent": [Rule("/workspace/**/.env"), Rule("/workspace/private/**"), Rule("/**", "read", "write")]})
    agent = view(vfs, env)
    await vfs.create(f"/workspaces/{W1}/app/.env", b"KEY=secret", parents=True)  # 10 bytes
    await vfs.create(f"/workspaces/{W1}/app/config.py", b"secret = 1\n")  # 11 bytes
    await vfs.create(f"/workspaces/{W1}/private/p.md", b"secret\n", parents=True)  # 7 bytes
    result = await agent.grep("secret", "/workspace")
    assert [(h.path, h.line) for h in result.hits] == [("/workspace/app/config.py", 1)]
    assert (result.files, result.bytes) == (3, 19)  # config.py, log.md and notes.md: never .env or p.md
    result = await agent.grep("secret", "/")
    assert [h.path for h in result.hits] == ["/workspace/app/config.py"]
    assert (result.files, result.bytes) == (4, 24)  # with /docs/guide.md
    assert (await agent.grep("secret", "/workspace", max_bytes=19)).files == 3  # unreadable bytes don't count
    with pytest.raises(SearchRefused):  # unreadable files do count toward the file cap: the walk stops past it
        await agent.grep("secret", "/workspace", max_files=4)
    await raises("EACCES", agent.grep("secret", "/workspace/app/.env"), "/workspace/app/.env")
    await raises("EACCES", agent.grep("secret", "/workspace/private"), "/workspace/private")  # /** matches it too
    assert (await agent.grep("mine", "/workspace/notes.md")).files == 1  # a file needs read on itself only
    assert (await agent.grep("secret", "/docs")).hits == []
    assert env.denials == {("agent", "read"): 2}


async def test_grep_follows_a_rule_on_a_directory_alone(vfs: VFSClient) -> None:
    # A rule without /** covers the directory, not its files, as for glob and pread.
    env = Policy({"agent": [Rule("/workspace/secret"), Rule("/**", "read", "write")]})
    agent = view(vfs, env)
    await vfs.create(f"/workspaces/{W1}/secret/a.txt", b"needle\n", parents=True)
    await raises("EACCES", agent.grep("needle", "/workspace/secret"), "/workspace/secret")
    assert [h.path for h in (await agent.grep("needle", "/workspace")).hits] == ["/workspace/secret/a.txt"]
    assert (await agent.pread("/workspace/secret/a.txt")).content == b"needle\n"


async def test_a_file_checked_alone_is_pinned(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    # The policy lets only a file be moved at these paths. If a directory takes the file's place after the view's
    # stat, the operation must not reach it. (A grep needs no stat: see test_grep_filters_what_replaced_a_file.)
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
    assert (await real_stat(f"/workspaces/{W1}/app/.env")).size == 10


async def test_grep_filters_what_replaced_a_file(vfs: VFSClient, monkeypatch: pytest.MonkeyPatch) -> None:
    # The view decides on the path alone: a directory that takes a checked file's place before the client's
    # snapshot is filtered like any other.
    env = Policy({"agent": [Rule("/workspace/**/.env"), Rule("/**", "read", "write")]})
    agent = view(vfs, env)
    app = f"/workspaces/{W1}/app"
    await vfs.create(app, b"secret\n")
    real_grep = vfs.grep

    async def swapped(*args: Any, **kwargs: Any) -> GrepResult:
        await vfs.unlink(app)
        await vfs.create(f"{app}/.env", b"KEY=secret\n", parents=True)
        return await real_grep(*args, **kwargs)

    monkeypatch.setattr(vfs, "grep", swapped)
    assert (await agent.grep("secret", "/workspace/app")).hits == []


async def test_grep_from_a_virtual_directory_checks_a_file_mount(vfs: VFSClient) -> None:
    # A mount whose real root is a file, under a rule on the mount point alone: searching it from / must not read it.
    env = Policy({"agent": [Rule("/docs"), Rule("/**", "read", "write")]})
    agent = view(vfs, env)
    await vfs.unlink("/docs/guide.md")
    await vfs.unlink("/docs")
    await vfs.create("/docs", b"TOPSECRET\n")
    await raises("EACCES", agent.grep("TOPSECRET", "/docs"), "/docs")
    result = await agent.grep("TOPSECRET", "/")
    assert (result.hits, result.files, result.bytes) == ([], 2, 8)  # /workspace's notes.md and log.md only


def test_rules() -> None:
    p = Policy(
        {
            "a": [Rule("/w/keep.md", "read"), Rule("/w/**", "read", "write"), Rule("/w/*/*.log"), Rule("/hidden/**")],
        },
        read_unmatched=True,
    )
    assert p.allows("a", "write", "/w") and p.allows("a", "write", "/w/x/y.md")  # ** matches the directory too
    assert not p.allows("a", "write", "/w/keep.md")  # the first match wins
    assert p.allows("a", "write", "/w/x/y.log")  # shadowed by /w/**
    assert p.allows("a", "append", "/w/z")  # write implies append
    assert not p.allows("a", "read", "/hidden") and not p.allows("a", "read", "/hidden/a/b")
    assert p.allows("a", "read", "/other") and not p.allows("a", "write", "/other")  # read_unmatched
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
    with pytest.raises(PolicyDenied):  # a speculative lookup: refused all the same, but not counted
        p.check("a", "write", "/w/keep.md", count_denials=False)
    assert p.denials == {("a", "write"): 1}
    denial = info.value
    assert isinstance(denial, PolicyDenied) and (denial.actor, denial.action) == ("a", "write")
    copy = pickle.loads(pickle.dumps(denial))
    assert type(copy) is PolicyDenied and (copy.path, copy.actor, copy.action, str(copy)) == (
        denial.path,
        denial.actor,
        denial.action,
        str(denial),
    )
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
    result = await agent.glob("*.md", "/workspace")
    assert [e.path for e in result.entries] == ["/workspace/log.md", "/workspace/notes.md"]
    await raises("ENOENT", agent.glob("*.md", "/workspace/none"), "/workspace/none")


async def test_glob_below_a_virtual_directory(vfs: VFSClient, policy: Policy) -> None:
    agent = view(vfs, policy)
    await vfs.create(f"/workspaces/{W1}/node_modules/m.md", b"", parents=True)
    await vfs.create(f"/workspaces/{W1}/src/a.py", b"", parents=True)

    async def paths(pattern: str, *excludes: str, limit: int | None = None) -> list[str]:
        return sorted(e.path for e in (await agent.glob(pattern, "/", excludes=excludes, limit=limit)).entries)

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
    mtimes = [e.mtime for e in (await agent.glob("**", "/")).entries if e.mtime is not None]
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
                seen = (await tester.glob(pattern, directory, excludes=excludes, hidden=hidden)).entries
            except VFSError as e:
                assert e.errno == "EINVAL", (pattern, excludes)
                continue
            real = await vfs.glob(pattern, _MIRROR + directory.rstrip("/"), excludes=excludes, hidden=hidden)
            expected = sorted(e.path.removeprefix(_MIRROR) for e in real)
            assert sorted(e.path for e in seen) == expected, (directory, pattern, excludes)


async def test_glob_reports_a_bad_root_at_its_path(
    vfs: VFSClient, policy: Policy, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A vfs_glob that reports ENOENT for its root (unreachable today: the client resolves the root in the same
    snapshot, so faked here) is reported at the path the root was resolved from, not at the root's id, and the view
    strips id and detail."""
    real_glob = globbing.glob
    fake_root = uuid4()  # vfs_glob returns ENOENT for a root that isn't there

    async def broken_glob(conn: AsyncConnection, root: UUID | None, pattern: str, **kwargs: Any) -> list[GlobMatch]:
        return await real_glob(conn, fake_root, pattern, **kwargs)

    monkeypatch.setattr(globbing, "glob", broken_glob)
    await raises("ENOENT", vfs.glob("*", f"/workspaces/{W1}"), f"/workspaces/{W1}")
    error = await raises("ENOENT", view(vfs, policy).glob("*", "/workspace"), "/workspace")
    assert str(fake_root) not in str(error)
    assert error.detail == ""


async def test_an_error_path_that_is_not_a_path_is_reported_at_the_target(
    vfs: VFSClient, policy: Policy, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A client error whose path isn't an absolute path (a node id, say) is reported at the target: the view
    neither shows the string nor fails on it. One warning is logged each time, without the string."""
    agent = view(vfs, policy)
    with caplog.at_level(logging.WARNING, logger="minifs.vfs.view"):
        fake = uuid4()

        async def broken_rename(src: str, dst: str, *, expect: UUID | None = None) -> None:
            raise VFSError("ENOENT", str(fake), detail="real detail")

        # Two targets: the error is reported at the first (the source). Only rename is patched, so the stat
        # `_pin` does on the source is the real one: this case must come before the loop below.
        monkeypatch.setattr(vfs, "rename", broken_rename)
        error = await raises(
            "ENOENT", agent.rename("/workspace/notes.md", "/workspace/moved.md"), "/workspace/notes.md"
        )
        assert error.detail == ""
        assert str(fake) not in str(error)
        bads = [str(uuid4()), "None", ""]
        for bad in bads:

            async def broken_stat(path: str, bad: str = bad) -> Entry:
                raise VFSError("ENOENT", bad, detail="real detail")

            monkeypatch.setattr(vfs, "stat", broken_stat)
            error = await raises("ENOENT", agent.stat("/workspace/notes.md"), "/workspace/notes.md")
            assert error.detail == ""
            if bad:
                assert bad not in str(error)
        messages = [r.getMessage() for r in caplog.records if "isn't absolute" in r.getMessage()]
        assert len(messages) == 4  # the rename case, then one per bad path
        for message in messages:
            assert "isn't absolute" in message
            assert "ENOENT" in message
            assert str(fake) not in message
            assert not any(bad and bad in message for bad in bads)


async def test_a_result_path_that_is_not_a_path_is_eio(
    vfs: VFSClient, policy: Policy, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A listing entry whose path isn't an absolute path (a client bug) fails the call as EIO at the directory,
    rather than being reported, and policy-checked, as the directory itself; the string is shown nowhere."""
    fake = str(uuid4())

    async def broken_readdir(path: str, *, after: str | None = None, limit: int | None = None) -> list[Entry]:
        return [Entry(fake, uuid4(), "file", 0, None)]

    monkeypatch.setattr(vfs, "readdir", broken_readdir)
    with caplog.at_level(logging.ERROR, logger="minifs.vfs.view"):
        error = await raises("EIO", view(vfs, policy).readdir("/workspace"), "/workspace")
    assert fake not in str(error)
    assert error.detail == ""
    messages = [r.getMessage() for r in caplog.records if "isn't absolute" in r.getMessage()]
    assert len(messages) == 1
    assert fake not in messages[0]
