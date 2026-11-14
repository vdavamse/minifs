"""VFSView.watch: events through the view, in visible paths and under the policy, over a real database."""

import asyncio
from collections.abc import AsyncIterator
from uuid import UUID

import pytest

from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import VFSClient
from minifs.vfs.core import VFSError
from minifs.vfs.policy import Policy, PolicyDenied, Rule
from minifs.vfs.view import VFSView, workspace_mounts
from minifs.vfs.view_watch import ViewWatch, WatchEvent
from minifs.vfs.watch import RESCAN, Watcher
from tests.test_watch import factory

W1, W2 = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"


@pytest.fixture
async def vfs(db: str) -> AsyncIterator[VFSClient]:
    async with open_pool(Connector(db), max_size=3) as pool:
        client = VFSClient(pool)
        await client.create(f"/workspaces/{W1}/notes.md", b"mine", parents=True)
        await client.create(f"/workspaces/{W1}/d/old.md", b"old\n", parents=True)
        await client.create(f"/workspaces/{W2}/secret.md", b"theirs", parents=True)
        await client.create("/docs/guide.md", b"guide", parents=True)
        yield client


@pytest.fixture
async def watcher(db: str) -> AsyncIterator[Watcher]:
    async with Watcher(factory(db)) as w:
        yield w


def agent_policy() -> Policy:
    """Everything in /workspace is readable and writable except any file named .env; /docs is readable."""
    return Policy(
        {"agent": [Rule("/workspace/**/.env"), Rule("/workspace/**", "read", "write"), Rule("/docs/**", "read")]}
    )


def view(vfs: VFSClient, watcher: Watcher, policy: Policy | None = None) -> VFSView:
    return VFSView(vfs, workspace_mounts(W1), policy or agent_policy(), "agent", watcher=watcher)


async def next_seen(w: ViewWatch) -> WatchEvent:
    return await asyncio.wait_for(w.get(), 10)


async def drained(w: ViewWatch) -> list[WatchEvent]:
    """Every event left until the watch ends, failing instead of hanging if it doesn't."""

    async def collect() -> list[WatchEvent]:
        return [e async for e in w]

    return await asyncio.wait_for(collect(), 10)


async def test_start_refusals(vfs: VFSClient, watcher: Watcher) -> None:
    """Starting a watch refuses, in visible paths: outside the mounts, missing, hidden, virtual, no watcher."""
    agent = view(vfs, watcher)
    with pytest.raises(VFSError) as info:
        async with agent.watch("/elsewhere"):
            pass
    assert (info.value.errno, info.value.path) == ("ENOENT", "/elsewhere")
    with pytest.raises(VFSError) as info:
        async with agent.watch("/workspace/missing"):
            pass
    assert (info.value.errno, info.value.path) == ("ENOENT", "/workspace/missing")
    assert "/workspaces/" not in str(info.value)
    await vfs.create(f"/workspaces/{W1}/.env", b"x")
    with pytest.raises(PolicyDenied) as info:
        async with agent.watch("/workspace/.env"):
            pass
    assert (info.value.errno, info.value.path) == ("EACCES", "/workspace/.env")
    with pytest.raises(VFSError) as info:
        async with agent.watch("/"):
            pass
    assert (info.value.errno, info.value.path) == ("EINVAL", "/")
    bare = VFSView(vfs, workspace_mounts(W1), agent_policy(), "agent")  # no watcher=
    with pytest.raises(RuntimeError):
        async with bare.watch("/workspace"):
            pass


async def test_directory_events_are_in_visible_paths(vfs: VFSClient, watcher: Watcher) -> None:
    """A directory watch reports a child's create, modify, move and delete in visible paths, never real ones."""
    agent = view(vfs, watcher)
    async with agent.watch("/workspace/d") as w:
        await agent.create("/workspace/d/a.md", b"x")
        created = await next_seen(w)
        await agent.append("/workspace/d/a.md", b"more\n")
        entry = await agent.stat("/workspace/d/a.md")
        modified = await next_seen(w)
        await agent.rename("/workspace/d/a.md", "/workspace/d/b.md")
        moved = await next_seen(w)
        await agent.unlink("/workspace/d/b.md")
        deleted = await next_seen(w)
    assert created == WatchEvent("create", "/workspace/d/a.md", kind="file")
    assert modified == WatchEvent("modify", "/workspace/d/a.md", size=entry.size, generation=entry.generation)
    assert moved == WatchEvent("move", "/workspace/d/b.md", kind="file", old_path="/workspace/d/a.md")
    assert deleted == WatchEvent("delete", "/workspace/d/b.md", kind="file")
    assert "/workspaces/" not in repr((created, modified, moved, deleted))


async def test_a_hidden_child_is_not_reported(vfs: VFSClient, watcher: Watcher) -> None:
    """A child the policy hides is delivered for no op: the next event after its life is a visible marker's."""
    agent = view(vfs, watcher)
    async with agent.watch("/workspace/d") as w:
        await vfs.create(f"/workspaces/{W1}/d/.env", b"KEY=1")
        await vfs.append(f"/workspaces/{W1}/d/.env", b"\nMORE=2\n")
        await vfs.unlink(f"/workspaces/{W1}/d/.env")
        await agent.create("/workspace/d/marker", b"")
        assert await next_seen(w) == WatchEvent("create", "/workspace/d/marker", kind="file")


async def test_a_rename_between_readable_and_hidden(vfs: VFSClient, watcher: Watcher) -> None:
    """A rename to a hidden name is the delete the actor can see; a rename back, the create."""
    agent = view(vfs, watcher)
    async with agent.watch("/workspace/d") as w:
        await vfs.rename(f"/workspaces/{W1}/d/old.md", f"/workspaces/{W1}/d/.env")
        assert await next_seen(w) == WatchEvent("delete", "/workspace/d/old.md", kind="file")
        await vfs.rename(f"/workspaces/{W1}/d/.env", f"/workspaces/{W1}/d/old.md")
        assert await next_seen(w) == WatchEvent("create", "/workspace/d/old.md", kind="file")


async def test_moves_across_the_view_name_only_this_side(vfs: VFSClient, watcher: Watcher) -> None:
    """A move into the watched directory is a create, out of it a delete: the other side is never named."""
    agent = view(vfs, watcher)
    async with agent.watch("/workspace/d") as w:
        await vfs.rename(f"/workspaces/{W2}/secret.md", f"/workspaces/{W1}/d/in.md")
        incoming = await next_seen(w)
        await vfs.rename(f"/workspaces/{W1}/d/in.md", f"/workspaces/{W2}/back.md")
        outgoing = await next_seen(w)
    assert incoming == WatchEvent("create", "/workspace/d/in.md", kind="file")
    assert outgoing == WatchEvent("delete", "/workspace/d/in.md", kind="file")
    blob = repr((incoming, outgoing))
    assert W2 not in blob and "secret" not in blob


async def test_a_preexisting_child_modify_is_reported(vfs: VFSClient, watcher: Watcher) -> None:
    """A modify of a child the watch never saw created is looked up once and reported; a hidden one isn't."""
    agent = view(vfs, watcher)
    await vfs.create(f"/workspaces/{W1}/d/.env", b"KEY=1")  # before the watch: its name is not known
    async with agent.watch("/workspace/d") as w:
        await agent.append("/workspace/d/old.md", b"more\n")
        entry = await agent.stat("/workspace/d/old.md")
        assert await next_seen(w) == WatchEvent(
            "modify", "/workspace/d/old.md", size=entry.size, generation=entry.generation
        )
        await vfs.append(f"/workspaces/{W1}/d/.env", b"\nMORE=2\n")
        await agent.create("/workspace/d/marker", b"")
        assert await next_seen(w) == WatchEvent("create", "/workspace/d/marker", kind="file")


async def test_a_file_watch_follows_a_rename_in_place(vfs: VFSClient, watcher: Watcher) -> None:
    """A file watch follows its node through a rename to a name the actor may read, and reports it."""
    agent = view(vfs, watcher)
    async with agent.watch("/workspace/notes.md") as w:
        await agent.append("/workspace/notes.md", b"!")
        entry = await agent.stat("/workspace/notes.md")
        assert await next_seen(w) == WatchEvent(
            "modify", "/workspace/notes.md", kind="file", size=entry.size, generation=entry.generation
        )
        await agent.rename("/workspace/notes.md", "/workspace/renamed.md")
        assert await next_seen(w) == WatchEvent(
            "move", "/workspace/renamed.md", kind="file", old_path="/workspace/notes.md"
        )
        assert w.path == "/workspace/renamed.md"
        await agent.append("/workspace/renamed.md", b"again\n")
        entry = await agent.stat("/workspace/renamed.md")
        assert await next_seen(w) == WatchEvent(
            "modify", "/workspace/renamed.md", kind="file", size=entry.size, generation=entry.generation
        )


@pytest.mark.parametrize("dst", [f"/workspaces/{W1}/d/notes.md", f"/workspaces/{W1}/.env"])
async def test_a_file_watch_ends_when_its_node_leaves(vfs: VFSClient, watcher: Watcher, dst: str) -> None:
    """A watched file moved to another directory, or to a name the actor may not read, is reported deleted
    and the watch ends: the view can't tell where the node went."""
    agent = view(vfs, watcher)
    async with agent.watch("/workspace/notes.md") as w:
        await vfs.rename(f"/workspaces/{W1}/notes.md", dst)
        assert await next_seen(w) == WatchEvent("delete", "/workspace/notes.md", kind="file")
        assert await drained(w) == []


async def test_a_moved_mount_point_ends_the_watch(vfs: VFSClient, watcher: Watcher) -> None:
    """A mount point whose real directory is moved away is reported deleted, and the watch ends."""
    agent = view(vfs, watcher)
    async with agent.watch("/workspace") as w:
        await vfs.rename(f"/workspaces/{W1}", "/workspaces/moved")
        assert await next_seen(w) == WatchEvent("delete", "/workspace", kind="dir")
        assert await drained(w) == []


async def test_a_rescan_is_reported_at_the_watched_path(vfs: VFSClient, watcher: Watcher) -> None:
    """A rescan reaches the actor at its visible watched path, and nothing else on it."""
    agent = view(vfs, watcher)
    async with agent.watch("/workspace/d") as w:
        watcher._broadcast(RESCAN, now=True)  # pyright: ignore[reportPrivateUsage]
        assert await next_seen(w) == WatchEvent("rescan", "/workspace/d")


async def test_a_failed_name_lookup_is_a_rescan(
    vfs: VFSClient, watcher: Watcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A modify whose name lookup fails is reported as a rescan: the actor re-checks the directory."""
    agent = view(vfs, watcher)

    async def broken(self: ViewWatch, child: UUID) -> str | None:
        raise OSError("down")

    monkeypatch.setattr(ViewWatch, "_lookup", broken)
    async with agent.watch("/workspace/d") as w:
        await agent.append("/workspace/d/old.md", b"more\n")
        assert await next_seen(w) == WatchEvent("rescan", "/workspace/d")


async def test_an_unreadable_docs_view(vfs: VFSClient, watcher: Watcher) -> None:
    """A policy that refuses read on /docs refuses to watch it."""
    agent = view(vfs, watcher, Policy({"agent": [Rule("/docs/**"), Rule("/workspace/**", "read", "write")]}))
    with pytest.raises(PolicyDenied):
        async with agent.watch("/docs"):
            pass
