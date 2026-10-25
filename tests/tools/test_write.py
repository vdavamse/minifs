"""``Write``: ladderframe's ``Write`` tests (``packages/harness/tests/test_tools.py``) through a real agent, its output
against ladderframe's on a corpus, and what the VFS adds: the policy, the chroot and lost races."""

import base64
import json
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

import pytest

import minifs.tools._text as text
from minifs.tools import TOOLS, Write
from minifs.tools.write import DESCRIPTION
from minifs.vfs.client import VFSClient
from minifs.vfs.core import FileVersion, ReadResult
from minifs.vfs.policy import Policy, Rule
from minifs.vfs.view import VFSView, workspace_mounts
from tests.tools.conftest import W1, W2, Deps, call_tool, retry_message, run_tool

GOLDEN = json.loads((Path(__file__).parent / "data" / "write_golden.json").read_text())
"""ladderframe's disk ``Write`` (2e347db and b0e0d5f) over a corpus: for each case, the old file (base64, or null
for none), the content written, and the result, metadata (``{path}`` for the file's path) and bytes it gave.
``data/gen_write_golden.py`` regenerates it. The diff helpers are copied verbatim, so this checks how ``Write``
combines them: BOM precedence, ``exists``, decoding with replacement characters, the bytes written."""

STALE = "/workspace/notes.md was modified since it was read. Read it again before writing."


async def content(client: VFSClient, path: str) -> bytes:
    return (await client.pread(f"/workspaces/{W1}/{path}", 0, 1 << 20)).content


async def write(deps: Deps, file_path: str, data: str) -> tuple[Any, dict[str, Any]]:
    """The result and metadata of a ``Write`` call."""
    call = await call_tool(Write, deps, content=data, filePath=file_path)
    assert call.part.metadata is not None
    return call.result, call.part.metadata


# The tool


async def test_schema(deps: Deps) -> None:
    call = await call_tool(Write, deps, content="x", filePath="a.txt")
    (tool_def,) = call.tool_defs
    assert (tool_def.name, tool_def.description, tool_def.sequential) == ("Write", DESCRIPTION, True)
    props = tool_def.parameters_json_schema["properties"]
    assert props["content"]["description"] == "The content to write to the file"
    assert props["filePath"]["description"] == "The absolute path to the file to write (must be absolute, not relative)"
    assert tool_def.parameters_json_schema["required"] == ["content", "filePath"]
    assert Write in TOOLS


# ladderframe's tests


async def test_creates_and_overwrites(deps: Deps, client: VFSClient) -> None:
    result, meta = await write(deps, "deep/new.txt", "a\nb\n")
    assert result == "Wrote file successfully."
    assert await content(client, "deep/new.txt") == b"a\nb\n"
    assert meta["exists"] is False and meta["additions"] == 2
    assert meta["filepath"] == "/workspace/deep/new.txt"
    result, meta = await write(deps, "/workspace/deep/new.txt", "a\nc\n")
    assert meta["exists"] is True
    assert "-b\n+c" in meta["diff"]
    assert await retry_message(Write, deps, content="x", filePath="deep") == (
        "Path is a directory, not a file: /workspace/deep"
    )


async def test_keeps_the_bom(deps: Deps, client: VFSClient) -> None:
    await client.create(f"/workspaces/{W1}/bom.txt", "﻿old".encode())
    await write(deps, "bom.txt", "new")
    assert await content(client, "bom.txt") == "﻿new".encode()


async def test_write_over_a_non_utf8_file(deps: Deps, client: VFSClient) -> None:
    await client.create(f"/workspaces/{W1}/latin.txt", b"caf\xe9\n")
    await write(deps, "latin.txt", "cafe\n")
    assert await content(client, "latin.txt") == b"cafe\n"


@pytest.mark.parametrize("case", GOLDEN, ids=[c["name"] for c in GOLDEN])
async def test_same_output_as_ladderframe(deps: Deps, client: VFSClient, case: dict[str, Any]) -> None:
    name = f"{case['name']}.txt"
    if case["old"] is not None:
        await client.create(f"/workspaces/{W1}/{name}", base64.b64decode(case["old"]))
    result, meta = await write(deps, name, case["content"])
    path = f"/workspace/{name}"
    expected = {k: v.replace("{path}", path) if isinstance(v, str) else v for k, v in case["metadata"].items()}
    assert (result, meta) == (case["result"], expected)
    assert await content(client, name) == base64.b64decode(case["written"])


# The VFS


async def test_creates_missing_parents(deps: Deps, client: VFSClient) -> None:
    await write(deps, "a/b/c.txt", "x")
    assert await content(client, "a/b/c.txt") == b"x"
    assert (await client.stat(f"/workspaces/{W1}/a/b")).is_dir


async def test_overwrite_is_a_new_generation(deps: Deps, client: VFSClient) -> None:
    before = await client.stat(f"/workspaces/{W1}/notes.md")
    _, meta = await write(deps, "notes.md", "changed\n")
    after = await client.stat(f"/workspaces/{W1}/notes.md")
    assert after.id == before.id and after.generation != before.generation
    assert meta["exists"] is True and (meta["additions"], meta["deletions"]) == (1, 1)


@pytest.mark.parametrize(
    ("path", "refused"),
    [
        ("/docs/guide.md", "/docs/guide.md"),
        ("/docs/new.md", "/docs/new.md"),
        ("/docs/sub/new.md", "/docs/sub"),  # the first directory the create would make
        ("log.md", "/workspace/log.md"),
    ],
)
async def test_denied_writes_are_the_result(deps: Deps, client: VFSClient, path: str, refused: str) -> None:
    """``/docs`` is read-only for the agent, and ``/workspace/log.md`` append-only, whether the file exists or not."""
    assert await run_tool(Write, deps, content="x", filePath=path) == f"Permission denied: write {refused}"
    assert (await client.pread("/docs/guide.md")).content == b"guide\n"
    assert [e.name for e in await client.readdir("/docs")] == ["guide.md"]
    assert [e.name for e in await client.readdir(f"/workspaces/{W1}")] == ["notes.md"]


async def test_denied_write_over_the_log(deps: Deps, client: VFSClient) -> None:
    await client.create(f"/workspaces/{W1}/log.md", b"entry\n")
    assert await run_tool(Write, deps, content="x", filePath="log.md") == "Permission denied: write /workspace/log.md"
    assert await content(client, "log.md") == b"entry\n"


@pytest.mark.parametrize(
    "path", [f"../workspaces/{W2}/secret.md", f"/workspaces/{W2}/secret.md", "/workspaces/x.md", "/new.md"]
)
async def test_paths_outside_the_view_are_not_found(deps: Deps, client: VFSClient, path: str) -> None:
    message = await retry_message(Write, deps, content="x", filePath=path)
    assert message == f"File not found: /{path.removeprefix('../').removeprefix('/')}"
    assert (await client.pread(f"/workspaces/{W2}/secret.md")).content == b"theirs\n"


async def test_directories(deps: Deps) -> None:
    """As in ladderframe, the permission is checked before whether the path is a directory."""
    assert await retry_message(Write, deps, content="x", filePath="/workspace") == (
        "Path is a directory, not a file: /workspace"
    )
    assert await run_tool(Write, deps, content="x", filePath="/") == "Permission denied: write /"  # the virtual root
    assert await run_tool(Write, deps, content="x", filePath="/docs") == "Permission denied: write /docs"


async def test_a_file_as_a_parent_is_not_found(deps: Deps) -> None:
    message = await retry_message(Write, deps, content="x", filePath="notes.md/x.txt")
    assert message == "File not found: /workspace/notes.md/x.txt"


# Lost races


Step = Callable[[], Awaitable[object]]


class Racing(VFSView):
    """The agent's view, where another client runs the next of ``before_write`` just before each write, and of
    ``before_create`` just before each create, while any are left."""

    def __init__(
        self, view: VFSView, client: VFSClient, before_write: Sequence[Step] = (), before_create: Sequence[Step] = ()
    ) -> None:
        super().__init__(client, {"/workspace": f"/workspaces/{W1}", "/docs": "/docs"}, view.policy, view.actor)
        self.before_write, self.before_create = list(before_write), list(before_create)
        self.writes = self.creates = 0

    async def write(self, path: str, data: bytes, *, expect: FileVersion | None = None) -> FileVersion:
        self.writes += 1
        if self.before_write:
            await self.before_write.pop(0)()
        return await super().write(path, data, expect=expect)

    async def create(self, path: str, data: bytes | None = None, *, parents: bool = False) -> FileVersion:
        self.creates += 1
        if self.before_create:
            await self.before_create.pop(0)()
        return await super().create(path, data, parents=parents)


def overwrite(client: VFSClient, name: str, data: bytes) -> Step:
    return lambda: client.write(f"/workspaces/{W1}/{name}", data)


def create(client: VFSClient, name: str, data: bytes) -> Step:
    return lambda: client.create(f"/workspaces/{W1}/{name}", data)


async def test_a_lost_race_is_tried_again(view: VFSView, client: VFSClient) -> None:
    racing = Racing(view, client, [overwrite(client, "notes.md", b"theirs 1\n")])
    _, meta = await write(Deps(racing), "notes.md", "mine\n")
    assert racing.writes == 2
    assert await content(client, "notes.md") == b"mine\n"
    assert "-theirs 1\n+mine" in meta["diff"]  # the diff is against what was overwritten


async def test_a_writer_that_keeps_winning_is_a_retry(view: VFSView, client: VFSClient) -> None:
    steps = [overwrite(client, "notes.md", f"theirs {i}\n".encode()) for i in (1, 2, 3)]
    racing = Racing(view, client, steps)
    assert await retry_message(Write, Deps(racing), content="mine\n", filePath="notes.md") == STALE
    assert racing.writes == 2
    assert await content(client, "notes.md") == b"theirs 2\n"


async def test_a_lost_create_overwrites(view: VFSView, client: VFSClient) -> None:
    racing = Racing(view, client, before_create=[create(client, "new.md", b"theirs\n")])
    _, meta = await write(Deps(racing), "new.md", "mine\n")
    assert (racing.creates, meta["exists"]) == (1, True)
    assert "-theirs\n+mine" in meta["diff"]
    assert await content(client, "new.md") == b"mine\n"


async def test_a_lost_create_then_a_lost_overwrite_is_a_retry(view: VFSView, client: VFSClient) -> None:
    racing = Racing(view, client, [overwrite(client, "new.md", b"again\n")], [create(client, "new.md", b"theirs\n")])
    message = await retry_message(Write, Deps(racing), content="mine\n", filePath="new.md")
    assert message == "/workspace/new.md was modified since it was read. Read it again before writing."
    assert await content(client, "new.md") == b"again\n"


async def test_a_file_removed_before_its_overwrite_is_created(view: VFSView, client: VFSClient) -> None:
    racing = Racing(view, client, [lambda: client.unlink(f"/workspaces/{W1}/notes.md")])
    _, meta = await write(Deps(racing), "notes.md", "mine\n")
    assert (racing.writes, racing.creates, meta["exists"]) == (1, 1, False)
    assert await content(client, "notes.md") == b"mine\n"


async def test_a_file_removed_twice_is_a_retry(view: VFSView, client: VFSClient) -> None:
    """Even a loss that isn't a lost compare-and-swap says to read again, not that the file is missing."""
    unlink = lambda: client.unlink(f"/workspaces/{W1}/notes.md")  # noqa: E731
    racing = Racing(view, client, [unlink], [create(client, "notes.md", b"theirs\n")])
    assert await retry_message(Write, Deps(racing), content="mine\n", filePath="notes.md") == STALE


async def test_parents_only_for_a_missing_parent(client: VFSClient) -> None:
    """The view checks ``write`` on every directory a ``parents`` create could make, so a file goes into an
    existing directory the agent may only write into without ``parents``."""
    policy = Policy({"agent": [Rule("/workspace/box", "read"), Rule("/workspace/**", "read", "write")]})
    deps = Deps(VFSView(client, workspace_mounts(W1), policy, "agent"))
    await client.mkdir(f"/workspaces/{W1}/box")
    assert await run_tool(Write, deps, content="x", filePath="box/in.txt") == "Wrote file successfully."
    denied = await run_tool(Write, deps, content="x", filePath="box/sub/in.txt")  # creating box/sub needs parents
    assert denied == "Permission denied: write /workspace/box"
    assert await content(client, "box/in.txt") == b"x"


async def test_a_denied_overwrite_reads_nothing(view: VFSView, client: VFSClient) -> None:
    """The permission is checked before the old text is read: refusing a ``Write`` over the append-only log doesn't
    read all of it."""
    await client.create(f"/workspaces/{W1}/log.md", b"entry\n" * 1000)
    reads = 0

    class Counting(VFSView):
        async def pread(self, path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
            nonlocal reads
            reads += 1
            return await super().pread(path, offset, limit)

    counting = Deps(Counting(client, workspace_mounts(W1), view.policy, view.actor))
    denied = await run_tool(Write, counting, content="x", filePath="log.md")
    assert denied == "Permission denied: write /workspace/log.md"
    assert reads == 0


async def test_a_change_between_pages_is_tried_again(
    view: VFSView, client: VFSClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The old text is read in pages; a write between two pages would mix versions, so the read starts over."""
    monkeypatch.setattr(text, "PAGE", 4)
    await client.write(f"/workspaces/{W1}/notes.md", b"0123456789\n")
    reads = 0

    class Paging(VFSView):
        async def pread(self, path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
            nonlocal reads
            reads += 1
            if reads == 2:
                await client.write(f"/workspaces/{W1}/notes.md", b"abcdefghij\n")
            return await super().pread(path, offset, limit)

    paging = Paging(client, {"/workspace": f"/workspaces/{W1}"}, view.policy, view.actor)
    _, meta = await write(Deps(paging), "notes.md", "new\n")
    assert reads == 2 + 3  # two pages before the change, then all three of the new version
    assert "-abcdefghij\n+new" in meta["diff"]
    assert await content(client, "notes.md") == b"new\n"
