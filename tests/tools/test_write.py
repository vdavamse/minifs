"""``Write``: ladderframe's ``Write`` tests (``packages/harness/tests/test_tools.py``) through a real agent, its output
against ladderframe's on a corpus, and what the VFS adds: the policy, the chroot and lost races."""

import asyncio
import base64
import json
import threading
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

import pytest

import minifs.tools._text as text
import minifs.tools.write as write_module
from minifs.tools import TOOLS, Write
from minifs.tools.deps import ToolSettings, WriteSettings
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
    await client.create(f"/workspaces/{W1}/bom.txt", "\ufeffold".encode())
    await write(deps, "bom.txt", "new")
    assert await content(client, "bom.txt") == "\ufeffnew".encode()


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
        ("/docs/sub/new.md", "/docs/sub/new.md"),
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


@pytest.mark.parametrize("path", ["a\nb.md", "/workspace/x\x1b[31m.md", "\u202eevil.md"])
async def test_control_characters_in_a_path_are_refused(deps: Deps, client: VFSClient, path: str) -> None:
    """A name with a newline, an escape sequence or a bidi override can't be created (#92): the path is refused
    before any round trip, with a message the model can act on."""
    message = await retry_message(Write, deps, content="x", filePath=path)
    assert message.endswith(": path contains a control character")
    assert [e.name for e in await client.readdir(f"/workspaces/{W1}")] == ["notes.md"]


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
        super().__init__(client, workspace_mounts(W1), view.policy, view.actor)
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


async def test_a_parent_removed_before_the_create_is_made_again(view: VFSView, client: VFSClient) -> None:
    await client.mkdir(f"/workspaces/{W1}/d")
    racing = Racing(view, client, before_create=[lambda: client.unlink(f"/workspaces/{W1}/d")])
    _, meta = await write(Deps(racing), "d/x.txt", "mine\n")
    assert (racing.creates, meta["exists"]) == (2, False)  # the second with parents
    assert await content(client, "d/x.txt") == b"mine\n"


async def test_a_file_replaced_by_a_directory_is_a_directory(view: VFSView, client: VFSClient) -> None:
    async def replace() -> None:
        await client.unlink(f"/workspaces/{W1}/notes.md")
        await client.mkdir(f"/workspaces/{W1}/notes.md")

    racing = Racing(view, client, [replace])
    message = await retry_message(Write, Deps(racing), content="mine\n", filePath="notes.md")
    assert message == "Path is a directory, not a file: /workspace/notes.md"


async def test_write_only_rules(client: VFSClient) -> None:
    """A rule granting ``write`` without ``read`` lets ``Write`` create files, but not overwrite them (the diff needs
    the old text); a rule granting nothing refuses the write, not the read, as ladderframe checks the write."""
    policy = Policy(
        {
            "agent": [
                Rule("/workspace/out/**", "write"),
                Rule("/workspace/none/**"),
                Rule("/workspace/**", "read", "write"),
            ]
        }
    )
    deps = Deps(VFSView(client, workspace_mounts(W1), policy, "agent"))
    await client.mkdir(f"/workspaces/{W1}/out")
    assert await run_tool(Write, deps, content="x", filePath="out/new.txt") == "Wrote file successfully."
    assert await content(client, "out/new.txt") == b"x"
    assert (
        await run_tool(Write, deps, content="y", filePath="out/new.txt")
        == "Permission denied: read /workspace/out/new.txt"
    )
    assert await content(client, "out/new.txt") == b"x"
    assert (
        await run_tool(Write, deps, content="x", filePath="none/a.txt")
        == "Permission denied: write /workspace/none/a.txt"
    )
    assert policy.denials == {("agent", "read"): 1, ("agent", "write"): 1}  # only the refusals


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


# The diff cap


class Preads(VFSView):
    """The agent's view, recording the ``(offset, limit)`` of each ``pread``."""

    def __init__(self, view: VFSView, client: VFSClient) -> None:
        super().__init__(client, workspace_mounts(W1), view.policy, view.actor)
        self.preads: list[tuple[int, int]] = []

    async def pread(self, path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
        self.preads.append((offset, limit))
        return await super().pread(path, offset, limit)


def capped(vfs: VFSView, max_diff_bytes: int = 100) -> Deps:
    return Deps(vfs, vfs_settings=ToolSettings(write=WriteSettings(max_diff_bytes=max_diff_bytes)))


async def test_over_the_cap_the_old_text_is_not_read(view: VFSView, client: VFSClient) -> None:
    """Over ``max_diff_bytes`` only the first bytes are read, for the BOM, and the metadata has no diff."""
    await client.create(f"/workspaces/{W1}/big.txt", b"old line\n" * 50)
    preads = Preads(view, client)
    result, meta = await write(capped(preads), "big.txt", "new\n")
    assert result == "Wrote file successfully."
    assert meta == {
        "filepath": "/workspace/big.txt",
        "exists": True,
        "diff": "",
        "additions": None,
        "deletions": None,
    }
    assert preads.preads == [(0, 3)]
    assert await content(client, "big.txt") == b"new\n"


async def test_at_the_cap_the_old_text_is_diffed(view: VFSView, client: VFSClient) -> None:
    await client.create(f"/workspaces/{W1}/big.txt", b"old line\n" * 50)
    _, meta = await write(capped(view, 450), "big.txt", "new\n")
    assert (meta["additions"], meta["deletions"]) == (1, 50)
    assert "-old line\n" in meta["diff"]


async def test_over_the_cap_the_bom_is_kept(view: VFSView, client: VFSClient) -> None:
    await client.create(f"/workspaces/{W1}/big.txt", text.BOM.encode() + b"x" * 200)
    await write(capped(view), "big.txt", "new\n")
    assert await content(client, "big.txt") == text.BOM.encode() + b"new\n"


@pytest.mark.parametrize(
    ("old", "kept"),
    [(b"x", b""), (b"\xef\xbb", b""), (text.BOM.encode(), text.BOM.encode())],
    ids=["one byte", "half a BOM", "only a BOM"],
)
async def test_over_the_cap_short_files(view: VFSView, client: VFSClient, old: bytes, kept: bytes) -> None:
    """Shorter than a BOM: the BOM read returns what there is, which is a BOM only if it is all of one."""
    await client.create(f"/workspaces/{W1}/short.txt", old)
    _, meta = await write(capped(view, 0), "short.txt", "new\n")
    assert meta["diff"] == ""
    assert await content(client, "short.txt") == kept + b"new\n"


async def test_over_the_cap_a_concurrent_writer_is_detected(view: VFSView, client: VFSClient) -> None:
    await client.create(f"/workspaces/{W1}/big.txt", b"x" * 200)
    racing = Racing(view, client, [overwrite(client, "big.txt", b"y" * 200)])
    _, meta = await write(capped(racing), "big.txt", "mine\n")
    assert racing.writes == 2
    assert (meta["diff"], await content(client, "big.txt")) == ("", b"mine\n")
    steps = [overwrite(client, "big.txt", f"theirs {i}".encode() * 50) for i in (1, 2, 3)]
    racing = Racing(view, client, steps)
    message = await retry_message(Write, capped(racing), content="mine\n", filePath="big.txt")
    assert message == "/workspace/big.txt was modified since it was read. Read it again before writing."
    assert await content(client, "big.txt") == b"theirs 2" * 50


async def test_a_file_grown_past_the_cap_after_its_stat_is_not_diffed(view: VFSView, client: VFSClient) -> None:
    """The size is checked again on the first page. ``TooLarge`` doesn't carry that page's version, so the
    compare-and-swap uses that of the BOM read."""
    big = f"/workspaces/{W1}/big.txt"
    await client.create(big, b"small\n")

    class Growing(Preads):
        async def stat(self, path: str) -> Any:
            entry = await super().stat(path)
            await client.write(big, b"grown\n" * 50)
            return entry

    growing = Growing(view, client)
    _, meta = await write(capped(growing), "big.txt", "new\n")
    assert meta["diff"] == ""
    assert growing.preads == [(0, text.PAGE), (0, 3)]
    assert await content(client, "big.txt") == b"new\n"


async def test_over_the_cap_write_only_rules_still_need_read(client: VFSClient) -> None:
    """The old text isn't read, but its BOM is, and what the policy allows doesn't depend on a file's size."""
    policy = Policy({"agent": [Rule("/workspace/out/**", "write"), Rule("/workspace/**", "read", "write")]})
    deps = capped(VFSView(client, workspace_mounts(W1), policy, "agent"))
    await client.create(f"/workspaces/{W1}/out/big.txt", b"x" * 200, parents=True)
    denied = await run_tool(Write, deps, content="y", filePath="out/big.txt")
    assert denied == "Permission denied: read /workspace/out/big.txt"
    assert await content(client, "out/big.txt") == b"x" * 200


def test_write_settings_are_checked() -> None:
    assert WriteSettings().max_diff_bytes == 1024 * 1024
    with pytest.raises(ValueError, match="at least 0"):
        WriteSettings(max_diff_bytes=-1)
    for bad in (1.5, True):
        with pytest.raises(TypeError, match="must be an int"):
            WriteSettings(max_diff_bytes=bad)  # pyright: ignore[reportArgumentType]


async def test_the_diff_does_not_stall_the_event_loop(
    deps: Deps, client: VFSClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """difflib takes seconds on a file of a few MB, so the diff runs in a worker thread: a coroutine runs while it
    does. Here the diff blocks until that coroutine releases it, which it can't if the diff holds the loop."""
    await client.create(f"/workspaces/{W1}/big.txt", b"".join(b"line %d\n" % i for i in range(50_000)))
    started, released = threading.Event(), threading.Event()
    diff_metadata = write_module.diff_metadata

    def blocking(*args: Any) -> dict[str, str | int]:
        started.set()
        assert released.wait(5), "the diff held the event loop"
        return diff_metadata(*args)

    async def concurrent() -> None:
        while not started.is_set():
            await asyncio.sleep(0.001)
        released.set()

    monkeypatch.setattr(write_module, "diff_metadata", blocking)
    new = "".join(f"line {i}\n" if i != 25_000 else "changed\n" for i in range(50_000))
    (_, meta), _ = await asyncio.gather(write(deps, "big.txt", new), concurrent())
    assert (meta["additions"], meta["deletions"]) == (1, 1)
