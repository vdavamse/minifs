"""``Edit``: ladderframe's ``Edit`` and replacer tests (``packages/harness/tests/test_tools.py``), the tool's through a
real agent and the replacers' unchanged, its output against ladderframe's on a corpus, and what the VFS adds: the
policy, the chroot, the size cap and lost races."""

import asyncio
import base64
import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import minifs.tools._text as text
import minifs.tools.edit as edit_module
from minifs.tools import TOOLS, Edit
from minifs.tools.deps import EditSettings, ToolSettings
from minifs.tools.edit import (
    DESCRIPTION,
    context_aware_replacer,
    indentation_flexible_replacer,
    replace,
    trimmed_boundary_replacer,
)
from minifs.vfs.client import VFSClient
from minifs.vfs.core import ReadResult
from minifs.vfs.policy import Policy, Rule
from minifs.vfs.view import VFSView, workspace_mounts
from tests.tools.conftest import W1, W2, Deps, call_tool, retry_message, run_tool
from tests.tools.test_write import Racing, create, overwrite

GOLDEN = json.loads((Path(__file__).parent / "data" / "edit_golden.json").read_text())
"""ladderframe's disk ``Edit`` (2e347db and b0e0d5f) over a corpus: for each case, the old file (base64, or null
for none), the call's arguments, and the result and metadata, or the retry message, it gave (``{path}`` for the
file's path), and the bytes left (null for no file). ``data/gen_edit_golden.py`` regenerates it."""

STALE = "/workspace/notes.md was modified since it was read. Read it again before writing."


async def content(client: VFSClient, path: str) -> bytes:
    return (await client.pread(f"/workspaces/{W1}/{path}", 0, 1 << 20)).content


async def put(client: VFSClient, path: str, data: bytes) -> None:
    await client.create(f"/workspaces/{W1}/{path}", data, parents=True)


async def edit(deps: Deps, file_path: str, old: str, new: str, replace_all: bool | None = None) -> dict[str, Any]:
    """The metadata of an ``Edit`` call, which must succeed."""
    call = await call_tool(Edit, deps, filePath=file_path, oldString=old, newString=new, replaceAll=replace_all)
    assert call.result == "Edit applied successfully."
    assert call.part.metadata is not None
    return call.part.metadata


async def edit_retry(deps: Deps, file_path: str, old: str, new: str, replace_all: bool | None = None) -> str:
    return await retry_message(Edit, deps, filePath=file_path, oldString=old, newString=new, replaceAll=replace_all)


# The tool


async def test_schema(deps: Deps) -> None:
    call = await call_tool(Edit, deps, filePath="notes.md", oldString="mine", newString="ours")
    (tool_def,) = call.tool_defs
    assert (tool_def.name, tool_def.description, tool_def.sequential) == ("Edit", DESCRIPTION, True)
    props = tool_def.parameters_json_schema["properties"]
    assert props["filePath"]["description"] == "The absolute path to the file to modify"
    assert props["oldString"]["description"] == "The text to replace"
    assert props["newString"]["description"] == "The text to replace it with (must be different from oldString)"
    assert props["replaceAll"]["description"] == "Replace all occurrences of oldString (default false)"
    assert tool_def.parameters_json_schema["required"] == ["filePath", "oldString", "newString"]
    assert Edit in TOOLS


# ladderframe's tests


async def test_replaces_a_unique_match(deps: Deps, client: VFSClient) -> None:
    await put(client, "f.py", b"def f():\n    return 1\n")
    meta = await edit(deps, "f.py", "return 1", "return 2")
    assert await content(client, "f.py") == b"def f():\n    return 2\n"
    assert meta["additions"] == 1 and meta["deletions"] == 1
    assert "-    return 1\n+    return 2" in meta["diff"]
    assert meta["filepath"] == "/workspace/f.py"
    await put(client, "g.py", b"    a = 1\n    b = 2\n")
    trimmed = await edit(deps, "g.py", "b = 2", "b = 3")
    assert " a = 1\n-b = 2\n+b = 3" in trimmed["diff"]


async def test_errors(deps: Deps, client: VFSClient) -> None:
    await put(client, "f.txt", b"x = 1\nx = 1\n")
    assert "identical" in await edit_retry(deps, "f.txt", "x", "x")
    assert "Found multiple matches" in await edit_retry(deps, "f.txt", "x = 1", "x = 2")
    assert "Could not find oldString" in await edit_retry(deps, "f.txt", "y = 1", "y = 2")
    assert "oldString cannot be empty" in await edit_retry(deps, "f.txt", "", "z")
    assert await edit_retry(deps, "missing.txt", "a", "b") == "File /workspace/missing.txt not found"
    await edit(deps, "f.txt", "x = 1", "x = 2", replace_all=True)
    assert await content(client, "f.txt") == b"x = 2\nx = 2\n"


async def test_with_empty_old_string_creates_the_file(deps: Deps, client: VFSClient) -> None:
    meta = await edit(deps, "new/file.txt", "", "hello\n")
    assert await content(client, "new/file.txt") == b"hello\n"
    assert (meta["additions"], meta["deletions"]) == (1, 0)


async def test_keeps_crlf_and_bom(deps: Deps, client: VFSClient) -> None:
    await put(client, "win.txt", "\ufeffone\r\ntwo\r\nthree\r\n".encode())
    await edit(deps, "win.txt", "one\ntwo", "uno\ndos")
    assert await content(client, "win.txt") == "\ufeffuno\r\ndos\r\nthree\r\n".encode()


async def test_refuses_non_utf8(deps: Deps, client: VFSClient) -> None:
    await put(client, "latin.txt", b"caf\xe9\n")
    assert (
        await edit_retry(deps, "latin.txt", "caf", "bar")
        == "Cannot edit /workspace/latin.txt: it is not a UTF-8 text file"
    )
    assert await content(client, "latin.txt") == b"caf\xe9\n"


async def test_a_file_as_a_parent_is_not_found(deps: Deps) -> None:
    """ladderframe's ``test_write_and_edit_report_os_errors``: on disk this is ``Cannot write``; the view reports it as
    a path that doesn't exist, as ``Write`` does."""
    assert await edit_retry(deps, "notes.md/new.txt", "", "y") == "File /workspace/notes.md/new.txt not found"
    assert await edit_retry(deps, "notes.md/new.txt", "a", "y") == "File /workspace/notes.md/new.txt not found"


async def test_concurrent_edits_of_one_file_keep_every_change(view: VFSView, client: VFSClient) -> None:
    """ladderframe locks the path, so all 20 edits apply. Here each is a compare-and-swap redone once, so an edit
    that loses twice is a retry; but no edit's change is lost, and none applies twice. This checks for lost updates
    only: whether the edits actually race depends on timing, so the redo itself is the ``Racing`` tests' job."""
    await put(client, "many.txt", "".join(f"line {i}\n" for i in range(20)).encode())

    async def one(i: int) -> bool:
        deps = Deps(VFSView(client, workspace_mounts(W1), view.policy, view.actor))
        call = await call_tool(Edit, deps, filePath="many.txt", oldString=f"line {i}\n", newString=f"done {i}\n")
        if call.retried:
            assert call.retry == "/workspace/many.txt was modified since it was read. Read it again before writing."
        return not call.retried

    applied = await asyncio.gather(*(one(i) for i in range(20)))
    assert any(applied)
    expected = "".join(f"done {i}\n" if ok else f"line {i}\n" for i, ok in enumerate(applied))
    assert (await content(client, "many.txt")).decode() == expected


@pytest.mark.parametrize("case", GOLDEN, ids=[c["name"] for c in GOLDEN])
async def test_same_output_as_ladderframe(deps: Deps, client: VFSClient, case: dict[str, Any]) -> None:
    name = f"{case['name']}.txt"
    if case["old"] is not None:
        await put(client, name, base64.b64decode(case["old"]))
    call = await call_tool(
        Edit,
        deps,
        filePath=name,
        oldString=case["oldString"],
        newString=case["newString"],
        replaceAll=case["replaceAll"],
    )
    path = f"/workspace/{name}"
    if "retry" in case:
        assert call.retry == case["retry"].replace("{path}", path)
    else:
        expected = {k: v.replace("{path}", path) if isinstance(v, str) else v for k, v in case["metadata"].items()}
        assert (call.result, call.part.metadata) == (case["result"], expected)
    if case["written"] is None:
        assert [e.name for e in await client.readdir(f"/workspaces/{W1}")] == ["notes.md"]
    else:
        assert await content(client, name) == base64.b64decode(case["written"])


# ladderframe's replacer tests, unchanged


def test_replace_fallback_matchers() -> None:
    code = "def f():\n    if x:\n        return 1\n    return 2\n"
    # wrong indentation: the line-trimmed matcher finds the whole lines, which newString replaces as given
    assert replace(code, "if x:\n    return 1", "    if y:\n        return 3") == (
        "def f():\n    if y:\n        return 3\n    return 2\n"
    )
    # collapsed whitespace
    assert replace("a  =   1\n", "a = 1", "a = 2") == "a = 2\n"
    # escaped newlines from a model that double-escaped
    assert replace('s = "x"\ny\n', 's = \\"x\\"', 's = "z"') == 's = "z"\ny\n'
    # a block whose middle line differs slightly is found by its first and last lines
    block = "start {\n    alpha_value = 1\n}\n"
    assert replace(block, "start {\n    alpha_valu = 1\n}", "start {}") == "start {}\n"
    # literal replacement: no `$&`-style substitution
    assert replace("a\n", "a", "$&") == "$&\n"


def test_replace_refuses_a_disproportionate_match() -> None:
    # the block anchors match one line further apart than oldString, and that extra line is huge
    content = "first\nm1\nm2\n" + "z" * 700 + "\nlast\n"
    with pytest.raises(ValueError, match="much larger than oldString"):
        replace(content, "first\nm1\nm2\nlast", "x")


def test_replace_other_fallback_matchers() -> None:
    # Called directly: in `replace` an earlier, looser matcher usually finds these first.
    nested = "class A:\n    def f(self):\n        pass\n"
    assert list(indentation_flexible_replacer(nested, "def f(self):\n    pass")) == ["    def f(self):\n        pass"]
    assert list(trimmed_boundary_replacer("x = 1\ny = 2\n", "\n\ny = 2\n\n")) == ["y = 2"]
    block = "begin\n  one\n  two\n  three\nend\n"
    assert list(context_aware_replacer(block, "begin\n  one\n  TWO\n  three\nend")) == [block.removesuffix("\n")]
    assert list(context_aware_replacer(block, "begin\n  ONE\n  TWO\n  three\nend")) == []
    # block anchor with several candidates: the most similar middle wins
    two = "if a:\n    alpha = 1\nfi\nif a:\n    zzzzz = 9\nfi\n"
    assert replace(two, "if a:\n    alpha = 2\nfi", "done") == "done\nif a:\n    zzzzz = 9\nfi\n"


def test_block_anchor_skips_scoring_very_long_lines() -> None:
    content = "start\n" + "x" * 20_000 + "\nend\n"
    begin = time.monotonic()
    with pytest.raises(ValueError, match="Could not find oldString"):
        replace(content, "start\n" + "y" * 20_000 + "\nend", "z")
    assert time.monotonic() - begin < 1


# The VFS


async def test_an_edit_is_a_new_generation(deps: Deps, client: VFSClient) -> None:
    before = await client.stat(f"/workspaces/{W1}/notes.md")
    await edit(deps, "notes.md", "mine", "ours")
    after = await client.stat(f"/workspaces/{W1}/notes.md")
    assert after.id == before.id and after.generation != before.generation
    assert await content(client, "notes.md") == b"ours\n"


@pytest.mark.parametrize(
    ("path", "old", "refused"),
    [
        ("/docs/guide.md", "guide", "/docs/guide.md"),
        ("/docs/new.md", "", "/docs/new.md"),
        ("/docs/sub/new.md", "", "/docs/sub/new.md"),
    ],
)
async def test_docs_are_read_only(deps: Deps, client: VFSClient, path: str, old: str, refused: str) -> None:
    assert (
        await run_tool(Edit, deps, filePath=path, oldString=old, newString="x") == f"Permission denied: write {refused}"
    )
    assert (await client.pread("/docs/guide.md")).content == b"guide\n"
    assert [e.name for e in await client.readdir("/docs")] == ["guide.md"]


async def test_a_missing_file_to_edit_is_not_found_even_where_denied(deps: Deps) -> None:
    """ladderframe checks the permission first; here, as for a path outside the view, a file that isn't there is
    not found: there is nothing to edit, so nothing would be written."""
    assert await edit_retry(deps, "/docs/missing.md", "a", "b") == "File /docs/missing.md not found"


async def test_the_log_is_append_only(deps: Deps, client: VFSClient) -> None:
    await put(client, "log.md", b"entry\n")
    denied = await run_tool(Edit, deps, filePath="log.md", oldString="entry", newString="forged")
    assert denied == "Permission denied: write /workspace/log.md"
    assert await content(client, "log.md") == b"entry\n"


@pytest.mark.parametrize("path", [f"../workspaces/{W2}/secret.md", f"/workspaces/{W2}/secret.md", "/new.md"])
async def test_paths_outside_the_view_are_not_found(deps: Deps, client: VFSClient, path: str) -> None:
    for old in ("theirs", ""):
        message = await edit_retry(deps, path, old, "x")
        assert message == f"File /{path.removeprefix('../').removeprefix('/')} not found"
    assert (await client.pread(f"/workspaces/{W2}/secret.md")).content == b"theirs\n"


async def test_directories(deps: Deps) -> None:
    """As in ladderframe, the permission is checked first, then an empty ``oldString``, then whether it's a file."""
    assert await edit_retry(deps, "/workspace", "a", "b") == "Path is a directory, not a file: /workspace"
    assert "oldString cannot be empty" in await edit_retry(deps, "/workspace", "", "b")
    assert (
        await run_tool(Edit, deps, filePath="/docs", oldString="a", newString="b") == "Permission denied: write /docs"
    )


async def test_size_cap(deps: Deps, client: VFSClient) -> None:
    await put(client, "big.txt", b"a" * 99 + b"\n")
    small = Deps(deps.vfs, vfs_settings=ToolSettings(edit=EditSettings(max_bytes=99)))
    assert await edit_retry(small, "big.txt", "a", "b", True) == (
        "Cannot edit /workspace/big.txt: it is 100 bytes, over the 99-byte limit for Edit. "
        "Use the Write tool to replace the whole file instead."
    )
    assert await content(client, "big.txt") == b"a" * 99 + b"\n"
    exact = Deps(deps.vfs, vfs_settings=ToolSettings(edit=EditSettings(max_bytes=100)))
    await edit(exact, "big.txt", "a", "b", True)
    assert ToolSettings().edit.max_bytes == 10 * 1024 * 1024
    with pytest.raises(ValueError):
        EditSettings(max_bytes=-1)
    with pytest.raises(TypeError):
        EditSettings(max_bytes=1.5)  # pyright: ignore[reportArgumentType]


async def test_a_file_grown_past_the_cap_is_refused_after_one_page(
    view: VFSView, client: VFSClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cap protects memory, so a file that grew past it after the stat isn't read in full first."""
    monkeypatch.setattr(text, "PAGE", 10)
    reads = 0

    class Growing(VFSView):
        async def pread(self, path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
            nonlocal reads
            reads += 1
            if reads == 1:
                await client.write(f"/workspaces/{W1}/notes.md", b"mine\n" * 100)
            return await super().pread(path, offset, limit)

    growing = Growing(client, workspace_mounts(W1), view.policy, view.actor)
    small = Deps(growing, vfs_settings=ToolSettings(edit=EditSettings(max_bytes=100)))
    message = await edit_retry(small, "notes.md", "mine", "ours", True)
    assert message.startswith("Cannot edit /workspace/notes.md: it is 500 bytes, over the 100-byte limit")
    assert reads == 1


async def test_replace_runs_off_the_event_loop(deps: Deps, monkeypatch: pytest.MonkeyPatch) -> None:
    """The replacers are CPU-bound (seconds for a miss in a few MB), so they run in a worker thread."""
    threads: list[threading.Thread] = []

    def recording(*args: Any) -> str:
        threads.append(threading.current_thread())
        return replace(*args)

    monkeypatch.setattr(edit_module, "replace", recording)
    await edit(deps, "notes.md", "mine", "ours")
    assert threads and threads[0] is not threading.main_thread()


async def test_write_only_rules(client: VFSClient) -> None:
    """Like ``Write``: a rule granting ``write`` without ``read`` lets an empty ``oldString`` create a file, but an edit
    needs the old text."""
    policy = Policy({"agent": [Rule("/workspace/out/**", "write"), Rule("/workspace/**", "read", "write")]})
    deps = Deps(VFSView(client, workspace_mounts(W1), policy, "agent"))
    await edit(deps, "out/new.txt", "", "x\n")
    assert await content(client, "out/new.txt") == b"x\n"
    for old in ("x", ""):
        denied = await run_tool(Edit, deps, filePath="out/new.txt", oldString=old, newString="y")
        assert denied == "Permission denied: read /workspace/out/new.txt"
    assert await content(client, "out/new.txt") == b"x\n"


# Lost races


async def test_a_concurrent_writer_is_redone_once(view: VFSView, client: VFSClient) -> None:
    await client.write(f"/workspaces/{W1}/notes.md", b"a\nmine\nb\n")
    racing = Racing(view, client, [overwrite(client, "notes.md", b"theirs\nmine\nb\n")])
    meta = await edit(Deps(racing), "notes.md", "mine", "ours")
    assert racing.writes == 2
    assert await content(client, "notes.md") == b"theirs\nours\nb\n"  # their change kept
    assert "-mine\n+ours" in meta["diff"] and "-a\n" not in meta["diff"]


async def test_old_string_gone_after_the_race(view: VFSView, client: VFSClient) -> None:
    racing = Racing(view, client, [overwrite(client, "notes.md", b"theirs\n")])
    assert "Could not find oldString" in await edit_retry(Deps(racing), "notes.md", "mine", "ours")
    assert racing.writes == 1
    assert await content(client, "notes.md") == b"theirs\n"


async def test_a_writer_that_keeps_winning_is_a_retry(view: VFSView, client: VFSClient) -> None:
    steps = [overwrite(client, "notes.md", f"mine {i}\n".encode()) for i in (1, 2, 3)]
    racing = Racing(view, client, steps)
    assert await edit_retry(Deps(racing), "notes.md", "mine", "ours") == STALE
    assert racing.writes == 2
    assert await content(client, "notes.md") == b"mine 2\n"


async def test_a_file_removed_before_the_write_is_not_found(view: VFSView, client: VFSClient) -> None:
    racing = Racing(view, client, [lambda: client.unlink(f"/workspaces/{W1}/notes.md")])
    assert await edit_retry(Deps(racing), "notes.md", "mine", "ours") == "File /workspace/notes.md not found"


async def test_a_lost_create_is_an_existing_file(view: VFSView, client: VFSClient) -> None:
    racing = Racing(view, client, before_create=[create(client, "new.md", b"theirs\n")])
    assert "oldString cannot be empty" in await edit_retry(Deps(racing), "new.md", "", "mine\n")
    assert await content(client, "new.md") == b"theirs\n"
