"""``Read``: ladderframe's Read tests over the view, VFS cases (policy, chroot, virtual root), and its output checked
against a copy of ladderframe's line reader."""

import io
import random
from typing import Any

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import (
    BinaryContent,
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel

from minifs.tools import TOOLS, Read
from minifs.tools.deps import ReadSettings, ToolSettings
from minifs.tools.read import DESCRIPTION, MAX_ATTACHMENT_BYTES
from minifs.vfs.client import VFSClient
from minifs.vfs.core import Entry, LineReadResult
from minifs.vfs.policy import Policy, Rule
from minifs.vfs.view import VFSView, workspace_mounts
from tests.tools.conftest import W1, W2, Deps, call_tool, retry_message, run_tool

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16
WS = f"/workspaces/{W1}"


async def put(client: VFSClient, path: str, data: bytes) -> None:
    """Creates ``path`` (a path in the agent's view, ``/workspace/...``) with ``data``."""
    await client.create(path.replace("/workspace", WS, 1), data, parents=True)


async def read_twice(deps: Deps, *calls: dict[str, Any]) -> list[Any]:
    """The results of ``Read`` calls made one after another in one run, so later calls see earlier results."""
    results: list[Any] = []

    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        assert isinstance(last, ModelRequest)
        results.extend(p.content for p in last.parts if isinstance(p, ToolReturnPart))
        if len(results) == len(calls):
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart("Read", calls[len(results)])])

    await Agent(FunctionModel(model), deps_type=Deps, tools=[Read]).run("go", deps=deps)
    return results


# The tool


async def test_schema_is_ladderframes(deps: Deps) -> None:
    call = await call_tool(Read, deps, filePath="notes.md")
    (tool_def,) = call.tool_defs
    assert tool_def.name == "Read" and tool_def.description == DESCRIPTION
    assert DESCRIPTION.startswith("Read a file or directory from the filesystem.")
    schema = tool_def.parameters_json_schema
    assert list(schema["properties"]) == ["filePath", "offset", "limit"]
    assert schema["required"] == ["filePath"]
    assert schema["properties"]["limit"]["description"] == "The maximum number of lines to read (defaults to 2000)"
    assert Read in TOOLS


# ladderframe's tests


async def test_read_file(client: VFSClient, deps: Deps) -> None:
    await put(client, "/workspace/data/sample.txt", b"line one\nline two\nneedle here\n")
    assert await run_tool(Read, deps, filePath="data/sample.txt") == (
        "<path>/workspace/data/sample.txt</path>\n<type>file</type>\n<content>\n"
        "1: line one\n2: line two\n3: needle here\n\n(End of file - total 3 lines)\n</content>"
    )
    partial = await run_tool(Read, deps, filePath="/workspace/data/sample.txt", offset=2, limit=1)
    assert "2: line two\n\n(Showing lines 2-2 of 3. Use offset=3 to continue.)" in partial


async def test_read_errors(client: VFSClient, deps: Deps) -> None:
    await put(client, "/workspace/data/sample.txt", b"line one\nline two\nneedle here\n")
    assert await retry_message(Read, deps, filePath="data/sample") == (
        "File not found: /workspace/data/sample\n\nDid you mean one of these?\n/workspace/data/sample.txt"
    )
    message = await retry_message(Read, deps, filePath="data/sample.txt", offset=9)
    assert message == "Offset 9 is out of range for this file (3 lines)"
    for args in ({"offset": -1}, {"limit": -1}):
        message = await retry_message(Read, deps, filePath="data/sample.txt", **args)
        assert message == "offset and limit must be non-negative integers"


async def test_read_directory_binary_image_and_empty(client: VFSClient, deps: Deps) -> None:
    await client.mkdir(f"{WS}/d/sub", parents=True)
    await put(client, "/workspace/d/b.bin", b"\x00\x01")
    await put(client, "/workspace/d/a.png", PNG)
    await put(client, "/workspace/d/empty.txt", b"")
    assert await run_tool(Read, deps, filePath="/workspace/d") == (
        "<path>/workspace/d</path>\n<type>directory</type>\n<entries>\n"
        "a.png\nb.bin\nempty.txt\nsub/\n\n(4 entries)\n</entries>"
    )
    paged = await run_tool(Read, deps, filePath="d", limit=1)
    assert "(Showing 1 of 4 entries. Use 'offset' parameter to read beyond entry 2)" in paged
    assert await retry_message(Read, deps, filePath="d/b.bin") == "Cannot read binary file: /workspace/d/b.bin"
    image = await run_tool(Read, deps, filePath="d/a.png")
    assert image == "Image read successfully"  # the ToolReturn's return_value; the attachment follows
    assert await run_tool(Read, deps, filePath="d/empty.txt") == (
        "<path>/workspace/d/empty.txt</path>\n<type>file</type>\n<content>\n"
        "\n\n(End of file - total 0 lines)\n</content>"
    )


async def test_read_attaches_images_and_pdfs(client: VFSClient, deps: Deps) -> None:
    pdf = b"%PDF-1.7\n" + bytes(range(256)) * 40  # past the 4 KB sample
    await put(client, "/workspace/doc.pdf", pdf)
    await put(client, "/workspace/a.png", PNG)
    attached: list[BinaryContent] = []

    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        assert isinstance(last, ModelRequest)
        for part in last.parts:
            if isinstance(part, UserPromptPart) and not isinstance(part.content, str):
                attached.extend(c for c in part.content if isinstance(c, BinaryContent))
        if any(isinstance(p, ToolReturnPart) for p in last.parts):
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart("Read", {"filePath": name}) for name in ("doc.pdf", "a.png")])

    await Agent(FunctionModel(model), deps_type=Deps, tools=[Read]).run("go", deps=deps)
    assert sorted((c.media_type, c.data) for c in attached) == [("application/pdf", pdf), ("image/png", PNG)]


async def test_read_caps_long_lines_and_bytes(client: VFSClient, deps: Deps) -> None:
    await put(client, "/workspace/long.txt", ("x" * 2500 + "\n" + "\n".join("y" * 1000 for _ in range(100))).encode())
    out = await run_tool(Read, deps, filePath="long.txt")
    assert "1: " + "x" * 2000 + "... (line truncated to 2000 chars)\n2: " in out
    assert "(Output capped at 50 KB. Showing lines 1-" in out


async def test_read_attaches_nested_agents_md_once(client: VFSClient, deps: Deps) -> None:
    await put(client, "/workspace/pkg/AGENTS.md", b"Use tabs.")
    await put(client, "/workspace/pkg/f.py", b"x = 1\n")
    await put(client, "/workspace/AGENTS.md", b"root instructions are not attached")
    first, again = await read_twice(deps, {"filePath": "pkg/f.py"}, {"filePath": "pkg/f.py"})
    assert first.endswith(
        "</content>\n\n<system-reminder>\nInstructions from: /workspace/pkg/AGENTS.md\nUse tabs.\n</system-reminder>"
    )
    assert "root instructions" not in first
    assert again.endswith("</content>")  # already shown in this conversation


async def test_read_line_numbers_agree_with_grep(client: VFSClient, view: VFSView, deps: Deps) -> None:
    await put(client, "/workspace/log.txt", b"progress 10%\rprogress 100%\nNEEDLE\n")
    (hit,) = (await view.grep("NEEDLE", "/workspace/log.txt")).hits
    assert hit.line == 2
    assert "\n2: NEEDLE\n" in await run_tool(Read, deps, filePath="log.txt")


async def test_read_refuses_oversized_attachments(client: VFSClient, deps: Deps) -> None:
    await put(client, "/workspace/big.png", PNG + b"\x00" * MAX_ATTACHMENT_BYTES)
    message = await retry_message(Read, deps, filePath="big.png")
    size = len(PNG) + MAX_ATTACHMENT_BYTES
    assert message == f"/workspace/big.png is too large to attach ({size} bytes, the limit is {MAX_ATTACHMENT_BYTES})"


# Instruction files


async def test_instructions_walk_to_the_workdir(client: VFSClient, deps: Deps) -> None:
    """The nearest directory first; per directory AGENTS.md wins over CLAUDE.md, with no fallback when it's the file
    being read or blank; nothing outside the working directory."""
    await put(client, "/workspace/a/CLAUDE.md", b"from a\n")
    await put(client, "/workspace/a/b/AGENTS.md", b"  \n")
    await put(client, "/workspace/a/b/CLAUDE.md", b"not used: AGENTS.md is first")
    await put(client, "/workspace/a/b/c/CLAUDE.md", b"from c")
    await put(client, "/workspace/a/b/c/f.txt", b"x\n")
    out = await run_tool(Read, deps, filePath="a/b/c/f.txt")
    assert out.endswith(
        "<system-reminder>\nInstructions from: /workspace/a/b/c/CLAUDE.md\nfrom c\n\n"
        "Instructions from: /workspace/a/CLAUDE.md\nfrom a\n</system-reminder>"
    )
    itself = await run_tool(Read, deps, filePath="a/b/c/CLAUDE.md")
    assert "Instructions from: /workspace/a/b/c/CLAUDE.md" not in itself
    assert "Instructions from: /workspace/a/CLAUDE.md" in itself
    assert "<system-reminder>" not in await run_tool(Read, deps, filePath="/docs/guide.md")


async def test_unreadable_instructions_are_skipped(client: VFSClient, view: VFSView) -> None:
    policy = Policy({"agent": [Rule("/workspace/pkg/AGENTS.md"), Rule("/workspace/**", "read", "write")]})
    deps = Deps(VFSView(client, workspace_mounts(W1), policy, "agent"))
    await put(client, "/workspace/pkg/AGENTS.md", b"secret")
    await put(client, "/workspace/pkg/CLAUDE.md", b"visible")
    await put(client, "/workspace/pkg/f.py", b"x\n")
    out = await run_tool(Read, deps, filePath="pkg/f.py")
    assert "secret" not in out
    assert "Instructions from: /workspace/pkg/CLAUDE.md\nvisible" in out


# The view


async def test_denied_read_is_the_result(client: VFSClient) -> None:
    rules = [Rule("/workspace/private/**"), Rule("/workspace/hidden.txt"), Rule("/workspace/**", "read", "write")]
    deps = Deps(VFSView(client, workspace_mounts(W1), Policy({"agent": rules}), "agent"))
    await put(client, "/workspace/private/key.txt", b"k\n")
    await put(client, "/workspace/hidden.txt", b"h\n")
    result = await run_tool(Read, deps, filePath="private/key.txt")
    assert result == "Permission denied: read /workspace/private/key.txt"
    assert await run_tool(Read, deps, filePath="private/nope") == "Permission denied: read /workspace/private/nope"
    assert await run_tool(Read, deps, filePath="private") == "Permission denied: read /workspace/private"
    # Unreadable entries are neither listed nor suggested.
    assert await retry_message(Read, deps, filePath="hidden") == "File not found: /workspace/hidden"
    listing = await run_tool(Read, deps, filePath="/workspace")
    assert listing.endswith("<entries>\nnotes.md\n\n(1 entries)\n</entries>")


@pytest.mark.parametrize("spelling", [f"../workspaces/{W2}/secret.md", f"/workspaces/{W2}/secret.md"])
async def test_other_workspaces_look_missing(deps: Deps, spelling: str) -> None:
    message = await retry_message(Read, deps, filePath=spelling)
    assert message == f"File not found: /workspaces/{W2}/secret.md"


async def test_root_lists_the_mounts(deps: Deps) -> None:
    assert await run_tool(Read, deps, filePath="/") == (
        "<path>/</path>\n<type>directory</type>\n<entries>\ndocs/\nworkspace/\n\n(2 entries)\n</entries>"
    )
    assert await run_tool(Read, deps, filePath="..") == await run_tool(Read, deps, filePath="/")
    assert await retry_message(Read, deps, filePath="/work") == (
        "File not found: /work\n\nDid you mean one of these?\n/workspace"
    )


async def test_suggestions_come_from_the_view(deps: Deps) -> None:
    assert await retry_message(Read, deps, filePath="/docs/guid") == (
        "File not found: /docs/guid\n\nDid you mean one of these?\n/docs/guide.md"
    )
    assert await retry_message(Read, deps, filePath="notes.md/x") == "File not found: /workspace/notes.md/x"


# Byte-identical output: ladderframe's _read_lines over the same bytes


def ladderframe_read_lines(
    data: bytes, start: int, limit: int, settings: ReadSettings
) -> tuple[list[str], int, bool, bool]:
    """ladderframe's ``_read_lines``, verbatim but for reading ``data`` instead of a path."""
    lines: list[str] = []
    size = 0
    count = 0
    more = False
    with io.BytesIO(data) as handle:
        for raw_bytes in handle:
            raw = raw_bytes.decode("utf-8", errors="replace")
            count += 1
            if count < start:
                continue
            if len(lines) >= limit:
                more = True
                continue
            line = raw.removesuffix("\n").removesuffix("\r")
            if len(line) > settings.max_line_length:
                line = line[: settings.max_line_length] + f"... (line truncated to {settings.max_line_length} chars)"
            line_size = len(line.encode("utf-8")) + (1 if lines else 0)
            if size + line_size > settings.max_bytes:
                return lines, count, True, True
            lines.append(line)
            size += line_size
    return lines, count, more, False


def ladderframe_output(path: str, data: bytes, offset: int | None, limit: int | None, settings: ReadSettings) -> str:
    """ladderframe's ``Read`` output for a text file holding ``data``, or its retry message."""
    start = offset or 1
    count = limit if limit is not None else settings.default_limit
    lines, total, more, capped = ladderframe_read_lines(data, start, count, settings)
    if total < start and not (total == 0 and start == 1):
        return f"Offset {start} is out of range for this file ({total} lines)"
    output = f"<path>{path}</path>\n<type>file</type>\n<content>\n"
    output += "\n".join(f"{start + i}: {line}" for i, line in enumerate(lines))
    last = start + len(lines) - 1
    if capped:
        output += (
            f"\n\n(Output capped at {settings.max_bytes // 1024} KB. "
            f"Showing lines {start}-{last}. Use offset={last + 1} to continue.)"
        )
    elif more:
        output += f"\n\n(Showing lines {start}-{last} of {total}. Use offset={last + 1} to continue.)"
    else:
        output += f"\n\n(End of file - total {total} lines)"
    return output + "\n</content>"


def random_text(rng: random.Random) -> bytes:
    """Lines of ASCII, multibyte characters, invalid UTF-8 and stray ``\\r``, long enough for small settings to cut."""
    pieces = [b"a", b"bc", b"\r", "\u00e9".encode(), "\u20ac".encode(), "\U0001f600".encode(), b"\xff", b"\xe2\x82"]
    lines: list[bytes] = []
    for _ in range(rng.choice([0, 1, 2, 5, 40])):
        lines.append(b"".join(rng.choice(pieces) for _ in range(rng.choice([0, 1, 3, 9, 20, 300]))))
    data = b"\n".join(lines)
    return data + b"\n" if rng.random() < 0.5 and data else data


SMALL = [
    ReadSettings(),
    ReadSettings(default_limit=7, max_line_length=10, max_bytes=1024 + 40),
    ReadSettings(default_limit=50, max_line_length=3, max_bytes=1024),
    ReadSettings(max_line_length=100, max_bytes=2048),
]


@pytest.mark.parametrize("seed", range(12))
async def test_output_is_ladderframes(client: VFSClient, view: VFSView, seed: int) -> None:
    rng = random.Random(seed)
    for i in range(10):
        data = random_text(rng)
        path = f"/workspace/r{seed}-{i}.txt"
        await put(client, path, data)
        settings = rng.choice(SMALL)
        deps = Deps(view, vfs_settings=ToolSettings(read=settings))
        offset = rng.choice([None, 0, 1, 2, 5, 30, 45])
        limit = rng.choice([None, 0, 1, 3, 100])
        args: dict[str, Any] = {"filePath": path, "offset": offset, "limit": limit}
        call = await call_tool(Read, deps, **args)
        if 0 in data[:4096] or (data and sum(1 for b in data[:4096] if b < 9 or 13 < b < 32) / len(data[:4096]) > 0.3):
            continue  # binary by ladderframe's rules too
        got = call.retry if call.retried else call.result
        assert got == ladderframe_output(path, data, offset, limit, settings), (data, args, settings)


async def test_a_line_the_byte_cap_cut_is_read_whole(client: VFSClient, view: VFSView) -> None:
    """The raw cap falls in line 2, which fits the output once line 1 is truncated: the tool refills from it."""
    settings = ReadSettings(max_line_length=100, max_bytes=1024)
    data = b"x" * 1000 + b"\n" + b"y" * 50 + b"\n" + b"z\n"
    await put(client, "/workspace/cut.txt", data)
    deps = Deps(view, vfs_settings=ToolSettings(read=settings))
    out = await run_tool(Read, deps, filePath="cut.txt")
    assert "\n2: " + "y" * 50 + "\n3: z\n" in out
    assert out == ladderframe_output("/workspace/cut.txt", data, None, None, settings)


@pytest.mark.parametrize("size", [51199, 51200, 60000])
async def test_long_lines_with_the_default_settings(client: VFSClient, deps: Deps, size: int) -> None:
    """Lines longer than the default byte cap, cut mid-character, with CRLF endings."""
    line = ("é" * size).encode()[:size]
    data = b"short\r\n" + line + b"\r\n" + line + b"\n" + b"x" * 1999 + b"\r\n" + b"end\r"
    await put(client, f"/workspace/long{size}.txt", data)
    for offset in (None, 2, 3, 5):
        out = await run_tool(Read, deps, filePath=f"long{size}.txt", offset=offset)
        assert out == ladderframe_output(f"/workspace/long{size}.txt", data, offset, None, ReadSettings())


async def test_huge_limit_and_offset(client: VFSClient, deps: Deps) -> None:
    """Models send ``limit=999999999999`` for everything; ``pread_lines`` takes less, so the tool clamps."""
    await put(client, "/workspace/a.txt", b"a\nb\n")
    out = await run_tool(Read, deps, filePath="a.txt", limit=2**40)
    assert out.endswith("1: a\n2: b\n\n(End of file - total 2 lines)\n</content>")
    message = await retry_message(Read, deps, filePath="a.txt", offset=2**64)
    assert message == f"Offset {2**64} is out of range for this file (2 lines)"


async def test_a_page_replaced_between_batches_is_read_again(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Another file moved to the read file's path after the page's first batch: the page is the new file's."""
    settings = ReadSettings(max_line_length=100, max_bytes=1024)  # one long line per batch
    old, new = (b"".join(c * 1000 + b"\n" for _ in range(10)) for c in (b"a", b"b"))
    await put(client, "/workspace/f.txt", old)
    await put(client, "/workspace/tmp.txt", new)
    calls = 0
    pread_lines = view.pread_lines

    async def replacing(*args: Any) -> LineReadResult:
        nonlocal calls
        calls += 1
        result = await pread_lines(*args)
        if calls == 1:
            await view.unlink("/workspace/f.txt")
            await view.rename("/workspace/tmp.txt", "/workspace/f.txt")
            assert (await pread_lines("/workspace/f.txt")).generation == result.generation  # only the id differs
        return result

    monkeypatch.setattr(view, "pread_lines", replacing)
    out = await run_tool(Read, Deps(view, vfs_settings=ToolSettings(read=settings)), filePath="f.txt")
    assert out == ladderframe_output("/workspace/f.txt", new, None, None, settings)


async def test_instruction_file_gone_before_its_read(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    await put(client, "/workspace/pkg/AGENTS.md", b"Use tabs.")
    await put(client, "/workspace/pkg/f.py", b"x\n")
    stat = view.stat

    async def deleting(path: str) -> Entry:
        entry = await stat(path)
        if path.endswith("AGENTS.md"):
            await view.unlink(path)
        return entry

    monkeypatch.setattr(view, "stat", deleting)
    out = await run_tool(Read, Deps(view), filePath="pkg/f.py")
    assert out.endswith("(End of file - total 1 lines)\n</content>")


async def test_directory_paging_edges(deps: Deps) -> None:
    assert await run_tool(Read, deps, filePath="/", offset=5) == (
        "<path>/</path>\n<type>directory</type>\n<entries>\n\n\n(2 entries)\n</entries>"
    )
    assert await run_tool(Read, deps, filePath="/", limit=0) == (
        "<path>/</path>\n<type>directory</type>\n<entries>\n\n\n"
        "(Showing 0 of 2 entries. Use 'offset' parameter to read beyond entry 1)\n</entries>"
    )
