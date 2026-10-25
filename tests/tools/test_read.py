"""``Read``: ladderframe's Read tests over the view, VFS cases (policy, chroot, virtual root), and its output checked
against a copy of ladderframe's line reader."""

import asyncio
import io
import random
from typing import Any

import pytest
from psycopg import AsyncConnection
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
from minifs.tools import read as read_module
from minifs.tools.deps import ReadSettings, ToolSettings
from minifs.tools.read import DESCRIPTION, MAX_ATTACHMENT_BYTES, SAMPLE_BYTES
from minifs.vfs.client import VFSClient
from minifs.vfs.core import LineReadResult, ReadResult, VFSError
from minifs.vfs.policy import Policy, Rule
from minifs.vfs.view import VFSView, workspace_mounts
from tests.tools.conftest import W1, W2, Deps, call_tool, retry_message, run_tool

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16
WS = f"/workspaces/{W1}"


def one_line_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Batches of one cut line each, as a page of long lines took before the line cap, so the tests can change the
    file between a page's batches."""

    def fetch(config: ReadSettings) -> int:
        return 4 * (config.max_line_length + 1) + 4  # a cut line and its \n, and 3 bytes of the next

    monkeypatch.setattr(read_module, "_fetch", fetch)


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


async def test_instruction_lookups_count_no_denials(client: VFSClient) -> None:
    """Every Read looks up AGENTS.md and CLAUDE.md, whatever the path is: the ones the policy refuses aren't
    denials the model asked for, so ``policy.denials`` counts only a Read the policy refuses, once."""
    rules = [Rule("/workspace/pkg/AGENTS.md"), Rule("/workspace/pkg/CLAUDE.md"), Rule("/workspace/pkg/secret.txt")]
    rules.append(Rule("/workspace/private"))  # a missing path's suggestions can't list it
    policy = Policy({"agent": [*rules, Rule("/workspace/**", "read", "write")]})
    deps = Deps(VFSView(client, workspace_mounts(W1), policy, "agent"))
    await put(client, "/workspace/pkg/AGENTS.md", b"no")
    await put(client, "/workspace/pkg/f.py", b"x\n")
    await put(client, "/workspace/pkg/blob.bin", b"\x00\x01" * 100)
    await put(client, "/workspace/pkg/sub/g.py", b"y\n")
    await put(client, "/workspace/pkg/secret.txt", b"s\n")
    assert "1: x" in await run_tool(Read, deps, filePath="pkg/f.py")
    assert (await call_tool(Read, deps, filePath="pkg/blob.bin")).retried
    assert "<type>directory</type>" in await run_tool(Read, deps, filePath="pkg/sub")
    assert (await call_tool(Read, deps, filePath="pkg/missing.py")).retried
    assert (
        await retry_message(Read, deps, filePath="private/missing.py")
        == "File not found: /workspace/private/missing.py"
    )
    assert "1: y" in await run_tool(Read, deps, filePath="pkg/sub/g.py")
    assert policy.denials == {}
    assert await run_tool(Read, deps, filePath="pkg/secret.txt") == "Permission denied: read /workspace/pkg/secret.txt"
    assert policy.denials == {("agent", "read"): 1}


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
    ReadSettings(max_line_length=60, max_bytes=100),  # a batch is bigger than the output cap
    ReadSettings(max_line_length=5, max_bytes=40),
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
            assert call.retry == f"Cannot read binary file: {path}"  # by ladderframe's rules too
            continue
        got = call.retry if call.retried else call.result
        assert got == ladderframe_output(path, data, offset, limit, settings), (data, args, settings)


async def test_a_line_the_byte_cap_cut_is_read_whole(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The raw cap falls in line 2, which fits the output once line 1 is truncated: the tool refills from it."""
    one_line_batches(monkeypatch)
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
    one_line_batches(monkeypatch)
    settings = ReadSettings(max_line_length=100, max_bytes=1024)  # one long line per batch
    old, new = (b"".join(c * 1000 + b"\n" for _ in range(10)) for c in (b"a", b"b"))
    await put(client, "/workspace/f.txt", old)
    await put(client, "/workspace/tmp.txt", new)
    calls = 0
    pread_lines = view.pread_lines

    async def replacing(*args: Any, **kwargs: Any) -> LineReadResult:
        nonlocal calls
        calls += 1
        result = await pread_lines(*args, **kwargs)
        if calls == 1:
            await view.unlink("/workspace/f.txt")
            await view.rename("/workspace/tmp.txt", "/workspace/f.txt")
            assert (await pread_lines("/workspace/f.txt")).generation == result.generation  # only the id differs
        return result

    monkeypatch.setattr(view, "pread_lines", replacing)
    out = await run_tool(Read, Deps(view, vfs_settings=ToolSettings(read=settings)), filePath="f.txt")
    assert out == ladderframe_output("/workspace/f.txt", new, None, None, settings)


async def test_instructions_already_shown_are_looked_up_not_read(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every candidate goes in the one ``pread_many``, a group per directory, so a CLAUDE.md behind an AGENTS.md file
    is never read; an instruction file already in the conversation, or the file being read, is read with limit 0,
    and still keeps CLAUDE.md out of its directory."""
    await put(client, "/workspace/pkg/AGENTS.md", b"Use tabs.")
    await put(client, "/workspace/pkg/CLAUDE.md", b"not used: AGENTS.md is first")
    await put(client, "/workspace/pkg/f.py", b"x\n")
    asked: list[list[tuple[str, int, int]]] = []
    transferred: list[list[bytes]] = []
    pread_many = view.pread_many

    async def recording(
        reads: list[tuple[str, int, int]], *, groups: list[int] | None = None, count_denials: bool = True
    ) -> list[ReadResult | VFSError]:
        asked.append(list(reads))
        assert groups == [0, 0] and not count_denials
        found = await pread_many(reads, groups=groups, count_denials=count_denials)
        transferred.append([r.content for r in found if isinstance(r, ReadResult)])
        return found

    monkeypatch.setattr(view, "pread_many", recording)
    first, again = await read_twice(Deps(view), {"filePath": "pkg/f.py"}, {"filePath": "pkg/f.py"})
    assert "Use tabs." in first and "not used" not in first
    assert again.endswith("</content>")
    agents, claude = "/workspace/pkg/AGENTS.md", "/workspace/pkg/CLAUDE.md"
    assert asked == [[(agents, 0, 2**31 - 1), (claude, 0, 2**31 - 1)], [(agents, 0, 0), (claude, 0, 2**31 - 1)]]
    assert transferred == [[b"Use tabs.", b""], [b"", b""]]
    asked.clear()
    assert "<system-reminder>" not in await run_tool(Read, Deps(view), filePath="pkg/AGENTS.md")
    assert asked == [[(agents, 0, 0), (claude, 0, 2**31 - 1)]]


async def test_directory_paging_edges(deps: Deps) -> None:
    assert await run_tool(Read, deps, filePath="/", offset=5) == (
        "<path>/</path>\n<type>directory</type>\n<entries>\n\n\n(2 entries)\n</entries>"
    )
    assert await run_tool(Read, deps, filePath="/", limit=0) == (
        "<path>/</path>\n<type>directory</type>\n<entries>\n\n\n"
        "(Showing 0 of 2 entries. Use 'offset' parameter to read beyond entry 1)\n</entries>"
    )


@pytest.mark.parametrize(
    ("name", "data", "mime"),
    [
        ("p.jpg", b"\xff\xd8\xff\xe0" + b"\x00" * 8, "image/jpeg"),
        ("p", b"GIF89a" + b"\x00" * 8, "image/gif"),
        ("p.bin", b"RIFF\x00\x00\x00\x00WEBPVP8 ", "image/webp"),  # the magic bytes win over the extension
        ("text.png", b"not a png\n", "image/png"),  # the extension alone, as ladderframe
    ],
)
async def test_attachment_types(client: VFSClient, deps: Deps, name: str, data: bytes, mime: str) -> None:
    await put(client, f"/workspace/{name}", data)
    seen: list[BinaryContent] = []

    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        assert isinstance(last, ModelRequest)
        for part in last.parts:
            if isinstance(part, UserPromptPart) and not isinstance(part.content, str):
                seen.extend(c for c in part.content if isinstance(c, BinaryContent))
        if any(isinstance(p, ToolReturnPart) for p in last.parts):
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart("Read", {"filePath": name})])

    await Agent(FunctionModel(model), deps_type=Deps, tools=[Read]).run("go", deps=deps)
    assert [(c.media_type, c.data) for c in seen] == [(mime, data)]


@pytest.mark.parametrize(
    ("name", "data"),
    [
        ("x.bin", b"plain text\n"),  # a binary extension
        ("..bin", b"plain text\n"),  # PurePath.suffix is .bin, as in ladderframe
        ("CACHE.PYC", b"plain text\n"),
        ("ctl.txt", b"\x01\x02\x03ab\n"),  # over 30% non-printable, no NUL
    ],
)
async def test_binary_files_are_refused(client: VFSClient, deps: Deps, name: str, data: bytes) -> None:
    await put(client, f"/workspace/{name}", data)
    assert await retry_message(Read, deps, filePath=name) == f"Cannot read binary file: /workspace/{name}"


async def test_limit_zero(client: VFSClient, deps: Deps) -> None:
    await put(client, "/workspace/a.txt", b"a\nb\n")
    assert await run_tool(Read, deps, filePath="a.txt", limit=0) == (
        "<path>/workspace/a.txt</path>\n<type>file</type>\n<content>\n"
        "\n\n(Showing lines 1-0 of 2. Use offset=1 to continue.)\n</content>"
    )


async def test_an_attachment_that_grew_past_the_limit(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    await put(client, "/workspace/a.png", PNG + b"\x00" * 5000)
    pread = view.pread

    async def growing(path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
        result = await pread(path, offset, limit)
        if limit == SAMPLE_BYTES:
            await view.append(path, b"\x00" * MAX_ATTACHMENT_BYTES)
        return result

    monkeypatch.setattr(view, "pread", growing)
    message = await retry_message(Read, Deps(view), filePath="a.png")
    assert message == (
        f"/workspace/a.png is too large to attach ({len(PNG) + 5000 + MAX_ATTACHMENT_BYTES} bytes, "
        f"the limit is {MAX_ATTACHMENT_BYTES})"
    )


async def test_a_page_from_another_file_than_the_sample(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The file becomes binary between the sample and the page: the tool samples again, and refuses it."""
    await put(client, "/workspace/f.txt", b"text\n")
    replaced = asyncio.Event()
    pread, pread_lines = view.pread, view.pread_lines

    async def sampling(path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
        result = await pread(path, offset, limit)
        if not replaced.is_set():
            await view.unlink(path)
            await view.create(path, b"\x00binary\n")
            replaced.set()
        return result

    async def paging(*args: Any, **kwargs: Any) -> LineReadResult:
        await replaced.wait()
        return await pread_lines(*args, **kwargs)

    monkeypatch.setattr(view, "pread", sampling)
    monkeypatch.setattr(view, "pread_lines", paging)
    assert await retry_message(Read, Deps(view), filePath="f.txt") == "Cannot read binary file: /workspace/f.txt"


async def test_a_page_that_never_settles(client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch) -> None:
    """The file changes after every batch: after three attempts the tool returns the last one's page, whole."""
    one_line_batches(monkeypatch)
    settings = ReadSettings(max_line_length=100, max_bytes=1024)  # one long line per batch, 7 lines a page
    data = b"".join(b"a" * 1000 + b"\n" for _ in range(10))
    await put(client, "/workspace/f.txt", data)
    sampled = asyncio.Event()
    calls = 0
    pread, pread_lines = view.pread, view.pread_lines

    async def sampling(path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
        result = await pread(path, offset, limit)
        sampled.set()
        return result

    async def rewriting(*args: Any, **kwargs: Any) -> LineReadResult:
        nonlocal calls
        await sampled.wait()  # the sample sees the generation the page starts at
        sampled.clear()
        calls += 1
        result = await pread_lines(*args, **kwargs)
        await view.write("/workspace/f.txt", data)  # same bytes, a new generation
        sampled.set()
        return result

    monkeypatch.setattr(view, "pread", sampling)
    monkeypatch.setattr(view, "pread_lines", rewriting)
    out = await run_tool(Read, Deps(view, vfs_settings=ToolSettings(read=settings)), filePath="f.txt")
    assert out == ladderframe_output("/workspace/f.txt", data, None, None, settings)
    assert calls == 2 + 2 + 8  # two batches per failed attempt, then lines 1 to 8 (the cap falls in line 8)


async def test_an_attachment_replaced_after_the_sample(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PNG (by its magic bytes alone) replaced by text between the sample and the whole read: the tool samples
    again and reads the text, rather than attach it as an image."""
    await put(client, "/workspace/a", PNG + b"\x01" * 5000)
    pread = view.pread
    replaced = False

    async def replacing(path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
        nonlocal replaced
        result = await pread(path, offset, limit)
        if not replaced:
            replaced = True
            await view.unlink(path)
            await view.create(path, b"now text\n")
        return result

    monkeypatch.setattr(view, "pread", replacing)
    out = await run_tool(Read, Deps(view), filePath="a")
    assert out.endswith("<content>\n1: now text\n\n(End of file - total 1 lines)\n</content>")


async def test_a_file_that_became_a_directory(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The page's first batch fails after the sample succeeded: the tool reads the path again."""
    await put(client, "/workspace/f", b"text\n")
    sampled = asyncio.Event()
    pread, pread_lines = view.pread, view.pread_lines
    swapped = False

    async def sampling(path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
        try:
            return await pread(path, offset, limit)
        finally:
            sampled.set()

    async def swapping(*args: Any, **kwargs: Any) -> LineReadResult:
        nonlocal swapped
        await sampled.wait()
        if not swapped:
            swapped = True
            await view.unlink("/workspace/f")
            await view.mkdir("/workspace/f")
        return await pread_lines(*args, **kwargs)

    monkeypatch.setattr(view, "pread", sampling)
    monkeypatch.setattr(view, "pread_lines", swapping)
    out = await run_tool(Read, Deps(view), filePath="f")
    assert out == "<path>/workspace/f</path>\n<type>directory</type>\n<entries>\n\n\n(0 entries)\n</entries>"


async def test_a_read_holds_at_most_three_connections(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Calls in flight at once, for a file five directories deep with instruction files on the way."""
    for d in ("a", "a/b/c"):
        await put(client, f"/workspace/{d}/CLAUDE.md", b"rules")
    await put(client, "/workspace/a/b/c/d/e/f.txt", b"x\n" * 3000)
    in_flight = peak = 0

    def counted(method: Any) -> Any:
        async def call(*args: Any, **kwargs: Any) -> Any:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            try:
                return await method(*args, **kwargs)
            finally:
                in_flight -= 1

        return call

    for name in ("stat", "readdir", "pread", "pread_lines", "pread_many"):
        monkeypatch.setattr(view, name, counted(getattr(view, name)))
    settings = ReadSettings(max_bytes=1024)
    out = await run_tool(Read, Deps(view, vfs_settings=ToolSettings(read=settings)), filePath="a/b/c/d/e/f.txt")
    assert "Instructions from: /workspace/a/b/c/CLAUDE.md" in out and "Instructions from: /workspace/a/CLAUDE.md" in out
    assert peak == 3  # the sample, the first batch and the instruction files


async def test_instruction_lookups_that_fail_or_find_a_directory(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An AGENTS.md directory falls through to CLAUDE.md, as ladderframe's ``is_file``; a lookup that fails (EIO)
    attaches nothing, rather than guess which names are there."""
    await put(client, "/workspace/a/CLAUDE.md", b"from a")
    await client.mkdir(f"{WS}/a/b/AGENTS.md", parents=True)
    await put(client, "/workspace/a/b/CLAUDE.md", b"from b")
    await put(client, "/workspace/a/b/f.txt", b"x\n")
    out = await run_tool(Read, Deps(view), filePath="a/b/f.txt")
    assert out.endswith(
        "<system-reminder>\nInstructions from: /workspace/a/b/CLAUDE.md\nfrom b\n\n"
        "Instructions from: /workspace/a/CLAUDE.md\nfrom a\n</system-reminder>"
    )

    async def failing(
        reads: list[tuple[str, int, int]], *, groups: list[int] | None = None, count_denials: bool = True
    ) -> list[ReadResult | VFSError]:
        raise VFSError("EIO", reads[0][0], "database error")

    monkeypatch.setattr(view, "pread_many", failing)
    out = await run_tool(Read, Deps(view), filePath="a/b/f.txt")
    assert out.endswith("<content>\n1: x\n\n(End of file - total 1 lines)\n</content>")


@pytest.mark.parametrize("depth", [0, 1, 5])
async def test_a_read_costs_three_statements_at_any_depth(
    client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch, depth: int
) -> None:
    """The sample, the page (one batch, even of long lines) and the instruction files, wherever the file is; a file
    in the working directory has no directories to look in. (Before #50: 4, 7 and 17 statements.)"""
    dirs = "".join(f"d{i}/" for i in range(depth))
    for i in range(0, depth, 2):
        await put(client, f"/workspace/{''.join(f'd{j}/' for j in range(i + 1))}CLAUDE.md", b"rules")
    await put(client, f"/workspace/{dirs}f.txt", b"".join(b"%d" % i * 3000 + b"\n" for i in range(30)))
    statements: list[str] = []
    execute = AsyncConnection.execute

    async def counting(self: AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        if "vfs_" in str(query):
            statements.append(str(query))
        return await execute(self, query, *args, **kwargs)

    monkeypatch.setattr(AsyncConnection, "execute", counting)
    out = await run_tool(Read, Deps(view), filePath=f"{dirs}f.txt")
    assert "... (line truncated to 2000 chars)" in out and "(Output capped at 50 KB. Showing lines 1-25." in out
    assert out.count("Instructions from: ") == (depth + 1) // 2
    assert len(statements) == (3 if depth else 2), statements


async def test_a_file_deleted_mid_page(client: VFSClient, view: VFSView, monkeypatch: pytest.MonkeyPatch) -> None:
    """The read starts again, and finds the path gone: not found, with suggestions."""
    one_line_batches(monkeypatch)
    settings = ReadSettings(max_line_length=100, max_bytes=1024)  # one long line per batch
    await put(client, "/workspace/f.txt", b"".join(b"a" * 1000 + b"\n" for _ in range(10)))
    await put(client, "/workspace/f.txt.bak", b"old\n")
    pread_lines = view.pread_lines
    calls = 0

    async def deleting(*args: Any, **kwargs: Any) -> LineReadResult:
        nonlocal calls
        calls += 1
        if calls == 2:
            await view.unlink("/workspace/f.txt")
        return await pread_lines(*args, **kwargs)

    monkeypatch.setattr(view, "pread_lines", deleting)
    message = await retry_message(Read, Deps(view, vfs_settings=ToolSettings(read=settings)), filePath="f.txt")
    assert message == "File not found: /workspace/f.txt\n\nDid you mean one of these?\n/workspace/f.txt.bak"
