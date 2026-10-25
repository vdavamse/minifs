"""The ``Grep`` tool, through a real agent: ladderframe's tests, then the view's paths, policy and excludes."""

import pytest

from minifs.tools import Grep, Read
from minifs.tools.deps import GrepSettings, ToolSettings
from minifs.tools.grep import DESCRIPTION
from minifs.vfs.client import MAX_TIMEOUT, VFSClient
from minifs.vfs.core import VFSError
from minifs.vfs.glob import GlobError
from minifs.vfs.policy import Policy, Rule
from minifs.vfs.search import GrepResult, SearchRefused
from minifs.vfs.view import MAX_GLOB_LIMIT, VFSView, workspace_mounts
from tests.tools.conftest import W1, W2, Deps, call_tool, retry_message, run_tool

WS = f"/workspaces/{W1}"
SAMPLE = "/workspace/data/sample.txt"


def configured(deps: Deps, **grep: object) -> Deps:
    return Deps(deps.vfs, vfs_settings=ToolSettings(grep=GrepSettings(**grep)))  # pyright: ignore[reportArgumentType]


async def make(client: VFSClient, files: dict[str, bytes]) -> None:
    for path, data in files.items():
        await client.create(f"{WS}/{path}", data, parents=True)


# ladderframe's tests


async def test_grep(client: VFSClient, deps: Deps) -> None:
    await make(client, {"data/sample.txt": b"line one\nline two\nneedle here\n"})
    assert await run_tool(Grep, deps, pattern="needle|two", path="data") == (
        f"Found 2 matches\n{SAMPLE}:\n  Line 2: line two\n  Line 3: needle here"
    )
    assert await run_tool(Grep, deps, pattern="needle", path="data/sample.txt", include="*.txt") == (
        f"Found 1 matches\n{SAMPLE}:\n  Line 3: needle here"
    )
    assert await run_tool(Grep, deps, pattern="needle", include="*.py") == "No files found"
    assert "regex parse error" in await retry_message(Grep, deps, pattern="(unclosed")


async def test_grep_truncates_at_limit(client: VFSClient, deps: Deps) -> None:
    await make(client, {"a.txt": b"hit\nhit\nhit\n"})
    out = await run_tool(Grep, configured(deps, limit=2), pattern="hit")
    assert out.startswith("Found 2 matches (more matches available)\n")
    assert out.endswith("\n\n(Results truncated. Consider using a more specific path or pattern.)")


async def test_grep_at_exactly_the_limit_is_not_truncated(client: VFSClient, deps: Deps) -> None:
    await make(client, {"a.py": b"hit\n", "b.py": b"hit\n"})
    out = await run_tool(Grep, configured(deps, limit=2), pattern="hit")
    assert "truncated" not in out and "more matches" not in out


async def test_read_line_numbers_agree_with_grep(client: VFSClient, deps: Deps) -> None:
    # A lone \r doesn't end a line, for Grep or Read.
    await make(client, {"log.txt": b"progress 10%\rprogress 100%\nNEEDLE\n"})
    assert "Line 2: NEEDLE" in await run_tool(Grep, deps, pattern="NEEDLE")
    assert "2: NEEDLE" in await run_tool(Read, deps, filePath="log.txt")


# Output


async def test_hits_are_grouped_by_file_in_path_order(client: VFSClient, deps: Deps) -> None:
    await make(client, {"b.md": b"x hit\n", "a/z.md": b"hit\nno\nhit again\n"})
    assert await run_tool(Grep, deps, pattern="hit") == (
        "Found 3 matches\n/workspace/a/z.md:\n  Line 1: hit\n  Line 3: hit again\n\n/workspace/b.md:\n  Line 1: x hit"
    )


async def test_word_boundaries_work(client: VFSClient, deps: Deps) -> None:
    # The \b regression the README warns about: Postgres's \b is a backspace, so it must be translated.
    await make(client, {"cfg.py": b"def parse_config(path):\ndef parse_configs():\n"})
    assert await run_tool(Grep, deps, pattern=r"\bparse_config\b") == (
        "Found 1 matches\n/workspace/cfg.py:\n  Line 1: def parse_config(path):"
    )


async def test_lines_drop_a_trailing_cr_and_long_lines_are_cut(client: VFSClient, deps: Deps) -> None:
    await make(client, {"crlf.txt": b"hit one\r\n", "long.txt": b"hit " + b"x" * 20 + b"\n"})
    assert await run_tool(Grep, configured(deps, max_line_chars=8), pattern="hit") == (
        "Found 2 matches\n/workspace/crlf.txt:\n  Line 1: hit one\n\n/workspace/long.txt:\n  Line 1: hit xxxx..."
    )


async def test_case_insensitive_with_the_inline_flag(client: VFSClient, deps: Deps) -> None:
    await make(client, {"a.md": b"Needle\n"})
    assert "Line 1: Needle" in await run_tool(Grep, deps, pattern="(?i)needle")
    assert await run_tool(Grep, deps, pattern="needle") == "No files found"


async def test_schema_is_ladderframes(deps: Deps) -> None:
    call = await call_tool(Grep, deps, pattern="mine")
    (tool_def,) = call.tool_defs
    assert (tool_def.name, tool_def.description) == ("Grep", DESCRIPTION)
    assert "rg" not in DESCRIPTION and "Agent tool" not in DESCRIPTION
    schema = tool_def.parameters_json_schema
    assert list(schema["properties"]) == ["pattern", "path", "include"]
    assert schema["required"] == ["pattern"]
    include = schema["properties"]["include"]["description"]
    assert include == 'File pattern to include in the search (e.g. "*.js", "*.{ts,tsx}")'


# Errors


def test_bad_settings_fail_at_construction() -> None:
    with pytest.raises(ValueError, match="grep limit"):
        GrepSettings(limit=-1)
    with pytest.raises(ValueError, match="grep max_line_chars"):
        GrepSettings(max_line_chars=MAX_GLOB_LIMIT + 1)
    with pytest.raises(TypeError, match="grep limit must be an int"):
        GrepSettings(limit=True)
    for timeout in (0, MAX_TIMEOUT * 2):
        with pytest.raises(ValueError, match="grep timeout"):
            GrepSettings(timeout=timeout)
    with pytest.raises(GlobError, match="error parsing glob 'a\\['"):
        GrepSettings(excludes=("a[",))


async def test_retries(client: VFSClient, deps: Deps) -> None:
    assert await retry_message(Grep, deps, pattern="") == "pattern is required"
    assert await retry_message(Grep, deps, pattern="x", path="nope") == "No such file or directory: /workspace/nope"
    no_dir = "No such file or directory: /workspace/notes.md/x"  # below a file
    assert await retry_message(Grep, deps, pattern="x", path="notes.md/x") == no_dir
    message = await retry_message(Grep, deps, pattern="x", include="*.{a")
    assert message.startswith("error parsing glob '*.{a': unclosed alternate group")
    too_many = await retry_message(Grep, deps, pattern="a{1000}")  # rg accepts it, ARE doesn't
    assert too_many.startswith("repetition count 1000 is too large")


async def test_refused_and_internal_errors_are_retries(deps: Deps, monkeypatch: pytest.MonkeyPatch) -> None:
    refusal = SearchRefused("files", "Search scope is over 10000 files; narrow the path or include pattern")

    async def refused(*args: object, **kwargs: object) -> GrepResult:
        raise refusal

    async def broken(*args: object, **kwargs: object) -> GrepResult:
        raise VFSError("EIO", "/workspace", "database error")

    monkeypatch.setattr(deps.vfs, "grep", refused)
    assert await retry_message(Grep, deps, pattern="x") == refusal.message
    monkeypatch.setattr(deps.vfs, "grep", broken)
    assert await retry_message(Grep, deps, pattern="x") == "Internal error reading /workspace"


async def test_paths_outside_the_view_are_not_found(deps: Deps) -> None:
    for path in (f"../workspaces/{W2}", f"/workspaces/{W2}", WS):  # another workspace, or this one's real path
        message = await retry_message(Grep, deps, pattern="theirs|mine", path=path)
        assert message.startswith("No such file or directory: /workspaces/")


async def test_root_searches_every_mount(deps: Deps) -> None:
    assert await run_tool(Grep, deps, pattern="guide|mine", path="/") == (
        "Found 2 matches\n/docs/guide.md:\n  Line 1: guide\n\n/workspace/notes.md:\n  Line 1: mine"
    )


async def test_policy(client: VFSClient) -> None:
    policy = Policy(
        {"agent": [Rule("/workspace/**/.env"), Rule("/workspace/private/**"), Rule("/**", "read", "write")]}
    )
    deps = Deps(VFSView(client, workspace_mounts(W1), policy, "agent"))
    await make(client, {"app/.env": b"KEY=secret\n", "app/config.py": b"secret = 1\n", "private/p.md": b"secret\n"})
    assert await run_tool(Grep, deps, pattern="secret") == (  # unreadable files are left out, not refused
        "Found 1 matches\n/workspace/app/config.py:\n  Line 1: secret = 1"
    )
    assert await run_tool(Grep, deps, pattern="x", path="private") == "Permission denied: read /workspace/private"
    assert await run_tool(Grep, deps, pattern="x", path="app/.env") == "Permission denied: read /workspace/app/.env"


async def test_excludes(client: VFSClient, deps: Deps) -> None:
    await make(
        client,
        {
            ".hidden/a.md": b"hit\n",
            ".git/config": b"hit\n",
            "node_modules/m/i.js": b"hit\n",
            "sub/.git/HEAD": b"hit\n",
        },
    )
    assert await run_tool(Grep, deps, pattern="hit") == "Found 1 matches\n/workspace/.hidden/a.md:\n  Line 1: hit"
    out = await run_tool(Grep, configured(deps, excludes=()), pattern="hit")  # .git stays excluded
    assert out.startswith("Found 2 matches\n") and "node_modules" in out and ".git" not in out
    assert "Line 1: hit" in await run_tool(Grep, deps, pattern="hit", path="node_modules")  # rg: relative to the root
