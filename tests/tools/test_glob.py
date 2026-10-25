"""The ``Glob`` tool, through a real agent: ladderframe's tests, then the view's paths, policy and excludes."""

from datetime import UTC, datetime, timedelta

import pytest
from psycopg import AsyncConnection

from minifs.tools import Glob
from minifs.tools.deps import ALWAYS_EXCLUDED, GlobSettings, ToolSettings
from minifs.vfs.client import VFSClient
from minifs.vfs.core import GlobResult, VFSError
from minifs.vfs.glob import GlobError
from minifs.vfs.policy import Policy, Rule
from minifs.vfs.view import MAX_GLOB_LIMIT, VFSView, workspace_mounts
from tests.tools.conftest import W1, W2, Deps, call_tool, retry_message, run_tool

WS = f"/workspaces/{W1}"


def limited(deps: Deps, limit: int) -> Deps:
    return Deps(deps.vfs, vfs_settings=ToolSettings(glob=GlobSettings(limit=limit)))


async def make(client: VFSClient, *paths: str) -> None:
    for path in paths:
        await client.create(f"{WS}/{path}", b"", parents=True)


async def age(client: VFSClient, conn: AsyncConnection, *paths: str) -> None:
    """Gives ``paths`` mtimes on consecutive days of January 2026, oldest first (so older than the fixture's)."""
    base = datetime(2026, 1, 1, tzinfo=UTC)
    for days, path in enumerate(paths):
        entry = await client.stat(f"{WS}/{path}")
        await conn.execute("UPDATE vfs_nodes SET mtime = %s WHERE id = %s", (base + timedelta(days=days), entry.id))


# ladderframe's tests


async def test_glob(client: VFSClient, deps: Deps) -> None:
    await make(client, "data/sample.txt")
    assert await run_tool(Glob, deps, pattern="*.txt", path="data") == "/workspace/data/sample.txt"
    assert await run_tool(Glob, deps, pattern="**/*.nothing") == "No files found"
    message = await retry_message(Glob, deps, pattern="*", path="data/sample.txt")
    assert message == "glob path must be a directory: /workspace/data/sample.txt"


async def test_glob_truncates_at_limit(client: VFSClient, deps: Deps, conn: AsyncConnection) -> None:
    await make(client, "a.py", "b.py", "c.py")
    await age(client, conn, "b.py", "c.py", "a.py")
    out = await run_tool(Glob, limited(deps, 2), pattern="*.py")
    assert out == (
        "/workspace/a.py\n/workspace/c.py\n\n"
        "(Results are truncated: showing first 2 results. Consider using a more specific path or pattern.)"
    )


async def test_glob_at_exactly_the_limit_is_not_truncated(client: VFSClient, deps: Deps) -> None:
    await make(client, "a.py", "b.py")
    out = await run_tool(Glob, limited(deps, 2), pattern="*.py")
    assert "truncated" not in out
    assert sorted(out.split("\n")) == ["/workspace/a.py", "/workspace/b.py"]


# Contract


async def test_schema_is_ladderframes(deps: Deps) -> None:
    (tool_def,) = (await call_tool(Glob, deps, pattern="*")).tool_defs
    assert tool_def.name == "Glob"
    assert tool_def.parameters_json_schema["required"] == ["pattern"]
    props = tool_def.parameters_json_schema["properties"]
    assert props["pattern"]["description"] == "The glob pattern to match files against"
    assert props["path"]["description"].startswith("The directory to search in. If not specified,")
    assert "Agent tool" not in (tool_def.description or "")


async def test_newest_first(client: VFSClient, deps: Deps, conn: AsyncConnection) -> None:
    await make(client, "a.md", "b.md", "c.md")
    await age(client, conn, "b.md", "c.md", "a.md")
    out = await run_tool(Glob, deps, pattern="*.md")
    assert out == "/workspace/notes.md\n/workspace/a.md\n/workspace/c.md\n/workspace/b.md"  # notes.md is from now


@pytest.mark.parametrize("path", [None, "", ".", "/workspace", "/workspace/"])
async def test_path_defaults_to_the_workdir(deps: Deps, path: str | None) -> None:
    args = {} if path is None else {"path": path}
    assert await run_tool(Glob, deps, pattern="*.md", **args) == "/workspace/notes.md"


async def test_relative_path_and_workdir(deps: Deps) -> None:
    docs = Deps(deps.vfs, vfs_workdir="/docs")
    assert await run_tool(Glob, docs, pattern="*") == "/docs/guide.md"
    assert await run_tool(Glob, deps, pattern="*", path="../docs") == "/docs/guide.md"


async def test_virtual_root(deps: Deps) -> None:
    out = await run_tool(Glob, deps, pattern="*.md", path="/")
    assert sorted(out.split("\n")) == ["/docs/guide.md", "/workspace/notes.md"]
    assert W1 not in out and W2 not in out


@pytest.mark.parametrize(
    ("path", "visible"),
    [
        ("missing", "/workspace/missing"),
        ("notes.md/x", "/workspace/notes.md/x"),
        (f"/workspaces/{W2}", f"/workspaces/{W2}"),
        (f"../workspaces/{W1}", f"/workspaces/{W1}"),
        ("/nope", "/nope"),
    ],
)
async def test_missing_directory(deps: Deps, path: str, visible: str) -> None:
    """A missing directory, one below a file, and one outside the view (another workspace, or this workspace's
    real path) look the same."""
    assert await retry_message(Glob, deps, pattern="*", path=path) == f"No such directory: {visible}"


@pytest.mark.parametrize(
    ("pattern", "message"),
    [
        ("a[", "error parsing glob 'a[': unclosed character class; missing ']'"),
        (
            "!*.md",
            "error parsing glob '!*.md': a leading '!' negates the glob in rg; pass it as an exclude, or escape it "
            "as '\\!'",
        ),
        ("", "error parsing glob '': empty pattern"),
        (
            "src/",
            "error parsing glob 'src/': a pattern ending in '/' matches only directories, and only files are listed",
        ),
    ],
)
async def test_bad_pattern_is_rgs_message(deps: Deps, pattern: str, message: str) -> None:
    assert await retry_message(Glob, deps, pattern=pattern) == message


# Hidden files and excludes


async def test_hidden_and_excluded(client: VFSClient, deps: Deps) -> None:
    await make(
        client,
        ".env.md",  # a hidden file is listed when the pattern matches it, as rg --files does
        ".github/ci.md",  # a hidden directory is not descended into
        ".git/HEAD.md",
        "node_modules/pkg/readme.md",
        "src/__pycache__/x.md",
        "src/main.md",
    )
    out = await run_tool(Glob, deps, pattern="**/*.md")
    assert sorted(out.split("\n")) == ["/workspace/.env.md", "/workspace/notes.md", "/workspace/src/main.md"]
    assert await run_tool(Glob, deps, pattern=".github/**") == "No files found"


async def test_excludes_are_configurable(client: VFSClient, deps: Deps) -> None:
    await make(client, "node_modules/a.md", "build/c.md")
    custom = Deps(deps.vfs, vfs_settings=ToolSettings(glob=GlobSettings(excludes=("build/",))))
    out = await run_tool(Glob, custom, pattern="**/*.md")
    assert sorted(out.split("\n")) == ["/workspace/node_modules/a.md", "/workspace/notes.md"]


async def test_path_inside_an_excluded_directory_is_searched(client: VFSClient, deps: Deps) -> None:
    """As in rg, excludes match relative to the search root."""
    await make(client, "node_modules/pkg/index.js")
    assert await run_tool(Glob, deps, pattern="*.js", path="node_modules/pkg") == (
        "/workspace/node_modules/pkg/index.js"
    )


# Policy


@pytest.fixture
def private_view(client: VFSClient) -> VFSView:
    rules = [Rule("/workspace/private/**"), Rule("/workspace/**", "read", "write"), Rule("/docs/**", "read")]
    return VFSView(client, workspace_mounts(W1), Policy({"agent": rules}), "agent")


async def test_unreadable_path_is_denied(client: VFSClient, private_view: VFSView) -> None:
    await make(client, "private/key.md")
    result = await run_tool(Glob, Deps(private_view), pattern="*", path="private")
    assert result == "Permission denied: read /workspace/private"


async def test_unreadable_files_are_left_out(client: VFSClient, private_view: VFSView) -> None:
    await make(client, "private/key.md", "public/a.md", "public/b.md")
    deps = Deps(private_view, vfs_settings=ToolSettings(glob=GlobSettings(limit=3)))
    out = await run_tool(Glob, deps, pattern="**/*.md")
    assert sorted(out.split("\n")) == ["/workspace/notes.md", "/workspace/public/a.md", "/workspace/public/b.md"]


def test_bad_settings_fail_at_construction() -> None:
    with pytest.raises(ValueError, match="glob limit"):
        GlobSettings(limit=-1)
    with pytest.raises(ValueError, match="glob limit"):
        GlobSettings(limit=MAX_GLOB_LIMIT + 1)
    for bad in (1.5, True):
        with pytest.raises(TypeError, match="glob limit must be an int"):
            GlobSettings(limit=bad)  # pyright: ignore[reportArgumentType]
    with pytest.raises(GlobError, match="error parsing glob 'a\\['"):
        GlobSettings(excludes=("a[",))


@pytest.mark.parametrize(
    ("path", "message"), [("missing", "error parsing glob 'a['"), ("/nope", "No such directory: /nope")]
)
async def test_error_order(deps: Deps, path: str, message: str) -> None:
    """A bad pattern is reported before a bad path inside the view; a path outside it is refused first."""
    assert (await retry_message(Glob, deps, pattern="a[", path=path)).startswith(message)


async def test_internal_error_names_only_the_visible_path(deps: Deps, monkeypatch: pytest.MonkeyPatch) -> None:
    async def broken(*args: object, **kwargs: object) -> GlobResult:
        raise VFSError("EIO", "/workspace", "database error")

    monkeypatch.setattr(deps.vfs, "glob", broken)
    assert await retry_message(Glob, deps, pattern="*") == "Internal error reading /workspace"


async def test_pattern_across_mount_points_is_a_retry(deps: Deps) -> None:
    message = await retry_message(Glob, deps, pattern="**/workspace/*.md", path="/")
    assert message.startswith("/: '**/workspace/*.md' spans mount points here")


async def test_always_excluded_holds_with_hidden_files(client: VFSClient, view: VFSView) -> None:
    """``ALWAYS_EXCLUDED`` keeps ``.git`` out even for a search that lists hidden files, as Grep's does. (Glob
    lists no hidden directories, so for Glob it only mirrors ladderframe's ``-g '!**/.git/**'``.)"""
    await make(client, ".git/HEAD.md", ".github/ci.md")
    result = await view.glob("**/*.md", "/workspace", excludes=ALWAYS_EXCLUDED, hidden=True)
    assert sorted(e.path for e in result.entries) == ["/workspace/.github/ci.md", "/workspace/notes.md"]
