"""The tools' foundation (deps, settings, paths, errors, tool construction), through a dummy tool run by a real
agent."""

import subprocess
import sys
import textwrap
from dataclasses import dataclass

import pytest
from pydantic_ai import Agent, ModelRetry, RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.toolsets import FunctionToolset
from pydantic_ai.usage import RunUsage

from minifs.tools import TOOLS, toolset
from minifs.tools.base import vfs_tool
from minifs.tools.deps import DEFAULT_SETTINGS, GrepSettings, HasVFS, ToolSettings, resolve, settings
from minifs.tools.errors import tool_error
from minifs.vfs.core import FileVersion, VFSError
from minifs.vfs.policy import PolicyDenied
from minifs.vfs.search import SearchRefused
from minifs.vfs.view import VFSView
from tests.tools.conftest import W2, Deps, call_tool, retry_message, run_tool

DESCRIPTION = "Shows a file's content, or replaces it."


async def peek(ctx: RunContext[HasVFS], filePath: str, content: str | None = None, stale: bool = False) -> str:
    """Show or write a file.

    Args:
        filePath: The path of the file
        content: New content to write, if any
        stale: Write expecting a version that never existed
    """
    path = resolve(ctx, filePath)
    try:
        if content is None:
            return f"{path}: {(await ctx.deps.vfs.pread(path)).content.decode()}"
        expect = None
        if stale:
            entry = await ctx.deps.vfs.stat(path)
            assert entry.id is not None
            expect = FileVersion(entry.id, 999, entry.size)
        await ctx.deps.vfs.write(path, content.encode(), expect=expect)
        return f"wrote {path}"
    except VFSError as e:
        return tool_error(e, "write" if content is not None else "read")


Peek = vfs_tool(peek, "Peek", DESCRIPTION)


@dataclass(frozen=True)
class OnlyVFS:
    """Deps with nothing but ``vfs``, frozen: what pyright must accept for ``RunContext[HasVFS]``."""

    vfs: VFSView


def context(deps: HasVFS) -> RunContext[HasVFS]:
    return RunContext(deps=deps, model=TestModel(), usage=RunUsage())


# Tool construction


async def test_schema_has_the_description_and_parameter_docs(deps: Deps) -> None:
    call = await call_tool(Peek, deps, filePath="notes.md")
    (tool_def,) = call.tool_defs
    assert tool_def.name == "Peek"
    assert tool_def.description == DESCRIPTION
    props = tool_def.parameters_json_schema["properties"]
    assert props["filePath"]["description"] == "The path of the file"
    assert tool_def.parameters_json_schema["required"] == ["filePath"]


def test_undocumented_parameter_is_refused() -> None:
    async def bad(ctx: RunContext[HasVFS], filePath: str) -> str:
        """No Args section."""
        return filePath

    with pytest.raises(Exception, match="filePath"):
        vfs_tool(bad, "Bad", DESCRIPTION)


async def test_invalid_arguments_are_a_retry(deps: Deps) -> None:
    call = await call_tool(Peek, deps, path="notes.md")
    assert call.retried


def test_toolset_holds_every_tool() -> None:
    tools = toolset()
    assert isinstance(tools, FunctionToolset)
    assert list(tools.tools.values()) == list(TOOLS)  # empty until #11, #12, #13, #39 and #40 add theirs


def test_agents_accept_the_tools_for_their_own_deps() -> None:
    """Checked by pyright: ``Tool[HasVFS]`` fits an agent over any deps with a ``vfs``."""
    agent: Agent[OnlyVFS, str] = Agent(TestModel(), deps_type=OnlyVFS, tools=[Peek])
    with_toolset: Agent[Deps, str] = Agent(TestModel(), deps_type=Deps, toolsets=[toolset()])
    assert agent.deps_type is OnlyVFS and with_toolset.deps_type is Deps


# Deps


async def test_deps_with_only_vfs_get_the_defaults(deps: Deps) -> None:
    only = OnlyVFS(deps.vfs)
    assert await run_tool(Peek, only, filePath="notes.md") == "/workspace/notes.md: mine\n"
    ctx = context(only)
    assert settings(ctx) is DEFAULT_SETTINGS
    assert resolve(ctx, "a") == "/workspace/a"


def test_settings_defaults_match_ladderframe() -> None:
    s = ToolSettings()
    assert (s.read.default_limit, s.read.max_line_length, s.read.max_bytes) == (2000, 2000, 50 * 1024)
    assert (s.glob.limit, s.grep.limit, s.grep.max_line_chars, s.grep.timeout) == (100, 100, 2000, 2.0)


def test_settings_from_deps(deps: Deps) -> None:
    custom = ToolSettings(grep=GrepSettings(limit=5))
    ctx = context(Deps(deps.vfs, vfs_settings=custom))
    assert settings(ctx).grep.limit == 5


# Paths


def test_workdir_that_isnt_a_string_is_the_default(deps: Deps) -> None:
    @dataclass
    class Broken:
        vfs: VFSView
        vfs_workdir: object = None

    assert resolve(context(Broken(deps.vfs)), "a") == "/workspace/a"


@pytest.mark.parametrize(
    ("workdir", "path", "resolved"),
    [
        ("/workspace", "notes.md", "/workspace/notes.md"),
        ("/workspace", "./a/../notes.md", "/workspace/notes.md"),
        ("/workspace", "/docs//guide.md", "/docs/guide.md"),
        ("/workspace", "//docs/guide.md", "/docs/guide.md"),
        ("/workspace", "../../../docs/guide.md", "/docs/guide.md"),
        ("/workspace", "", "/workspace"),
        ("/workspace", "~/x", "/workspace/~/x"),
        ("/docs", "guide.md", "/docs/guide.md"),
        ("workspace", "a", "/workspace/a"),  # a relative workdir is anchored at /
    ],
)
def test_resolve(deps: Deps, workdir: str, path: str, resolved: str) -> None:
    ctx = context(Deps(deps.vfs, vfs_workdir=workdir))
    assert resolve(ctx, path) == resolved


async def test_relative_path_resolves_against_workdir(deps: Deps) -> None:
    assert await run_tool(Peek, deps, filePath="notes.md") == "/workspace/notes.md: mine\n"
    docs = Deps(deps.vfs, vfs_workdir="/docs")
    assert await run_tool(Peek, docs, filePath="guide.md") == "/docs/guide.md: guide\n"


@pytest.mark.parametrize("workspace", [W2, "nope"])
@pytest.mark.parametrize("prefix", ["../", "/"])
@pytest.mark.parametrize("content", [None, "x"])
async def test_other_workspaces_look_missing(deps: Deps, workspace: str, prefix: str, content: str | None) -> None:
    """Another workspace's file, read or written, looks exactly like a file that doesn't exist."""
    message = await retry_message(Peek, deps, filePath=f"{prefix}workspaces/{workspace}/secret.md", content=content)
    assert message == f"File not found: /workspaces/{workspace}/secret.md"


# Errors


async def test_eacces_is_the_result(deps: Deps) -> None:
    result = await run_tool(Peek, deps, filePath="/docs/guide.md", content="x")
    assert result == "Permission denied: write /docs/guide.md"


async def test_enoent_is_a_retry(deps: Deps) -> None:
    assert await retry_message(Peek, deps, filePath="missing.md") == "File not found: /workspace/missing.md"


async def test_eisdir_is_a_retry(deps: Deps) -> None:
    assert await retry_message(Peek, deps, filePath=".") == "Path is a directory, not a file: /workspace"


async def test_estale_is_a_retry(deps: Deps) -> None:
    message = await retry_message(Peek, deps, filePath="notes.md", content="new", stale=True)
    assert message == "/workspace/notes.md was modified since it was read. Read it again before writing."
    assert await run_tool(Peek, deps, filePath="notes.md") == "/workspace/notes.md: mine\n"


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (VFSError("EIO", "/workspace/a", "database error"), "Internal error reading /workspace/a"),
        (VFSError("ENOTDIR", "/workspace/a/b"), "File not found: /workspace/a/b"),
        (VFSError("EINVAL", "/", "'**' spans mount points here"), "/: '**' spans mount points here"),
        (VFSError("EINVAL", "/x"), "EINVAL: /x"),
        (VFSError("EXDEV", "/x"), "EXDEV: /x"),
        (SearchRefused("timeout", "The search timed out. Narrow it."), "The search timed out. Narrow it."),
    ],
)
def test_retry_messages(error: VFSError | SearchRefused, message: str) -> None:
    with pytest.raises(ModelRetry) as info:
        tool_error(error, "read")
    assert info.value.message == message
    assert info.value.__cause__ is None


def test_message_override() -> None:
    with pytest.raises(ModelRetry, match=r"^No such directory: /workspace/x$"):
        tool_error(VFSError("ENOENT", "/workspace/x"), "read", ENOENT="No such directory: {path}")
    with pytest.raises(AssertionError, match="ENOENTT"):
        tool_error(VFSError("ENOENT", "/workspace/x"), "read", ENOENTT="No such directory: {path}")


def test_denial_names_the_refused_action() -> None:
    """The policy's action wins over the one the tool passed: a write tool's refused read says read."""
    assert tool_error(PolicyDenied("/docs/a", "agent", "read"), "write") == "Permission denied: read /docs/a"
    assert tool_error(VFSError("EACCES", "/docs/a"), "write") == "Permission denied: write /docs/a"


# Packaging


def test_vfs_imports_without_pydantic_ai() -> None:
    """``minifs.vfs`` and ``minifs.db`` (every module) import with pydantic-ai blocked; ``minifs.tools`` says which
    extra it needs."""
    script = textwrap.dedent(
        """
        import importlib, pkgutil, sys
        sys.modules["pydantic_ai"] = None  # import pydantic_ai -> ImportError
        import minifs.db, minifs.vfs
        for package in (minifs.db, minifs.vfs):
            for module in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
                importlib.import_module(module.name)
        assert not any(name.startswith("pydantic_ai.") for name in sys.modules)
        try:
            import minifs.tools
        except ImportError as e:
            assert "minifs[pydantic-ai]" in str(e), e
        else:
            raise AssertionError("minifs.tools imported without pydantic-ai")
        """
    )
    subprocess.run([sys.executable, "-c", script], check=True, timeout=60)
