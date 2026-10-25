"""``Glob``: file paths matching a glob pattern, newest first, with ladderframe's contract over ``VFSView.glob``
(README: Agent tools). Hidden directories aren't descended into, ``.git`` and the settings' excludes are skipped,
and a pattern is parsed as rg parses ``-g`` (README: Glob, Divergences from rg)."""

from __future__ import annotations

from pydantic_ai import ModelRetry, RunContext

from minifs.tools.base import vfs_tool
from minifs.tools.deps import ALWAYS_EXCLUDED, HasVFS, resolve, settings
from minifs.tools.errors import tool_error
from minifs.vfs.core import VFSError
from minifs.vfs.glob import GlobError
from minifs.vfs.search import SearchRefused

DESCRIPTION = """- Fast file pattern matching tool that works with any codebase size
- Supports glob patterns like "**/*.js" or "src/**/*.ts"
- Returns matching file paths
- Use this tool when you need to find files by name patterns
- You have the capability to call multiple tools in a single response. It is always better to speculatively perform multiple searches as a batch that are potentially useful."""


async def glob(ctx: RunContext[HasVFS], pattern: str, path: str | None = None) -> str:
    """Find files by glob pattern.

    Args:
        pattern: The glob pattern to match files against
        path: The directory to search in. If not specified, the current working directory will be used. IMPORTANT: Omit this field to use the default directory. DO NOT enter "undefined" or "null" - simply omit it for the default behavior. Must be a valid directory path if provided.
    """
    config = settings(ctx).glob
    search = resolve(ctx, path or ".")
    excludes = (*ALWAYS_EXCLUDED, *config.excludes)
    try:
        result = await ctx.deps.vfs.glob(pattern, search, excludes=excludes, limit=config.limit)
    except GlobError as e:
        raise ModelRetry(str(e)) from None
    except (VFSError, SearchRefused) as e:
        # ENOTDIR is both "path is a file" and "a parent is a file"; a stat tells them apart. It runs in a
        # snapshot of its own, so a concurrent change can only pick the other of these two retry messages.
        if isinstance(e, VFSError) and e.errno == "ENOTDIR" and await _is_file(ctx, search):
            raise ModelRetry(f"glob path must be a directory: {search}") from None
        # A missing path, one below a file, and one outside the view all look alike, as on disk.
        return tool_error(e, "read", ENOENT="No such directory: {path}", ENOTDIR="No such directory: {path}")

    if not result.entries:
        return "No files found"
    output = [entry.path for entry in result.entries]
    if result.truncated:
        output += [
            "",
            f"(Results are truncated: showing first {config.limit} results. Consider using a more specific path or "
            "pattern.)",
        ]
    return "\n".join(output)


async def _is_file(ctx: RunContext[HasVFS], path: str) -> bool:
    try:
        return (await ctx.deps.vfs.stat(path)).kind == "file"
    except VFSError:
        return False


Glob = vfs_tool(glob, "Glob", DESCRIPTION)
