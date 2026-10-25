"""``Grep``: lines matching a regular expression, grouped by file, with ladderframe's contract over ``VFSView.grep``
(README: Agent tools). The pattern is parsed as rg parses it and searched in Postgres (README: Search, Regex
dialect); hidden files are searched, ``.git`` and the settings' excludes skipped, and hits come in order of path,
then line."""

from __future__ import annotations

from pydantic_ai import ModelRetry, RunContext

from minifs._regex import RegexError
from minifs.tools.base import vfs_tool
from minifs.tools.deps import ALWAYS_EXCLUDED, HasVFS, resolve, settings
from minifs.tools.errors import tool_error
from minifs.vfs.core import VFSError
from minifs.vfs.glob import GlobError
from minifs.vfs.search import GrepHit, SearchRefused

DESCRIPTION = """- Fast content search tool that works with any codebase size
- Searches file contents using regular expressions
- Supports full regex syntax (eg. "log.*Error", "function\\s+\\w+", etc.)
- Use (?i) for a case-insensitive search; lookaround and backreferences aren't supported
- Filter files by pattern with the include parameter (eg. "*.js", "*.{ts,tsx}")
- Returns file paths and line numbers with matching lines
- Use this tool when you need to find files containing specific patterns"""


async def grep(ctx: RunContext[HasVFS], pattern: str, path: str | None = None, include: str | None = None) -> str:
    """Search file contents with a regular expression.

    Args:
        pattern: The regex pattern to search for in file contents
        path: The directory to search in. Defaults to the current working directory.
        include: File pattern to include in the search (e.g. "*.js", "*.{ts,tsx}")
    """
    if not pattern:
        raise ModelRetry("pattern is required")
    config = settings(ctx).grep
    search = resolve(ctx, path or ".")
    try:
        result = await ctx.deps.vfs.grep(
            pattern,
            search,
            include=include,
            excludes=(*ALWAYS_EXCLUDED, *config.excludes),
            limit=config.limit,
            max_line_chars=config.max_line_chars,
            timeout=config.timeout,
        )
    except (RegexError, GlobError) as e:  # rg's own messages
        raise ModelRetry(str(e)) from None
    except (VFSError, SearchRefused) as e:
        # A missing path, one below a file, and one outside the view all look alike, as on disk.
        missing = "No such file or directory: {path}"
        return tool_error(e, "read", ENOENT=missing, ENOTDIR=missing)

    if not result.hits:
        return "No files found"
    output = [f"Found {len(result.hits)} matches{' (more matches available)' if result.truncated else ''}"]
    current = ""
    for hit in result.hits:
        if hit.path != current:
            if current:
                output.append("")
            current = hit.path
            output.append(f"{hit.path}:")
        output.append(f"  Line {hit.line}: {_text(hit)}")
    if result.truncated:
        output += ["", "(Results truncated. Consider using a more specific path or pattern.)"]
    return "\n".join(output)


def _text(hit: GrepHit) -> str:
    """The hit's line as ladderframe shows it: without a trailing ``\\r``, and ``...`` after a cut line."""
    text = hit.text.rstrip("\r")
    return text + "..." if hit.cut else text


Grep = vfs_tool(grep, "Grep", DESCRIPTION)
