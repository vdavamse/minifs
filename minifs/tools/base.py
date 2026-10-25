"""How a tool is built: a plain ``async def`` taking ``RunContext[HasVFS]``, whose Google-style docstring gives
the parameter descriptions, wrapped in a ``pydantic_ai.Tool`` with ladderframe's name and description:

    DESCRIPTION = \"\"\"Reads a file ...\"\"\"  # ladderframe's text

    async def read(ctx: RunContext[HasVFS], filePath: str, offset: int | None = None) -> str:
        \"\"\"Read a file or directory.

        Args:
            filePath: The absolute path to the file or directory to read
            offset: The line number to start reading from (1-indexed)
        \"\"\"

    Read = vfs_tool(read, "Read", DESCRIPTION)
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Concatenate

from pydantic_ai import RunContext, Tool

from minifs.tools.deps import HasVFS


def vfs_tool(
    function: Callable[Concatenate[RunContext[HasVFS], ...], Awaitable[Any]],
    name: str,
    description: str,
    *,
    sequential: bool = False,
) -> Tool[HasVFS]:
    """``function`` as the tool ``name``. Every parameter must be described in its docstring. A ``sequential``
    tool (one that writes) runs alone: pydantic-ai doesn't run other tool calls of the same response alongside it,
    so a model's ``Write`` then ``Read`` of one file see each other in order."""
    tool: Tool[HasVFS] = Tool(
        function,
        takes_ctx=True,
        name=name,
        description=description,
        docstring_format="google",
        require_parameter_descriptions=True,
        sequential=sequential,
    )
    return tool
