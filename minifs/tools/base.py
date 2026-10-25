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

import asyncio
import functools
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Concatenate

from pydantic_ai import ModelRetry, RunContext, Tool
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.run import AgentRunResult

from minifs import metrics
from minifs.tools.deps import HasVFS, settings
from minifs.tools.errors import DENIED
from minifs.vfs.budget import budget


def vfs_tool(
    function: Callable[Concatenate[RunContext[HasVFS], ...], Awaitable[Any]],
    name: str,
    description: str,
    *,
    sequential: bool = False,
) -> Tool[HasVFS]:
    """``function`` as the tool ``name``. Every parameter must be described in its docstring. A ``sequential``
    tool (one that writes) runs alone: pydantic-ai doesn't run other tool calls of the same response alongside it,
    so a model's ``Write`` then ``Read`` of one file see each other in order.

    Each call runs under the ``budget`` of the tool's settings (``ToolSettings.<name>.budget``, e.g. ``read``), and
    its latency is recorded in ``minifs.tool.duration`` by ``gen_ai.tool.name`` and outcome: ``ok``, ``denied`` (a
    policy denial returned as the result), ``retry`` (``ModelRetry``), ``cancelled`` or ``error``. A tool whose name
    isn't a ``ToolSettings`` field (``read``, ...) gets no budget of its own: only the client's statement timeout."""

    @functools.wraps(function)
    async def call(ctx: RunContext[HasVFS], *args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        outcome = "error"
        try:
            with budget(getattr(getattr(settings(ctx), name.lower(), None), "budget", None)):
                result = await function(ctx, *args, **kwargs)
            outcome = "denied" if isinstance(result, str) and result.startswith(DENIED) else "ok"
            return result
        except ModelRetry:
            outcome = "retry"
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            metrics.TOOL_DURATION.record(
                time.perf_counter() - start, {"gen_ai.tool.name": name, "minifs.outcome": outcome}
            )

    tool: Tool[HasVFS] = Tool(
        call,
        takes_ctx=True,
        name=name,
        description=description,
        docstring_format="google",
        require_parameter_descriptions=True,
        sequential=sequential,
    )
    return tool


class RunMetrics(AbstractCapability[Any]):
    """Records how many calls to ``tools`` (by name; default: every minifs tool) an agent run made, in
    ``minifs.run.tool_calls``, once per run that ends with a result: ``Agent(..., capabilities=[RunMetrics()])``."""

    def __init__(self, tools: Sequence[str] | None = None) -> None:
        if tools is None:
            from minifs.tools import TOOLS

            tools = [tool.name for tool in TOOLS]
        self.tools = frozenset(tools)

    async def after_run(self, ctx: RunContext[Any], *, result: AgentRunResult[Any]) -> AgentRunResult[Any]:
        calls = sum(
            isinstance(part, ToolCallPart) and part.tool_name in self.tools
            for message in result.new_messages()
            if isinstance(message, ModelResponse)
            for part in message.parts
        )
        metrics.RUN_TOOL_CALLS.record(calls)
        return result
