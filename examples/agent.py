"""A minimal pydantic-ai agent over a minifs workspace (README: Quickstart).

    export MINIFS_DSN=postgresql://...
    uv run python examples/provision.py                  # migrates, seeds a workspace, prints its id
    MINIFS_MODEL=anthropic:claude-sonnet-5-5 uv run --with 'pydantic-ai-slim[anthropic,logfire]' \\
        python examples/agent.py <workspace-id> "test_calc.py fails. Fix the bug."

``MINIFS_MODEL`` is this example's own variable (pydantic-ai has none for the model): any pydantic-ai model name,
such as ``openrouter:<slug>``. The provider reads its key from its usual variable (``OPENROUTER_API_KEY``). Without it,
the agent runs offline on pydantic-ai's ``TestModel``, which calls ``Glob`` and ``Grep`` with made-up arguments. With
logfire installed (the ``logfire`` extra), every model and tool call is printed to the console, nothing sent.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections.abc import AsyncIterable
from dataclasses import dataclass

from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import (
    AgentStreamEvent,
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    PartEndEvent,
    TextPart,
    ThinkingPart,
)
from pydantic_ai.models import Model
from pydantic_ai.models.test import TestModel

from minifs.db.pool import Connector
from minifs.tools import Edit, Glob, Grep, Read, Write
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.policy import DEFAULT_POLICY
from minifs.vfs.service import open_vfs
from minifs.vfs.view import VFSView, workspace_mounts


@dataclass
class Deps:
    vfs: VFSView  # all the tools need; vfs_workdir and vfs_settings are optional


# One agent per process, for every run and workspace: the workspace is in the deps.
agent = Agent(deps_type=Deps, tools=[Read, Write, Edit, Glob, Grep], retries=3)


async def show(ctx: RunContext[Deps], events: AsyncIterable[AgentStreamEvent]) -> None:
    """Logs the model's text and each tool call and result, in full, as they happen."""
    async for event in events:
        if isinstance(event, PartEndEvent) and isinstance(event.part, TextPart | ThinkingPart):
            logging.info("%s: %s", event.part.part_kind, event.part.content)
        elif isinstance(event, FunctionToolCallEvent):
            logging.info("call %s(%s)", event.part.tool_name, event.part.args_as_json_str())
        elif isinstance(event, FunctionToolResultEvent):
            logging.info("%s %s:\n%s", event.part.tool_name, event.part.outcome, event.part.model_response_str())


async def main(dsn: str, workspace_id: str, prompt: str, model: Model | str) -> str:
    # On Lakebase: Connector(conninfo, password=fetch_token), called for each new connection.
    cache = DirEntryCache()  # one client and cache per process, shared by every run
    async with open_vfs(Connector(dsn), cache=cache) as vfs:
        view = VFSView(vfs.client, workspace_mounts(workspace_id), DEFAULT_POLICY, "agent")  # one view per run
        result = await agent.run(prompt, deps=Deps(vfs=view), model=model, event_stream_handler=show)
    logging.info("directory cache: %s", cache.stats())
    return result.output


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:  # timed spans of the run, model calls and tools: uv run --with 'pydantic-ai-slim[logfire]' ...
        import logfire  # pyright: ignore[reportMissingImports]

        logfire.configure(send_to_logfire=False)  # pyright: ignore[reportUnknownMemberType]
        logfire.instrument_pydantic_ai()  # pyright: ignore[reportUnknownMemberType]
    except ImportError:
        logging.info("without logfire: add --with 'pydantic-ai-slim[logfire]' for timed spans")
    model = os.environ.get("MINIFS_MODEL") or TestModel(call_tools=["Glob", "Grep"])
    print(asyncio.run(main(os.environ.get("MINIFS_DSN", ""), sys.argv[1], " ".join(sys.argv[2:]) or "Hi", model)))
