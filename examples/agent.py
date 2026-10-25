"""A pydantic-ai agent over a minifs workspace, wired as a long-running service would be (README: Using the tools
with pydantic-ai).

    MINIFS_DSN=postgresql://... uv run minifs db migrate   # once per database
    MINIFS_DSN=postgresql://... uv run python examples/agent.py

It seeds a new workspace with a small Python project whose ``add`` subtracts, asks the agent to fix it and note the
fix in ``CHANGES.md``, prints the agent's answer and the workspace's files, and logs the directory cache's stats.

The model is ``$MINIFS_MODEL`` (any pydantic-ai model name) if set, else Claude if ``ANTHROPIC_API_KEY`` is set.
Either needs the provider's extra and key: ``uv run --with 'pydantic-ai-slim[anthropic]'``, or for OpenRouter
``--with 'pydantic-ai-slim[openrouter]'`` with ``OPENROUTER_API_KEY`` and ``MINIFS_MODEL=openrouter:<model slug>``.
With neither, it runs offline: a ``FunctionModel`` plays the task with a fixed script of tool calls (Glob, Read,
Grep, Edit, Write, Read).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import UUID, uuid4

from pydantic_ai import Agent, AgentRunResult
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel

from minifs.db.pool import Connector, Pools
from minifs.tools import Edit, Glob, Grep, Read, Write
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.client import VFSClient
from minifs.vfs.policy import DEFAULT_POLICY
from minifs.vfs.view import VFSView, workspace_mounts
from minifs.vfs.watch import Watcher


@dataclass
class Deps:
    vfs: VFSView  # all the tools need; vfs_workdir ("/workspace") and vfs_settings are optional


# One agent per process, shared by every run and every workspace: the deps carry the workspace, and the model can
# be given per run. retries=3 is ladderframe's tool_retries.
agent = Agent(deps_type=Deps, tools=[Read, Write, Edit, Glob, Grep], retries=3)


@asynccontextmanager
async def open_client(connector: Connector) -> AsyncGenerator[tuple[VFSClient, DirEntryCache]]:
    """The process's client and directory cache. The cache follows the process's one LISTEN connection (the
    ``Watcher``), so another process's rename or delete evicts what it changed; every use of an entry is checked in
    the statement that uses it anyway, so a stale one costs a slower lookup, never the wrong file."""
    cache = DirEntryCache()
    async with Pools(connector) as pools, Watcher(connector) as watcher, cache.follow(watcher):
        client = VFSClient(pools.write, search_pool=pools.search, cache=cache)
        try:
            yield client, cache
        finally:
            await client.settle()


async def run_task(client: VFSClient, workspace_id: UUID, prompt: str, model: Model | str) -> AgentRunResult[str]:
    """One agent run in one workspace: a view per run, over the shared client."""
    view = VFSView(client, workspace_mounts(workspace_id), DEFAULT_POLICY, "agent")
    return await agent.run(prompt, deps=Deps(vfs=view), model=model)


# The demo task

PROMPT = "test_calc.py fails. Fix the bug, then add a line about the fix to CHANGES.md."

FILES = {
    "calc.py": "def add(a, b):\n    return a - b\n\n\ndef mul(a, b):\n    return a * b\n",
    "test_calc.py": "from calc import add, mul\n\n\ndef test_add():\n    assert add(2, 3) == 5\n\n\ndef test_mul():\n"
    "    assert mul(2, 3) == 6\n",
    "README.md": "# calc\n\nTiny arithmetic helpers.\n",
}
"""The seeded workspace, by path relative to it."""

SCRIPT: list[tuple[str, dict[str, object]]] = [
    ("Glob", {"pattern": "**/*.py"}),
    ("Read", {"filePath": "/workspace/calc.py"}),
    ("Grep", {"pattern": r"return a - b", "include": "*.py"}),
    ("Edit", {"filePath": "/workspace/calc.py", "oldString": "return a - b", "newString": "return a + b"}),
    ("Write", {"filePath": "/workspace/CHANGES.md", "content": "- Fix add(), which subtracted.\n"}),
    ("Read", {"filePath": "/workspace/CHANGES.md"}),
]
"""The offline model's tool calls, one per response."""

ANSWER = "Fixed add() in /workspace/calc.py and noted it in /workspace/CHANGES.md."


def scripted(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """The offline model: the next call of ``SCRIPT``, whatever the last one returned, then ``ANSWER``."""
    step = sum(isinstance(m, ModelResponse) for m in messages)
    if step < len(SCRIPT):
        name, args = SCRIPT[step]
        return ModelResponse(parts=[ToolCallPart(name, args)])
    return ModelResponse(parts=[TextPart(ANSWER)])


def choose_model() -> Model | str:
    """``$MINIFS_MODEL``, else Claude if ``ANTHROPIC_API_KEY`` is set, else the offline ``scripted`` model."""
    if name := os.environ.get("MINIFS_MODEL"):
        return name
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic:claude-sonnet-5-5"
    return FunctionModel(scripted)


async def seed(client: VFSClient, workspace_id: UUID) -> None:
    """Writes ``FILES`` into the workspace, as ingestion would: through the client, not the agent's view."""
    for name, text in FILES.items():
        await client.create(f"/workspaces/{workspace_id}/{name}", text.encode(), parents=True)


async def workspace_files(client: VFSClient, workspace_id: UUID) -> dict[str, str]:
    """Every file of the workspace, by path relative to it, with its text."""
    root = f"/workspaces/{workspace_id}"
    files: dict[str, str] = {}
    for entry in await client.glob("**", root, hidden=True):
        files[entry.path.removeprefix(root + "/")] = (await client.pread(entry.path, 0, entry.size)).content.decode()
    return files


_log = logging.getLogger(__name__)


async def main(dsn: str = "") -> str:
    """Runs the demo task in a new workspace on ``dsn`` (empty: the ``PG*`` environment variables) and returns the
    agent's answer."""
    # A static DSN with its password. On Databricks Lakebase, pass a callable that returns a fresh OAuth token
    # instead; it is called for every new connection, since tokens expire after an hour:
    #     Connector("host=... dbname=databricks_postgres user=... sslmode=require", password=fetch_token)
    connector = Connector(dsn)
    async with open_client(connector) as (client, cache):
        workspace_id = uuid4()
        await seed(client, workspace_id)
        result = await run_task(client, workspace_id, PROMPT, choose_model())
        print(result.output)
        for name, text in sorted((await workspace_files(client, workspace_id)).items()):
            print(f"\n--- /workspace/{name}\n{text}", end="")
        _log.info("directory cache: %s", cache.stats())
        return result.output


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(main(os.environ.get("MINIFS_DSN", "")))
