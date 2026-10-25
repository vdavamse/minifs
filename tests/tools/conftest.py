"""The tools' test harness: a view over two workspaces, deps holding it, and helpers that run one tool call
through a real ``Agent`` driven by a ``FunctionModel``, so schema generation, argument validation and
``ModelRetry`` handling run as they do for a model."""

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import pytest
from pydantic_ai import Agent, Tool
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.tools import ToolDefinition

from minifs.db.pool import Connector, open_pool
from minifs.tools.deps import HasVFS, ToolSettings
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.client import VFSClient
from minifs.vfs.policy import DEFAULT_POLICY, Policy
from minifs.vfs.view import VFSView, workspace_mounts

os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")

W1, W2 = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
"""The agent's workspace (``/workspace`` in the view) and another one, which the view must not reach."""


@dataclass
class Deps:
    vfs: VFSView
    vfs_workdir: str = "/workspace"
    vfs_settings: ToolSettings = field(default_factory=ToolSettings)


@pytest.fixture(params=[False, True], ids=["uncached", "cached"])
async def client(db: str, request: pytest.FixtureRequest) -> AsyncIterator[VFSClient]:
    """A client over a tree with ``notes.md`` in the agent's workspace, ``secret.md`` in the other one and
    ``/docs/guide.md``. Every tool test runs twice: without a directory cache and with one, warmed by a stat of each
    seeded file, so the cached run resolves those paths from the cache."""
    async with open_pool(Connector(db), max_size=3) as pool:
        client = VFSClient(pool, cache=DirEntryCache(ttl=60) if request.param else None)
        await client.create(f"/workspaces/{W1}/notes.md", b"mine\n", parents=True)
        await client.create(f"/workspaces/{W2}/secret.md", b"theirs\n", parents=True)
        await client.create("/docs/guide.md", b"guide\n", parents=True)
        for path in (f"/workspaces/{W1}/notes.md", f"/workspaces/{W2}/secret.md", "/docs/guide.md"):
            await client.stat(path)
        yield client
        await client.settle()


@pytest.fixture
def view(client: VFSClient) -> VFSView:
    """The agent's view: ``/workspace`` (read and write) and ``/docs`` (read only), under a fresh policy."""
    return VFSView(client, workspace_mounts(W1), Policy(DEFAULT_POLICY.rules), "agent")


@pytest.fixture
def deps(view: VFSView) -> Deps:
    return Deps(vfs=view)


@dataclass
class Call:
    """What one tool call gave the model: its result, or the retry prompt of a ``ModelRetry`` (or of arguments
    that failed validation, as a list of errors)."""

    part: ToolReturnPart
    tool_defs: list[ToolDefinition]

    @property
    def retried(self) -> bool:
        return self.part.outcome == "retried"

    @property
    def result(self) -> Any:
        assert not self.retried, f"the tool asked for a retry: {self.part.content}"
        return self.part.content

    @property
    def retry(self) -> str:
        assert self.retried, f"the tool returned: {self.part.content}"
        assert isinstance(self.part.content, str), self.part.content  # not a validation error list
        return self.part.content


async def call_tool(tool: Tool[HasVFS], deps: HasVFS, /, **args: Any) -> Call:
    """Runs an agent whose model calls ``tool`` once with ``args``, then stops."""
    seen: list[Call] = []

    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        assert isinstance(last, ModelRequest)
        for part in last.parts:
            if isinstance(part, ToolReturnPart):
                seen.append(Call(part, info.function_tools))
                return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart(tool.name, args)])

    agent = Agent(FunctionModel(model), deps_type=type(deps), tools=[tool])
    await agent.run("go", deps=deps)
    assert len(seen) == 1
    return seen[0]


async def run_tool(tool: Tool[HasVFS], deps: HasVFS, /, **args: Any) -> Any:
    """The result of calling ``tool`` with ``args``; fails if it asked for a retry."""
    return (await call_tool(tool, deps, **args)).result


async def retry_message(tool: Tool[HasVFS], deps: HasVFS, /, **args: Any) -> str:
    """The ``ModelRetry`` message of calling ``tool`` with ``args``; fails if it returned."""
    return (await call_tool(tool, deps, **args)).retry
