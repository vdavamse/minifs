"""The examples: ``provision.py`` seeds a workspace, and ``agent.py``'s ``main`` fixes it through all five tools,
driven by a scripted ``FunctionModel``, with the directory cache on and following a Watcher. The README's snippet is
``agent.py``'s code and runs as pasted."""

import logging
import re
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.models.test import TestModel

from examples import agent as example
from examples import provision
from minifs.db.pool import Connector
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.client import VFSClient
from minifs.vfs.service import open_vfs

ROOT = Path(__file__).parents[2]

PROMPT = "test_calc.py fails. Fix the bug, then add a line about the fix to CHANGES.md."

SCRIPT: list[tuple[str, dict[str, object]]] = [
    ("Glob", {"pattern": "**/*.py"}),
    ("Read", {"filePath": "/workspace/calc.py"}),
    ("Grep", {"pattern": r"return a - b", "include": "*.py"}),
    ("Edit", {"filePath": "/workspace/calc.py", "oldString": "return a - b", "newString": "return a + b"}),
    ("Write", {"filePath": "/workspace/CHANGES.md", "content": "- Fix add(), which subtracted.\n"}),
    ("Read", {"filePath": "/workspace/CHANGES.md"}),
]
"""The scripted model's tool calls, one per response."""

ANSWER = "Fixed add() in /workspace/calc.py and noted it in /workspace/CHANGES.md."


class Scripted:
    """A model that makes ``SCRIPT``'s calls in order, whatever they return, then answers; it keeps what each call
    returned."""

    def __init__(self) -> None:
        self.returns: list[ToolReturnPart] = []
        self.calls: list[ToolCallPart] = []

    def __call__(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        assert isinstance(last, ModelRequest)
        self.returns += [p for p in last.parts if isinstance(p, ToolReturnPart)]
        step = sum(isinstance(m, ModelResponse) for m in messages)
        if step < len(SCRIPT):
            name, args = SCRIPT[step]
            self.calls.append(ToolCallPart(name, args))
            return ModelResponse(parts=[self.calls[-1]])
        return ModelResponse(parts=[TextPart(ANSWER)])

    async def stream(self, messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str | DeltaToolCalls]:
        """The same response, streamed: the example's ``show`` handler makes every run a streamed one."""
        for part in self(messages, info).parts:
            if isinstance(part, TextPart):
                yield part.content
            else:
                assert isinstance(part, ToolCallPart)
                yield {0: DeltaToolCall(name=part.tool_name, json_args=part.args_as_json_str())}

    def model(self) -> FunctionModel:
        return FunctionModel(self, stream_function=self.stream)


async def workspace_files(db: str, workspace_id: UUID) -> dict[str, str]:
    """Every file of the workspace in Postgres, read by a client of its own without a cache."""
    root = f"/workspaces/{workspace_id}"
    async with open_vfs(Connector(db), min_size=0) as client:
        assert client.cache is None
        return {
            entry.path.removeprefix(root + "/"): (await client.pread(entry.path, 0, entry.size)).content.decode()
            for entry in await client.glob("**", root, hidden=True)
        }


FIXED = {
    **provision.FILES,
    "calc.py": provision.FILES["calc.py"].replace("return a - b", "return a + b"),
    "CHANGES.md": "- Fix add(), which subtracted.\n",
}


@pytest.fixture
def caches(monkeypatch: pytest.MonkeyPatch) -> list[DirEntryCache]:
    """The caches the example opens its client with."""
    seen: list[DirEntryCache] = []

    @asynccontextmanager
    async def spy(connector: Connector, **kwargs: Any) -> AsyncGenerator[VFSClient]:
        seen.append(kwargs["cache"])
        async with open_vfs(connector, **kwargs) as client:
            yield client

    monkeypatch.setattr(example, "open_vfs", spy)
    return seen


async def test_scripted_run_fixes_the_workspace(db: str, caches: list[DirEntryCache]) -> None:
    workspace_id = await provision.provision(db)
    model = Scripted()
    assert await example.main(db, str(workspace_id), PROMPT, model.model()) == ANSWER

    assert [(p.tool_name, p.args) for p in model.calls] == SCRIPT
    assert [p.outcome for p in model.returns] == ["success"] * len(SCRIPT), [p.content for p in model.returns]
    glob, read, grep, edit, write, read_back = (str(p.content) for p in model.returns)
    assert set(glob.splitlines()) == {"/workspace/calc.py", "/workspace/test_calc.py"}
    assert "2:     return a - b" in read
    assert "/workspace/calc.py:" in grep and "Line 2: " in grep
    assert edit.startswith("Edit applied successfully.")
    assert write.startswith("Wrote file successfully.")
    assert "1: - Fix add(), which subtracted." in read_back
    assert await workspace_files(db, workspace_id) == FIXED

    # The cache followed a Watcher (entering follow() clears it once), and the agent's later calls resolved shared
    # path components from it, though the run never lists a directory. The run takes well under the 2 s TTL.
    [cache] = caches
    stats = cache.stats()
    assert stats.clears == 1, stats
    assert stats.expired == 0, f"the run outlasted the TTL: {stats}"
    assert stats.hits > 0, stats


async def test_main_runs_offline_on_test_model(
    db: str, caches: list[DirEntryCache], caplog: pytest.LogCaptureFixture
) -> None:
    """The example's offline default: TestModel calls Glob and Grep with made-up arguments, and nothing is written."""
    workspace_id = await provision.provision(db)
    with caplog.at_level(logging.INFO):
        output = await example.main(db, str(workspace_id), "Hi", TestModel(call_tools=["Glob", "Grep"]))
    assert isinstance(output, str)
    assert any(r.getMessage().startswith("directory cache: CacheStats(") for r in caplog.records)
    assert await workspace_files(db, workspace_id) == provision.FILES


def readme_snippet() -> str:
    readme = (ROOT / "README.md").read_text()
    [snippet] = re.findall(r"<!-- examples/agent\.py -->\n```python\n(.*?)```\n", readme, re.DOTALL)
    return snippet


def test_readme_snippet_is_the_example() -> None:
    """Below its imports, the README's snippet is the example's code, verbatim."""
    _imports, body = readme_snippet().split("\n\n\n", 1)
    assert body in (ROOT / "examples" / "agent.py").read_text()


async def test_readme_snippet_runs(db: str) -> None:
    """The snippet, imports and all, runs as pasted."""
    namespace: dict[str, Any] = {"__name__": "readme_snippet"}
    exec(compile(readme_snippet(), "README.md", "exec"), namespace)
    workspace_id = await provision.provision(db)
    assert await namespace["main"](db, str(workspace_id), PROMPT, Scripted().model()) == ANSWER
    assert await workspace_files(db, workspace_id) == FIXED
