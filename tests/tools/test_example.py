"""The runnable example (``examples/agent.py``): its scripted run fixes the seeded workspace through all five tools,
with the directory cache on and following a Watcher, and the README's snippet is the example's own code."""

import logging
import re
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pydantic_ai.messages import ModelRequest, ModelResponse, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel

from examples import agent as example
from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import VFSClient

ROOT = Path(__file__).parents[2]


async def test_scripted_run_fixes_the_workspace(db: str) -> None:
    workspace_id = uuid4()
    async with example.open_client(Connector(db)) as (client, cache):
        await example.seed(client, workspace_id)
        result = await example.run_task(client, workspace_id, example.PROMPT, FunctionModel(example.scripted))
        stats = cache.stats()

    assert result.output == example.ANSWER
    calls = [p for m in result.all_messages() if isinstance(m, ModelResponse) for p in m.parts]
    assert [(p.tool_name, p.args) for p in calls if isinstance(p, ToolCallPart)] == example.SCRIPT
    returns = [p for m in result.all_messages() if isinstance(m, ModelRequest) for p in m.parts]
    returns = [p for p in returns if isinstance(p, ToolReturnPart)]
    assert [p.outcome for p in returns] == ["success"] * len(example.SCRIPT), [p.content for p in returns]
    glob, read, grep, edit, write, read_back = (p.content for p in returns)
    assert set(str(glob).splitlines()) == {"/workspace/calc.py", "/workspace/test_calc.py"}
    assert "2:     return a - b" in str(read)
    assert "/workspace/calc.py:" in str(grep) and "Line 2: " in str(grep)
    assert str(edit).startswith("Edit applied successfully.")
    assert str(write).startswith("Wrote file successfully.")
    assert "1: - Fix add(), which subtracted." in str(read_back)

    # The tree in Postgres, read back by a client of its own, without a cache.
    async with open_pool(Connector(db), max_size=1) as pool:
        files = await example.workspace_files(VFSClient(pool), workspace_id)
    assert files == {
        **example.FILES,
        "calc.py": example.FILES["calc.py"].replace("return a - b", "return a + b"),
        "CHANGES.md": "- Fix add(), which subtracted.\n",
    }

    # After the first lookup of a path, the agent's later calls in the same directory resolve from the cache,
    # though the scripted run never lists a directory.
    assert stats.hits > 0, stats


async def test_main_runs_offline(
    db: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("MINIFS_MODEL", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with caplog.at_level(logging.INFO, logger=example.__name__):
        assert await example.main(db) == example.ANSWER
    out = capsys.readouterr().out
    assert "--- /workspace/calc.py\n" in out and "return a + b" in out and "--- /workspace/CHANGES.md\n" in out
    [record] = [r for r in caplog.records if r.name == example.__name__]
    assert record.getMessage().startswith("directory cache: CacheStats(")


def test_choose_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINIFS_MODEL", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert isinstance(example.choose_model(), FunctionModel)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "key")
    assert example.choose_model() == "anthropic:claude-sonnet-5-5"
    monkeypatch.setenv("MINIFS_MODEL", "openai:some-model")
    assert example.choose_model() == "openai:some-model"


def readme_snippet() -> str:
    readme = (ROOT / "README.md").read_text()
    [snippet] = re.findall(r"<!-- examples/agent\.py -->\n```python\n(.*?)```\n", readme, re.DOTALL)
    return snippet


def test_readme_snippet_is_the_example() -> None:
    """Below its imports, the README's service snippet is the example's code, verbatim."""
    _imports, body = readme_snippet().split("\n\n\n", 1)
    assert body in (ROOT / "examples" / "agent.py").read_text()


async def test_readme_snippet_runs(db: str) -> None:
    """The snippet, imports and all, runs as pasted: a task through its ``open_client`` and ``run_task``."""
    namespace: dict[str, Any] = {"__name__": "readme_snippet"}
    exec(compile(readme_snippet(), "README.md", "exec"), namespace)
    workspace_id = uuid4()
    async with namespace["open_client"](Connector(db)) as (client, cache):
        await example.seed(client, workspace_id)
        result = await namespace["run_task"](client, workspace_id, example.PROMPT, FunctionModel(example.scripted))
        assert result.output == example.ANSWER
        assert "return a + b" in (await example.workspace_files(client, workspace_id))["calc.py"]
        assert cache.stats().hits > 0
