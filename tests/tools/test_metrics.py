"""The OpenTelemetry metrics (minifs.metrics) the tools and the VFS record, read back through an SDK reader, and
the per-tool latency budget."""

import subprocess
import sys
from dataclasses import replace

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from minifs import metrics
from minifs.tools import Glob, Grep, Read, RunMetrics, Write
from minifs.tools.deps import GrepSettings, ReadSettings, ToolSettings
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.client import VFSClient
from minifs.vfs.search import SearchRefused
from tests.otel import histogram_sum, total
from tests.tools.conftest import W1, Deps, call_tool, retry_message, run_tool


async def test_tool_calls_are_timed_by_name_and_outcome(deps: Deps) -> None:
    def calls(outcome: str) -> float:
        return total("minifs.tool.duration", {"gen_ai.tool.name": "Read", "minifs.outcome": outcome})

    ok, retry = calls("ok"), calls("retry")
    denied = total("minifs.tool.duration", {"gen_ai.tool.name": "Write", "minifs.outcome": "denied"})
    await run_tool(Read, deps, filePath="/workspace/notes.md")
    await retry_message(Read, deps, filePath="/workspace/missing.md")
    assert (await run_tool(Write, deps, filePath="/docs/new.md", content="x")).startswith("Permission denied")
    assert calls("ok") == ok + 1 and calls("retry") == retry + 1
    assert total("minifs.tool.duration", {"gen_ai.tool.name": "Write", "minifs.outcome": "denied"}) == denied + 1


async def test_run_metrics_counts_the_minifs_tool_calls_of_a_run(deps: Deps) -> None:
    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(
                parts=[
                    ToolCallPart("Read", {"filePath": "/workspace/notes.md"}),
                    ToolCallPart("Glob", {"pattern": "*.md"}),
                    ToolCallPart("other", {}),
                ]
            )
        return ModelResponse(parts=[TextPart("done")])

    agent = Agent(FunctionModel(model), deps_type=Deps, tools=[Read, Glob], capabilities=[RunMetrics()])

    @agent.tool_plain
    def other() -> str:  # pyright: ignore[reportUnusedFunction]
        return "not ours"

    runs, calls = total("minifs.run.tool_calls"), histogram_sum("minifs.run.tool_calls")
    await agent.run("go", deps=deps)
    assert total("minifs.run.tool_calls") == runs + 1
    assert histogram_sum("minifs.run.tool_calls") == calls + 2


async def test_policy_denials_are_exported(deps: Deps) -> None:
    attributes = {"minifs.actor": "agent", "minifs.action": "write"}
    before = total("minifs.policy.denials", attributes)
    await run_tool(Write, deps, filePath="/docs/new.md", content="x")
    assert total("minifs.policy.denials", attributes) == before + 1
    assert deps.vfs.policy.denials["agent", "write"] == 1


async def test_regex_rejections_are_counted_by_construct(deps: Deps) -> None:
    def rejections(construct: str) -> float:
        return total("minifs.regex.rejections", {"minifs.construct": construct})

    look, back = rejections("look_around"), rejections("backreference")
    await retry_message(Grep, deps, pattern="mine(?=x)")
    await retry_message(Grep, deps, pattern=r"(m)\1")
    assert rejections("look_around") == look + 1 and rejections("backreference") == back + 1


async def test_searches_record_duration_bytes_and_refusals(deps: Deps) -> None:
    scans, scanned = total("minifs.search.duration", {"minifs.outcome": "ok"}), histogram_sum("minifs.search.bytes")
    await run_tool(Grep, deps, pattern="mine", path="/workspace")
    assert total("minifs.search.duration", {"minifs.outcome": "ok"}) == scans + 1
    assert histogram_sum("minifs.search.bytes") == scanned + len(b"mine\n")

    refused = total("minifs.search.refusals", {"minifs.reason": "bytes"})
    with pytest.raises(SearchRefused):
        await deps.vfs.grep("mine", "/workspace", max_bytes=1)
    assert total("minifs.search.refusals", {"minifs.reason": "bytes"}) == refused + 1


async def test_reads_record_their_bytes(deps: Deps) -> None:
    def read(op: str) -> float:
        return histogram_sum("minifs.read.bytes", {"minifs.op": op})

    lines, pread = read("pread_lines"), read("pread")
    await deps.vfs.pread_lines("/workspace/notes.md")
    await deps.vfs.pread("/workspace/notes.md", 0, 2)
    assert read("pread_lines") == lines + len(b"mine\n") and read("pread") == pread + 2


async def test_cache_lookups_and_hits_are_observed(client: VFSClient) -> None:
    cache = DirEntryCache(ttl=60)
    metrics.observe_cache(cache)
    lookups, hits = total("minifs.cache.lookups"), total("minifs.cache.hits")
    cached = VFSClient(client.pool, cache=cache)
    await cached.stat(f"/workspaces/{W1}/notes.md")  # cold: misses, then stores the walk
    await cached.stat(f"/workspaces/{W1}/notes.md")  # warm: a hit per component
    stats = cache.stats()
    assert total("minifs.cache.lookups") - lookups == stats.lookups
    assert total("minifs.cache.hits") - hits == stats.hits > 0


async def test_a_spent_budget_is_a_timeout_retry(deps: Deps) -> None:
    timeouts = total("minifs.timeouts", {"minifs.reason": "budget"})
    slow = replace(deps, vfs_settings=ToolSettings(read=ReadSettings(budget=1e-9)))
    message = await retry_message(Read, slow, filePath="/workspace/notes.md")
    assert message.startswith("Timed out on /workspace/notes.md")
    assert total("minifs.timeouts", {"minifs.reason": "budget"}) > timeouts


async def test_budgets_are_validated() -> None:
    for bad in (0, -1.0, True):
        for settings in (ReadSettings, GrepSettings):
            try:
                settings(budget=bad)
            except ValueError:
                continue
            raise AssertionError(f"{settings.__name__}(budget={bad!r}) was accepted")
    assert ReadSettings(budget=None).budget is None


async def test_unlimited_budget_reads(deps: Deps) -> None:
    free = replace(deps, vfs_settings=ToolSettings(read=ReadSettings(budget=None)))
    assert "mine" in await run_tool(Read, free, filePath="/workspace/notes.md")
    assert not (await call_tool(Read, free, filePath="/workspace/notes.md")).retried


def test_metrics_are_no_ops_without_opentelemetry() -> None:
    code = (
        "import sys; sys.modules['opentelemetry'] = None\n"
        "import minifs.metrics as m\n"
        "assert not m.AVAILABLE\n"
        "m.TOOL_DURATION.record(1.0, {'gen_ai.tool.name': 'Read'}); m.POLICY_DENIALS.add(1)\n"
        "import minifs.vfs.service  # everything but the tools, which need pydantic-ai, which needs opentelemetry\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
