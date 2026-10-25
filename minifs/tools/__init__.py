"""pydantic-ai tools over a ``VFSView``, with ladderframe's (opencode's) contracts: ``Read``, ``Write``, ``Edit``,
``Glob`` and ``Grep``. They need the ``pydantic-ai`` extra (``pip install 'minifs[pydantic-ai]'``).

    @dataclass
    class Deps:
        vfs: VFSView

    agent = Agent(model, deps_type=Deps, tools=[Read, Write, Edit, Glob, Grep])  # or toolsets=[toolset()]
    await agent.run("fix the failing test", deps=Deps(vfs=view))

The deps protocol, settings and path resolution are in ``minifs.tools.deps``, the error mapping in
``minifs.tools.errors``.
"""

from __future__ import annotations

try:
    from pydantic_ai import Tool
    from pydantic_ai.toolsets import FunctionToolset
except ImportError as e:  # pragma: no cover - exercised by test_import_without_pydantic_ai
    raise ImportError("minifs.tools needs the pydantic-ai extra: pip install 'minifs[pydantic-ai]'") from e

from minifs.tools.deps import HasVFS, ToolSettings
from minifs.tools.edit import Edit
from minifs.tools.glob import Glob
from minifs.tools.grep import Grep
from minifs.tools.read import Read
from minifs.tools.write import Write

TOOLS: tuple[Tool[HasVFS], ...] = (Read, Write, Edit, Glob, Grep)
"""Every tool, in the order the model sees them."""


def toolset() -> FunctionToolset[HasVFS]:
    """A toolset of every tool, for ``Agent(..., toolsets=[toolset()])``."""
    return FunctionToolset(list(TOOLS))


__all__ = ["TOOLS", "Edit", "Glob", "Grep", "HasVFS", "Read", "ToolSettings", "Write", "toolset"]
