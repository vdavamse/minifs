"""Provisions a demo workspace for ``examples/agent.py``: migrates the database, then writes a small Python project
whose ``add`` subtracts into a new workspace, and prints the workspace's id.

    MINIFS_DSN=postgresql://... uv run python examples/provision.py

Seeding is ingestion's job, not the agent's, so it writes through the client, not an agent view. It is one-shot, so
it opens no directory cache (README: Using the tools with pydantic-ai).
"""

from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

from minifs.db.migrate import migrate
from minifs.db.pool import Connector
from minifs.vfs.service import open_vfs

FILES = {
    "calc.py": "def add(a, b):\n    return a - b\n\n\ndef mul(a, b):\n    return a * b\n",
    "test_calc.py": "from calc import add, mul\n\n\ndef test_add():\n    assert add(2, 3) == 5\n\n\ndef test_mul():\n"
    "    assert mul(2, 3) == 6\n",
    "README.md": "# calc\n\nTiny arithmetic helpers.\n",
}
"""The demo workspace, by path relative to it."""


async def provision(dsn: str) -> UUID:
    connector = Connector(dsn)
    async with await connector() as conn:
        await conn.set_autocommit(True)
        await migrate(conn)
    workspace_id = uuid4()
    async with open_vfs(connector, min_size=0) as vfs:
        for name, text in FILES.items():
            await vfs.client.create(f"/workspaces/{workspace_id}/{name}", text.encode(), parents=True)
    return workspace_id


if __name__ == "__main__":
    print(asyncio.run(provision(os.environ.get("MINIFS_DSN", ""))))
