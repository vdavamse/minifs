"""Latency budgets: how long an operation, or a whole tool call, may spend in the database (README: Protecting the
database).

Every transaction ``VFSClient`` runs sets ``statement_timeout`` (``SET LOCAL``, so it ends with the transaction and
nothing outlives it on a pooled connection) to the client's ``timeout``, or to what is left of the current
``budget`` if that is less. A budget is a deadline for everything run inside it, in this task and the tasks it
starts: the tools run each call under their settings' ``budget``.

    with budget(10.0):
        await view.pread_lines(path)   # each statement gets at most what's left of the 10 s
        await view.stat(other)

A statement cancelled by the timeout, a budget used up before a statement starts, and a pool that hands out no
connection in time all raise ``VFSError("ETIMEDOUT")`` (counted in ``minifs.timeouts``), except a grep statement
cancelled by its timeout, which grep reports as ``SearchRefused("timeout")``.
"""

from __future__ import annotations

import contextlib
import contextvars
import math
import time
from collections.abc import Generator
from typing import Any, Literal

from psycopg import AsyncConnection

from minifs import metrics
from minifs.vfs.core import VFSError

DEFAULT_TIMEOUT = 30.0
"""Seconds any one statement of ``VFSClient`` may run by default: a backstop well above any healthy operation."""

MAX_TIMEOUT = (2**31 - 1) / 1000
"""The largest timeout in seconds: ``statement_timeout`` is an int of milliseconds."""

_deadline: contextvars.ContextVar[float | None] = contextvars.ContextVar("minifs_deadline", default=None)


@contextlib.contextmanager
def budget(seconds: float | None) -> Generator[None]:
    """Runs the block with a deadline ``seconds`` from now (None: no budget of its own). A budget inside another
    never extends it: the earlier deadline holds."""
    if seconds is None:
        yield
        return
    if not 0 < seconds <= MAX_TIMEOUT:
        raise ValueError(f"budget must be over 0 and at most {MAX_TIMEOUT} seconds, not {seconds!r}")
    deadline = time.monotonic() + seconds
    outer = _deadline.get()
    token = _deadline.set(deadline if outer is None else min(outer, deadline))
    try:
        yield
    finally:
        _deadline.reset(token)


def remaining(timeout: float | None, path: str) -> float | None:
    """The seconds the next statement may run: ``timeout``, or what is left of the budget if less (None: no
    limit). Raises ETIMEDOUT for ``path`` if the budget is used up."""
    deadline = _deadline.get()
    if deadline is None:
        return timeout
    left = deadline - time.monotonic()
    if left <= 0:
        raise timed_out(path, "budget")
    return left if timeout is None else min(timeout, left)


async def set_timeout(conn: AsyncConnection[Any], seconds: float | None) -> None:
    """``SET LOCAL statement_timeout`` to ``seconds`` (rounded up to a millisecond) for the rest of the current
    transaction; nothing for None."""
    if seconds is not None:
        await conn.execute("SELECT set_config('statement_timeout', %s, true)", (f"{_milliseconds(seconds)}ms",))


def begin(seconds: float | None, *, repeatable: bool = False, joining: bool = False) -> str:
    """The simple query that starts a transaction (``repeatable``: REPEATABLE READ) with ``statement_timeout`` at
    ``seconds`` (None: the session's, which a role or database setting may give). ``joining``: only the timeout,
    for a transaction already open; empty if there is nothing to set."""
    statements = [] if joining else ["BEGIN ISOLATION LEVEL REPEATABLE READ" if repeatable else "BEGIN"]
    if seconds is not None:
        statements.append(f"SET LOCAL statement_timeout = {_milliseconds(seconds)}")
    return "; ".join(statements)


def _milliseconds(seconds: float) -> int:
    return min(2**31 - 1, max(1, math.ceil(seconds * 1000)))


def timed_out(path: str, reason: Literal["statement", "budget", "pool"]) -> VFSError:
    """The ETIMEDOUT error for ``path``, counted in ``minifs.timeouts``."""
    metrics.TIMEOUTS.add(1, {"minifs.reason": reason})
    messages = {
        "statement": "statement timeout",
        "budget": "latency budget used up",
        "pool": "no database connection in time",
    }
    return VFSError("ETIMEDOUT", path, messages[reason])
