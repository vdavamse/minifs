"""How a tool reports a VFS error to the model, as ladderframe's tools do: a denial is the tool's result (retrying
won't help, so the model reads it and moves on), anything else a ``ModelRetry`` that says what to change.

Every tool catches at its boundary and hands the error over:

    try:
        result = await ctx.deps.vfs.pread_lines(path, ...)
    except (VFSError, SearchRefused) as e:
        return tool_error(e, "read")

A tool overrides a message where its ladderframe counterpart words it differently, with ``{path}`` for the path:
``tool_error(e, "read", ENOENT="No such directory: {path}")``.
"""

from __future__ import annotations

from pydantic_ai import ModelRetry

from minifs.vfs.core import VFSError
from minifs.vfs.policy import Action, PolicyDenied
from minifs.vfs.search import SearchRefused

MESSAGES: dict[str, str] = {
    "ENOENT": "File not found: {path}",
    "ENOTDIR": "File not found: {path}",  # a component is a file: on disk, ladderframe finds nothing there either
    "EISDIR": "Path is a directory, not a file: {path}",
    "EEXIST": "File already exists: {path}",
    "ENOTEMPTY": "Directory not empty: {path}",
    "EBUSY": "Path is a mount point and can't be moved or removed: {path}",
    "ESTALE": "{path} was modified since it was read. Read it again before writing.",
    "ENAMETOOLONG": "File name too long: {path}",
    "EIO": "Internal error reading {path}",
}
"""The ``ModelRetry`` message for each errno. EINVAL, and any errno not here, gets ``<path>: <the error's message>``
(the view's messages name no real paths), or ``<errno>: <path>`` without one."""

_OVERRIDABLE = frozenset(MESSAGES) | {"EINVAL"}


def denied(action: Action, path: str) -> str:
    """The result of a call the policy refused."""
    return f"Permission denied: {action} {path}"


def tool_error(error: VFSError | SearchRefused, action: Action, **messages: str) -> str:
    """What the tool returns for ``error``: the denial for EACCES, naming the action the policy refused (``action``,
    what the call needs, for an EACCES that doesn't say); otherwise raises ``ModelRetry``. ``messages`` override
    ``MESSAGES`` by errno. A ``SearchRefused``'s text is already written for the model."""
    unknown = messages.keys() - _OVERRIDABLE
    assert not unknown, f"no such errno to override: {sorted(unknown)}"
    if isinstance(error, SearchRefused):
        raise ModelRetry(str(error)) from None
    if error.errno == "EACCES":
        return denied(error.action if isinstance(error, PolicyDenied) else action, error.path)
    template = messages.get(error.errno) or MESSAGES.get(error.errno)
    if template is not None:
        raise ModelRetry(template.format(path=error.path)) from None
    raise ModelRetry(f"{error.path}: {error.message}" if error.message else f"{error.errno}: {error.path}") from None
