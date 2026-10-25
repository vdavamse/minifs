"""What the tools need from the agent's deps: a ``vfs`` (``HasVFS``), and optionally a working directory and
settings, which they read with ``getattr`` and a default, so the caller's own deps dataclass only needs ``vfs``.

    @dataclass
    class Deps:
        vfs: VFSView
        vfs_workdir: str = "/workspace"           # optional: where relative paths resolve
        vfs_settings: ToolSettings = ToolSettings()  # optional: the tools' limits
"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass, field
from typing import Protocol

from pydantic_ai import RunContext

from minifs.vfs.glob import compile_excludes
from minifs.vfs.view import MAX_GLOB_LIMIT, VFSView

DEFAULT_WORKDIR = "/workspace"


class HasVFS(Protocol):
    """The deps the tools take (``RunContext[HasVFS]``): anything with a ``vfs`` attribute, frozen or not."""

    @property
    def vfs(self) -> VFSView: ...


@dataclass(frozen=True)
class ReadSettings:
    default_limit: int = 2000
    """Lines returned when the call gives no ``limit``."""
    max_line_length: int = 2000
    """Characters kept of each line; longer lines are cut."""
    max_bytes: int = 50 * 1024
    """Output cap per call."""


ALWAYS_EXCLUDED: tuple[str, ...] = ("**/.git/**",)
"""Skipped by every search whatever the settings say, as ladderframe hard-codes ``-g '!**/.git/**'``."""

DEFAULT_EXCLUDES: tuple[str, ...] = ("**/node_modules/**", "**/.venv/**", "**/__pycache__/**", "**/.mypy_cache/**")
"""Skipped by default, in place of rg's ``.gitignore`` handling: unambiguous caches only (README: Differences
from ladderframe). Patterns are rg's ``-g '!...'`` excludes, matched relative to the search root."""


@dataclass(frozen=True)
class GlobSettings:
    limit: int = 100
    """Paths returned at most."""
    excludes: tuple[str, ...] = DEFAULT_EXCLUDES
    """Skipped as well as ``ALWAYS_EXCLUDED``, which a caller can't turn off. A glob from a virtual directory
    (``/``) restates each exclude in every mount, so avoid the forms ``VFSView.glob`` can't restate there: a
    ``**`` segment followed by one that can match a directory at or above a mount point (``**/w*/`` or
    ``**/workspace/**``, though not ``**/*.pyc``), a brace or class with ``/``, an escaped ``/``, a trailing
    ``//``. They make every such glob EINVAL, which the model sees as a retry naming a pattern it never wrote."""

    def __post_init__(self) -> None:
        """A bad limit or exclude is the caller's mistake, so it fails here, not as a retry the model can't fix."""
        if isinstance(self.limit, bool) or not isinstance(self.limit, int):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(f"glob limit must be an int, not {self.limit!r}")
        if not 0 <= self.limit <= MAX_GLOB_LIMIT:
            raise ValueError(f"glob limit must be 0 to {MAX_GLOB_LIMIT}, not {self.limit}")
        compile_excludes(self.excludes)  # GlobError


@dataclass(frozen=True)
class GrepSettings:
    limit: int = 100
    """Hits returned at most."""
    max_line_chars: int = 2000
    """Characters kept of each hit line; longer lines are cut."""
    timeout: float = 2.0
    """Seconds each of a search's statements may run."""


@dataclass(frozen=True)
class ToolSettings:
    """The tools' limits, one object per tool, with ladderframe's defaults. Set ``vfs_settings`` on the deps to
    change them."""

    read: ReadSettings = field(default_factory=ReadSettings)
    glob: GlobSettings = field(default_factory=GlobSettings)
    grep: GrepSettings = field(default_factory=GrepSettings)


DEFAULT_SETTINGS = ToolSettings()


def workdir(ctx: RunContext[HasVFS]) -> str:
    """The deps' ``vfs_workdir``, ``/workspace`` by default (or if it isn't a string), anchored at ``/``."""
    value = getattr(ctx.deps, "vfs_workdir", DEFAULT_WORKDIR)
    return "/" + value.lstrip("/") if isinstance(value, str) else DEFAULT_WORKDIR


def settings(ctx: RunContext[HasVFS]) -> ToolSettings:
    """The deps' ``vfs_settings``, ladderframe's defaults by default (or if it isn't a ``ToolSettings``)."""
    value = getattr(ctx.deps, "vfs_settings", DEFAULT_SETTINGS)
    return value if isinstance(value, ToolSettings) else DEFAULT_SETTINGS


def resolve(ctx: RunContext[HasVFS], path: str) -> str:
    """``path`` as the absolute visible path the view takes: resolved against ``workdir(ctx)`` when relative, with
    ``.``, ``..`` and repeated slashes removed, as ladderframe's ``resolve_path`` (but no ``~``). ``..`` stops at
    ``/``; the view still checks every path against its mounts."""
    joined = posixpath.normpath(posixpath.join(workdir(ctx), path))
    return "/" + joined.lstrip("/") if joined.startswith("//") else joined
