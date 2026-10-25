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

from minifs.vfs.client import MAX_TIMEOUT
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
"""Skipped by default, in place of rg's ``.gitignore`` handling: unambiguous caches only (docs/design.md: The agent
tools in detail). Patterns are rg's ``-g '!...'`` excludes, matched relative to the search root."""


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
    excludes: tuple[str, ...] = DEFAULT_EXCLUDES
    """Skipped as well as ``ALWAYS_EXCLUDED``, which a caller can't turn off. A grep from a virtual directory
    (``/``) restates each exclude in every mount, as a glob does, so avoid the forms ``GlobSettings.excludes``
    lists (``**/workspace/**``, say): they make every such grep EINVAL, which the model sees as a retry naming a
    pattern it never wrote."""

    def __post_init__(self) -> None:
        """A bad setting is the caller's mistake, so it fails here, not as a retry the model can't fix."""
        for name in ("limit", "max_line_chars"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"grep {name} must be an int, not {value!r}")
            if not 0 <= value <= MAX_GLOB_LIMIT:
                raise ValueError(f"grep {name} must be 0 to {MAX_GLOB_LIMIT}, not {value}")
        if isinstance(self.timeout, bool) or not 0 < self.timeout <= MAX_TIMEOUT:
            raise ValueError(f"grep timeout must be over 0 and at most {MAX_TIMEOUT} seconds, not {self.timeout!r}")
        compile_excludes(self.excludes)  # GlobError


@dataclass(frozen=True)
class EditSettings:
    max_bytes: int = 10 * 1024 * 1024
    """Largest file ``Edit`` takes, since it holds the whole file in memory; ``Write`` replaces larger ones."""

    def __post_init__(self) -> None:
        if isinstance(self.max_bytes, bool) or not isinstance(self.max_bytes, int):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(f"edit max_bytes must be an int, not {self.max_bytes!r}")
        if self.max_bytes < 0:
            raise ValueError(f"edit max_bytes must be at least 0, not {self.max_bytes}")


@dataclass(frozen=True)
class WriteSettings:
    max_diff_bytes: int = 1024 * 1024
    """Largest existing file ``Write`` reads to diff against its new text. A larger one is overwritten without
    reading it (ladderframe has no such limit), and the metadata has no diff. Only the old file counts: new content
    of any size is diffed against an old file under the cap."""

    def __post_init__(self) -> None:
        if isinstance(self.max_diff_bytes, bool) or not isinstance(self.max_diff_bytes, int):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(f"write max_diff_bytes must be an int, not {self.max_diff_bytes!r}")
        if self.max_diff_bytes < 0:
            raise ValueError(f"write max_diff_bytes must be at least 0, not {self.max_diff_bytes}")


@dataclass(frozen=True)
class ToolSettings:
    """The tools' limits, one object per tool: ladderframe's defaults, and minifs's own caps for ``Edit`` and
    ``Write``. Set ``vfs_settings`` on the deps to change them."""

    read: ReadSettings = field(default_factory=ReadSettings)
    glob: GlobSettings = field(default_factory=GlobSettings)
    grep: GrepSettings = field(default_factory=GrepSettings)
    edit: EditSettings = field(default_factory=EditSettings)
    write: WriteSettings = field(default_factory=WriteSettings)


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
