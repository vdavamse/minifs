"""Glob patterns for ``vfs_glob``: the translator to anchored Postgres AREs and the pruning hints (README: Glob).

Patterns are ripgrep's ``-g`` globs, compiled by the same code: the ``minifs._regex`` extension applies the
gitignore rules of rg's ``ignore`` crate (a pattern with no ``/`` matches the file name at any depth; one with a
``/`` is anchored at the search root, and a leading ``/`` only anchors), hands the result to ``globset`` and
emits its regex as ARE. ``*`` and ``?`` stay within one path segment, ``**`` as a whole segment spans
directories, ``[...]`` is a class (``!`` or ``^`` negates), ``{a,b}`` alternates and may nest, and a backslash
makes the next character literal. Unlike rg, ``?`` and classes match one character, not one byte, and a class
never matches ``/`` (README: Glob, Divergences from rg). Paths are relative to the search root.

The translation uses no ARE classes, word boundaries or case folding, only literal characters and explicit
ranges, so a regex means the same under every database locale.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from uuid import UUID

from psycopg import AsyncConnection

from minifs import _regex


class GlobError(ValueError):
    """A glob pattern that can't be used: one globset (and so rg) rejects, with globset's message, a pattern that
    only matches directories, one rg would read as a negation or a comment, or one too large."""

    def __init__(self, pattern: str, message: str) -> None:
        super().__init__(f"error parsing glob '{pattern}': {message}")
        self.pattern = pattern
        self.message = message


@dataclass(frozen=True)
class GlobPlan:
    """What ``vfs_glob`` needs for one pattern: the regex, and the hints that let it walk less of the tree."""

    regex: str  # anchored ARE over the path relative to the search root
    prefix: tuple[str, ...] = ()  # literal leading directories: walk only that subtree
    max_depth: int | None = None  # deepest level below the prefix that can match; None when unbounded (**)
    name_like: str | None = None  # LIKE pattern every matching file name satisfies


@dataclass(frozen=True)
class Excludes:
    """Exclude patterns as ``vfs_glob`` takes them."""

    # A file is skipped when its path matches one; a directory isn't walked when its path plus a trailing '/'
    # does (an exclude ending in '/', like node_modules/, matches only directories).
    regexes: tuple[str, ...] = ()
    prune_dirs: tuple[str, ...] = ()  # names of directories never walked, at any depth (`**/<name>/**`)


@dataclass(frozen=True)
class GlobMatch:
    id: UUID
    path: str  # relative to the search root, no leading /
    size: int
    mtime: datetime


def glob_to_regex(pattern: str) -> str:
    """Translate a ripgrep-style glob to an anchored Postgres ARE over relative paths."""
    return compile_glob(pattern).regex


@lru_cache(maxsize=1024)
def compile_glob(pattern: str) -> GlobPlan:
    """The regex and the pruning hints for ``pattern``. Raises GlobError."""
    regex, prefix, max_depth, name_like, only_dir = _compile(pattern)
    if only_dir:
        raise GlobError(pattern, "a pattern ending in '/' matches only directories, and only files are listed")
    return GlobPlan(regex, tuple(prefix), max_depth, name_like)


def _compile(pattern: str) -> tuple[str, list[str], int | None, str | None, bool]:
    try:
        return _regex.compile_glob(pattern)
    except _regex.RegexError as e:
        raise GlobError(pattern, e.message) from None


def compile_excludes(patterns: Iterable[str], *, prune: bool = True) -> Excludes:
    """The regexes and pruned directory names for exclude patterns (rg's ``-g '!...'``). Raises GlobError.

    As in gitignore, a trailing ``/`` limits a pattern to directories and doesn't anchor it: ``build/`` skips
    every directory named build. With ``prune``, ``**/<name>/**`` and ``<name>/`` with a literal name become
    pruned directory names instead of regexes, so the walk never enters those directories. The result is the
    same either way.
    """
    if isinstance(patterns, str):
        raise TypeError("excludes must be a list of patterns, not a str")
    regexes: list[str] = []
    prune_dirs: list[str] = []
    for pattern in dict.fromkeys(patterns):
        name = _pruned_dir_name(pattern) if prune else None
        if name is not None:
            prune_dirs.append(name)
            continue
        regex, _, _, _, only_dir = _compile(pattern)
        # Directories are tested as "path/", and files never end in '/'. The lookbehind keeps a match of "path/"
        # itself (from an alternative ending in '/', like {a/,b}) from excluding the directory "path".
        regexes.append(regex.removesuffix("$") + ("/$" if only_dir else "(?<!/)/?$"))
    return Excludes(tuple(regexes), tuple(prune_dirs))


async def glob(
    conn: AsyncConnection,
    root: UUID | None,
    pattern: str,
    *,
    excludes: Sequence[str] = (),
    hidden: bool = False,
    limit: int | None = None,
    hints: bool = True,
) -> list[GlobMatch]:
    """The files below directory ``root`` (None: ``/``) that match ``pattern``, newest first, as ``vfs_glob``
    returns them. ``.git`` directories are never walked. Without ``hints`` it walks the whole tree (for tests).

    Raises GlobError for a bad pattern, and ENOENT / ENOTDIR (see ``minifs.db.errors``) for a bad ``root``.
    """
    if limit is not None and limit < 0:
        raise ValueError(f"limit must not be negative, got {limit}")
    plan = compile_glob(pattern)
    ex = compile_excludes(excludes, prune=hints)  # before the query, so that a str is refused
    if not hints:
        plan = GlobPlan(plan.regex)
    cur = await conn.execute(
        "SELECT id, path, size, mtime FROM vfs_glob(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            root,
            plan.regex,
            plan.name_like,
            plan.max_depth,
            list(ex.regexes),
            hidden,
            limit,
            list(plan.prefix),
            list(ex.prune_dirs),
        ),
    )
    return [GlobMatch(*row) for row in await cur.fetchall()]


def _pruned_dir_name(pattern: str) -> str | None:
    """``name`` for an exclude ``**/name/**`` or ``name/`` with a literal name, else None."""
    if pattern.startswith("**/") and pattern.endswith("/**"):
        name = pattern[3:-3]
    elif pattern.endswith("/"):
        name = pattern[:-1]
    else:
        return None
    if not name or any(c in name for c in "/\\*?[]{}") or name in (".", "..") or name[0] in "!#":
        return None
    return name
