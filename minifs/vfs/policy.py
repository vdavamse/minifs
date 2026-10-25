"""The write policy: what each actor may do where (README: *Workspaces and permissions*).

Each actor has an ordered list of path rules, like mount options; the first rule whose pattern matches a path
decides, so specific rules go before general ones. A rule grants any of ``read``, ``write`` and ``append``, and
``write`` implies ``append`` (replacing a file is the stronger right). A rule granting nothing denies everything.
A path no rule matches may be read but not written. Rules match the paths of the agent's view (the chroot,
``minifs.vfs.view``), normalised, so ``/workspace/../docs/a.md`` meets the ``/docs/**`` rule.

Patterns are absolute paths whose segments may use ``*``, ``?`` and ``[...]`` (``[!...]`` negates) within one
segment (``fnmatch``), and ``**`` as a whole segment for zero or more segments: ``/workspace/**`` matches
``/workspace`` itself and everything below it. Glob syntax that ``fnmatch`` would read literally, which the Glob
tool's globs give a meaning (``{a,b}``, ``[^...]``, ``[[:alpha:]]`` and backslash escapes), is refused, and so
is a ``[`` not closed within its segment (``[!/]``), so a rule can't silently match nothing.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from functools import lru_cache
from typing import Literal, get_args

from minifs.vfs.core import VFSError, split

Action = Literal["read", "write", "append"]
_ACTIONS: frozenset[str] = frozenset(get_args(Action))
_log = logging.getLogger(__name__)


class PolicyDenied(VFSError):
    """EACCES from the policy: ``actor`` may not ``action`` at ``path``. ``VFSError.at()`` and the view's
    ``_mapped`` report a plain ``VFSError``, so policy checks stay outside them, where the tools see ``action``."""

    def __init__(self, path: str, actor: str, action: Action) -> None:
        super().__init__("EACCES", path, f"{actor} may not {action} here")
        self.actor = actor
        self.action: Action = action

    def __reduce__(self) -> tuple[type[PolicyDenied], tuple[str, str, Action]]:  # pyright: ignore[reportIncompatibleMethodOverride]
        return (PolicyDenied, (self.path, self.actor, self.action))


@dataclass(frozen=True, init=False)
class Rule:
    """``actions`` granted on the paths ``pattern`` matches: ``Rule("/docs/**", "read")``."""

    pattern: str
    actions: frozenset[Action]
    segments: tuple[str, ...] = field(repr=False, compare=False)

    def __init__(self, pattern: str, *actions: Action) -> None:
        unknown = set(actions) - _ACTIONS
        if unknown:
            raise ValueError(f"unknown actions {sorted(unknown)} in rule {pattern!r}")
        segments = tuple(pattern.split("/")[1:]) if pattern != "/" else ()
        if not pattern.startswith("/") or any(s in ("", ".", "..") for s in segments):
            raise ValueError(f"rule pattern must be a normalised absolute path: {pattern!r}")
        if any(c in pattern for c in "{}\\") or "[^" in pattern:
            raise ValueError(
                f"rule pattern has braces, a [^ class or a backslash, which fnmatch reads literally: {pattern!r}"
            )
        if any(_bad_class(segment) for segment in segments):
            raise ValueError(
                f"rule pattern has a [ not closed in its segment, or a [: in a class, which fnmatch reads literally:"
                f" {pattern!r}"
            )
        object.__setattr__(self, "pattern", pattern)
        object.__setattr__(self, "actions", frozenset(actions))
        object.__setattr__(self, "segments", segments)

    def matches(self, path: str) -> bool:
        """Whether the pattern matches ``path`` (normalised first)."""
        return _match(self.segments, split(path))

    def grants(self, action: Action) -> bool:
        return action in self.actions or (action == "append" and "write" in self.actions)


class Policy:
    """Ordered rules per actor. ``denials`` counts by ``(actor, action)`` the operations the policy refused that
    the caller asked for (``check``), for the write-policy denials metric: one per refused tool call or view
    operation.

    Lookups a caller makes on its own account, where a refusal only means "skip it", pass
    ``count_denials=False`` and aren't counted (logged at DEBUG instead): Read's instruction files, its
    suggestions for a missing path, and its page read alongside the sample that decides. Nor are entries a view
    leaves out of a listing, which aren't refusals. The counter belongs to the policy object, so
    ``DEFAULT_POLICY``'s is process-wide: build a ``Policy`` of its ``rules`` for a counter of your own."""

    def __init__(self, rules: Mapping[str, Sequence[Rule]]) -> None:
        self.rules: dict[str, tuple[Rule, ...]] = {actor: tuple(r) for actor, r in rules.items()}
        self.denials: Counter[tuple[str, Action]] = Counter()

    @property
    def actors(self) -> Iterable[str]:
        return self.rules.keys()

    def allows(self, actor: str, action: Action, path: str, *, subtree: bool = False) -> bool:
        """Whether ``actor`` may ``action`` at ``path`` and, with ``subtree``, at every path below it too (what
        moving a directory needs). The subtree answer is conservative: a rule that could match below ``path``
        and doesn't grant ``action`` refuses, unless an earlier rule covers the whole subtree. KeyError for an
        actor without rules."""
        segments = split(path)
        for rule in self.rules[actor]:
            rests = _rests(rule.segments, segments)
            if not subtree:
                if any(_only_globstars(rest) for rest in rests):
                    return rule.grants(action)
            elif rests:
                if not rule.grants(action):
                    return False
                if any(rest and _only_globstars(rest) for rest in rests):  # ends in **: covers the subtree
                    return True
        return action == "read"

    def allows_any(self, actor: str, action: Action, path: str) -> bool:
        """Whether ``actor`` may ``action`` at ``path`` or at some path below it. Conservative the other way: True
        unless a rule that doesn't grant ``action`` covers the whole subtree before any rule that could grant it."""
        segments = split(path)
        for rule in self.rules[actor]:
            rests = _rests(rule.segments, segments)
            if not rests:
                continue
            if rule.grants(action):
                return True
            if any(rest and _only_globstars(rest) for rest in rests):
                return False
        return action == "read"

    def check(
        self, actor: str, action: Action, path: str, *, subtree: bool = False, count_denials: bool = True
    ) -> None:
        """Raises EACCES (``PolicyDenied``) for ``path`` unless ``actor`` may ``action`` there (``allows``), and
        counts the denial unless ``count_denials`` is False (a speculative lookup)."""
        if not self.allows(actor, action, path, subtree=subtree):
            if count_denials:
                self.denials[actor, action] += 1
                _log.info("write policy denied %s %s on %s", actor, action, path)
            else:
                _log.debug("write policy refused the lookup %s %s on %s", actor, action, path)
            raise PolicyDenied(path, actor, action)


def _bad_class(segment: str) -> bool:
    """Whether ``segment`` has a ``[`` that fnmatch reads literally because no ``]`` closes it (``[!/]*`` is split
    by its ``/``), or a class with ``[:`` in it (a POSIX class, which fnmatch doesn't know). Scans as fnmatch does:
    a ``]`` first in a class, after any ``!``, is literal."""
    i, n = 0, len(segment)
    while i < n:
        c, i = segment[i], i + 1
        if c != "[":
            continue
        j = i
        if j < n and segment[j] == "!":
            j += 1
        if j < n and segment[j] == "]":
            j += 1
        while j < n and segment[j] != "]":
            j += 1
        if j >= n or "[:" in segment[i:j]:
            return True
        i = j + 1
    return False


def _match(pattern: tuple[str, ...], path: tuple[str, ...]) -> bool:
    return any(_only_globstars(rest) for rest in _rests(pattern, path))


@lru_cache(maxsize=4096)
def _rests(pattern: tuple[str, ...], path: tuple[str, ...]) -> frozenset[tuple[str, ...]]:
    """What can remain of ``pattern`` once ``path`` is matched against its start: ``pattern`` matches ``path`` if
    a remainder is empty or all ``**``, and may match a path below ``path`` if there is any remainder."""
    if not path:
        return frozenset([pattern])
    if not pattern:
        return frozenset()
    head = pattern[0]
    if head == "**":
        return _rests(pattern[1:], path) | _rests(pattern, path[1:])
    return _rests(pattern[1:], path[1:]) if fnmatchcase(path[0], head) else frozenset()


def _only_globstars(rest: tuple[str, ...]) -> bool:
    return all(segment == "**" for segment in rest)


DEFAULT_POLICY = Policy(
    {
        # The README's table, as ordered rules.
        "agent": [
            Rule("/workspace/log.md", "read", "append"),
            Rule("/workspace/**", "read", "write"),
            Rule("/docs/**", "read"),
        ],
        "ingestion": [
            Rule("/workspace/**"),
            Rule("/docs/**", "read", "write"),
        ],
        "operator": [
            Rule("/workspace/log.md", "read"),
            Rule("/workspace/**", "read", "write"),
            Rule("/docs/**", "read"),
        ],
    }
)
