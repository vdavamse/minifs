"""The write policy: what each actor may do where (README: *Workspaces and permissions*).

Each actor has an ordered list of path rules, like mount options; the first rule whose pattern matches a path
decides, so specific rules go before general ones. A rule grants any of ``read``, ``write`` and ``append``, and
``write`` implies ``append`` (replacing a file is the stronger right). A rule granting nothing denies everything.
A path no rule matches is refused, unless the policy is built with ``read_unmatched=True``, which lets it be read
(not written). Rules match the paths of the agent's view (the chroot, ``minifs.vfs.view``), normalised, so
``/workspace/../docs/a.md`` meets the ``/docs/**`` rule.

Patterns are absolute paths whose segments may use ``*``, ``?`` and ``[...]`` (``[!...]`` negates) within one
segment (``fnmatch``), and ``**`` as a whole segment for zero or more segments: ``/workspace/**`` matches
``/workspace`` itself and everything below it. Glob syntax that ``fnmatch`` would read literally, which the Glob
tool's globs give a meaning (``{a,b}``, ``[^...]``, ``[[:alpha:]]`` and backslash escapes), is refused, and so
is a ``[`` not closed within its segment (``[!/]``), so a rule can't silently match nothing.

Grant rules match names byte for byte, case-sensitively. A rule with no actions (a hide rule) also matches case
and Unicode variants of the names it spells: names are compared after NFKC normalisation, case folding and with
the format characters (Unicode category Cf, such as zero-width spaces) ignored, since a hide rule that missed a
variant would leak it, while a loosened grant would grant more than written. A rule with some actions is a grant
and stays exact. A hide rule's ``?`` and ``[...]`` match one character of the folded name. Suffixes and trailing
spaces are other names: use ``*`` (``.env*``). Over-hiding is the trade-off: ``Rule("/workspace/Makefile")`` also
hides ``MAKEFILE``.
"""

from __future__ import annotations

import logging
import unicodedata
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from functools import lru_cache
from typing import Literal, get_args

from minifs.vfs.core import VFSError, split, split_trusted

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
    """``actions`` granted on the paths ``pattern`` matches: ``Rule("/docs/**", "read")``. A rule with no actions
    hides those paths instead, and matching then uses ``match_segments``: the folded segments, which also cover
    case and Unicode variants of the names the pattern spells. A rule that grants matches ``segments`` byte for
    byte."""

    pattern: str
    actions: frozenset[Action]
    segments: tuple[str, ...] = field(repr=False, compare=False)
    hides: bool = field(repr=False, compare=False)
    match_segments: tuple[str, ...] = field(repr=False, compare=False)

    def __init__(self, pattern: str, *actions: Action) -> None:
        unknown = set(actions) - _ACTIONS
        if unknown:
            raise ValueError(f"unknown actions {sorted(unknown)} in rule {pattern!r}")
        segments = tuple(pattern.split("/")[1:]) if pattern != "/" else ()
        if not pattern.startswith("/") or any(s in ("", ".", "..") for s in segments):
            raise ValueError(f"rule pattern must be a normalised absolute path: {pattern!r}")
        _no_literal_globs(pattern, segments)
        hides = not actions
        match_segments = _folded(segments) if hides else segments
        if hides:
            _no_literal_globs(pattern, match_segments)  # the folded pattern is what matching uses
        object.__setattr__(self, "pattern", pattern)
        object.__setattr__(self, "actions", frozenset(actions))
        object.__setattr__(self, "segments", segments)
        object.__setattr__(self, "hides", hides)
        object.__setattr__(self, "match_segments", match_segments)

    def matches(self, path: str) -> bool:
        """Whether the pattern matches ``path`` (normalised first)."""
        segments = split(path)
        return _match(self.match_segments, _folded(segments) if self.hides else segments)

    def grants(self, action: Action) -> bool:
        return action in self.actions or (action == "append" and "write" in self.actions)


class Policy:
    """Ordered rules per actor. ``denials`` counts by ``(actor, action)`` the operations the policy refused that
    the caller asked for (``check``), for the write-policy denials metric: one per refused tool call or view
    operation.

    Lookups a caller makes on its own account, where a refusal only means "skip it", pass ``count_denials=False``
    and aren't counted (logged at DEBUG instead): Read's instruction files, its suggestions for a missing path, and
    its page read alongside the sample that decides. Nor are entries a view leaves out of a listing, which aren't
    refusals. The counter belongs to the policy object, so ``DEFAULT_POLICY``'s is process-wide: build
    ``Policy(p.rules, read_unmatched=p.read_unmatched)`` for a counter of your own (``Policy(p.rules)`` alone would
    drop ``read_unmatched``).

    ``read_unmatched=True`` lets a path no rule matches be read, as before 0.1.0: a mount the rules don't cover is then
    readable by every actor of the policy. Without it, a real directory, the mount point included, is readable only
    where a rule matches it: ``/workspace/**`` matches ``/workspace`` itself, while ``/workspace/src/**`` alone leaves
    ``/workspace`` unreadable."""

    def __init__(self, rules: Mapping[str, Sequence[Rule]], *, read_unmatched: bool = False) -> None:
        self.rules: dict[str, tuple[Rule, ...]] = {actor: tuple(r) for actor, r in rules.items()}
        self.denials: Counter[tuple[str, Action]] = Counter()
        self.read_unmatched = read_unmatched

    @property
    def actors(self) -> Iterable[str]:
        return self.rules.keys()

    def matches(self, actor: str, path: str) -> bool:
        """Whether some rule of ``actor`` matches ``path`` itself, so that the first such rule decides it. KeyError
        for an actor without rules; ``path`` as ``allows`` takes it."""
        segments = split_trusted(path)
        folded = _folded(segments)
        return any(_match(rule.match_segments, folded if rule.hides else segments) for rule in self.rules[actor])

    def allows(self, actor: str, action: Action, path: str, *, subtree: bool = False) -> bool:
        """Whether ``actor`` may ``action`` at ``path`` and, with ``subtree``, at every path below it too (what
        moving a directory needs). The subtree answer is conservative: a rule that could match below ``path``
        and doesn't grant ``action`` refuses, unless an earlier rule covers the whole subtree. KeyError for an
        actor without rules. ``path`` isn't checked for control characters (``split_trusted``): it is as often a
        path the server listed as one from a caller, which ``check`` and the operation itself refuse."""
        segments = split_trusted(path)
        folded = _folded(segments)
        for rule in self.rules[actor]:
            rests = _rests(rule.match_segments, folded if rule.hides else segments)
            if not subtree:
                if any(_only_globstars(rest) for rest in rests):
                    return rule.grants(action)
            elif rests:
                if not rule.grants(action):
                    return False
                if any(rest and _only_globstars(rest) for rest in rests):  # ends in **: covers the subtree
                    return True
        return action == "read" and self.read_unmatched

    def allows_any(self, actor: str, action: Action, path: str) -> bool:
        """Whether ``actor`` may ``action`` at ``path`` or at some path below it. Conservative the other way: True
        unless a rule that doesn't grant ``action`` covers the whole subtree before any rule that could grant it."""
        segments = split_trusted(path)
        folded = _folded(segments)
        for rule in self.rules[actor]:
            rests = _rests(rule.match_segments, folded if rule.hides else segments)
            if not rests:
                continue
            if rule.grants(action):
                return True
            if any(rest and _only_globstars(rest) for rest in rests):
                return False
        return action == "read" and self.read_unmatched

    def check(
        self, actor: str, action: Action, path: str, *, subtree: bool = False, count_denials: bool = True
    ) -> None:
        """Raises EACCES (``PolicyDenied``) for ``path`` unless ``actor`` may ``action`` there (``allows``), and
        counts the denial unless ``count_denials`` is False (a speculative lookup)."""
        split(path)  # EINVAL before EACCES for a path with a control character: ``allows`` doesn't look
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


def _no_literal_globs(pattern: str, segments: tuple[str, ...]) -> None:
    """Raises ValueError if ``segments`` has glob syntax that fnmatch reads literally; the message names
    ``pattern`` as written. Called on a rule's own segments and, when the rule hides, on the folded ones too, so
    e.g. a fullwidth left square bracket U+FF3B (which folds to ``[``) is refused in a hide rule."""
    joined = "/".join(segments)
    if any(c in joined for c in "{}\\") or "[^" in joined:
        raise ValueError(
            f"rule pattern has braces, a [^ class or a backslash, which fnmatch reads literally: {pattern!r}"
        )
    if any(_bad_class(segment) for segment in segments):
        raise ValueError(
            f"rule pattern has a [ not closed in its segment, or a [: in a class, which fnmatch reads literally:"
            f" {pattern!r}"
        )


@lru_cache(maxsize=4096)
def _fold(name: str) -> str:
    """The form of ``name`` a hide rule matches by: NFKC normalisation, case folding and NFKC again (folding can
    denormalise), then the format characters dropped (Unicode category Cf, such as the zero-width space U+200B)."""
    name = unicodedata.normalize("NFKC", unicodedata.normalize("NFKC", name).casefold())
    return "".join(c for c in name if unicodedata.category(c) != "Cf")


@lru_cache(maxsize=4096)
def _folded(segments: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(_fold(segment) for segment in segments)


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
