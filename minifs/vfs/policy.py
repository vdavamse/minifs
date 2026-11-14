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

Grant rules match names byte for byte, case-sensitively. A rule with no actions (a hide rule) matches what its
pattern matches byte for byte and, in addition, the case and Unicode variants of the names it spells: the folded
pattern against the folded name, where folding drops the format characters (Unicode category Cf, such as
zero-width spaces), then applies NFKC normalisation, case folding and NFKC again, and a ``?`` or ``[...]`` of
the pattern matches one character of the folded name. The byte-exact and the folded match are tried per
segment, not per character, so one segment that needs both (``[!a-z]*.env`` against ``Config.ENV``) isn't
hidden; spell such rules as two rules or with ``*``. A hide rule that missed a variant would leak it, while a
loosened grant would grant more than written, so a rule with some actions is a grant and stays exact. A hide
rule may not contain a character that folds to glob syntax, such as the fullwidth asterisk U+FF0A, which folds
to ``*``, nor one in a ``[...]`` class that folds to more or fewer than one character, which would split the
class. Suffixes and trailing spaces are other names: use ``*`` (``.env*``). Over-hiding is the trade-off:
``Rule("/workspace/Makefile")`` also hides ``MAKEFILE``.
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
    hides those paths instead: it matches what ``pattern`` matches byte for byte (``segments``) and, in addition,
    the case and Unicode variants of the names it spells (``match_segments``, the folded pattern, against the
    folded path), the two tried per segment (``alternatives``), so it may not contain a character that folds to
    glob syntax. A rule that grants matches ``segments`` byte for byte."""

    pattern: str
    actions: frozenset[Action]
    segments: tuple[str, ...] = field(repr=False, compare=False)
    hides: bool = field(repr=False, compare=False)
    match_segments: tuple[str, ...] = field(repr=False, compare=False)
    alternatives: tuple[tuple[str, ...], ...] = field(repr=False, compare=False)

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
            _no_folded_glob_syntax(pattern)
            _no_literal_globs(pattern, match_segments)  # the folded pattern is what matching uses
            _no_folded_class_splits(pattern, segments)
        alternatives = tuple(zip(segments, match_segments, strict=False)) if hides else tuple((s,) for s in segments)
        object.__setattr__(self, "pattern", pattern)
        object.__setattr__(self, "actions", frozenset(actions))
        object.__setattr__(self, "segments", segments)
        object.__setattr__(self, "hides", hides)
        object.__setattr__(self, "match_segments", match_segments)
        object.__setattr__(self, "alternatives", alternatives)

    def matches(self, path: str) -> bool:
        """Whether the pattern matches ``path`` (normalised first)."""
        return any(_only_globstars(rest) for rest in _rule_rests(self, split(path)))

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
        return any(any(_only_globstars(rest) for rest in _rule_rests(rule, segments)) for rule in self.rules[actor])

    def allows(self, actor: str, action: Action, path: str, *, subtree: bool = False) -> bool:
        """Whether ``actor`` may ``action`` at ``path`` and, with ``subtree``, at every path below it too (what
        moving a directory needs). The subtree answer is conservative: a rule that could match below ``path``
        and doesn't grant ``action`` refuses, unless an earlier rule covers the whole subtree. KeyError for an
        actor without rules. ``path`` isn't checked for control characters (``split_trusted``): it is as often a
        path the server listed as one from a caller, which ``check`` and the operation itself refuse."""
        segments = split_trusted(path)
        for rule in self.rules[actor]:
            rests = _rule_rests(rule, segments)
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
        for rule in self.rules[actor]:
            rests = _rule_rests(rule, segments)
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
    e.g. a fullwidth left curly bracket U+FF5B (which folds to ``{``, past ``_no_folded_glob_syntax``) is
    refused in a hide rule."""
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


_GLOB_SYNTAX = "*?[]!-"


def _no_folded_glob_syntax(pattern: str) -> None:
    """Raises ValueError if a hide rule's ``pattern`` has a character, not itself one of ``*?[]!-``, that folds
    to one of them: the folded pattern would read it as glob syntax, so the rule would hide more than it spells.
    E.g. the fullwidth asterisk U+FF0A folds to ``*``, and the fullwidth hyphen-minus U+FF0D to ``-``, which
    would turn a class of literal characters into a range. Grant rules aren't folded, so they aren't checked."""
    for c in pattern:
        if c not in _GLOB_SYNTAX and any(g in _fold(c) for g in _GLOB_SYNTAX):
            raise ValueError(
                f"rule pattern has {c!r} (U+{ord(c):04X}), which folds to glob syntax fnmatch would read: {pattern!r}"
            )


def _no_folded_class_splits(pattern: str, segments: tuple[str, ...]) -> None:
    """Raises ValueError if a hide rule's ``pattern`` has a character inside a ``[...]`` class that folds to more
    or fewer than one character: the folded class would gain or lose members, so fnmatch would read it as a
    different class than the one spelled. E.g. the sharp s U+00DF folds to ``ss`` and the zero-width space U+200B
    to nothing. Scans as ``_bad_class`` does: a ``]`` first in a class, after any ``!``, is literal. Grant rules
    aren't folded, so they aren't checked."""
    for segment in segments:
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
                member = segment[j]
                if len(_fold(member)) != 1:
                    raise ValueError(
                        f"rule pattern has {member!r} (U+{ord(member):04X}) in a class, which folds to"
                        f" {len(_fold(member))} characters, splitting the class fnmatch would match: {pattern!r}"
                    )
                j += 1
            i = j + 1


@lru_cache(maxsize=4096)
def _fold(name: str) -> str:
    """The form of ``name`` a hide rule matches by: the format characters (Unicode category Cf, such as the
    zero-width space U+200B) dropped first, so a combining mark they separate can still compose, then NFKC
    normalisation, case folding and NFKC again (folding can denormalise), then the format characters dropped
    once more (folding can produce them)."""
    dropped = "".join(c for c in name if unicodedata.category(c) != "Cf")
    folded = unicodedata.normalize("NFKC", unicodedata.normalize("NFKC", dropped).casefold())
    return "".join(c for c in folded if unicodedata.category(c) != "Cf")


@lru_cache(maxsize=4096)
def _folded(segments: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(_fold(segment) for segment in segments)


def _rule_rests(rule: Rule, segments: tuple[str, ...]) -> frozenset[tuple[tuple[str, ...], ...]]:
    """What can remain of ``rule``'s pattern once ``segments`` is matched against its start. Each of the rule's
    pattern segments carries the alternatives it matches by (``rule.alternatives``): the raw segment alone for a
    grant rule, the raw and the folded segment for a hide rule, and the path segments are built in the same
    shape, so ``_rests`` tries the byte-exact and the folded match per segment."""
    path = _folded_alts(segments) if rule.hides else _exact_alts(segments)
    return _rests(rule.alternatives, path)


@lru_cache(maxsize=4096)
def _exact_alts(segments: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    """The path form of a grant rule's match: every segment matches byte for byte alone."""
    return tuple((segment,) for segment in segments)


@lru_cache(maxsize=4096)
def _folded_alts(segments: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    """The path form of a hide rule's match: every segment matches raw or folded."""
    return tuple(zip(segments, _folded(segments), strict=False))


@lru_cache(maxsize=4096)
def _rests(
    pattern: tuple[tuple[str, ...], ...], path: tuple[tuple[str, ...], ...]
) -> frozenset[tuple[tuple[str, ...], ...]]:
    """What can remain of ``pattern`` once ``path`` is matched against its start: ``pattern`` matches ``path`` if
    a remainder is empty or all ``**``, and may match a path below ``path`` if there is any remainder. Segments
    are tuples of alternatives, paired by position: a path segment matches a pattern segment if some form of it
    matches the pattern's form at the same position (raw against raw, folded against folded), and ``**`` is a
    whole segment (the same in every alternative)."""
    if not path:
        return frozenset([pattern])
    if not pattern:
        return frozenset()
    head = pattern[0]
    if head[0] == "**":
        return _rests(pattern[1:], path) | _rests(pattern, path[1:])
    if any(fnmatchcase(p, q) for p, q in zip(path[0], head, strict=False)):
        return _rests(pattern[1:], path[1:])
    return frozenset()


def _only_globstars(rest: tuple[tuple[str, ...], ...]) -> bool:
    return all(segment[0] == "**" for segment in rest)


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
