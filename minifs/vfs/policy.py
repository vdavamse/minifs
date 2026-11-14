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
pattern matches byte for byte and, in addition, the case and Unicode variants of the names it spells: each
segment of the folded pattern against the folded name, where folding a name drops the format characters
(Unicode category Cf, such as zero-width spaces), then applies NFKC normalisation, case folding and NFKC again.
The byte-exact and the folded match are tried per segment, not per character, so one segment that needs both
(``[!a-z]*.env`` against ``Config.ENV``) isn't hidden; spell such rules as two rules or with ``*``. A hide rule
that missed a variant would leak it, while a loosened grant would grant more than written, so a rule with some
actions is a grant and stays exact. A segment folds only where folding keeps its glob syntax: a segment with a
``[...]`` class, or one with a character that folds to glob syntax (the fullwidth asterisk U+FF0A folds to
``*``), matches byte for byte only, so spell such a class with both cases (``[Ee]``). A ``?`` matches one
character of the folded name (the sharp s folds to ``ss``). Suffixes and trailing spaces are other names: use
``*`` (``.env*``). Over-hiding is the trade-off: ``Rule("/workspace/Makefile")`` also hides ``MAKEFILE``, and a
hide rule naming ``/Docs/**`` hides the ``/docs`` mount.

Each rule compiles its alternatives once into a regex (``Rule.regex``) over the joined path string (for a hide
rule the dual string, every name followed by a NUL and its folded form), so a lookup without ``subtree`` is one
``fullmatch`` per rule instead of the recursive per-segment match (``_rests``, kept for ``subtree``,
``allows_any`` and as the reference the differential test checks by).
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from fnmatch import fnmatchcase, translate
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
    hides those paths instead: it matches what ``pattern`` matches byte for byte and, in addition, the case and
    Unicode variants of the names it spells, each segment of the folded pattern against the folded name, the two
    tried per segment (``alternatives``). A segment folds only where folding keeps its glob syntax: one with a
    ``[...]`` class, or with a character that folds to glob syntax, matches byte for byte only. A rule that
    grants matches byte for byte."""

    pattern: str
    actions: frozenset[Action]
    segments: tuple[str, ...] = field(repr=False, compare=False)
    alternatives: tuple[tuple[str, ...], ...] = field(repr=False, compare=False)
    regex: re.Pattern[str] = field(repr=False, compare=False)
    """The compiled ``alternatives`` (``_rule_regex``), whose ``fullmatch`` decides a lookup against the path
    string (``_path_string``) or, for a hide rule, its dual string (``_dual_string``)."""

    def __init__(self, pattern: str, *actions: Action) -> None:
        unknown = set(actions) - _ACTIONS
        if unknown:
            raise ValueError(f"unknown actions {sorted(unknown)} in rule {pattern!r}")
        segments = tuple(pattern.split("/")[1:]) if pattern != "/" else ()
        if not pattern.startswith("/") or any(s in ("", ".", "..") for s in segments):
            raise ValueError(f"rule pattern must be a normalised absolute path: {pattern!r}")
        _no_literal_globs(pattern, segments)
        alternatives = (
            tuple(_pattern_alts(segment) for segment in segments) if not actions else tuple((s,) for s in segments)
        )
        object.__setattr__(self, "pattern", pattern)
        object.__setattr__(self, "actions", frozenset(actions))
        object.__setattr__(self, "segments", segments)
        object.__setattr__(self, "alternatives", alternatives)
        object.__setattr__(self, "regex", _rule_regex(alternatives, not actions))

    @property
    def hides(self) -> bool:
        """Whether the rule hides the paths it matches instead of granting on them: a rule with no actions."""
        return not self.actions

    def matches(self, path: str) -> bool:
        """Whether the pattern matches ``path`` (normalised first)."""
        split(path)  # EINVAL for a path with a control character, as before
        plain = _path_string(path)
        return bool(self.regex.fullmatch(_dual_string(plain) if self.hides else plain))

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
        return self._first_match(actor, path) is not None

    def _first_match(self, actor: str, path: str) -> Rule | None:
        """The first rule of ``actor`` whose regex fullmatches ``path`` (the path string for a grant rule, the dual
        string for a hide rule, built once and only when a hide rule is reached), or None. A path holding a NUL,
        which no stored name can, would break the dual string's separator, so it goes through the reference
        matcher (``_rests``) instead, which answers it as before."""
        rules = self.rules[actor]
        if "\x00" in path:
            segments = split_trusted(path)
            return next((rule for rule in rules if _matches_by_rests(rule, segments)), None)
        plain = _path_string(path)
        dual: str | None = None
        for rule in rules:
            if rule.hides:
                if dual is None:
                    dual = _dual_string(plain)
                if rule.regex.fullmatch(dual):
                    return rule
            elif rule.regex.fullmatch(plain):
                return rule
        return None

    def allows(self, actor: str, action: Action, path: str, *, subtree: bool = False) -> bool:
        """Whether ``actor`` may ``action`` at ``path`` and, with ``subtree``, at every path below it too (what
        moving a directory needs). The subtree answer is conservative: a rule that could match below ``path``
        and doesn't grant ``action`` refuses, unless an earlier rule covers the whole subtree. KeyError for an
        actor without rules. ``path`` isn't checked for control characters (``split_trusted``): it is as often a
        path the server listed as one from a caller, which ``check`` and the operation itself refuse."""
        if subtree:
            segments = split_trusted(path)
            for rule in self.rules[actor]:
                rests = _rule_rests(rule, segments)
                if rests:
                    if not rule.grants(action):
                        return False
                    if any(rest and _only_globstars(rest) for rest in rests):  # ends in **: covers the subtree
                        return True
        else:
            rule = self._first_match(actor, path)
            if rule is not None:
                return rule.grants(action)
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
                # %r (#98, CONTRIBUTING): split() above refuses control characters; repr also escapes the
                # invisible ones a name may hold (U+200B, U+200F).
                _log.info("write policy denied %s %s on %r", actor, action, path)
            else:
                _log.debug("write policy refused the lookup %s %s on %r", actor, action, path)
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
    """Raises ValueError if a rule's own ``segments`` have glob syntax that fnmatch reads literally, so the rule
    can't silently match nothing: braces, a ``[^`` class or a backslash, a ``[`` not closed within its segment
    (``[!/]``), or ``[:`` in a class (a POSIX class). The message names ``pattern`` as written."""
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


def _pattern_alts(segment: str) -> tuple[str, ...]:
    """The alternatives a hide rule's pattern ``segment`` matches by: the raw and the folded segment, ``(s, f)``,
    where folding keeps its glob structure, so the segment also matches the folded name; the raw segment alone,
    ``(s,)``, otherwise, so it matches byte for byte only (what ``_rests`` does with a 1-tuple). Folding doesn't
    keep the structure where the segment has a ``[`` (a class, whose members, ranges or negation folding could
    change), where the folded form has glob syntax the raw one doesn't spell as such (a fullwidth asterisk folds
    to ``*``, a fullwidth bracket to ``[``), where folding changes the count of ``*`` or ``?``, or where the
    segment is ``**``, which stays ``("**",)``: a whole segment, not two wildcards."""
    folded = _fold_pattern(segment)
    foldable = (
        "[" not in segment
        and not any(c in folded for c in "[]{}\\")
        and segment.count("*") == folded.count("*")
        and segment.count("?") == folded.count("?")
        and segment != "**"
    )
    return (segment, folded) if foldable else (segment,)


def _fold_pattern(segment: str) -> str:
    """The form of a hide rule's pattern ``segment`` that matching folds a name to: NFKC normalisation, case
    folding and NFKC again, without dropping the format characters, so a zero-width space in a pattern stays and
    can't join two ``*`` into ``**``. ASCII takes the fast path (``lower``), exact because NFKC is the identity on
    ASCII and case folding equals ``lower`` there, and ASCII has no format characters to drop."""
    if segment.isascii():
        return segment.lower()
    return unicodedata.normalize("NFKC", unicodedata.normalize("NFKC", segment).casefold())


def _segment_regex(segment: str, exclude: str) -> str:
    """The regex for one raw pattern ``segment`` (never ``**``), matching one name of the string a rule regex is
    fullmatched against: ``?`` becomes a class matching anything but the characters of ``exclude`` (the separators
    a name never contains), a ``[...]`` class becomes the regex ``fnmatch.translate`` gives that class token alone,
    its ``(?s:...)`` group kept so a class fnmatch reads as "any character" (``[!z-a]``, an empty range negated)
    also matches a newline, which a name from before migration 016 can hold, behind a lookahead that keeps
    ``exclude`` out of it too, and any other character is literal (``re.escape``).

    A run of ``*`` is one star, and the stars are then spelled as ``fnmatch.translate`` spells them: the pieces
    between two stars go in atomic groups with a lazy wildcard, ``(?>[^{exclude}]*?piece)``, and only the last
    star stays a plain greedy ``[^{exclude}]*``, followed by the piece after it (``*a*a*b`` is
    ``(?>[^{exclude}]*?a)(?>[^{exclude}]*?a)[^{exclude}]*b``). Without the atomic groups a name that nearly
    matches makes the matcher try every way of sharing it between the stars (``/w/*a*a*a*a*b`` against 250 ``a``
    takes seconds); taking the leftmost match of a piece loses nothing, because the star after it absorbs whatever
    follows, and a segment's regex is followed only by ``/``, a NUL or the end of the string, so no piece has to
    reach past a separator. Classes are scanned exactly as ``_bad_class`` scans them, which ``_no_literal_globs``
    has been through, so every ``[`` closes within the segment."""
    chunks: list[list[str]] = [[]]  # the pieces the stars divide the segment into: one more than the stars
    after_star = False
    i, n = 0, len(segment)
    while i < n:
        c, i = segment[i], i + 1
        if c == "*":
            if not after_star:  # a run of consecutive "*" is one star, as in fnmatch.translate
                chunks.append([])
            after_star = True
            continue
        after_star = False
        if c == "?":
            chunks[-1].append(f"[^{exclude}]")
        elif c == "[":
            j = i
            if j < n and segment[j] == "!":
                j += 1
            if j < n and segment[j] == "]":
                j += 1
            while j < n and segment[j] != "]":
                j += 1
            if j >= n:
                raise AssertionError(f"unclosed [ in rule segment {segment!r}")
            translated = translate(segment[i - 1 : j + 1])
            shape = re.fullmatch(r"\(\?s:(.*)\)\\[Zz]", translated, re.DOTALL)
            if shape is None:
                raise AssertionError(f"fnmatch.translate has an unexpected shape: {translated!r}")
            chunks[-1].append(f"(?![{exclude}])(?s:{shape[1]})")
            i = j + 1
        else:
            chunks[-1].append(re.escape(c))
    pieces = ["".join(chunk) for chunk in chunks]
    if len(pieces) == 1:  # no star at all
        return pieces[0]
    out = [pieces[0]]  # the piece before the first star, as it is
    out += [f"(?>[^{exclude}]*?{piece})" for piece in pieces[1:-1]]  # each star but the last, atomic
    out.append(f"[^{exclude}]*{pieces[-1]}")  # the last star stays greedy, so the piece after it can backtrack
    return "".join(out)


_SEGMENT_END = r"(?=/|\Z)"


def _rule_regex(alternatives: tuple[tuple[str, ...], ...], hides: bool) -> re.Pattern[str]:
    """A rule's compiled regex: its segments (``_alt_regex``) in order, with the ``**`` segments spelled as
    ``_segment_regex`` spells stars, one level up. The runs of segments between two ``**`` match a fixed number of
    segments each, so every ``**`` but the last is an atomic lazy group taking the leftmost place its run
    matches, ``(?>(?:/[^/]+)*?run(?=/|\\Z))``, and only the last ``**`` stays greedy. The ``**`` after a run
    absorbs whatever follows it, so nothing is lost, and a deep path that nearly matches can't make the regex try
    every way of sharing its segments between the globstars."""
    runs: list[str] = [""]
    for alt in alternatives:
        if alt == ("**",):
            runs.append("")
        else:
            # Each segment's regex consumes exactly one whole segment, however it matches inside it, so it is
            # atomic: a later failure can't retry another way of matching it (the other branch of a foldable hide
            # segment, another split between its stars), which would multiply the work across segments.
            runs[-1] += f"(?>{_alt_regex(alt, hides)}{_SEGMENT_END})"
    if len(runs) == 1:
        return re.compile(runs[0])
    middle = "".join(f"(?>(?:/[^/]+)*?{run}{_SEGMENT_END})" for run in runs[1:-1])
    return re.compile(f"{runs[0]}{middle}(?:/[^/]+)*{runs[-1]}")


def _alt_regex(alt: tuple[str, ...], hides: bool) -> str:
    """The regex one segment of a rule's ``alternatives`` contributes, matching ``/name`` in the string a lookup
    is fullmatched against (``_path_string``, or ``_dual_string`` for a hide rule), never ``**`` (``_rule_regex``
    spells those): a hide rule's foldable segment ``(s, f)`` matches a name's raw form against ``s`` or its
    folded form against ``f`` (per segment, #93), where the ``/`` folding a fullwidth solidus gives is the NUL of
    the dual string; a hide rule's exact-only segment ``(s,)`` matches the raw form and skips the folded one; a
    grant rule's segment ``(s,)`` matches the raw form alone."""
    if len(alt) == 2:
        raw = _segment_regex(alt[0], "/\x00")  # a raw name holds neither separator
        folded = _segment_regex(alt[1].replace("/", "\x00"), "/")  # a folded name can hold the NUL of a folded /
        return f"/(?:{raw}\x00[^/]*|[^/\x00]*\x00{folded})"
    exact = _segment_regex(alt[0], "/\x00")
    return f"/{exact}\x00[^/]*" if hides else f"/{exact}"


@lru_cache(maxsize=65536)
def _fold(name: str) -> str:
    """The form of ``name`` a hide rule matches by: the format characters (Unicode category Cf, such as the
    zero-width space U+200B) dropped first, so a combining mark they separate can still compose, then NFKC
    normalisation, case folding and NFKC again (folding can denormalise), then the format characters dropped
    once more (folding can produce them). ASCII takes the fast path (``lower``), exact because ASCII has no
    format characters, NFKC is the identity on it, and case folding equals ``lower`` there."""
    if name.isascii():
        return name.lower()
    dropped = "".join(c for c in name if unicodedata.category(c) != "Cf")
    folded = unicodedata.normalize("NFKC", unicodedata.normalize("NFKC", dropped).casefold())
    return "".join(c for c in folded if unicodedata.category(c) != "Cf")


@lru_cache(maxsize=16384)
def _folded(segments: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(_fold(segment) for segment in segments)


def _rule_rests(rule: Rule, segments: tuple[str, ...]) -> frozenset[tuple[tuple[str, ...], ...]]:
    """What can remain of ``rule``'s pattern once ``segments`` is matched against its start. Each of the rule's
    pattern segments carries the alternatives it matches by (``rule.alternatives``): the raw segment alone for a
    grant rule or for a hide rule's segment that folding would change the glob syntax of, the raw and the folded
    segment for a foldable one. A hide rule's path segments are built as ``(raw, folded)`` pairs (``_folded_alts``)
    and a grant rule's as 1-tuples (``_exact_alts``), so ``_rests`` tries the byte-exact and the folded match per
    segment, and a 1-tuple pattern segment compares the raw path segment alone."""
    path = _folded_alts(segments) if rule.hides else _exact_alts(segments)
    return _rests(rule.alternatives, path)


@lru_cache(maxsize=16384)
def _exact_alts(segments: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    """The path form of a grant rule's match: every segment matches byte for byte alone."""
    return tuple((segment,) for segment in segments)


@lru_cache(maxsize=16384)
def _folded_alts(segments: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    """The path form of a hide rule's match: every segment matches raw or folded."""
    return tuple(zip(segments, _folded(segments), strict=True))


@lru_cache(maxsize=16384)
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


def _matches_by_rests(rule: Rule, segments: tuple[str, ...]) -> bool:
    """Whether ``rule`` matches ``segments`` by the reference matcher (``_rests`` over the rule's alternatives),
    what the lookups did before ``Rule.regex``: for a path holding a NUL (``Policy._first_match``) and the
    differential test (tests/test_policy.py)."""
    return any(_only_globstars(rest) for rest in _rule_rests(rule, segments))


def _path_string(path: str) -> str:
    """The string a rule's regex is fullmatched against for a grant rule: ``""`` for the root, else
    ``/seg1/seg2...`` with the segments ``split_trusted`` gives, so normalised, and it raises what it raises
    today for a path it refuses. An already normalised absolute path (what the server listed, the common case) is
    returned unchanged."""
    if path == "/":
        return ""
    if (
        path.startswith("/")
        and not path.endswith("/")
        and "//" not in path
        and "/./" not in path + "/"
        and "/../" not in path + "/"
    ):
        return path
    return "".join("/" + s for s in split_trusted(path))


def _dual_string(path_string: str) -> str:
    """The string a hide rule's regex is fullmatched against: every segment of ``path_string`` as the raw name, a
    NUL and the folded name (``_fold``), where the ``/`` folding can give (a fullwidth solidus) is spelled as a
    NUL too, so ``/`` only ever separates segments and a name holds neither ``/`` nor a NUL. The root is ``""``,
    as in ``_path_string``."""
    if not path_string:
        return ""
    return "".join("/" + raw + "\x00" + _fold(raw).replace("/", "\x00") for raw in path_string[1:].split("/"))


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
