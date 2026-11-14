"""The compiled rule regex (``Rule.regex``, #77) without a database: unit tests of the pieces a lookup is built
from, and a differential test of every regex lookup against the reference matcher (``_rests``) over random
patterns and paths, built from the characters folding makes tricky (#93)."""

from __future__ import annotations

import contextlib
import random

from minifs.vfs.core import split
from minifs.vfs.policy import (
    Action,
    Policy,
    Rule,
    _matches_by_rests,  # pyright: ignore[reportPrivateUsage]
    _path_string,  # pyright: ignore[reportPrivateUsage]
)

ZWSP = chr(0x200B)  # zero-width space: a format character, dropped when a name folds
FW_LOWER_A = chr(0xFF41)  # fullwidth "a": folds to "a"
FW_ASTERISK = chr(0xFF0A)  # fullwidth asterisk: folds to "*", so such a pattern segment isn't foldable
FW_SOLIDUS = chr(0xFF0F)  # fullwidth solidus: folds to "/", a NUL in the dual string
SHARP_S = chr(0xDF)  # folds to "ss"
FI_LIGATURE = chr(0xFB01)  # folds to "fi"
COMBINING_ACUTE = chr(0x301)
E_ACUTE = chr(0xE9)  # what "e" + COMBINING_ACUTE folds to: NFKC composes them
FW_UPPER_A = chr(0xFF21)  # fullwidth "A": folds to "a"
FW_UPPER_B = chr(0xFF22)
FW_UPPER_S = chr(0xFF33)
FW_LOWER_S = chr(0xFF53)
FW_UPPER_F = chr(0xFF26)
FW_UPPER_I = chr(0xFF29)
FW_FULL_STOP = chr(0xFF0E)  # fullwidth ".": folds to "."
FW_UPPER_E = chr(0xFF25)
FW_LOWER_N = chr(0xFF4E)
FW_LOWER_V = chr(0xFF56)
FW_LOWER_X = chr(0xFF58)

LITERALS = [
    "a",
    "A",
    "b",
    ".env",
    ".ENV",
    "x",
    "-",
    "!",
    "]",
    FW_LOWER_A,
    FW_ASTERISK,
    SHARP_S,
    FI_LIGATURE,
    ZWSP,
    FW_SOLIDUS,
    "e" + COMBINING_ACUTE,
    E_ACUTE,
]
CLASSES = ["[a-c]", "[!a]", "[A-Z]", "[]a]", "[!]x]", "[z-a]"]
TOKENS = LITERALS + ["*", "?"] + CLASSES
NAMES = LITERALS + [
    "B",
    "SS",
    "ss",
    "FI",
    "fi",
    ".Env",
    "x" + ZWSP,
    FW_UPPER_A,
    FW_UPPER_B,
    FW_UPPER_S * 2,
    FW_LOWER_S * 2,
    FW_UPPER_F + FW_UPPER_I,
    FW_FULL_STOP + FW_UPPER_E + FW_LOWER_N + FW_LOWER_V,
    FW_LOWER_X + ZWSP,
]


def _random_pattern(rng: random.Random) -> str:
    """A random rule pattern of 1-4 segments of 1-3 tokens each, or a whole ``**`` segment."""
    segments = [
        "**" if rng.random() < 0.2 else "".join(rng.choice(TOKENS) for _ in range(rng.randint(1, 3)))
        for _ in range(rng.randint(1, 4))
    ]
    return "/" + "/".join(segments)


def _random_path(rng: random.Random) -> str:
    """A random normalised path of 0-4 segments, each name 1-2 tokens of the names' pool."""
    return "/" + "/".join(
        "".join(rng.choice(NAMES) for _ in range(rng.randint(1, 2))) for _ in range(rng.randint(0, 4))
    )


def _rules_for(pattern: str) -> list[Rule]:
    """The hide and the grant rule for ``pattern``, skipping what ``Rule`` refuses as main does: ValueError for
    glob syntax fnmatch would read literally. Compiling the regex must never raise (a rule main loads must still
    load), so ``re.error`` isn't suppressed."""
    rules: list[Rule] = []
    variants: tuple[tuple[Action, ...], ...] = ((), ("read",))
    for actions in variants:
        with contextlib.suppress(ValueError):
            rules.append(Rule(pattern, *actions))
    return rules


def test_path_string() -> None:
    """``_path_string`` returns a normalised path unchanged (the fast path) and normalises the rest."""
    assert _path_string("/") == ""
    assert _path_string("/a/b") == "/a/b"
    assert _path_string("/a//b") == "/a/b"
    assert _path_string("/a/./b") == "/a/b"
    assert _path_string("/a/../b") == "/b"
    assert _path_string("/a/") == "/a"


def test_the_root_rule_matches_only_the_root() -> None:
    for rule in (Rule("/"), Rule("/", "read")):
        assert rule.matches("/"), ascii(rule.pattern)
        assert not rule.matches("/a"), ascii(rule.pattern)
        assert not rule.matches("/a/b"), ascii(rule.pattern)


def test_a_globstar_rule_matches_its_segment_and_below() -> None:
    for rule in (Rule("/w/**"), Rule("/w/**", "read")):
        assert rule.matches("/w"), ascii(rule.pattern)
        assert rule.matches("/w/x/y"), ascii(rule.pattern)
        assert not rule.matches("/x"), ascii(rule.pattern)
        assert not rule.matches("/wx"), ascii(rule.pattern)


def test_a_hide_rule_matches_a_folded_fullwidth_solidus() -> None:
    hide = Rule(f"/a{FW_SOLIDUS}b")  # folds to "a/b": the dual string spells its "/" as a NUL
    assert hide.matches(f"/a{FW_SOLIDUS}b")  # byte for byte
    assert hide.matches(f"/A{FW_SOLIDUS}B")  # folded: both names fold to "a/b"
    assert not hide.matches("/a/b")  # two real segments are another path
    assert not Rule(f"/a{FW_SOLIDUS}b", "read").matches(f"/A{FW_SOLIDUS}B")  # a grant stays exact


def test_an_impossible_class_compiles_and_matches_nothing() -> None:
    """``[z-a]``: fnmatch translates the empty range to a class that never matches, and the rule still loads."""
    for rule in (Rule("/[z-a]"), Rule("/[z-a]", "read")):
        for path in ("/z", "/a", "/-", "/[z-a]"):
            assert not rule.matches(path), ascii(path)
            assert not _matches_by_rests(rule, split(path)), ascii(path)


def test_rule_regexes_agree_with_the_reference_matcher() -> None:
    """Over 20,000 (rule, path) pairs: ``Rule.matches``, answered by one ``fullmatch`` of the compiled regex,
    decides exactly as the reference matcher ``_matches_by_rests``, for hide and grant rules alike (#77, #93)."""
    rng = random.Random(77)
    rules: list[Rule] = []
    while len(rules) < 80:
        rules.extend(_rules_for(_random_pattern(rng)))
    paths = [_random_path(rng) for _ in range(300)]
    checks = 0
    for rule in rules:
        for path in paths:
            by_regex = rule.matches(path)
            by_rests = _matches_by_rests(rule, split(path))
            assert by_regex == by_rests, (
                f"pattern {ascii(rule.pattern)} on path {ascii(path)}: regex {by_regex}, rests {by_rests}"
            )
            checks += 1
    assert checks >= 20_000, checks


def test_policy_lookups_agree_with_the_reference_matcher() -> None:
    """``Policy.allows`` (without ``subtree``) and ``Policy.matches``, answered by the compiled regexes, decide
    exactly as the same first-match-wins loop over the reference matcher, for random 1-3-rule policies."""
    rng = random.Random(77)
    pool: list[Rule] = []
    while len(pool) < 30:
        pool.extend(_rules_for(_random_pattern(rng)))
    paths = [_random_path(rng) for _ in range(150)]
    checks = 0
    for _ in range(100):
        rules = tuple(rng.sample(pool, rng.randint(1, 3)))
        read_unmatched = rng.random() < 0.5
        policy = Policy({"agent": rules}, read_unmatched=read_unmatched)
        for path in paths:
            segments = split(path)
            first = next((rule for rule in rules if _matches_by_rests(rule, segments)), None)
            want_allows = first.grants("read") if first is not None else read_unmatched
            want_matches = first is not None
            got_allows = policy.allows("agent", "read", path)
            got_matches = policy.matches("agent", path)
            names = [ascii(rule.pattern) for rule in rules]
            assert got_allows == want_allows, (
                f"rules {names} on path {ascii(path)}: allows {got_allows}, reference {want_allows}"
            )
            assert got_matches == want_matches, (
                f"rules {names} on path {ascii(path)}: matches {got_matches}, reference {want_matches}"
            )
            checks += 1
    assert checks >= 10_000, checks
