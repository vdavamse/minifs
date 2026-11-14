"""The compiled rule regex (``Rule.regex``, #77) without a database: unit tests of the pieces a lookup is built
from, and a differential test of every regex lookup against the reference matcher (``_rests``) over random
patterns and paths, built from the characters folding makes tricky (#93) and from the control characters a name
from before migration 016 can hold (#142)."""

from __future__ import annotations

import contextlib
import random
import time
from collections.abc import Callable
from functools import partial

from minifs.vfs.core import CONTROL_CHARACTERS, split, split_trusted
from minifs.vfs.policy import (
    Action,
    Policy,
    Rule,
    _matches_by_rests,  # pyright: ignore[reportPrivateUsage]
    _path_string,  # pyright: ignore[reportPrivateUsage]
)

ACTIONS: tuple[Action, ...] = ("read", "write", "append")
ZWSP = chr(0x200B)  # zero-width space: a format character, dropped when a name folds
NEWLINE = chr(10)  # a control character: split refuses a path with one, split_trusted and the policy don't
SOH = chr(1)  # another control character, so a class fnmatch reads as "any character" is tried against one
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
CLASSES = ["[a-c]", "[!a]", "[A-Z]", "[]a]", "[!]x]", "[z-a]", "[!z-a]", "[!b-a]"]
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
    NEWLINE,
    SOH,
]


def _random_pattern(rng: random.Random) -> str:
    """A random rule pattern of 1-4 segments of 1-3 tokens each, or a whole ``**`` segment."""
    segments = [
        "**" if rng.random() < 0.2 else "".join(rng.choice(TOKENS) for _ in range(rng.randint(1, 3)))
        for _ in range(rng.randint(1, 4))
    ]
    return "/" + "/".join(segments)


def _random_path(rng: random.Random) -> str:
    """A random normalised path of 0-4 segments, each name 1-2 tokens of the names' pool, which holds control
    characters too: a name from before migration 016 can hold one (#142)."""
    return "/" + "/".join(
        "".join(rng.choice(NAMES) for _ in range(rng.randint(1, 2))) for _ in range(rng.randint(0, 4))
    )


def _unnormalised(rng: random.Random, path: str) -> str:
    """``path`` spelled another way that names the same file: a random one of its slashes as ``//``, ``/./`` or a
    ``/x/../`` round trip, or a trailing slash. What ``split_trusted`` gives is unchanged, so the lookups, which
    normalise a path first (``_path_string``), must answer as they do for ``path`` itself."""
    if rng.random() < 0.25:
        return path + "/"
    slash = rng.choice([i for i, c in enumerate(path) if c == "/"])
    return path[:slash] + rng.choice(("//", "/./", "/x/../")) + path[slash + 1 :]


def _rules_for(pattern: str) -> list[Rule]:
    """The hide and the grant rules for ``pattern``, skipping what ``Rule`` refuses as main does: ValueError for
    glob syntax fnmatch would read literally. Compiling the regex must never raise (a rule main loads must still
    load), so ``re.error`` isn't suppressed."""
    rules: list[Rule] = []
    variants: tuple[tuple[Action, ...], ...] = ((), ("read",), ("write",), ("read", "append"))
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


def test_a_negated_empty_range_matches_a_newline() -> None:
    """``[!z-a]``: fnmatch translates a negated empty range to "any character", which its ``(?s:...)`` group makes
    a newline too. A name from before migration 016 can hold one, and the policy's lookups take a path unchecked
    (``split_trusted``), so the hide rule must hide that name (#142). ``Rule.matches`` isn't tried here: it checks
    the path first (``split``) and refuses a control character."""
    hide = Rule("/w/[!z-a]env")
    rules = [hide, Rule("/w/**", "read")]
    policy = Policy({"agent": rules})
    for hidden in (f"/w/{NEWLINE}env", f"/w/{SOH}env"):
        segments = split_trusted(hidden)
        assert _matches_by_rests(hide, segments), ascii(hidden)  # the reference hides it
        first = next((rule for rule in rules if _matches_by_rests(rule, segments)), None)
        assert first is hide, ascii(hidden)
        assert policy.matches("agent", hidden) == (first is not None), ascii(hidden)
        assert not policy.allows("agent", "read", hidden), ascii(hidden)  # the hide rule grants nothing
        assert not policy.allows("agent", "write", hidden), ascii(hidden)


def _best_of(call: Callable[[], bool], runs: int = 5, bound: float = 0.05) -> tuple[bool, float]:
    """``call()``'s answer and its fastest time over up to ``runs`` runs, so a preempted runner or a GC pause in one
    run can't fail a timing bound. It stops at the first run under ``bound``, and after a run twenty times over it
    (backtracking, not noise), so a regression fails in one slow run instead of five."""
    best = float("inf")
    answer = False
    for _ in range(runs):
        started = time.perf_counter()
        answer = call()
        best = min(best, time.perf_counter() - started)
        if best < bound or best > 20 * bound:
            break
    return answer, best


def test_deep_paths_dont_backtrack_across_segments() -> None:
    """Several ``**``, or many segments, against a deep path the agent picks that nearly matches: each segment is
    an atomic group and each ``**`` but the last an atomic lazy one (``_rule_regex``), so the matcher doesn't try
    every way of sharing the segments between the globstars, or both branches of every foldable hide segment.
    ``/**/a/**/a/**/a/**/b`` against 200 ``/a`` took 47 s before (#142)."""
    deep_a = "/a" * 200 + "/c"
    cases = (
        ("/**/a/**/a/**/a/**/b", deep_a),
        ("/**/a/**/a/**/b", deep_a),
        ("/w" + "/*" * 20 + "/x", "/w" + "/a" * 20 + "/y"),
        ("/workspace/**/.git/**/config", "/workspace" + "/.git" * 200 + "/x"),
        ("/**/*/*/*.env", "/a" * 200 + "/x"),
    )
    for pattern, path in cases:
        for rule in (Rule(pattern), Rule(pattern, "read")):
            name = f"{ascii(pattern)} hides={rule.hides}"
            want = _matches_by_rests(rule, split_trusted(path))
            policy = Policy({"agent": [rule]})
            got, took = _best_of(partial(policy.matches, "agent", path))
            assert got == want, name
            assert took < 0.05, f"{name} took {took * 1000:.1f} ms"


def test_many_stars_dont_backtrack() -> None:
    """Several stars and a long name that nearly matches: each star but the last goes in an atomic group with a
    lazy wildcard (``_segment_regex``, as ``fnmatch.translate`` spells it), so the matcher takes the leftmost
    match of each piece instead of trying every way of sharing the name between the stars. Plain greedy stars
    took seconds on these; the reference matcher is fast because fnmatch compiles the same way (#142)."""
    path = "/w/" + "a" * 250
    for rule in (Rule("/w/*a*a*a*a*b"), Rule("/w/*a*a*a*a*b", "read"), Rule("/w/*_*_*.env")):
        name = ascii(rule.pattern)
        segments = split_trusted(path)
        want = _matches_by_rests(rule, segments)
        assert not want, name  # the name holds neither a "b" nor a "_", so no rule matches it

        got, took = _best_of(partial(rule.matches, path))
        assert got == want, name
        assert took < 0.05, f"{name} took {took * 1000:.1f} ms"

        rules = (rule, Rule("/w/**", "read"))  # a rule that does match, so allows answers as the first rule
        policy = Policy({"agent": list(rules)})
        first = next((other for other in rules if _matches_by_rests(other, segments)), None)
        for action in ACTIONS:
            want_allows = first.grants(action) if first is not None else False
            got_allows, took = _best_of(partial(policy.allows, "agent", action, path))
            assert got_allows == want_allows, f"{name} {action}"
            assert took < 0.05, f"{name} {action} took {took * 1000:.1f} ms"


def test_rule_regexes_agree_with_the_reference_matcher() -> None:
    """Over 20,000 (rule, path) pairs: ``Rule.matches``, answered by one ``fullmatch`` of the compiled regex,
    decides exactly as the reference matcher ``_matches_by_rests``, for hide and grant rules alike (#77, #93).
    The paths hold no control character: ``Rule.matches`` checks the path it is given (``split``) and refuses one,
    so the names with a control character are the policy differential's business, which doesn't check (#142)."""
    rng = random.Random(77)
    rules: list[Rule] = []
    while len(rules) < 80:
        rules.extend(_rules_for(_random_pattern(rng)))
    paths = [p for p in (_random_path(rng) for _ in range(350)) if not CONTROL_CHARACTERS.search(p)]
    checks = 0
    for rule in rules:
        for path in paths:
            by_regex = rule.matches(path)
            by_rests = _matches_by_rests(rule, split_trusted(path))
            assert by_regex == by_rests, (
                f"pattern {ascii(rule.pattern)} on path {ascii(path)}: regex {by_regex}, rests {by_rests}"
            )
            checks += 1
    assert checks >= 20_000, checks


def test_policy_lookups_agree_with_the_reference_matcher() -> None:
    """``Policy.allows`` (without ``subtree``) and ``Policy.matches``, answered by the compiled regexes, decide
    exactly as the same first-match-wins loop over the reference matcher, for random 1-3-rule policies and the
    three actions. The lookups take a path unchecked (``split_trusted``), so a path may hold a control character,
    and a fraction of them is looked up in a spelling ``_path_string`` has to normalise first (#142)."""
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
        names = [ascii(rule.pattern) for rule in rules]
        for path in paths:
            looked_up = (path, _unnormalised(rng, path)) if rng.random() < 0.5 else (path,)
            for looked in looked_up:
                segments = split_trusted(looked)
                first = next((rule for rule in rules if _matches_by_rests(rule, segments)), None)
                want_matches = first is not None
                got_matches = policy.matches("agent", looked)
                assert got_matches == want_matches, (
                    f"rules {names} on path {ascii(looked)}: matches {got_matches}, reference {want_matches}"
                )
                for action in ACTIONS:
                    want_allows = first.grants(action) if first is not None else (action == "read" and read_unmatched)
                    got_allows = policy.allows("agent", action, looked)
                    assert got_allows == want_allows, (
                        f"rules {names} on path {ascii(looked)}: allows {action} {got_allows}, reference {want_allows}"
                    )
                checks += 1
    assert checks >= 10_000, checks


def test_star_and_globstar_heavy_rules_agree_with_the_reference_matcher() -> None:
    """The differential again, with patterns heavy in ``*`` and ``**`` and deeper paths, so the atomic groups
    (each star but the last, each ``**`` but the last, every segment) are exercised on paths that match as well
    as on near misses (#142)."""
    rng = random.Random(142)
    tokens = [*TOKENS, *(["*"] * 6), "a", "b", FW_SOLIDUS]
    # A small alphabet, so deep paths often match, and no control characters, which Rule.matches refuses.
    names = ["a", "b", "ab", "A", FW_SOLIDUS, "x", SHARP_S]
    checks = globstars = deep_matches = 0
    while checks < 20_000:
        segments = [
            "**" if rng.random() < 0.35 else "".join(rng.choice(tokens) for _ in range(rng.randint(1, 4)))
            for _ in range(rng.randint(1, 6))
        ]
        pattern = "/" + "/".join(segments)
        several = segments.count("**") >= 2
        globstars += several
        rules: list[Rule] = []
        variants: tuple[tuple[Action, ...], ...] = ((), ("read",))  # Rule.matches ignores the actions: one grant
        for actions in variants:
            with contextlib.suppress(ValueError):
                rules.append(Rule(pattern, *actions))
        for rule in rules:
            for _ in range(20):
                path = "/" + "/".join(
                    "".join(rng.choice(names) for _ in range(rng.randint(1, 2))) for _ in range(rng.randint(0, 7))
                )
                want = _matches_by_rests(rule, split_trusted(path))
                assert rule.matches(path) == want, f"pattern {ascii(rule.pattern)} on path {ascii(path)}: rests {want}"
                checks += 1
                deep_matches += several and want
    assert deep_matches >= 500, deep_matches  # matches through two or more **, where a middle ** must stop right
    assert globstars >= 100, globstars  # enough patterns with two or more ** to exercise the atomic lazy groups


def test_a_path_holding_a_nul_is_decided_by_the_reference_matcher() -> None:
    """No stored name holds a NUL, which the dual string uses as its separator, so such a path goes through the
    reference matcher and is answered as before #142, in both directions."""
    nul = chr(0)
    policy = Policy({"agent": [Rule("/w/[s]?cret"), Rule("/w/.env"), Rule("/w/**", "read")]})
    for path in (f"/w/s{nul}cret", f"/w/.env{nul}x", f"/w/a{nul}b"):
        segments = split_trusted(path)
        first = next((rule for rule in policy.rules["agent"] if _matches_by_rests(rule, segments)), None)
        assert policy.matches("agent", path) == (first is not None), ascii(path)
        assert policy.allows("agent", "read", path) == (first is not None and first.grants("read")), ascii(path)
