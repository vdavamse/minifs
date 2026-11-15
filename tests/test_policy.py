"""The compiled rule regex (``Rule.regex``, #77) without a database: unit tests of the pieces a lookup is built
from, and a differential test of every regex lookup against the reference matcher (``_rests``) over random
patterns and paths, built from the characters folding makes tricky (#93) and from the control characters a name
from before migration 016 can hold (#142)."""

from __future__ import annotations

import contextlib
import random
import re
import time
import warnings
from collections.abc import Callable, Iterable
from fnmatch import fnmatchcase
from functools import partial

from minifs.vfs.core import CONTROL_CHARACTERS, MAX_COMPONENTS, split, split_trusted
from minifs.vfs.policy import (
    Action,
    Policy,
    Rule,
    _exact_alts,  # pyright: ignore[reportPrivateUsage]
    _fold_pattern,  # pyright: ignore[reportPrivateUsage]
    _fold_segment,  # pyright: ignore[reportPrivateUsage]
    _folded_alts,  # pyright: ignore[reportPrivateUsage]
    _folded_class,  # pyright: ignore[reportPrivateUsage]
    _matches_by_rests,  # pyright: ignore[reportPrivateUsage]
    _path_string,  # pyright: ignore[reportPrivateUsage]
    _pattern_alts,  # pyright: ignore[reportPrivateUsage]
    _rests,  # pyright: ignore[reportPrivateUsage]
    _rule_rests,  # pyright: ignore[reportPrivateUsage]
    _segment_regex,  # pyright: ignore[reportPrivateUsage]
)

ACTIONS: tuple[Action, ...] = ("read", "write", "append")
ZWSP = chr(0x200B)  # zero-width space: a format character, dropped when a name folds
NEWLINE = chr(10)  # a control character: split refuses a path with one, split_trusted and the policy don't
SOH = chr(1)  # another control character, so a class fnmatch reads as "any character" is tried against one
FW_LOWER_A = chr(0xFF41)  # fullwidth "a": folds to "a"
FW_ASTERISK = chr(0xFF0A)  # fullwidth asterisk: folds to "*", so such a pattern segment isn't foldable
FW_SOLIDUS = chr(0xFF0F)  # fullwidth solidus: folds to "/", a NUL in the dual string
FW_DIGIT_FIVE = chr(0xFF15)  # fullwidth "5": inside the raw FW_SOLIDUS-FW_DIGIT_NINE range
FW_DIGIT_NINE = chr(0xFF19)  # fullwidth "9": folds to "9"
SHARP_S = chr(0xDF)  # folds to "ss"
FI_LIGATURE = chr(0xFB01)  # folds to "fi"
COMBINING_ACUTE = chr(0x301)
E_ACUTE = chr(0xE9)  # what "e" + COMBINING_ACUTE folds to: NFKC composes them
FW_UPPER_A = chr(0xFF21)  # fullwidth "A": folds to "a"
FW_UPPER_B = chr(0xFF22)
FW_UPPER_R = chr(0xFF32)  # fullwidth "R": folds to "r"
FW_UPPER_Z = chr(0xFF3A)  # fullwidth "Z": folds to "z"
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
ASCII_UPPER = [chr(cp) for cp in range(ord("A"), ord("Z") + 1)]
ASCII_LOWER = [chr(cp) for cp in range(ord("a"), ord("z") + 1)]
ASCII_DIGITS = [chr(cp) for cp in range(ord("0"), ord("9") + 1)]
FW_RANGE_CLASS = "[" + FW_UPPER_A + "-" + FW_UPPER_Z + "]"  # a fullwidth range: it gains "a"-"z" (#140)
# #148: an endpoint folds to "/", which _alt_regex spells as the NUL in both the pattern and the dual string.
SOLIDUS_RANGE_CLASS = "[" + FW_SOLIDUS + "-" + FW_DIGIT_NINE + "]"
CLASSES = [
    "[a-c]",
    "[!a]",
    "[A-Z]",
    "[]a]",
    "[!]x]",
    "[z-a]",
    "[!z-a]",
    "[!b-a]",
    "[a-cA-C]",  # #140: mixed-case members, so a folded class branch is exercised
    FW_RANGE_CLASS,  # #140: a fullwidth range, so a non-ASCII class body folds
    "[0-Z]",  # #148: a range spanning punctuation: the folded class must not widen past its members
    "[]-A-Z]",  # #148: a leading ] is a literal member, and the - after it parses as fnmatch parses it
    SOLIDUS_RANGE_CLASS,  # #148: an endpoint folds to "/": the dual string's NUL machinery is exercised
]
TOKENS = LITERALS + ["*", "?"] + CLASSES
NAMES = LITERALS + [
    "B",
    "SS",
    "ss",
    "FI",
    "fi",
    ".Env",
    "x" + ZWSP,
    "5",
    "9",
    FW_DIGIT_FIVE,
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


def test_fold_segment_enumerates_a_positive_class() -> None:
    """#140, #148: ``_fold_segment`` folds the literal runs of a hide rule's segment and replaces each positive
    class with the explicit list of the characters it matches once folded (``_folded_class``), so ``[A-Z]*`` also
    matches "readme" and nothing else, asserted by behaviour rather than by exact text. A negated class leaves
    the whole segment byte for byte only (None), a member that folds to several characters (the sharp s) adds
    nothing, a class that gains nothing stays as written, and a segment whose folded runs would gain glob
    syntax stays None."""
    folded = _fold_segment("[A-Z]*")
    assert folded is not None and folded.endswith("*")
    letters = folded[:-1]  # the explicit class: the 52 letters and no range that could span "_"
    assert all(fnmatchcase(c, letters) for c in ASCII_LOWER + ASCII_UPPER)
    assert not fnmatchcase("_", letters)
    plain = _fold_segment("[A-Z]")
    assert plain is not None and plain == letters
    assert _fold_segment("Sec[A-Z]") == "sec" + plain  # the literal run folds as well
    assert _fold_segment("[Ee]nv") == "[Ee]nv"  # e and E are both present, and E folds to e: nothing to add
    assert _fold_segment("[a-cA-C]") == "[a-cA-C]"  # A-C folds to a-c, which is a member already
    assert _fold_segment("[!A-Z]*") is None  # a negated class: the whole segment stays byte for byte only
    assert _fold_segment("[!R]*") is None
    assert _fold_segment("[!A-]") is None
    fw = _fold_segment(FW_RANGE_CLASS)
    assert fw is not None
    # the fullwidth range gains a-z, its members' folds, and nothing else
    assert all(fnmatchcase(c, fw) for c in [*ASCII_LOWER, FW_UPPER_A, FW_UPPER_Z])
    assert not fnmatchcase("A", fw) and not fnmatchcase("_", fw) and not fnmatchcase("-", fw)
    many = "[A-Z" + "".join(chr(b) + "-" + chr(b + 4000) for b in range(0x4E00, 0x4E00 + 5 * 4001, 4001)) + "]"
    assert _fold_segment(many) == many  # five 4000-wide ranges: over 16384 candidates in all, no folds gained
    wide = "[A-" + FW_UPPER_Z + "]x"  # a range wider than 4096: the class stays as written, gaining no folds
    assert _fold_segment(wide) == "[A-" + FW_UPPER_Z + "]x"
    sharp = "stra[" + SHARP_S + "]e"
    assert _fold_segment(sharp) == sharp  # the sharp s folds to "ss": matched only as written
    a_dash = _fold_segment("[A-]")
    assert a_dash is not None
    assert all(fnmatchcase(c, a_dash) for c in ("A", "a", "-"))  # the trailing - stays literal
    assert not fnmatchcase("_", a_dash)
    assert not Rule("/w/[A-]").matches("/w/_")  # [A-a] would have hidden _ and B-Z
    assert Rule("/w/[A-]").matches("/w/a") and Rule("/w/[A-]").matches("/w/-")
    assert _fold_segment("Secret") == "secret"  # a class-free segment folds whole, as before
    assert _fold_segment("**") is None  # a whole segment, not two wildcards
    assert _fold_segment("a" + FW_ASTERISK) is None  # the folded run gains a *
    assert _fold_segment(FW_LOWER_A + chr(0xFF3B)) is None  # the folded run gains a [
    assert _fold_segment("[A") is None  # a [ not closed in the segment: a rule refuses it before folding


def _oracle_class_chars(body: str) -> tuple[set[str] | None, list[str]]:
    """The characters a positive class ``body`` must match once folded, and its candidates, computed
    independently of ``_folded_class`` (the oracle of #148): the members are the candidates ``fnmatchcase``
    accepts for the class as written (its characters and every code point of each ``x-y`` triple in it), and
    the set gains the single-character folds of those members. A triple spanning more than 4096 code points
    leaves the class as written (None: no folds gained)."""
    token = "[" + body + "]"
    candidates = sorted(set(body))
    for start in range(len(body) - 2):
        x, dash, y = body[start : start + 3]
        if dash == "-":
            if ord(y) - ord(x) > 4096:
                return None, candidates
            candidates += [chr(cp) for cp in range(ord(x), ord(y) + 1)]
    members: set[str] = set()
    for c in candidates:
        if fnmatchcase(c, token):
            members.add(c)
    chars = set(members)
    for c in members:
        folded = _fold_pattern(c)
        if len(folded) == 1:
            chars.add(folded)
    return chars, candidates


def _check_emitted_class(body: str, probe: Iterable[str]) -> None:
    """Checks an emitted class two ways (#148): it matches a character of ``probe`` exactly where the oracle
    (``_oracle_class_chars``) holds that character, and it compiles through ``_segment_regex`` with the warnings
    as errors (no ``re.error``, no FutureWarning)."""
    chars, _ = _oracle_class_chars(body)
    emitted = _folded_class(body)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        re.compile(_segment_regex(emitted, "/"))
    assert chars is not None, ascii(body)
    for c in probe:
        assert fnmatchcase(c, emitted) == (c in chars), (ascii(body), ascii(emitted), ascii(c))


def test_folded_class_emits_an_explicit_list_of_its_members_and_their_folds() -> None:
    """#148: an emitted class matches exactly the members of the body and their single-character folds,
    checked two ways (``_check_emitted_class``), including the bodies whose structure the old splicing got
    wrong: a range spanning punctuation, a leading ``]``, a trailing ``-``, a fullwidth solidus endpoint and
    a ``!`` the sorted order would put first."""
    for body, probe in (
        ("A-Z", ASCII_LOWER + ASCII_UPPER + ["_", "^", "`", chr(0xC0)]),
        ("0-Z", ASCII_LOWER + ASCII_UPPER + ASCII_DIGITS + ["_", "^", "`"]),  # not [0-Z0-z]: no "_"
        ("A-", ["A", "a", "-", "_", "B"]),  # the trailing - stays literal, it can't start a range
        ("A!", ["A", "!", "a", "_", "z"]),  # the emitted list can't read as a negation
        ("]-A-Z", ["-", "Z", "z", "A", "b", "]"]),  # a leading ] parses as fnmatch parses it
        ("Za!--", ASCII_DIGITS + [".", ":", "!", "-", "Z", "a", "z", "b"]),
        (chr(0xC0) + "-" + chr(0xDE), [chr(0xC0), chr(0xD7), chr(0xE0), chr(0xF7), chr(0xDE), "a"]),
        (FW_SOLIDUS + "-" + FW_DIGIT_NINE, [FW_SOLIDUS, FW_DIGIT_NINE, FW_DIGIT_FIVE, "/", "9", "5", "0", "#"]),
        ("^" + FW_LOWER_A, ["^", "a", FW_LOWER_A, "b"]),  # translate escapes a leading ^ in the list
        ("[a", ["[", "a", "A", "b", "]"]),  # translate escapes a leading [ in the list
    ):
        _check_emitted_class(body, probe)


def test_folded_class_never_hides_more_than_its_members_and_their_folds() -> None:
    """#148 review regressions: splicing folded members and ranges into the class text could widen it:
    ``[0-Z]`` became ``[0-Z0-z]``, which matches ``_``; the folded range of U+00C0-U+00DE spanned U+00F7; a
    trailing ``-`` moved and joined another range; a reversed range gained what it never matched raw. Enumeration takes
    the membership from fnmatch itself, so it can lose an addition but never invent one."""

    def policy_for(pattern: str) -> Policy:
        return Policy({"agent": [Rule(pattern), Rule("/w/**", "read")]})

    def hides(pattern: str, *names: str) -> None:
        policy = policy_for(pattern)
        for name in names:
            assert not policy.allows("agent", "read", f"/w/{name}"), (ascii(pattern), ascii(name))

    def keeps(pattern: str, *names: str) -> None:
        policy = policy_for(pattern)
        for name in names:
            assert policy.allows("agent", "read", f"/w/{name}"), (ascii(pattern), ascii(name))

    keeps("/w/[0-Z]*", "_config", "^x", "`x")  # the old [0-Z0-z] spanned "_", "^" and "`"
    keeps("/w/[" + chr(0xC0) + "-" + chr(0xDE) + "]*", chr(0xF7) + "x")  # the old folded range spanned U+00F7
    keeps("/w/[" + chr(0xFF21) + "-Z]", *ASCII_LOWER)  # a reversed range matches nothing raw, gains nothing
    keeps("/w/[_-Z]", *ASCII_LOWER)  # likewise
    keeps("/w/[Za!--]", *ASCII_DIGITS, ".", ":")  # the old parse of "!--" moved the trailing "-"
    hides("/w/[]-A-Z]", "-", "Z", "z")  # what it matches raw, and the fold of its "Z"
    keeps("/w/[]-A-Z]", "b", "A")  # and nothing else: the raw class matches neither "b" nor "A"


def test_a_class_folding_to_a_solidus_agrees_with_the_reference_matcher() -> None:
    """#148: a fullwidth solidus member folds to "/" in the emitted list, which is fine for single characters:
    ``_alt_regex`` replaces "/" with the NUL in both the pattern and the dual string, and ``_rests`` compares
    "/" with "/": ``Rule.matches`` and ``_matches_by_rests`` must agree. The range spans the fullwidth
    solidus and digits, so the class gains "/" and "0"-"9"."""
    hide = Rule("/w/" + SOLIDUS_RANGE_CLASS)
    for path in ("/w/#", "/w/!", "/w/5", "/w/9", "/w/" + FW_DIGIT_FIVE, "/w/" + FW_SOLIDUS):
        assert hide.matches(path) == _matches_by_rests(hide, split_trusted(path)), ascii(path)
    assert hide.matches("/w/" + FW_DIGIT_FIVE)  # raw: the fullwidth 5 is inside the range as written
    assert hide.matches("/w/9")  # folded: the fullwidth 9 endpoint folds to "9"
    assert hide.matches("/w/5")  # folded: the fullwidth 5 is a member and folds to "5"
    assert not hide.matches("/w/#")  # "#" is in neither the raw range nor the emitted list


def test_folded_class_agrees_with_an_independent_enumeration() -> None:
    """The oracle property of ``_folded_class`` (#148): over 300 random positive bodies built from a pool of
    tricky characters and ranges between random pool characters, the emitted class matches a character exactly
    where the independently written enumeration (``_oracle_class_chars``) holds it (for every candidate and
    for 50 random characters outside them), and compiles through ``_segment_regex`` with the warnings as
    errors. Bodies ``Rule`` refuses are skipped, as main would never build them."""
    rng = random.Random(148)
    pool = [
        "a",
        "B",
        "m",
        "Z",
        "z",
        "0",
        "5",
        "9",
        "_",
        "-",
        "]",
        FW_LOWER_A,
        FW_UPPER_A,
        FW_UPPER_R,
        FW_UPPER_Z,
        SHARP_S,
        FI_LIGATURE,
        FW_SOLIDUS,
        FW_DIGIT_NINE,
        chr(0xC0),
        chr(0xDE),
        chr(0xF7),
        E_ACUTE,
        "e" + COMBINING_ACUTE,
    ]
    checked = wide = 0
    while checked < 300:
        parts: list[str] = []
        if rng.random() < 0.25:
            parts.append("]")  # sometimes a ] first, where it is a literal member
        for _ in range(rng.randint(1, 4)):
            if rng.random() < 0.4:
                parts.append(f"{rng.choice(pool)}-{rng.choice(pool)}")  # a range between random pool characters
            else:
                parts.append(rng.choice(pool))
        body = "".join(parts)
        try:
            Rule(f"/w/[{body}]")
        except ValueError:
            continue  # a body main refuses (a [: or a [^ it would read literally): the oracle doesn't judge it
        chars, candidates = _oracle_class_chars(body)
        emitted = _folded_class(body)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            re.compile(_segment_regex(emitted, "/"))
        if chars is None:  # a range wider than 4096: the class stays as written
            assert emitted == f"[{body}]", ascii(body)
            wide += 1
            checked += 1
            continue
        known = set(candidates)
        outside: list[str] = []
        while len(outside) < 50:
            cp = rng.choice(
                [rng.randint(1, 0x2FFF), rng.randint(0x3100, 0xD7FF), rng.randint(0xE000, 0x10FFFF)]
            )  # no surrogate
            if chr(cp) not in known:
                outside.append(chr(cp))
        for c in candidates + outside:
            assert fnmatchcase(c, emitted) == (c in chars), (ascii(body), ascii(emitted), ascii(c))
        checked += 1
    assert checked - wide >= 150, wide  # most bodies are enumerated, not left as written


def test_class_free_segments_fold_exactly_as_before() -> None:
    """#140 changed only segments with a class: a class-free segment must give exactly the alternatives the
    whole-segment fold of #93 gave, folded string included, so a hide rule never hides less than before. The
    old logic, copied here as the reference over generated segments of the literals' pool (which holds no "[",
    so every segment is class-free)."""

    def old_pattern_alts(segment: str) -> tuple[str, ...]:
        folded = _fold_pattern(segment)
        foldable = (
            "[" not in segment
            and not any(c in folded for c in "[]{}\\")
            and segment.count("*") == folded.count("*")
            and segment.count("?") == folded.count("?")
            and segment != "**"
        )
        return (segment, folded) if foldable else (segment,)

    rng = random.Random(140)
    tokens = LITERALS + ["*", "?"]
    for _ in range(3000):
        segment = "".join(rng.choice(tokens) for _ in range(rng.randint(1, 5)))
        assert _pattern_alts(segment) == old_pattern_alts(segment), ascii(segment)
    assert _pattern_alts("**") == ("**",) == old_pattern_alts("**")


def test_a_hide_rule_class_matches_the_folded_names() -> None:
    """#140, #148: a hide rule's positive class also matches the folded forms of what it lists, so ``[A-Z]*``
    hides readme too; a segment with a negated class matches byte for byte only, so it adds no case variants;
    a member that folds to several characters is matched only as written; and a grant rule stays byte-exact."""
    fw_readme = FW_UPPER_R + "eadme"  # a fullwidth R: it folds to "readme"

    def policy_for(pattern: str) -> Policy:
        return Policy({"agent": [Rule(pattern), Rule("/w/**", "read")]})

    def hides(pattern: str, *names: str) -> None:
        policy = policy_for(pattern)
        for name in names:
            assert not policy.allows("agent", "read", f"/w/{name}"), (ascii(pattern), ascii(name))

    def keeps(pattern: str, *names: str) -> None:
        policy = policy_for(pattern)
        for name in names:
            assert policy.allows("agent", "read", f"/w/{name}"), (ascii(pattern), ascii(name))

    hides("/w/[A-Z]*", "readme", "README", "Readme", fw_readme)  # the 52-letter list matches every "readme"
    keeps("/w/[A-Z]*", "_x")  # _ is in neither [A-Z] nor the enumerated list, raw or folded
    hides("/w/[!a-z]*", "README")  # byte for byte, R is not in [a-z] ...
    keeps("/w/[!a-z]*", "readme")  # ... and a negated class doesn't fold: no case variants
    hides("/w/[!A-Z]*", "readme")  # byte for byte, r is not in [A-Z] ...
    keeps("/w/[!A-Z]*", "README")  # ... and the segment is byte for byte only, where R is in [A-Z] (#148)
    hides("/w/stra[" + SHARP_S + "]e", "stra" + SHARP_S + "e")  # byte for byte ...
    keeps("/w/stra[" + SHARP_S + "]e", "strasse")  # ... the sharp s folds to "ss": matched only as written
    hides("/w/[Ee]nv", "ENV", "env")  # the folded [Ee]nv matches "env"
    grant = Rule("/w/[A-Z]*", "read")
    assert not grant.matches("/w/readme") and grant.matches("/w/Readme")  # a grant stays byte-exact


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


def test_rests_decides_deep_paths() -> None:
    """#145: ``_rests`` recursed once per path segment, so a deep enough path overflowed the stack. It now runs
    as a set of pattern positions advanced once per segment, so the issue's repro is decided at any depth: the
    ``/**`` read rule can grant below the deep nest (``allows_any`` is True), ``allows(subtree=True)`` answers
    without raising, and a full match through two globstars is still decided exactly."""
    policy = Policy({"a": [Rule("/workspace/**/.git/**/config"), Rule("/**", "read")]})
    for depth in (MAX_COMPONENTS - 1, MAX_COMPONENTS, 5000):  # at the stored-path limit, and past it
        deep = "/workspace" + "/.git" * depth
        assert policy.allows_any("a", "read", deep) is True, depth
        # the hide rule can still match below (a .git/**/config) and grants nothing: the subtree isn't readable
        assert policy.allows("a", "read", deep, subtree=True) is False, depth
    rule = Rule("/workspace/**/.git/**/config")
    matched = split_trusted("/workspace" + "/.git" * 5000 + "/config")
    assert _matches_by_rests(rule, matched)  # the two globstars absorb the nest; "config" ends it
    assert not _matches_by_rests(rule, split_trusted("/workspace" + "/.git" * 5000))  # no "config": no match


def _recursive_rests(
    pattern: tuple[tuple[str, ...], ...], path: tuple[tuple[str, ...], ...]
) -> frozenset[tuple[tuple[str, ...], ...]]:
    """The old recursive ``_rests``, copied here as the reference the iterative one (#145) is checked against.
    No cache: it is only ever called on the short paths ``_random_path`` builds, where the recursion is safe."""
    if not path:
        return frozenset([pattern])
    if not pattern:
        return frozenset()
    head = pattern[0]
    if head[0] == "**":
        return _recursive_rests(pattern[1:], path) | _recursive_rests(pattern, path[1:])
    if any(fnmatchcase(p, q) for p, q in zip(path[0], head, strict=False)):
        return _recursive_rests(pattern[1:], path[1:])
    return frozenset()


def test_rests_agree_with_the_recursive_matcher() -> None:
    """The iterative ``_rests`` (#145) returns exactly the frozensets the old recursive matcher returned: over
    2000 random patterns, each as a hide ``Rule(p)`` and a grant ``Rule(p, "read")`` (skipping the patterns
    ``Rule`` refuses), against a few random paths each. The exact frozensets are compared, not just the match
    booleans, so a rest that is present in one and absent in the other fails too. ``_rule_rests`` builds the
    path form itself (``_folded_alts`` for a hide rule, ``_exact_alts`` otherwise), and the reference is fed
    exactly that form."""
    # Fixed edge cases the generator doesn't draw: the root rule's empty pattern, and runs of globstars.
    for pattern_alts, path_alts in (
        ((), ()),
        ((), (("a",),)),
        ((("**",),), ()),
        ((("**",), ("**",)), (("a",), ("b",))),
        ((("a",), ("**",), ("**",)), (("a",),)),
    ):
        assert _rests(pattern_alts, path_alts) == _recursive_rests(pattern_alts, path_alts), (pattern_alts, path_alts)
    assert _rests((), (("a",),)) == frozenset()  # the root rule matches nothing below the root
    rng = random.Random(145)
    patterns = 0
    variants: tuple[tuple[Action, ...], ...] = ((), ("read",))  # the hide rule and the grant rule
    while patterns < 2000:
        pattern = _random_pattern(rng)
        rules: list[Rule] = []
        for actions in variants:
            with contextlib.suppress(ValueError):
                rules.append(Rule(pattern, *actions))
        if not rules:
            continue  # a pattern Rule refuses (as main would): nothing to compare
        patterns += 1
        for rule in rules:
            for _ in range(3):
                path = _random_path(rng)
                segments = split_trusted(path)
                path_alts = _folded_alts(segments) if rule.hides else _exact_alts(segments)
                want = _recursive_rests(rule.alternatives, path_alts)
                name = f"pattern {ascii(rule.pattern)} hides={rule.hides} on path {ascii(path)}"
                assert _rests(rule.alternatives, path_alts) == want, name
                assert _rule_rests(rule, segments) == want, name
