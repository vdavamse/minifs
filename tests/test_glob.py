"""The glob translator and pruning hints (minifs.vfs.glob), and the walker vfs_glob."""

import random
import re
import subprocess
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from psycopg import AsyncConnection

from minifs import _regex
from minifs.vfs.core import VFSError
from minifs.vfs.glob import GlobError, GlobPlan, compile_excludes, compile_glob, glob, glob_to_regex
from tests.test_tree import create, mkdir_p

# The oracle exists only in development builds; test_regex_extension.py fails without it.
_needs_oracle = pytest.mark.skipif(not _regex.HAS_ORACLE, reason="minifs._regex was built without the oracle feature")

# Translator

# Paths that a pattern must and must not match, with the regex evaluated by Python (see _python_match).
SEMANTICS: list[tuple[str, list[str], list[str]]] = [
    ("*", ["a", "a/b", ".x"], []),
    ("*.md", ["a.md", "x/a.md", "x/y/.md"], ["a.mdx", "a.md/b"]),
    ("?", ["a", "x/é", "x/😀", "\n"], ["ab", ""]),
    ("src/*.py", ["src/a.py"], ["x/src/a.py", "src/a/b.py"]),
    ("/src/*.py", ["src/a.py"], ["x/src/a.py"]),
    ("/a", ["a"], ["x/a"]),
    ("**/*.py", ["a.py", "x/y/a.py"], ["a.pyc"]),
    ("src/**", ["src/a", "src/a/b"], ["src", "x/src/a"]),
    ("a/**/b", ["a/b", "a/x/y/b"], ["a/xb", "b"]),
    ("a**b", ["ab", "axxb", "x/ab"], ["a/b"]),
    ("[abc]", ["a", "x/c"], ["d"]),
    ("[!x]", ["a", "é"], ["x"]),
    ("[^x]", ["a"], ["x"]),
    ("/a[!x]b", ["acb"], ["a/b", "axb"]),  # rg's negated class matches '/', ours never
    ("/a[.-0]b", ["a.b", "a0b"], ["a/b"]),  # nor does a range across it
    ("[]]", ["]"], ["a"]),
    ("[[:alpha:]]", ["[]", ":]", "a]"], ["a"]),  # no POSIX classes in rg: '[' ':' 'a' ... then a ']'
    ("[é-ö]", ["é", "ö", "ò"], ["e", "😀"]),  # characters, not bytes
    ("[😀-😂]", ["😁"], ["😃"]),
    ("*.{md,txt}", ["a.md", "a.txt"], ["a.rs"]),
    ("{a,{b,c}}", ["a", "c"], ["d"]),  # nested braces (rg 14.1 rejects them, globset 0.4.18 accepts)
    ("{a/b,c}", ["a/b", "c"], ["x/a/b", "x/c"]),  # a '/' anywhere anchors the pattern
    ("a{b,}", ["ab"], ["a"]),  # empty alternatives are dropped
    ("x{**,y}/z", ["x/z", "xq/z", "xy/z"], ["x/q/z"]),  # ** opening an alternative is a *
    (r"\*", ["*"], ["a"]),
    (r"\a", ["a"], ["\\a"]),
    ("a+b(c)$", ["a+b(c)$"], ["aab(c)"]),
    ("é", ["é"], ["e"]),
    ("a b ", ["a b"], ["a b "]),  # rg drops trailing whitespace
    ("a\\ ", ["a "], ["a"]),  # unless escaped
    ("\\!a", ["!a"], ["a"]),
    ("\\#a", ["#a"], ["a"]),
    ("a\nb", ["a\nb"], ["ab"]),  # names may contain newlines
]


@pytest.mark.parametrize(("pattern", "matches", "non_matches"), SEMANTICS)
def test_translation(pattern: str, matches: list[str], non_matches: list[str]) -> None:
    regex = glob_to_regex(pattern)
    assert regex.startswith("^") and regex.endswith("$")
    assert {p: _python_match(regex, p) for p in matches + non_matches} == {
        **dict.fromkeys(matches, True),
        **dict.fromkeys(non_matches, False),
    }


def test_regex_text() -> None:
    any_char = "[\\u0001-\U0010ffff]"
    not_slash = "[\\u0001-\\u002E0-\U0010ffff]"
    assert glob_to_regex("*.md") == f"^(?:/?|{any_char}*/){not_slash}*\\.md$"
    assert glob_to_regex("src/{a,b}/*.py") == f"^src/[ab]/{not_slash}*\\.py$"
    assert glob_to_regex("/[.-0]") == "^[\\u002E0]$"
    assert glob_to_regex("/[/]") == "^(?:(?!))$"  # a class without '/' can be empty
    assert glob_to_regex("/a\nb") == r"^a\u000Ab$"
    assert glob_to_regex("x/{a,b}{c,dd}") == "^x/[ab](?:c|dd)$"
    # Braces stay inline, so they cost their length, not the product of their alternatives.
    assert glob_to_regex("/" + "{a,b}" * 50) == "^" + "[ab]" * 50 + "$"


# Rejected by globset, with its message, so by rg too (test_rejections_match_rg).
SYNTAX_ERRORS = [
    ("[ab", "unclosed character class; missing ']'"),
    ("[]", "unclosed character class; missing ']'"),
    ("[!]", "unclosed character class; missing ']'"),
    ("{a,b", "unclosed alternate group; missing '}' (maybe escape '{' with '[{]'?)"),
    ("{a,[}]", "unclosed alternate group; missing '}' (maybe escape '{' with '[{]'?)"),  # [}] is a class
    ("a}", "unopened alternate group; missing '{' (maybe escape '}' with '[}]'?)"),
    ("[z-a]", "invalid range; 'z' > 'a'"),
    ("a\\", "dangling '\\'"),
]

OTHER_ERRORS = [
    ("", "empty pattern"),
    ("  ", "empty pattern"),
    ("/", "empty pattern"),
    ("a\0b", "NUL character"),
    ("!a", "a leading '!' negates the glob in rg; pass it as an exclude, or escape it as '\\!'"),
    ("#a", "a leading '#' makes the glob a comment, which rg ignores; escape it as '\\#'"),
    ("x" * 65 * 1024, "glob is too long (65 KB, the limit is 64 KB)"),
    ("{" * 65 + "a" + "}" * 65, "braces nested too deeply"),
    ("{" * 20_000 + "a" + "}" * 20_000, "braces nested too deeply"),  # refused before globset recurses
    (
        "?" * 5000,
        "glob is too large (5005 characters and classes once translated, the limit is 4096); use a shorter pattern",
    ),
    ("src/", "a pattern ending in '/' matches only directories, and only files are listed"),
    ("**/", "a pattern ending in '/' matches only directories, and only files are listed"),
    ("src\\/", "a pattern ending in '/' matches only directories, and only files are listed"),
]


@pytest.mark.parametrize(("pattern", "message"), SYNTAX_ERRORS + OTHER_ERRORS)
def test_bad_patterns(pattern: str, message: str) -> None:
    with pytest.raises(GlobError) as info:
        compile_glob(pattern)
    assert info.value.message == message
    assert str(info.value) == f"error parsing glob '{pattern}': {message}"


# rg 14.1 (what apt installs) bundles an older globset, which drops a stray '}' (README: Divergences).
@pytest.mark.parametrize(("pattern", "message"), [e for e in SYNTAX_ERRORS if e[0] != "a}"])
def test_rejections_match_rg(pattern: str, message: str, rg_path: str, tmp_path: Path) -> None:
    result = subprocess.run([rg_path, "--files", "-g", pattern], cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 2
    assert f"error parsing glob '{pattern}': {message}" in result.stderr


@pytest.mark.parametrize(
    ("pattern", "plan"),
    [
        ("*.md", GlobPlan(glob_to_regex("*.md"), name_like="%.md")),
        ("README.md", GlobPlan(glob_to_regex("README.md"), name_like="README.md")),
        ("docs/guides/**/*.md", GlobPlan(glob_to_regex("docs/guides/**/*.md"), ("docs", "guides"), None, "%.md")),
        ("src/*/*.py", GlobPlan(glob_to_regex("src/*/*.py"), ("src",), 2, "%.py")),
        ("src/a.py", GlobPlan(r"^src/a\.py$", ("src",), 1, "a.py")),
        ("src/**", GlobPlan(glob_to_regex("src/**"), ("src",), None, None)),
        ("src/*.[ch]", GlobPlan(glob_to_regex("src/*.[ch]"), ("src",), 1, "%._")),
        ("{a,b}", GlobPlan(glob_to_regex("{a,b}"), name_like="_")),  # one character
        ("{ab,c}", GlobPlan(glob_to_regex("{ab,c}"))),  # % filters nothing
        ("{src,lib}/x_1%.py", GlobPlan(r"^(?:src|lib)/x_1%\.py$", (), 2, r"x\_1\%.py")),
        ("{src/a,src/b/c}/*.py", GlobPlan(glob_to_regex("{src/a,src/b/c}/*.py"), ("src",), 3, "%.py")),
        ("{src/a,src/b}/c", GlobPlan(r"^(?:src/a|src/b)/c$", ("src",), 2, "c")),
        ("src/*.{md,txt}", GlobPlan(glob_to_regex("src/*.{md,txt}"), ("src",), 1, "%.%")),
        ("a?b**c", GlobPlan(glob_to_regex("a?b**c"), name_like="a_b%c")),
        ("a/{b,c/}d", GlobPlan(glob_to_regex("a/{b,c/}d"), ("a",), 2, "%d")),
    ],
)
def test_hints(pattern: str, plan: GlobPlan) -> None:
    assert compile_glob(pattern) == plan


def test_exclude_regexes() -> None:
    excludes = compile_excludes(["**/node_modules/**", "**/.venv/**", "*.log", "**/a*/**", "build/", "a/b/"])
    assert excludes.prune_dirs == ("node_modules", ".venv", "build")
    # Directories are tested as "path/": every exclude also matches them, the ones ending in '/' only them.
    log, a_star, a_b = excludes.regexes
    assert (log, a_b) == (glob_to_regex("*.log")[:-1] + "(?<!/)/?$", "^a/b/$")
    assert [_python_match(a_star, p) for p in ["x/ab/c", "ab/", "x/ab/c/", "ba/c"]] == [True, False, True, False]
    # A directory isn't excluded by a match of its path plus '/' (rg never tests that).
    assert [_python_match(compile_excludes(["{a/,b}"]).regexes[0], p) for p in ["a/", "a", "b/", "b"]] == [
        False,
        False,
        True,
        True,
    ]
    unpruned = compile_excludes(["**/node_modules/**", "build/"], prune=False)
    assert unpruned.prune_dirs == ()
    assert [_python_match(unpruned.regexes[1], p) for p in ["build/", "x/build/", "build", "xbuild/"]] == [
        True,
        True,
        False,
        False,
    ]
    assert compile_excludes(["build\\/"]).regexes == unpruned.regexes[1:]  # an escaped '/' is one too
    # What follows rg's -g '!': a leading '!' or '#' is a character.
    assert [_python_match(compile_excludes(["#x", "!y"]).regexes[i], p) for i, p in [(0, "a/#x"), (1, "!y")]] == [
        True,
        True,
    ]
    assert compile_excludes(["!x/"]).prune_dirs == ("!x",)
    for bad in ["/", "[a", "{a"]:
        with pytest.raises(GlobError):
            compile_excludes([bad])
    with pytest.raises(TypeError):
        compile_excludes("*.log")


# Differential: the translator against ripgrep's own matcher (`ignore`'s overrides, through the extension's
# oracle), on generated patterns and paths.
#
# The oracle matches bytes, the translation characters, and rg's classes can match '/'. So a class goes to the
# oracle without '/': a negated one with '/' added ([!a] as [!a/], and **/ in front if that adds the pattern's
# first '/', which would anchor it), a range across '/' split around it. For an ASCII pattern, every non-ASCII
# character of the path reaches the oracle as a 'z', which no pattern mentions: then one byte is one character.
# A non-ASCII pattern with '?' or a class that could meet a non-ASCII character is compared on ASCII paths only.

_ASCII_NAME_CHARS = ["a", "b", "c", ".", "-", "*", "?", "[", "]", "{", "}", ",", "\\", "!", "#", " ", "\n"]
_NAME_CHARS = [*_ASCII_NAME_CHARS, "😁", "é", "ö", "日"]
_LITERALS = ["a", "b", "c", "é", "日", ".", "-", r"\*", r"\?", r"\[", r"\{", r"\,", "\\\\", "!", "#", " "]
_TO_ORACLE = {
    "[!a]": "[!a/]", "[^b]": "[^b/]", "[!é-ö]": "[!é-ö/]", "[!]a]": "[!]a/]", "[+-0]": "[+-.0]", "[!+-0]": "[!+-0/]",
}  # fmt: skip
_CLASSES = ["[ab]", "[a-c]", "[é-ö]", "[😀-😂]", "[]a]", "[.-]", "[[:alpha:]]", "[*?]", "[\\]", *_TO_ORACLE]
_BYTEWISE = ["?", "[é-ö]", "[😀-😂]", "[!", "[^"]  # differ from rg on non-ASCII characters


def _gen_segment(rng: random.Random, depth: int, nested: bool) -> str:
    out: list[str] = []
    for _ in range(rng.randint(1, 4)):
        r = rng.random()
        if r < 0.45:
            out.append(rng.choice(_LITERALS))
        elif r < 0.6:
            out.append(rng.choice(["*", "**"]))
        elif r < 0.7:
            out.append("?")
        elif r < 0.85:
            out.append(rng.choice(_CLASSES))
        elif depth == 0 or (nested and depth < 2):
            alts = [_gen_segment(rng, depth + 1, nested) for _ in range(rng.randint(1, 3))]
            if rng.random() < 0.2:
                alts.append(rng.choice(["", "**", "x/y"]))
            out.append("{" + ",".join(alts) + "}")
        else:
            out.append(rng.choice(_LITERALS))
    return "".join(out)


def _gen_pattern(rng: random.Random, nested: bool = True) -> str:
    segments: list[str] = []
    for i in range(rng.randint(1, 4)):
        globstar = rng.random() < (0.2 if i else 0.15) and (not segments or segments[-1] != "**")
        segments.append("**" if globstar else _gen_segment(rng, 0, nested))
    pattern = "/".join(segments)
    if rng.random() < 0.1:
        pattern = "/" + pattern
    if rng.random() < 0.05:
        pattern += rng.choice(["/", " ", "\\ ", "\\"])
    return pattern


def _gen_path(rng: random.Random, chars: Sequence[str]) -> str:
    names = ["".join(rng.choice(chars) for _ in range(rng.randint(1, 4))) for _ in range(rng.randint(1, 4))]
    return "/".join(n for n in names if n not in (".", "..")) or "a"


def _paths_for(rng: random.Random, pattern: str) -> list[str]:
    """Random paths, plus paths built from the pattern so that matches aren't rare."""
    ascii_only = not pattern.isascii() and any(token in pattern for token in _BYTEWISE)
    chars = _ASCII_NAME_CHARS if ascii_only else _NAME_CHARS
    paths = [_gen_path(rng, chars) for _ in range(20)]
    for _ in range(20):
        built: list[str] = []
        for segment in pattern.lstrip("/").split("/"):
            if segment == "**":
                built.extend(_gen_path(rng, chars).split("/")[: rng.randint(0, 2)])
                continue
            pool = [c for c in segment if c not in "*?[]{},\\!^:" and (c.isascii() or not ascii_only)] or ["a"]
            built.append("".join(rng.choice(pool) for _ in range(rng.randint(1, 4))))
        paths.append("/".join(n for n in built if n not in (".", "..")) or "a")
    return paths


def _oracle_pattern(pattern: str) -> str:
    oracle = pattern
    for ours, theirs in _TO_ORACLE.items():
        oracle = oracle.replace(ours, theirs)
    if "/" in oracle and "/" not in pattern:
        oracle = "**/" + oracle
    return oracle


def _oracle_path(pattern: str, path: str) -> str:
    return re.sub(r"[^\x00-\x7f]", "z", path) if pattern.isascii() else path


def _compiles(pattern: str) -> bool:
    try:
        compile_glob(pattern)
    except GlobError:
        return False
    return True


def _cases(seed: int, n: int, *, nested: bool = True) -> list[tuple[str, list[str]]]:
    rng = random.Random(seed)
    patterns = [p for p in (_gen_pattern(rng, nested) for _ in range(n)) if _compiles(p)]
    return [(p, _paths_for(rng, p)) for p in patterns]


def _python_match(regex: str, path: str) -> bool:
    # ARE's . and [^/] match a newline, Python's . only with DOTALL, and Python's $ also matches before a final
    # newline, ARE's only at the end (\Z). The \uXXXX escapes mean the same in both.
    return re.search(regex.removesuffix("$") + r"\Z", path, re.DOTALL) is not None


@_needs_oracle
def test_translator_agrees_with_rg() -> None:
    mismatches: list[tuple[str, str, bool]] = []
    for pattern, paths in _cases(seed=27, n=1500):
        regex = glob_to_regex(pattern)
        for path in paths:
            expected = _regex.oracle_glob_is_match(_oracle_pattern(pattern), _oracle_path(pattern, path))
            if _python_match(regex, path) != expected:
                mismatches.append((pattern, path, expected))
    assert mismatches[:20] == []


def _ancestors(path: str) -> list[str]:
    parts = path.split("/")
    return ["/".join(parts[:i]) for i in range(1, len(parts))]


@_needs_oracle
@pytest.mark.parametrize("prune", [True, False])
def test_excludes_agree_with_rg(prune: bool) -> None:
    # rg drops a file when an exclude matches it or one of its directories (which it then doesn't enter). vfs_glob
    # tests files with the regexes, and directories as "path/" with the regexes and by pruned name.
    mismatches: list[tuple[str, str, bool]] = []
    rng = random.Random(32)
    cases = _cases(seed=33, n=800)
    extra = [
        "**/node_modules/**", "build/", "{a/,b}", "b{a,/}", "{node_modules/,dist/}", "a/*", "a**/", "*/", "#a", "!a",
        "\\!a", "#*/", "!!a", "#",
    ]  # fmt: skip
    # Paths that the extras with a leading '!' or '#' (a character in an exclude) do and don't match.
    fixed = ["#a", "x/!a", "!a/y", "a", "!!a", "\\!a", "x/#a/z", "#", "#b/c", "!a"]
    for pattern, paths in [*cases, *((p, [*fixed, *_paths_for(rng, p)]) for p in extra)]:
        ex = compile_excludes([pattern], prune=prune)  # any include compiles as an exclude too
        oracle = _oracle_pattern(pattern)
        for path in paths:
            p = _oracle_path(pattern, path)
            expected = _regex.oracle_glob_is_match(oracle, p, exclude=True) or any(
                _regex.oracle_glob_is_match(oracle, d, is_dir=True, exclude=True) for d in _ancestors(p)
            )
            dirs = _ancestors(path)
            got = any(_python_match(r, path) for r in ex.regexes) or any(
                d.rsplit("/", 1)[-1] in ex.prune_dirs or any(_python_match(r, d + "/") for r in ex.regexes)
                for d in dirs
            )
            if got != expected:
                mismatches.append((pattern, path, expected))
    assert mismatches[:20] == []


@_needs_oracle
def test_translator_rejects_what_rg_rejects() -> None:
    rng = random.Random(28)
    disagreements: list[tuple[str, str]] = []
    for _ in range(3000):
        pattern = _gen_pattern(rng).replace("\\]", rng.choice(["]", "[", "{", "}"]))
        try:
            compile_glob(pattern)
            ours = "ok"
        except GlobError as e:
            ours = e.message
        try:
            _regex.oracle_glob_is_match(pattern, "a")
            theirs = "ok"
        except ValueError as e:
            theirs = str(e)
        # rg reports the original pattern: "error parsing glob '<pattern>': <message>". We refuse a few more.
        agree = theirs.endswith(f": {ours}") if theirs != "ok" else ours == "ok"
        ours_only = ours.startswith("a pattern ending in '/'") or ours == "empty pattern" or pattern[:1] in ("!", "#")
        if not agree and not ours_only:
            disagreements.append((pattern, f"{ours} / {theirs}"))
    assert disagreements[:20] == []


@_needs_oracle
def test_oracle_agrees_with_the_rg_binary(rg_path: str, tmp_path: Path) -> None:
    # The oracle is rg's matcher as a library; this checks it against the binary on a real tree. The apt rg
    # (14.1) rejects nested braces, so the patterns have none.
    rng = random.Random(29)
    files = {_gen_path(rng, _NAME_CHARS) for _ in range(300)}
    dirs = {f.rsplit("/", 1)[0] for f in files if "/" in f}
    dirs = {"/".join(d.split("/")[:i]) for d in dirs for i in range(1, d.count("/") + 2)}
    files -= dirs
    for f in files:
        (tmp_path / f).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / f).write_text("")
    mismatches: list[tuple[str, set[str], set[str]]] = []
    for pattern, _ in _cases(seed=30, n=150, nested=False):
        if pattern.endswith("/"):
            continue
        result = subprocess.run(
            [rg_path, "--files", "--hidden", "--no-ignore", "--null", "-g", pattern],
            cwd=tmp_path,
            capture_output=True,
        )
        assert result.returncode in (0, 1), (pattern, result.stderr)
        listed = {p for p in result.stdout.decode().split("\0") if p}
        expected = {f for f in files if _regex.oracle_glob_is_match(pattern, f)}
        if listed != expected:
            mismatches.append((pattern, listed - expected, expected - listed))
    assert mismatches[:10] == []


# Where the translation deliberately differs from rg: checked against the binary, so a change in rg shows up.
DIVERGENCES = [
    ("?.md", "é.md", True, False),  # ? is one character; rg's is one byte
    ("[!a].md", "é.md", True, False),
    ("/a[!x]b", "a/b", False, True),  # a class never matches '/'; rg's negated classes and ranges do
    ("a[!x]b", "a/b", False, True),  # even unanchored
    ("/a[/]b", "a/b", False, True),  # nor an explicit '/'
    ("/a[x/]b", "a/b", False, True),
    ("/a[.-0]b", "a/b", False, True),
]


@pytest.mark.parametrize(("pattern", "path", "ours", "rg"), DIVERGENCES)
def test_divergences_from_rg(pattern: str, path: str, ours: bool, rg: bool, rg_path: str, tmp_path: Path) -> None:
    assert _python_match(glob_to_regex(pattern), path) == ours
    (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / path).write_text("")
    result = subprocess.run([rg_path, "--files", "-g", pattern], cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode in (0, 1), result.stderr
    assert (result.stdout.strip() == path) == rg


def test_globset_version_skew_with_rg(rg_path: str, tmp_path: Path) -> None:
    # rg 14's bundled globset drops a stray '}' and rejects nested braces; ours (0.4.18+) does the reverse.
    version = subprocess.run([rg_path, "--version"], capture_output=True, text=True).stdout.split()[1]
    if not version.startswith("14."):
        pytest.skip(f"checks rg 14's globset, this is rg {version}")
    for name in ["a", "a}", "b"]:
        (tmp_path / name).write_text("")
    stray = subprocess.run([rg_path, "--files", "-g", "a}"], cwd=tmp_path, capture_output=True, text=True)
    assert stray.stdout.split() == ["a"]
    nested = subprocess.run([rg_path, "--files", "-g", "{a,{b}}"], cwd=tmp_path, capture_output=True, text=True)
    assert nested.returncode == 2 and "nested alternate groups are not allowed" in nested.stderr
    with pytest.raises(GlobError):
        compile_glob("a}")
    assert _python_match(glob_to_regex("{a,{b}}"), "b")


def test_glob_to_are() -> None:
    assert _regex.glob_to_are("src/*.py") == glob_to_regex("src/*.py")
    with pytest.raises(_regex.RegexError, match="matches only directories"):
        _regex.glob_to_are("src/")


def _like_regex(like: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(like):
        c = like[i]
        if c == "\\":
            out.append(re.escape(like[i + 1]))
            i += 1
        else:
            out.append({"%": ".*", "_": "."}.get(c, re.escape(c)))
        i += 1
    return "".join(out)


def test_hints_hold_for_every_match() -> None:
    # The hints are derived from the regex's syntax tree: every path the regex matches must satisfy them.
    violations: list[tuple[str, str, GlobPlan]] = []
    for pattern, paths in _cases(seed=31, n=1500):
        plan = compile_glob(pattern)
        for path in paths:
            if not _python_match(plan.regex, path):
                continue
            *dirs, name = path.split("/")
            ok = tuple(dirs[: len(plan.prefix)]) == plan.prefix
            ok &= plan.max_depth is None or len(dirs) + 1 - len(plan.prefix) <= plan.max_depth
            ok &= plan.name_like is None or re.fullmatch(_like_regex(plan.name_like), name, re.DOTALL) is not None
            if not ok:
                violations.append((pattern, path, plan))
    assert violations[:20] == []


async def test_postgres_agrees_with_the_translator(db_any_locale: str) -> None:
    # The same regexes evaluated by Postgres, on C-collated text as vfs_glob does, in every locale.
    cases = [(glob_to_regex(p), path) for p, paths in _cases(seed=9, n=300) for path in paths]
    async with await AsyncConnection.connect(db_any_locale, autocommit=True) as conn:
        cur = await conn.execute(
            'SELECT array_agg(path COLLATE "C" ~ regex ORDER BY i) FROM unnest(%s::text[], %s::text[])'
            " WITH ORDINALITY AS c(regex, path, i)",
            ([r for r, _ in cases], [p for _, p in cases]),
        )
        row = await cur.fetchone()
    assert row is not None
    got: list[bool] = row[0]
    mismatches = [(r, p) for (r, p), g in zip(cases, got, strict=True) if g != _python_match(r, p)]
    assert mismatches[:20] == []


# vfs_glob


async def _tree(conn: AsyncConnection, root: UUID | None, files: list[str]) -> dict[str, UUID]:
    """Creates each file (with its directories) below ``root``; returns path -> id."""
    ids: dict[str, UUID] = {}
    base = await _segments_of(conn, root)
    for path in files:
        *dirs, name = path.split("/")
        parent = await mkdir_p(conn, *base, *dirs)
        ids[path] = await create(conn, parent, name)
    return ids


async def _segments_of(conn: AsyncConnection, node: UUID | None) -> list[str]:
    segments: list[str] = []
    while node is not None:
        cur = await conn.execute("SELECT parent_id, name FROM vfs_nodes WHERE id = %s", (node,))
        row = await cur.fetchone()
        assert row is not None
        node = row[0]
        segments.insert(0, row[1])
    return segments


async def _paths(
    conn: AsyncConnection, root: UUID | None, pattern: str, *, excludes: Sequence[str] = (), hidden: bool = False
) -> set[str]:
    """The paths ``glob`` returns, checking that they don't depend on the pruning hints."""
    with_hints = await glob(conn, root, pattern, excludes=excludes, hidden=hidden)
    without = await glob(conn, root, pattern, excludes=excludes, hidden=hidden, hints=False)
    assert with_hints == without
    return {m.path for m in with_hints}


async def test_glob_matches_files_relative_to_the_root(conn: AsyncConnection) -> None:
    await _tree(conn, None, ["w/src/a.py", "w/src/pkg/b.py", "w/src/pkg/c.txt", "w/README.md", "other/x.py"])
    w = await mkdir_p(conn, "w")
    assert await _paths(conn, w, "*.py") == {"src/a.py", "src/pkg/b.py"}
    assert await _paths(conn, w, "src/*.py") == {"src/a.py"}
    assert await _paths(conn, w, "src/**/*.py") == {"src/a.py", "src/pkg/b.py"}
    assert await _paths(conn, w, "src/**") == {"src/a.py", "src/pkg/b.py", "src/pkg/c.txt"}
    assert await _paths(conn, w, "**") == {"src/a.py", "src/pkg/b.py", "src/pkg/c.txt", "README.md"}
    assert await _paths(conn, w, "src") == set()  # directories aren't returned
    assert await _paths(conn, None, "*.py") == {"w/src/a.py", "w/src/pkg/b.py", "other/x.py"}
    assert await _paths(conn, None, "w/src/*.py") == {"w/src/a.py"}


async def test_missing_prefix_gives_no_rows(conn: AsyncConnection) -> None:
    await _tree(conn, None, ["src/a.py", "file"])
    assert await _paths(conn, None, "nope/**/*.py") == set()
    assert await _paths(conn, None, "file/*.py") == set()  # the prefix is a file
    assert await _paths(conn, None, "src/nope/x") == set()


async def test_hidden_directories_are_not_walked(conn: AsyncConnection) -> None:
    await _tree(conn, None, [".github/ci.yml", ".foo.yml", "a/.b/c.yml", "a/d.yml"])
    assert await _paths(conn, None, "*.yml") == {".foo.yml", "a/d.yml"}
    assert await _paths(conn, None, ".github/**") == set()
    assert await _paths(conn, None, ".github/ci.yml") == set()
    assert await _paths(conn, None, "*.yml", hidden=True) == {".github/ci.yml", ".foo.yml", "a/.b/c.yml", "a/d.yml"}
    assert await _paths(conn, None, ".github/**", hidden=True) == {".github/ci.yml"}
    # The root itself may be hidden: it was asked for.
    github = await mkdir_p(conn, ".github")
    assert await _paths(conn, github, "*") == {"ci.yml"}


async def test_git_is_never_walked(conn: AsyncConnection) -> None:
    await _tree(conn, None, [".git/config", "a/.git/HEAD", "a/b.txt", "x/.git"])
    assert await _paths(conn, None, "**", hidden=True) == {"a/b.txt", "x/.git"}  # a .git file is a file
    assert await _paths(conn, None, ".git/*", hidden=True) == set()


async def test_excludes(conn: AsyncConnection) -> None:
    await _tree(conn, None, ["src/a.py", "src/node_modules/m.py", "node_modules/n.py", "src/b.log", "build/c.py"])
    assert await _paths(conn, None, "*.py", excludes=["**/node_modules/**"]) == {"src/a.py", "build/c.py"}
    assert await _paths(conn, None, "**", excludes=["*.log", "build"]) == {
        "src/a.py",
        "src/node_modules/m.py",
        "node_modules/n.py",
    }
    # A prefix through an excluded directory gives nothing, as the full walk would.
    assert await _paths(conn, None, "node_modules/*.py", excludes=["**/node_modules/**"]) == set()
    assert await _paths(conn, None, "build/*.py", excludes=["build"]) == set()
    # A trailing '/' matches only directories, at any depth unless the pattern has another '/'.
    await _tree(conn, None, ["a/build/d.py", "a/x/build"])
    everything = await _paths(conn, None, "**")
    assert everything - await _paths(conn, None, "**", excludes=["build/", "node_modules/"]) == {
        "src/node_modules/m.py",
        "node_modules/n.py",
        "build/c.py",
        "a/build/d.py",
    }  # a/x/build is a file
    assert everything - await _paths(conn, None, "**", excludes=["a/build/"]) == {"a/build/d.py"}
    # Excludes match paths relative to the search root.
    src = await mkdir_p(conn, "src")
    assert await _paths(conn, src, "*", excludes=["src/*"]) == {"a.py", "node_modules/m.py", "b.log"}
    with pytest.raises(TypeError):
        await glob(conn, None, "*", excludes="*.log")


async def test_newest_first_and_limit(conn: AsyncConnection) -> None:
    ids = await _tree(conn, None, ["a", "b", "c", "d/e"])
    base = datetime(2026, 1, 1, tzinfo=UTC)
    for i, path in enumerate(["b", "d/e", "a"]):
        await conn.execute("UPDATE vfs_nodes SET mtime = %s WHERE id = %s", (base + timedelta(days=i), ids[path]))
    await conn.execute("UPDATE vfs_nodes SET mtime = %s WHERE id = %s", (base, ids["c"]))  # ties b: path order
    matches = await glob(conn, None, "**")
    assert [m.path for m in matches] == ["a", "d/e", "b", "c"]
    assert matches[0].id == ids["a"] and matches[0].size == 0 and matches[0].mtime == base + timedelta(days=2)
    assert [m.path for m in await glob(conn, None, "**", limit=2)] == ["a", "d/e"]
    with pytest.raises(ValueError):
        await glob(conn, None, "**", limit=-1)


async def test_bad_root(conn: AsyncConnection) -> None:
    file = await create(conn, None, "f")
    missing = uuid.uuid4()
    for root, errno in [(file, "ENOTDIR"), (missing, "ENOENT")]:
        with pytest.raises(VFSError) as info:
            await glob(conn, root, "*")
        # Returned by vfs_glob, not raised.
        assert (info.value.errno, info.value.path, info.value.db_error) == (errno, str(root), None)
        assert info.value.detail


async def test_classes_on_non_ascii_names(db_any_locale: str) -> None:
    async with await AsyncConnection.connect(db_any_locale, autocommit=True) as conn:
        await _tree(conn, None, ["é.md", "E.md", "日本.md", "ö/x.md", "a.md"])
        assert await _paths(conn, None, "[é-ö].md") == {"é.md"}
        assert await _paths(conn, None, "[!a].md") == {"é.md", "E.md", "ö/x.md"}
        assert await _paths(conn, None, "[[:alpha:]].md") == set()  # no POSIX classes, as in rg
        assert await _paths(conn, None, "?.md") == {"é.md", "E.md", "a.md", "ö/x.md"}  # ? is one character
        assert await _paths(conn, None, "??.md") == {"日本.md"}
        assert await _paths(conn, None, "[é-ö]/*") == {"ö/x.md"}
        assert await _paths(conn, None, "é*") == {"é.md"}  # no case folding
        await _tree(conn, None, ["😁", "😃"])
        assert await _paths(conn, None, "[😀-😂]") == {"😁"}


async def test_like_wildcards_in_names_are_literal(conn: AsyncConnection) -> None:
    await _tree(conn, None, ["a_b", "axb", "a%b", "a\\b"])
    assert await _paths(conn, None, "a_b") == {"a_b"}
    assert await _paths(conn, None, "a%b") == {"a%b"}
    assert await _paths(conn, None, "a\\\\b") == {"a\\b"}


# Differential: pruning hints on == off, on a random tree and random patterns.

_TREE_NAMES = ["src", "lib", "docs", "a", "b", ".hidden", "node_modules", ".git", "x.py", "y.md", "z.txt", "é.py"]


async def test_hints_do_not_change_results(conn: AsyncConnection) -> None:
    rng = random.Random(18)
    files: set[str] = set()
    while len(files) < 150:
        depth = rng.randint(1, 5)
        dirs = [rng.choice(_TREE_NAMES[:8]) for _ in range(depth - 1)]
        files.add("/".join([*dirs, rng.choice(_TREE_NAMES[8:])]))
    # A name can't be both a file and a directory.
    dir_paths = {"/".join(f.split("/")[:i]) for f in files for i in range(1, f.count("/") + 1)}
    await _tree(conn, None, sorted(files - dir_paths))
    root = await mkdir_p(conn, "src")
    patterns = [
        "*.py", "src/**/*.py", "src/*/*.md", "{src,lib}/**/x.py", "a/b/*", "a/**", "**/b/*.txt", "src/a/b/x.py",
        "src/{a,b}/*", "*/*/*", "node_modules/**", "*.{py,md}", "[ab]/*.py", "**/é.py", "docs/a/*.t?t",
        ".hidden/*", "src/.hidden/**", "a/node_modules/x.py", "{src/a,src/b/c}/*.py", "{a,b/a}/**/{x,y}.*",
        "{{a,},b}**/x.py", "*.[mt]?", "src/{a,b}*/{x,y}.py",
    ]  # fmt: skip
    excludes_options = [[], ["**/node_modules/**"], ["**/node_modules/**", "*.md", "b"], ["a/", "src/b/"]]
    for pattern in patterns:
        for excludes in excludes_options:
            for hidden in (False, True):
                for start in (None, root):
                    await _paths(conn, start, pattern, excludes=excludes, hidden=hidden)


# Smoke benchmark: 10k nodes, src/**/*.py. Not a gate: CI hardware varies, so this prints and checks loosely.


async def test_10k_node_tree_smoke_benchmark(conn: AsyncConnection) -> None:
    rows: list[tuple[UUID, UUID | None, str, str]] = []
    top = {name: uuid.uuid4() for name in ("src", "docs", "tests")}
    rows += [(i, None, name, "dir") for name, i in top.items()]
    for name, parent in top.items():
        for d in range(30):
            dir_id = uuid.uuid4()
            rows.append((dir_id, parent, f"{name}{d}", "dir"))
            for f in range(112):
                rows.append((uuid.uuid4(), dir_id, f"f{f}.{'py' if f % 2 else 'md'}", "file"))
    assert len(rows) > 10_000
    async with conn.cursor() as cur, cur.copy("COPY vfs_nodes (id, parent_id, name, kind) FROM STDIN") as copy:
        for row in rows:
            await copy.write_row(row)
    await conn.execute("ANALYZE vfs_nodes")
    await glob(conn, None, "src/**/*.py")  # warm up
    start = time.perf_counter()
    matches = await glob(conn, None, "src/**/*.py")
    elapsed = time.perf_counter() - start
    assert len(matches) == 30 * 56
    print(f"src/**/*.py over {len(rows)} nodes: {elapsed * 1000:.1f} ms")
    assert elapsed < 2  # the target is 50 ms; this only catches a pathological plan
