"""The glob translator and pruning hints (minifs.vfs.glob), and the walker vfs_glob."""

import random
import re
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID

import bracex
import pytest
from psycopg import AsyncConnection
from wcmatch import glob as wcglob

from minifs.vfs.glob import MAX_ALTERNATIVES, GlobError, GlobPlan, compile_excludes, compile_glob, glob, glob_to_regex
from tests.test_tree import create, mkdir_p, raises_errno

# Translator


@pytest.mark.parametrize(
    ("pattern", "regex"),
    [
        ("*", r"^(?:.*/)?[^/]*$"),
        ("*.md", r"^(?:.*/)?[^/]*\.md$"),
        ("?", r"^(?:.*/)?[^/]$"),
        ("src/*.py", r"^src/[^/]*\.py$"),
        ("/src/*.py", r"^src/[^/]*\.py$"),
        ("**/*.py", r"^(?:[^/]+/)*[^/]*\.py$"),
        ("src/**", r"^src/.*$"),
        ("a/**/b", r"^a/(?:[^/]+/)*b$"),
        ("a**b", r"^(?:.*/)?a[^/]*b$"),
        ("[abc]", r"^(?:.*/)?[a-c]$"),  # ranges are merged
        ("[ax]", r"^(?:.*/)?[ax]$"),
        ("[!x]", r"^(?:.*/)?[^x\u002f]$"),
        ("[^x]", r"^(?:.*/)?[^x\u002f]$"),
        ("[a-z]", r"^(?:.*/)?[a-z]$"),
        ("[.-0]", r"^(?:.*/)?[\u002e0]$"),  # the range loses '/'
        ("[]]", r"^(?:.*/)?[\u005d]$"),
        ("[[:digit:]]", r"^(?:.*/)?[0-9]$"),
        ("*.{md,txt}", r"^(?:.*/)?[^/]*\.(?:md|txt)$"),
        ("{a,{b,c}}", r"^(?:.*/)?(?:a|(?:b|c))$"),
        ("{a,a}", r"^(?:.*/)?(?:a)$"),
        ("{a,b}/*", r"^(?:a|b)/[^/]*$"),
        ("{a/b,c}", r"^(?:a/b|c)$"),  # expanded: the alternatives have different segments
        ("{a,}**/x", r"^(?:a[^/]*/x|(?:[^/]+/)*x)$"),  # ** is a globstar only in the second
        ("{{a,},b}**/x", r"^(?:a[^/]*/x|(?:[^/]+/)*x|b[^/]*/x)$"),  # empty alternatives at any depth expand
        ("{,a}{,b}**/x", r"^(?:(?:[^/]+/)*x|b[^/]*/x|a[^/]*/x|ab[^/]*/x)$"),
        ("x/*{q/r,{a,}}*/y", r"^(?:x/[^/]*q/r[^/]*/y|x/[^/]*a[^/]*/y|x/(?:[^/]+/)*y)$"),
        ("{,a/b}/y", r"^(?:/y|a/b/y)$"),  # only a leading '/' of the pattern anchors
        ("[😀-😂]", r"^(?:.*/)?[\U0001f600-\U0001f602]$"),
        ("[[:ascii:]]", r"^(?:.*/)?[\u0000-\u002e0-\u007f]$"),
        (r"\*", r"^(?:.*/)?\*$"),
        (r"\a", r"^(?:.*/)?a$"),
        ("a+b(c)$", r"^(?:.*/)?a\+b\(c\)\$$"),
        ("é", r"^(?:.*/)?é$"),
    ],
)
def test_translation(pattern: str, regex: str) -> None:
    assert glob_to_regex(pattern) == regex


@pytest.mark.parametrize(
    ("pattern", "message"),
    [
        ("", "empty pattern"),
        ("[ab", "unclosed '['"),
        ("[]", "unclosed '['"),
        ("[!]", "unclosed '['"),
        ("{a,b", "unclosed '{'"),
        ("{a,[}]", "unclosed '{'"),  # [}] is a class
        ("{a,[b}", "unclosed '['"),
        ("a\0b", "NUL character"),
        ("{a,b/c}" * 11, "more than 1024 brace alternatives"),
        ("{a,b/c}" * 10 + "x" * 16 * 1024, "the translated regex is over 16 KB"),
        ("{a/" * 1000 + "}" * 1000, "braces nested too deeply"),
        ("src/", "a pattern ending in '/' matches only directories, and only files are listed"),
        ("**/", "a pattern ending in '/' matches only directories, and only files are listed"),
        ("src\\/", "a pattern ending in '/' matches only directories, and only files are listed"),
    ],
)
def test_bad_patterns(pattern: str, message: str) -> None:
    with pytest.raises(GlobError) as info:
        compile_glob(pattern)
    assert info.value.message == message


@pytest.mark.parametrize(
    ("pattern", "plan"),
    [
        ("*.md", GlobPlan(r"^(?:.*/)?[^/]*\.md$", name_like="%.md")),
        ("README.md", GlobPlan(r"^(?:.*/)?README\.md$", name_like="README.md")),
        ("docs/guides/**/*.md", GlobPlan(r"^docs/guides/(?:[^/]+/)*[^/]*\.md$", ("docs", "guides"), None, "%.md")),
        ("src/*/*.py", GlobPlan(r"^src/[^/]*/[^/]*\.py$", ("src",), 2, "%.py")),
        ("src/a.py", GlobPlan(r"^src/a\.py$", ("src",), 1, "a.py")),
        ("src/**", GlobPlan(r"^src/.*$", ("src",), None, None)),
        ("src/*.[ch]", GlobPlan(r"^src/[^/]*\.[ch]$", ("src",), 1, "%._")),
        ("{a,b}", GlobPlan(r"^(?:.*/)?(?:a|b)$")),  # % filters nothing
        ("{src,lib}/x_1%.py", GlobPlan(r"^(?:src|lib)/x_1%\.py$", (), 2, r"x\_1\%.py")),
        ("{src/a,src/b/c}/*.py", GlobPlan(r"^(?:src/a/[^/]*\.py|src/b/c/[^/]*\.py)$", ("src",), 3, "%.py")),
        ("{src/a,src/b}/c", GlobPlan(r"^(?:src/a/c|src/b/c)$", ("src",), 2, "c")),
        ("src/*.{md,txt}", GlobPlan(r"^src/[^/]*\.(?:md|txt)$", ("src",), 1, "%.%")),
        ("a?b**c", GlobPlan(r"^(?:.*/)?a[^/]b[^/]*c$", name_like="a_b%c")),
    ],
)
def test_hints(pattern: str, plan: GlobPlan) -> None:
    assert compile_glob(pattern) == plan


def test_braces_without_separators_are_not_expanded() -> None:
    # Expanded, "*{a,b}" * 10 would be 1,024 alternatives of 70 characters each.
    assert glob_to_regex("*{a,b}" * 10) == "^(?:.*/)?" + "[^/]*(?:a|b)" * 10 + "$"


def test_exclude_regexes() -> None:
    excludes = compile_excludes(["**/node_modules/**", "**/.venv/**", "*.log", "**/a*/**", "build/", "a/b/"])
    assert excludes.prune_dirs == ("node_modules", ".venv", "build")
    # Directories are tested as "path/": every exclude also matches them, the ones ending in '/' only them.
    assert excludes.regexes == (r"^(?:.*/)?[^/]*\.log/?$", r"^(?:[^/]+/)*a[^/]*/.*/?$", r"^a/b/$")
    unpruned = compile_excludes(["**/node_modules/**", "build/"], prune=False)
    assert unpruned.prune_dirs == ()
    assert unpruned.regexes == (r"^(?:[^/]+/)*node_modules/.*/?$", r"^(?:.*/)?build/$")
    assert compile_excludes(["build\\/"]).regexes == (r"^(?:.*/)?build/$",)  # an escaped '/' is one too
    for doubled in ["build//", "/", "build\\//"]:
        with pytest.raises(GlobError):
            compile_excludes([doubled])
    with pytest.raises(TypeError):
        compile_excludes("*.log")


# Differential: the translator against wcmatch, on generated patterns and paths.
#
# wcmatch is configured as rg's globs behave: ** spans directories (GLOBSTAR), {a,b} alternates (BRACE), and
# * matches a leading dot (DOTGLOB; hidden directories are the walker's business). A pattern without '/'
# matches the name at any depth, so wcmatch gets it with **/ in front. The generator stays inside the syntax
# both agree on: no '/' in classes, no bash brace sequences ({1..3}), no single-element braces, no
# unclosed brackets (wcmatch takes those literally, we reject them), no ranges across '/' (wcmatch drops
# the whole range, we drop only the '/'), and no empty path segments (wcmatch collapses a//b).

_WC_FLAGS = wcglob.GLOBSTAR | wcglob.BRACE | wcglob.DOTGLOB

_NAME_CHARS = ["😁", "a", "b", "c", "é", "ö", "日", ".", "-", "*", "?", "[", "]", "{", "}", ",", "\\"]
_LITERALS = ["a", "b", "c", "é", "日", ".", "-", r"\*", r"\?", r"\[", r"\{", r"\,", "\\\\"]
_CLASSES = [
    "[ab]", "[!a]", "[^b]", "[a-c]", "[é-ö]", "[!é-ö]", "[😀-😂]", "[]a]", "[.-]", "[[:alpha:]]", "[*?]", "[\\]]",
]  # fmt: skip


def _gen_segment(rng: random.Random, depth: int = 0) -> str:
    out: list[str] = []
    for _ in range(rng.randint(1, 4)):
        r = rng.random()
        if r < 0.45:
            out.append(rng.choice(_LITERALS))
        elif r < 0.6:
            out.append("*")
        elif r < 0.7:
            out.append("?")
        elif r < 0.85:
            out.append(rng.choice(_CLASSES))
        elif depth < 2:
            alts = [_gen_segment(rng, depth + 1) for _ in range(rng.randint(2, 3))]
            if rng.random() < 0.2:
                alts.append("")
            out.append("{" + ",".join(alts) + "}")
        else:
            out.append(rng.choice(_LITERALS))
    return "".join(out)


def _gen_pattern(rng: random.Random) -> str:
    segments: list[str] = []
    for i in range(rng.randint(1, 4)):
        globstar = rng.random() < (0.2 if i else 0.15) and (not segments or segments[-1] != "**")
        segments.append("**" if globstar else _gen_segment(rng))
    if segments[-1] == "**":
        segments.append(_gen_segment(rng))
    pattern = "/".join(segments)
    if rng.random() < 0.1 and "/" in pattern:
        pattern = "/" + pattern
    return pattern


def _gen_path(rng: random.Random) -> str:
    names = ["".join(rng.choice(_NAME_CHARS) for _ in range(rng.randint(1, 4))) for _ in range(rng.randint(1, 4))]
    return "/".join(n for n in names if n not in (".", "..")) or "a"


def _wcmatcher(pattern: str) -> Callable[[str], bool]:
    if "/" not in pattern:
        pattern = "**/" + pattern
    return wcglob.compile(pattern.lstrip("/"), flags=_WC_FLAGS).match


def _paths_for(rng: random.Random, pattern: str) -> list[str]:
    """Random paths, plus paths built from the pattern so that matches aren't rare."""
    paths = [_gen_path(rng) for _ in range(20)]
    for _ in range(20):
        built: list[str] = []
        for segment in pattern.lstrip("/").split("/"):
            if segment == "**":
                built.extend(_gen_path(rng).split("/")[: rng.randint(0, 2)])
                continue
            chars = [c for c in segment if c not in "*?[]{},\\!^:"] or ["a"]
            built.append("".join(rng.choice(chars) for _ in range(rng.randint(1, 4))))
        paths.append("/".join(n for n in built if n not in (".", "..")) or "a")
    return paths


def _comparable(pattern: str) -> bool:
    # An empty alternative can leave an empty segment (x/{,a}/y gives x//y), which wcmatch collapses, or an
    # empty pattern, which our **/ prefix turns into a different one. (bracex drops an empty alternative at the
    # pattern's edge, hence the sentinels.) Patterns over the size limits are refused, not compared.
    try:
        compile_glob(pattern)
        wrapped = bracex.expand("<" + pattern.removeprefix("/") + ">", keep_escapes=True, limit=MAX_ALTERNATIVES)
    except (GlobError, bracex.ExpansionLimitException):
        return False
    expansions = [e[1:-1] for e in wrapped]
    return all(e and "//" not in e and not e.startswith("/") and not e.endswith("/") for e in expansions)


def _cases(seed: int, n: int) -> list[tuple[str, list[str]]]:
    rng = random.Random(seed)
    patterns = [p for p in (_gen_pattern(rng) for _ in range(n)) if _comparable(p)]
    return [(p, _paths_for(rng, p)) for p in patterns]


def _python_match(regex: str, path: str) -> bool:
    # ARE's . and [^/] match a newline, Python's . only with DOTALL. The \uXXXX escapes mean the same in both.
    return re.search(regex, path, re.DOTALL) is not None


def test_translator_agrees_with_wcmatch() -> None:
    mismatches: list[tuple[str, str, bool]] = []
    for pattern, paths in _cases(seed=8, n=1000):
        regex = glob_to_regex(pattern)
        matcher = _wcmatcher(pattern)
        for path in paths:
            expected = matcher(path)
            if _python_match(regex, path) != expected:
                mismatches.append((pattern, path, expected))
    assert mismatches[:20] == []


async def test_postgres_agrees_with_the_translator(db_any_locale: str) -> None:
    # The same regexes evaluated by Postgres, on C-collated text as vfs_glob does, in both locales.
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
    with raises_errno("ENOTDIR"):
        await glob(conn, file, "*")
    with raises_errno("ENOENT"):
        await glob(conn, uuid.uuid4(), "*")


async def test_classes_on_non_ascii_names(db_any_locale: str) -> None:
    async with await AsyncConnection.connect(db_any_locale, autocommit=True) as conn:
        await _tree(conn, None, ["é.md", "E.md", "日本.md", "ö/x.md", "a.md"])
        assert await _paths(conn, None, "[é-ö].md") == {"é.md"}
        assert await _paths(conn, None, "[!a].md") == {"é.md", "E.md", "ö/x.md"}
        assert await _paths(conn, None, "[[:alpha:]].md") == {"E.md", "a.md", "ö/x.md"}  # POSIX classes are ASCII
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
