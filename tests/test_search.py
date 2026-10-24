"""Search (migration 008, minifs.vfs.search, VFSClient.grep): vfs_search, the scope and its caps."""

import json
import subprocess
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from minifs import _regex
from minifs.cli import main
from minifs.db.pool import Connector, open_pool
from minifs.vfs import regex
from minifs.vfs import search as searching
from minifs.vfs.client import VFSClient
from minifs.vfs.core import VFSError
from minifs.vfs.glob import GlobError
from minifs.vfs.regex import RegexError
from minifs.vfs.search import GrepHit, SearchRefused

Pool = AsyncConnectionPool[AsyncConnection[Any]]
Hit = tuple[str, int, int, str]  # path, line, offset, text

needs_oracle = pytest.mark.skipif(not _regex.HAS_ORACLE, reason="minifs._regex was built without the oracle feature")


@pytest.fixture
async def pool(db: str) -> AsyncIterator[Pool]:
    async with open_pool(Connector(db), max_size=3) as p:
        yield p


@pytest.fixture
def vfs(pool: Pool) -> VFSClient:
    return VFSClient(pool)


async def put(vfs: VFSClient, path: str, *chunks: bytes) -> None:
    """Creates ``path`` (and its parents) from the first chunk and appends the others, one extent or more each
    when the content before them ends in a newline."""
    await vfs.create(path, chunks[0] if chunks else b"", parents=True)
    for chunk in chunks[1:]:
        await vfs.append(path, chunk)


def hits(hit_list: list[GrepHit]) -> list[Hit]:
    return [(h.path, h.line, h.offset, h.text) for h in hit_list]


def per_line(files: dict[str, bytes], pattern: str, ignore_case: bool = False) -> list[Hit]:
    """What rg finds: each line of each UTF-8 file tested on its own by the Rust regex oracle, in path order."""
    out: list[Hit] = []
    for path in sorted(files, key=lambda p: p.encode()):
        content = files[path]
        try:
            text = content.decode()
        except UnicodeDecodeError:
            continue
        offset = 0
        lines = text.removesuffix("\n").split("\n") if text else []
        for number, line in enumerate(lines, 1):
            if _regex.oracle_is_match(pattern, line, ignore_case):
                out.append((path, number, offset, line))
            offset += len(line.encode()) + 1
    return out


# Content with several extents per file (each append after a newline starts one), multi-byte characters, CRLF,
# empty lines, no trailing newline, a hidden file and long lines.
CHUNKS: dict[str, list[bytes]] = {
    "/w/src/app.py": [
        b"import os\n\ndef main():\n    print('hello')\n",
        b"\n\ndef helper(x):\n    return x  # foo\n",
        b"class Foo:\n    bar = 1\n",
    ],
    "/w/src/util.py": [b"def foo():\n    pass\n", b"FOO = 'foo bar'\n"],
    "/w/docs/readme.md": [b"# Stra\xc3\x9fe caf\xc3\xa9\n\nfoo bar baz\n", b"\xce\xa3\xcf\x83 word wordy\n"],
    "/w/docs/notes.txt": [b"windows\r\nline\r\n", b"no newline at the end"],
    "/w/.hidden/secret.py": [b"def foo(): pass\n"],
    "/w/long.txt": [b"x" * 70_000 + b" foo\n", b"short foo\n" * 3],
    "/w/empty.txt": [],
    "/w/blank.txt": [b"\n\n\n"],
    "/w/blank-extents.txt": [b"a\n", b"\n", b"\n", b"b"],  # extents that are one empty line each
}
PATTERNS = [
    "foo",
    "^def ",
    r"foo\)?$",
    "^$",
    r"\bword\b",
    "straße",
    "Σσ",
    "é",
    ".",
    r"\s",
    r"\r$",
    r"[^a-z\s]+",
    r"\d",
    "a|",
    "^",
    r"x{3}\s",
    "(?i)FOO",
    "nothing matches this",
]


async def put_all(vfs: VFSClient) -> dict[str, bytes]:
    for path, chunks in CHUNKS.items():
        await put(vfs, path, *chunks)
    return {path: b"".join(chunks) for path, chunks in CHUNKS.items()}


@needs_oracle
async def test_hits_equal_a_per_line_search(db_any_locale: str) -> None:
    async with open_pool(Connector(db_any_locale), max_size=2) as pool:
        vfs = VFSClient(pool)
        files = await put_all(vfs)
        for pattern in PATTERNS:
            for ignore_case in (False, True):
                result = await vfs.grep(pattern, "/w", ignore_case=ignore_case, limit=10_000, max_line_chars=None)
                assert hits(result.hits) == per_line(files, pattern, ignore_case), (pattern, ignore_case)
                assert not result.truncated
                assert (result.files, result.bytes) == (len(files), sum(map(len, files.values())))


@needs_oracle
async def test_many_extents_and_a_new_generation(vfs: VFSClient, conn: AsyncConnection) -> None:
    chunks = [f"line {i} {'foo' if i % 7 == 0 else 'bar'}\n".encode() * 50 for i in range(40)]
    await put(vfs, "/f.txt", *chunks)
    cur = await conn.execute("SELECT count(*) FROM vfs_extents")
    assert (await cur.fetchone()) == (40,)
    files = {"/f.txt": b"".join(chunks)}
    result = await vfs.grep(r"^line \d*7 foo$", limit=10_000)
    assert hits(result.hits) == per_line(files, r"^line \d*7 foo$")
    assert len(result.hits) == 50  # i = 7 (17, 27 and 37 are bar)

    await vfs.compact("/f.txt")
    assert hits((await vfs.grep(r"^line \d*7 foo$", limit=10_000)).hits) == per_line(files, r"^line \d*7 foo$")

    await vfs.write("/f.txt", b"new foo\n")
    assert hits((await vfs.grep("foo")).hits) == [("/f.txt", 1, 0, "new foo")]


async def test_agrees_with_rg(vfs: VFSClient, rg_path: str, tmp_path: Path) -> None:
    files = await put_all(vfs)
    for path, content in files.items():
        local = tmp_path / path.removeprefix("/")
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(content)
    for pattern in ["foo", "^def ", r"\bword\b", r"\s$", "é", "(?i)straße", "^$"]:
        rg = subprocess.run(
            [rg_path, "--json", "--hidden", "--no-config", "-e", pattern, "."],
            cwd=tmp_path,
            capture_output=True,
            check=False,
        )
        assert rg.returncode in (0, 1), rg.stderr
        expected: list[tuple[str, int, int]] = []
        for message in map(json.loads, rg.stdout.splitlines()):
            if message["type"] == "match":
                data = message["data"]
                path = "/" + data["path"]["text"].removeprefix("./")
                expected.append((path, data["line_number"], data["absolute_offset"]))
        expected.sort(key=lambda hit: (hit[0].encode(), hit[1]))
        result = await vfs.grep(pattern, limit=10_000)
        assert [(h.path, h.line, h.offset) for h in result.hits] == expected, pattern


async def test_offsets_are_pread_offsets(vfs: VFSClient) -> None:
    await put_all(vfs)
    result = await vfs.grep("foo", limit=10_000, max_line_chars=None)
    assert result.hits
    for hit in result.hits:
        read = await vfs.pread(hit.path, hit.offset, len(hit.text.encode()))
        assert read.content.decode() == hit.text


async def test_no_phantom_last_line(vfs: VFSClient) -> None:
    await put(vfs, "/a", b"a\n")
    await put(vfs, "/b", b"a\n\nb\n")
    await put(vfs, "/c", b"a\n", b"\n")
    assert hits((await vfs.grep("^$")).hits) == [("/b", 2, 2, ""), ("/c", 2, 2, "")]
    assert hits((await vfs.grep(r"a$")).hits) == [("/a", 1, 0, "a"), ("/b", 1, 0, "a"), ("/c", 1, 0, "a")]


async def test_dot_and_negated_classes_stay_on_the_line(vfs: VFSClient) -> None:
    await put(vfs, "/f", b"start\nmiddle\nend\n")
    assert (await vfs.grep("start.*end")).hits == []
    assert (await vfs.grep("start[^x]*end")).hits == []
    assert hits((await vfs.grep(r"^\w+$")).hits) == [
        ("/f", 1, 0, "start"),
        ("/f", 2, 6, "middle"),
        ("/f", 3, 13, "end"),
    ]


async def test_utf8_split_across_appends(vfs: VFSClient) -> None:
    await put(vfs, "/f", b"caf\xc3", b"\xa9 au lait\n")  # é cut in half between the appends
    assert hits((await vfs.grep("café")).hits) == [("/f", 1, 0, "café au lait")]
    assert hits((await vfs.grep(r"caf. au")).hits) == [("/f", 1, 0, "café au lait")]


async def test_non_utf8_is_skipped(vfs: VFSClient, conn: AsyncConnection) -> None:
    await put(vfs, "/latin1", b"caf\xe9 foo\n")
    await put(vfs, "/nul", b"foo\x00bar\n")
    await put(vfs, "/png", b"\x89PNG\r\n\x1a\nfoo\n")
    await put(vfs, "/mixed", b"bad \xff foo\n", b"good foo\n")  # two extents: one bad one skips the file
    await put(vfs, "/ok", b"foo\n")
    cur = await conn.execute(
        "SELECT count(*) FROM vfs_extents e JOIN vfs_nodes n ON n.id = e.node_id WHERE name = 'mixed'"
    )
    assert (await cur.fetchone()) == (2,)
    assert hits((await vfs.grep("foo")).hits) == [("/ok", 1, 0, "foo")]
    await vfs.compact("/mixed")  # the layout doesn't change the result
    assert hits((await vfs.grep("foo")).hits) == [("/ok", 1, 0, "foo")]


async def test_sees_a_write_committed_just_before(vfs: VFSClient, db: str) -> None:
    await put(vfs, "/log", b"start\n")
    async with open_pool(Connector(db), max_size=1) as search_pool:
        searcher = VFSClient(search_pool)
        assert (await searcher.grep("needle")).hits == []
        for i in range(20):
            await vfs.append("/log", f"needle {i}\n".encode())
            result = await searcher.grep("needle", limit=1000)
            assert [h.line for h in result.hits] == list(range(2, i + 3))


async def test_include_scope_is_glob(vfs: VFSClient) -> None:
    await put_all(vfs)
    for include in ["*.py", "src/**", "**/*.md", "docs/*.{md,txt}", "*.nothing", "long.txt"]:
        globbed = {e.path for e in await vfs.glob(include, "/w", hidden=True)}
        hit_paths = {h.path for h in (await vfs.grep(".", "/w", include=include, limit=10_000)).hits}
        assert hit_paths <= globbed, include
        assert hit_paths == globbed - {"/w/empty.txt"}, include  # every non-empty file has a line matching '.'


async def test_hidden_and_git(vfs: VFSClient) -> None:
    await put(vfs, "/r/.hidden/a", b"foo\n")
    await put(vfs, "/r/.git/config", b"foo\n")
    await put(vfs, "/r/.env", b"foo\n")
    await put(vfs, "/r/b", b"foo\n")
    assert [h.path for h in (await vfs.grep("foo", "/r")).hits] == ["/r/.env", "/r/.hidden/a", "/r/b"]
    assert [h.path for h in (await vfs.grep("foo", "/r", hidden=False)).hits] == ["/r/.env", "/r/b"]
    # An explicit file path is searched whatever the include pattern and hidden say.
    assert [h.path for h in (await vfs.grep("foo", "/r/.git/config", include="*.md", hidden=False)).hits] == [
        "/r/.git/config"
    ]


async def test_paths(vfs: VFSClient) -> None:
    await put(vfs, "/a/b/c.txt", b"foo\n")
    await put(vfs, "/a/d.txt", b"foo\n")
    assert [h.path for h in (await vfs.grep("foo")).hits] == ["/a/b/c.txt", "/a/d.txt"]
    assert [h.path for h in (await vfs.grep("foo", "/a/b/")).hits] == ["/a/b/c.txt"]
    assert [h.path for h in (await vfs.grep("foo", "/a/./b/../d.txt")).hits] == ["/a/d.txt"]
    await raises("ENOENT", vfs.grep("foo", "/missing"))
    await raises("ENOTDIR", vfs.grep("foo", "/a/d.txt/x"))
    await put(vfs, "/e/empty", b"")
    result = await vfs.grep("", "/e")
    assert (result.hits, result.files, result.bytes) == ([], 1, 0)


async def test_order_limit_and_truncation(vfs: VFSClient) -> None:
    await put(vfs, "/z", b"foo\n" * 3)
    await put(vfs, "/B", b"foo\n")
    await put(vfs, "/a/x", b"foo\n" * 2)
    await put(vfs, "/é", b"foo\n")
    everything = (await vfs.grep("foo")).hits
    assert [(h.path, h.line) for h in everything] == [
        ("/B", 1), ("/a/x", 1), ("/a/x", 2), ("/z", 1), ("/z", 2), ("/z", 3), ("/é", 1),
    ]  # fmt: skip
    result = await vfs.grep("foo", limit=3)
    assert (result.hits, result.truncated) == (everything[:3], True)
    result = await vfs.grep("foo", limit=7)
    assert (result.hits, result.truncated) == (everything, False)
    result = await vfs.grep("foo", limit=0)
    assert (result.hits, result.truncated) == ([], True)
    await raises("EINVAL", vfs.grep("foo", limit=-1))


async def test_fixed_strings(vfs: VFSClient) -> None:
    await put(vfs, "/f", b"a.b\naxb\nA.B\n(x)\n")
    assert [h.text for h in (await vfs.grep("a.b")).hits] == ["a.b", "axb"]
    assert [h.text for h in (await vfs.grep("a.b", fixed=True)).hits] == ["a.b"]
    assert [h.text for h in (await vfs.grep("a.b", fixed=True, ignore_case=True)).hits] == ["a.b", "A.B"]
    assert [h.text for h in (await vfs.grep("(x)", fixed=True)).hits] == ["(x)"]
    with pytest.raises(RegexError):
        await vfs.grep("a\nb", fixed=True)


async def test_long_lines_are_cut(vfs: VFSClient) -> None:
    await put(vfs, "/f", "é".encode() * 3000 + b" foo\nfoo\n")
    result = await vfs.grep("foo")
    assert [(h.line, len(h.text), h.cut) for h in result.hits] == [(1, 2000, True), (2, 3, False)]
    assert result.hits[0].text == "é" * 2000
    result = await vfs.grep("foo", max_line_chars=None)
    assert [(len(h.text), h.cut) for h in result.hits] == [(3004, False), (3, False)]


async def test_bad_patterns_come_first(vfs: VFSClient) -> None:
    with pytest.raises(RegexError):
        await vfs.grep("(", "/missing")
    with pytest.raises(RegexError):
        await vfs.grep(r"\n", "/missing")
    with pytest.raises(GlobError):
        await vfs.grep("foo", "/missing", include="[")


async def test_scope_caps_read_no_content(vfs: VFSClient, db: str) -> None:
    await put(vfs, "/w/a", b"foo\n" * 100)
    await put(vfs, "/w/b", b"foo\n")
    result = await vfs.grep("foo", "/w", max_files=2, max_bytes=404)
    assert (result.files, result.bytes, len(result.hits)) == (2, 404, 100)
    # A lock on the extents blocks every content read: a refusal must come without one.
    async with await AsyncConnection.connect(db) as locker:
        await locker.execute("LOCK TABLE vfs_extents IN ACCESS EXCLUSIVE MODE")
        start = time.monotonic()
        with pytest.raises(SearchRefused) as info:
            await vfs.grep("foo", "/w", max_bytes=403)
        assert info.value.reason == "bytes"
        assert "narrow the path or include pattern" in str(info.value)
        with pytest.raises(SearchRefused) as info:
            await vfs.grep("foo", "/w", max_files=1)
        assert info.value.reason == "files"
        assert time.monotonic() - start < 1
        # Within the caps, the search waits for the lock until its timeout.
        with pytest.raises(SearchRefused) as info:
            await vfs.grep("foo", "/w", timeout=0.2)
        assert info.value.reason == "timeout"
        assert str(info.value) == "Search timed out; narrow the pattern or path"
        await locker.rollback()
    assert len((await vfs.grep("foo", "/w")).hits) == 100  # the connection is fine afterwards


async def test_bad_timeouts(vfs: VFSClient) -> None:
    for timeout in (0, -1, 1e10):
        await raises("EINVAL", vfs.grep("foo", timeout=timeout))


async def test_a_single_file_over_the_cap(vfs: VFSClient) -> None:
    await put(vfs, "/f", b"foo\n" * 10)
    with pytest.raises(SearchRefused) as info:
        await vfs.grep("foo", "/f", max_bytes=39)
    assert info.value.reason == "bytes"
    assert len((await vfs.grep("foo", "/f", max_bytes=40)).hits) == 10


async def test_one_snapshot_for_scope_and_scan(vfs: VFSClient, db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    await put(vfs, "/w/a", b"old foo\n")
    await put(vfs, "/w/b", b"foo\n")
    search = searching.search

    async def search_after_writes(*args: Any, **kwargs: Any) -> Any:
        async with open_pool(Connector(db), max_size=1) as other_pool:
            other = VFSClient(other_pool)
            await other.write("/w/a", b"new foo\n" * 1000)  # committed between the scope and the scan
            await other.rename("/w/b", "/w/c")
        return await search(*args, **kwargs)

    monkeypatch.setattr(searching, "search", search_after_writes)
    result = await vfs.grep("foo", "/w", limit=10_000)
    assert hits(result.hits) == [("/w/a", 1, 0, "old foo"), ("/w/b", 1, 0, "foo")]
    assert (result.files, result.bytes) == (2, 12)


async def test_stops_at_the_limit(vfs: VFSClient, conn: AsyncConnection) -> None:
    for i in range(30):
        await put(vfs, f"/w/{i:02}", b"foo\n" * 1000)
    result = await vfs.grep("foo", "/w", limit=1500)
    lines = [(h.path, h.line) for h in result.hits]
    assert lines[998:1002] == [("/w/00", 999), ("/w/00", 1000), ("/w/01", 1), ("/w/01", 2)]
    assert (len(result.hits), result.truncated) == (1500, True)


async def test_stops_inside_a_file(vfs: VFSClient, conn: AsyncConnection) -> None:
    # One large file of matching lines, in many extents, holding all of vfs_extents: with statistics that say
    # so, the planner may sort the file's extents rather than walk the index, and the search must still stop
    # at the first extents instead of filtering the whole file.
    await put(vfs, "/app.log", b"".join(f"INFO request {i}\n".encode() for i in range(600_000)))
    await conn.execute("ANALYZE vfs_extents")
    await vfs.grep("absent")  # validated and warm
    await vfs.grep("INFO")
    start = time.monotonic()
    result = await vfs.grep("INFO")
    limited = time.monotonic() - start
    assert [h.line for h in result.hits] == list(range(1, 101))
    assert result.truncated
    start = time.monotonic()
    assert (await vfs.grep("absent", timeout=60)).hits == []  # filters every extent
    absent = time.monotonic() - start
    assert limited * 3 < absent, (limited, absent)


async def test_validation_under_a_short_timeout(conn: AsyncConnection) -> None:
    slow = regex.translate(r"(?:.*\b){19}x")
    async with conn.transaction():
        with pytest.raises(psycopg.errors.QueryCanceled):
            await regex.validate(conn, slow, timeout=0.001)
        # The connection's transaction is still usable, and the pattern wasn't judged by the caller's timeout.
        await conn.execute("SELECT 1")
    assert (slow.pattern, False) not in regex._too_expensive  # pyright: ignore[reportPrivateUsage]


async def test_byte_order_mark(vfs: VFSClient, rg_path: str, tmp_path: Path) -> None:
    content = b"\xef\xbb\xbffoo\nfoo \xef\xbb\xbf\n"
    await put(vfs, "/bom", content)
    await put(vfs, "/bom-only", b"\xef\xbb\xbf\n")
    assert hits((await vfs.grep("^foo")).hits) == [("/bom", 1, 3, "foo"), ("/bom", 2, 7, "foo \ufeff")]
    assert hits((await vfs.grep("^$")).hits) == [("/bom-only", 1, 3, "")]
    (tmp_path / "bom").write_bytes(content)
    rg = subprocess.run([rg_path, "--json", "--no-config", "-e", "^foo", "bom"], cwd=tmp_path, capture_output=True)
    matches = [m["data"] for m in map(json.loads, rg.stdout.splitlines()) if m["type"] == "match"]
    assert [(m["line_number"], m["lines"]["text"]) for m in matches] == [(1, "foo\n"), (2, "foo \ufeff\n")]


async def test_fixed_string_with_nul(vfs: VFSClient) -> None:
    await put(vfs, "/f", b"a b\n")
    assert (await vfs.grep("a\0b", fixed=True)).hits == []
    assert (await vfs.grep("a\0b", fixed=True, ignore_case=True)).hits == []


async def test_excludes(vfs: VFSClient) -> None:
    await put(vfs, "/w/node_modules/x/a.js", b"foo\n")
    await put(vfs, "/w/build/b.js", b"foo\n")
    await put(vfs, "/w/src/c.js", b"foo\n")
    await put(vfs, "/w/src/c.min.js", b"foo\n")
    result = await vfs.grep("foo", "/w", excludes=["**/node_modules/**", "build/", "*.min.js"])
    assert [h.path for h in result.hits] == ["/w/src/c.js"]
    assert result.files == 1
    with pytest.raises(GlobError):
        await vfs.grep("foo", "/missing", excludes=["["])
    with pytest.raises(TypeError):
        await vfs.grep("foo", excludes="build/")


async def test_cut_ignores_a_final_cr(vfs: VFSClient) -> None:
    await put(vfs, "/f", b"a" * 10 + b"\r\n" + b"b" * 11 + b"\r\n")
    result = await vfs.grep(".", max_line_chars=10)
    assert [(h.text, h.cut) for h in result.hits] == [("a" * 10, False), ("b" * 10, True)]


def test_cli_grep(db: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    for path, content in [("/w/a.py", b"def foo():\n    return 1\n"), ("/w/b.md", b"Foo\nfoo\n")]:
        source = tmp_path / "source"
        source.write_bytes(content)
        assert main(["put", "--dsn", db, "-p", path, str(source)]) == 0
    assert main(["grep", "--dsn", db, "foo", "/w"]) == 0
    assert capsys.readouterr().out == "/w/a.py:1:def foo():\n/w/b.md:2:foo\n"
    assert main(["grep", "--dsn", db, "-i", "-g", "*.md", "foo", "/w"]) == 0
    assert capsys.readouterr().out == "/w/b.md:1:Foo\n/w/b.md:2:foo\n"
    assert main(["grep", "--dsn", db, "-m", "1", "-F", "foo"]) == 0
    captured = capsys.readouterr()
    assert captured.out == "/w/a.py:1:def foo():\n"
    assert "more" in captured.err
    assert main(["grep", "--dsn", db, "(", "/w"]) == 1
    assert capsys.readouterr().err.startswith("minifs grep: ")


async def raises(errno: str, call: Any) -> VFSError:
    with pytest.raises(VFSError) as info:
        await call
    assert info.value.errno == errno, info.value
    return info.value
