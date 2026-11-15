"""The ``minifs ls|cat|put|rm|mv`` commands."""

import io
import sys
import time
from pathlib import Path

import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from minifs import cli
from minifs.cli import main
from minifs.vfs.client import VFSClient
from minifs.vfs.core import MAX_FILE_SIZE, ReadResult

Capture = pytest.CaptureFixture[bytes]


class _Stdin:
    def __init__(self, data: bytes) -> None:
        self.buffer = io.BytesIO(data)


def run(monkeypatch: pytest.MonkeyPatch, db: str, *args: str) -> int:
    """Run a command with the db conninfo in ``$MINIFS_DSN``: it holds the test container's password, which
    must never go through argv."""
    monkeypatch.setenv("MINIFS_DSN", db)
    return main(list(args))


def _conninfo_with_password(db: str, password: str) -> str:
    """The test database's conninfo with another password."""
    params = conninfo_to_dict(db)
    params["password"] = password
    return make_conninfo("", **{k: str(v) for k, v in params.items() if v is not None})


def _conninfo_without_password(db: str) -> str:
    """The test database's conninfo, password dropped."""
    params = conninfo_to_dict(db)
    params.pop("password", None)
    return make_conninfo("", **{k: str(v) for k, v in params.items() if v is not None})


def test_put_cat_ls(db: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    source = tmp_path / "a.txt"
    source.write_bytes(b"from a file\n")
    assert run(monkeypatch, db, "put", "-p", "/docs/a.txt", str(source)) == 0
    assert run(monkeypatch, db, "cat", "/docs/a.txt") == 0
    assert capsysbinary.readouterr().out == b"from a file\n"

    assert run(monkeypatch, db, "put", "-a", "/docs/a.txt", str(source)) == 0
    assert run(monkeypatch, db, "cat", "/docs//a.txt") == 0
    assert capsysbinary.readouterr().out == b"from a file\n" * 2

    assert run(monkeypatch, db, "ls") == 0
    assert capsysbinary.readouterr().out == b"docs/\n"
    assert run(monkeypatch, db, "ls", "-l", "/docs") == 0
    kind, size, _mtime, name = capsysbinary.readouterr().out.decode().split()
    assert (kind, size, name) == ("file", "24", "a.txt")
    assert run(monkeypatch, db, "ls", "/docs/a.txt") == 0
    assert capsysbinary.readouterr().out == b"/docs/a.txt\n"


def test_put_from_stdin_replaces(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(b"one"))
    assert run(monkeypatch, db, "put", "/f") == 0
    monkeypatch.setattr(sys, "stdin", _Stdin(b"two"))
    assert run(monkeypatch, db, "put", "/f") == 0
    monkeypatch.setattr(sys, "stdin", _Stdin(b"!"))
    assert run(monkeypatch, db, "put", "--append", "/g") == 0  # append creates a missing file
    assert run(monkeypatch, db, "cat", "/f") == 0
    assert run(monkeypatch, db, "cat", "/g") == 0
    assert capsysbinary.readouterr().out == b"two!"


def test_cat_large_file(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    data = bytes(range(256)) * 9000  # > 2 chunks
    monkeypatch.setattr(sys, "stdin", _Stdin(data))
    assert run(monkeypatch, db, "put", "/big") == 0
    assert run(monkeypatch, db, "cat", "/big") == 0
    assert capsysbinary.readouterr().out == data


def test_mv_rm_and_errors(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(b"x"))
    assert run(monkeypatch, db, "put", "-p", "/a/f") == 0
    assert run(monkeypatch, db, "mv", "/a/f", "/a/g") == 0
    assert run(monkeypatch, db, "ls", "/a") == 0
    assert capsysbinary.readouterr().out == b"g\n"

    assert run(monkeypatch, db, "rm", "/a") == 1
    assert capsysbinary.readouterr().err.decode().startswith("minifs rm: ENOTEMPTY: /a")
    assert run(monkeypatch, db, "rm", "/a/g", "/a") == 0
    assert run(monkeypatch, db, "cat", "/a/g") == 1
    err = capsysbinary.readouterr().err.decode()
    assert err.startswith("minifs cat: ENOENT: /a/g")
    assert run(monkeypatch, db, "put", "/no/parent") == 1
    assert "ENOENT: /no/parent" in capsysbinary.readouterr().err.decode()
    assert run(monkeypatch, db, "ls", "relative") == 1
    assert "EINVAL: relative" in capsysbinary.readouterr().err.decode()


def test_bad_password_fails_at_once(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setenv("MINIFS_DSN", _conninfo_with_password(db, "Wrong-Pass-5c1d"))
    started = time.monotonic()
    assert main(["ls"]) == 1
    assert time.monotonic() - started < 10
    err = capsysbinary.readouterr().err.decode()
    assert "password authentication failed" in err
    assert "Wrong-Pass-5c1d" not in err


def test_cat_refuses_a_file_replaced_midway(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(b"A" * 10))
    assert run(monkeypatch, db, "put", "/f") == 0
    monkeypatch.setattr(cli, "_CAT_CHUNK", 4)
    original = VFSClient.pread
    calls = 0

    async def pread_then_replace(self: VFSClient, path: str, offset: int = 0, limit: int = 16_000) -> ReadResult:
        nonlocal calls
        calls += 1
        if calls == 2:  # same name, new node, generation 0 again
            await self.unlink(path)
            await self.create(path, b"B" * 10)
        return await original(self, path, offset, limit)

    monkeypatch.setattr(VFSClient, "pread", pread_then_replace)
    assert run(monkeypatch, db, "cat", "/f") == 1
    captured = capsysbinary.readouterr()
    assert captured.out == b"AAAA"
    assert b"ESTALE: /f" in captured.err


def test_glob(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(b"x"))
    assert run(monkeypatch, db, "put", "-p", "/w/a.md") == 0
    assert run(monkeypatch, db, "glob", "*.md") == 0
    assert capsysbinary.readouterr().out == b"/w/a.md\n"
    assert run(monkeypatch, db, "glob", "[", "/w") == 1
    assert capsysbinary.readouterr().err.startswith(b"minifs glob: ")


def test_glob_bounds(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    """#85: the walk under /w keeps w/a.md, w/b.md and w/sub, w/sub/c.md; ``--max-nodes`` and ``--timeout``
    lift or tighten the library's bounds."""
    for path in ("/w/a.md", "/w/b.md", "/w/sub/c.md"):
        monkeypatch.setattr(sys, "stdin", _Stdin(b"x"))
        assert run(monkeypatch, db, "put", "-p", path) == 0
    capsysbinary.readouterr()
    assert run(monkeypatch, db, "glob", "**", "/w", "--max-nodes", "1") == 1
    err = capsysbinary.readouterr().err
    assert err.startswith(b"minifs glob: ") and b"Glob scope is over 1 files and directories" in err
    assert run(monkeypatch, db, "glob", "**", "/w", "--max-nodes", "none", "--timeout", "5") == 0
    assert sorted(capsysbinary.readouterr().out.split()) == [b"/w/a.md", b"/w/b.md", b"/w/sub/c.md"]
    assert run(monkeypatch, db, "glob", "**", "/w", "--max-nodes", "4") == 0
    capsysbinary.readouterr()
    for bad in (
        ["--max-nodes", "-1"],
        ["--max-nodes", "x"],
        ["--timeout", "x"],
        ["--timeout", "0"],
        ["--timeout", "inf"],
    ):
        with pytest.raises(SystemExit) as info:
            run(monkeypatch, db, "glob", "**", "/w", *bad)
        assert info.value.code == 2
        assert b"usage:" in capsysbinary.readouterr().err


def test_dsn_with_a_password_warns_without_showing_it(
    db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture
) -> None:
    assert main(["ls", "--dsn", _conninfo_with_password(db, "PlaceHolder-7f3a")]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "warning: --dsn holds a password" in err
    assert "PlaceHolder-7f3a" not in err

    # The URI form, on a port nothing listens on so the connection is refused quickly. The password goes in
    # percent-encoded too: no form of it may show.
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "2")
    for password in ("PlaceHolder-7f3a", "Place%20Holder-7f3a"):
        assert main(["ls", "--dsn", f"postgresql://u:{password}@127.0.0.1:1/x"]) == 1
        err = capsysbinary.readouterr().err.decode()
        assert "warning: --dsn holds a password" in err
        assert "PlaceHolder-7f3a" not in err
        assert "Place%20Holder-7f3a" not in err
        assert "Place Holder-7f3a" not in err


def test_no_warning_without_a_password_in_argv(
    db: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsysbinary: Capture
) -> None:
    monkeypatch.setenv("MINIFS_DSN", db)
    assert main(["ls"]) == 0
    assert "warning" not in capsysbinary.readouterr().err.decode()

    path = tmp_path / "dsn"
    path.write_text(db + "\n")  # the trailing newline must be stripped
    assert main(["ls", "--dsn-file", str(path)]) == 0
    assert "warning" not in capsysbinary.readouterr().err.decode()


def test_password_prompt(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    prompts: list[str] = []
    password = str(conninfo_to_dict(db)["password"])  # read at run time; never written down

    def right(prompt: str = "") -> str:
        prompts.append(prompt)
        return password

    monkeypatch.setattr(cli, "_has_terminal", lambda: True)  # pytest has no terminal
    monkeypatch.setattr(cli.getpass, "getpass", right)
    monkeypatch.setenv("MINIFS_DSN", _conninfo_without_password(db))
    assert main(["ls", "-W"]) == 0
    assert prompts == ["Password: "]

    def wrong(prompt: str = "") -> str:
        return "PlaceHolder-7f3a"

    monkeypatch.setattr(cli.getpass, "getpass", wrong)
    assert main(["ls", "-W"]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "password authentication failed" in err
    assert "PlaceHolder-7f3a" not in err


def test_password_prompt_needs_a_terminal(monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    """Without a terminal ``-W`` refuses instead of reading the password from stdin, which is ``put``'s data;
    a getpass that ends at the prompt (EOF, Ctrl-C) is "no password entered", not a traceback."""

    def never(prompt: str = "") -> str:
        pytest.fail("getpass must not be called without a terminal")

    monkeypatch.setattr(cli, "_has_terminal", lambda: False)
    monkeypatch.setattr(cli.getpass, "getpass", never)
    assert main(["ls", "-W"]) == 1
    assert "-W needs a terminal" in capsysbinary.readouterr().err.decode()

    def eof(prompt: str = "") -> str:
        raise EOFError

    monkeypatch.setattr(cli, "_has_terminal", lambda: True)
    monkeypatch.setattr(cli.getpass, "getpass", eof)
    assert main(["ls", "-W"]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "no password entered" in err
    assert "Traceback" not in err


def test_unparseable_conninfo_is_not_shown(monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    assert main(["ls", "--dsn", "password=PlaceHolder-7f3a junk"]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "invalid connection string (not shown" in err
    assert "PlaceHolder-7f3a" not in err

    monkeypatch.setenv("MINIFS_DSN", "password=PlaceHolder-7f3a junk")
    assert main(["ls"]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "invalid connection string (not shown" in err
    assert "PlaceHolder-7f3a" not in err


@pytest.mark.parametrize(
    ("dsn", "expected", "forbidden"),
    [
        # libpq quotes the sslmode value back: the one error here that only passes because _mask replaces the
        # password (with _mask deleted, "PH-7f3a" would show instead of "****").
        ("host=127.0.0.1 port=1 sslmode=PH-7f3a password=PH-7f3a", 'invalid sslmode value: "****"', ("PH-7f3a",)),
        # Unparseable conninfos: the CLI's own fixed text, never libpq's message, which quotes the input.
        ("postgresql://u:PH-7f3a@[h", "invalid connection string (not shown", ("PH-7f3a",)),
        ("host=h PH-7f3a", "invalid connection string (not shown", ("PH-7f3a",)),
        # A bad percent escape: the password as written and its percent-encoded form must both stay hidden.
        ("postgresql://u:PH%zz7f3a@h/db", "invalid connection string (not shown", ("PH%zz7f3a", "PH%25zz7f3a")),
    ],
)
def test_errors_that_quote_the_password_are_masked(
    monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture, dsn: str, expected: str, forbidden: tuple[str, ...]
) -> None:
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "2")
    assert main(["ls", "--dsn", dsn]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert expected in err
    for secret in forbidden:
        assert secret not in err


# URIs libpq misreads: it ends the userinfo at the first @ before the first /, so an unescaped @ or / in the
# password leaves part of it in a field (host, port, host list, dbname) error messages quote without masking.
_MISREAD_URIS = (
    "postgresql://u:PH@ss7f3a@127.0.0.1:1/x",
    "postgresql://u:PH/ss7f3a@127.0.0.1:1/x",
    "postgres://u:PH@ss7f3a@127.0.0.1:1/x",
    "postgres://u:PH/ss7f3a@127.0.0.1:1/x",
    "postgresql://u:PH-1@127.0.0.1:7f3a@h/x",  # the rest lands in the port
    "postgresql://u:PH-1@ss:7f3a@127.0.0.1/x",  # in the host and port
    "postgresql://u:PH-1@127.0.0.1,7f3a:5@h/x",  # in the host list
    "postgresql://u:PH-1@localhost/7f3a@h/x",  # in the database name
    "postgresql://u:P?7f3a@b:c@h/x",  # libpq doesn't end the userinfo at ?
    "postgresql://127.0.0.1:1/db@7f3a",  # a database name with @: refused, %40 writes it
    "postgresql://u:PH-1@127.0.0.1?port=7f3a@h/db",  # a ? in the password: the rest lands in a query parameter
    "postgresql://u:PH-1@127.0.0.1?sslmode=7f3a@h:1/db",
    "postgresql://u:PH-7f3a/x?hostaddr=127.0.0.1&application_name=b@h/db",  # no userinfo, port = the password
    "postgresql://127.0.0.1:1/db?user=me@7f3a",  # a query value with @: refused, %40 writes it
)
# The same passwords written so libpq reads them right: these must reach the connection, not the refusal.
_MISREAD_SAFE_URIS = (
    "postgresql://u:PH%40ss7f3a@127.0.0.1:1/x",
    "postgresql://u:PH?ss7f3a@127.0.0.1:1/x",
    "postgresql://127.0.0.1:1/db?user=me%40corp",
    "postgresql://u:PH-7f3a@127.0.0.1:1/db?user=me%40corp",
    "postgresql://127.0.0.1:1/db%407f3a",
)


@pytest.mark.parametrize("dsn", _MISREAD_URIS)
def test_misread_uri_is_refused(monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture, dsn: str) -> None:
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "2")
    assert main(["ls", "--dsn", dsn]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "write @ as %40" in err
    assert "7f3a" not in err


@pytest.mark.parametrize("dsn", _MISREAD_SAFE_URIS)
def test_misread_uri_counter_cases_reach_the_connection(
    monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture, dsn: str
) -> None:
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "2")
    assert main(["ls", "--dsn", dsn]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "Connection refused" in err
    assert "%40" not in err


def test_misread_uri() -> None:
    misread = cli._misread_uri  # pyright: ignore[reportPrivateUsage]
    for dsn in _MISREAD_URIS:
        assert misread(dsn)
    for dsn in _MISREAD_SAFE_URIS:
        assert not misread(dsn)
    keyword = "host=127.0.0.1 port=1 password=PH-7f3a"
    assert not misread(keyword)  # not a URI: nothing libpq could misread


@pytest.mark.parametrize("password", ["a", "1", "%25"])
def test_short_password_hides_the_error(monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture, password: str) -> None:
    """A password under _MIN_MASKED characters isn't masked (it would garble the message and show where it
    matched), so the whole libpq error is withheld."""
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "2")
    assert main(["ls", "--dsn", f"host=127.0.0.1 port=1 password={password}"]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "database error (not shown: the password is too short to mask)" in err
    assert "Connection refused" not in err


@pytest.mark.parametrize(
    ("argv", "expected", "forbidden"),
    [
        # argparse quotes argv in its errors: _Parser.error masks the passwords out of them.
        (["ls", "--password=PH7f3a"], "unrecognized arguments: ****", ("PH7f3a",)),
        # -WPH7f3a leaves "-PH7f3a" over as an unrecognized word: masked whole, no fragment may show.
        (["ls", "-WPH7f3a"], "unrecognized arguments: ****", ("7f3a",)),
        (["ls", "--dns=postgresql://u:PH@ss7f3a@h/x"], "unrecognized arguments: ****", ("ss7f3a",)),
        # An invalid subcommand choice quotes the whole word: the URI userinfo masking covers it.
        (["postgresql://u:PH7f3a@h/x"], "invalid choice", ("PH7f3a",)),
        # A misplaced --dsn: the subcommand choice error quotes the URI, here with a space in the password.
        (["--dsn", "postgresql://u:PH 7f3a@h/x", "ls"], "invalid choice", ("7f3a",)),
        # argparse's repr doubles the backslash of an escaped quote in a keyword value.
        (["--dsn", "host=h password='PH \\'7f3a'", "ls"], "invalid choice", ("7f3a",)),
        # -W doesn't take a value: a password starting with -- is an unrecognized word, not an option name.
        (["ls", "-W", "--PH-7f3a"], "unrecognized arguments: ****", ("7f3a",)),
        (["ls", "--dns"], "unrecognized arguments: ****", ("dns",)),
        # One of the CLI's own options (here glob's, given to ls) is still named.
        (["ls", "--hidden"], "unrecognized arguments: --hidden", ()),
        (["put", "/x", "-W", "-p", "-PH7f3a"], "unrecognized arguments: ****", ("7f3a",)),
        # A value given to a flag that takes none.
        (["glob", "x", "--hidden=PH-7f3a"], "ignored explicit argument '****'", ("7f3a",)),
        # -W taken for an option with a value, before the subcommand: the password is in the command slot.
        (["-W", "PH-7f3a", "ls"], "invalid choice: '****'", ("7f3a",)),
        # A mistyped command name is still shown.
        (["lss"], "invalid choice: 'lss'", ()),
    ],
)
def test_argparse_errors_mask_passwords(
    capsysbinary: Capture, argv: list[str], expected: str, forbidden: tuple[str, ...]
) -> None:
    with pytest.raises(SystemExit) as info:
        main(argv)
    assert info.value.code == 2
    err = capsysbinary.readouterr().err.decode()
    assert expected in err
    for secret in forbidden:
        assert secret not in err


def test_dsn_file_encoding(db: str, tmp_path: Path, capsysbinary: Capture) -> None:
    with_bom = tmp_path / "dsn"
    with_bom.write_bytes(b"\xef\xbb\xbf" + db.encode() + b"\n")  # a UTF-8 BOM must be tolerated
    assert main(["ls", "--dsn-file", str(with_bom)]) == 0
    capsysbinary.readouterr()

    not_utf8 = tmp_path / "latin1"
    not_utf8.write_bytes(b"host=h password=PH\xe97f3a\n")  # \xe9 is not valid UTF-8 on its own
    assert main(["ls", "--dsn-file", str(not_utf8)]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "--dsn-file is not UTF-8 text" in err
    assert "7f3a" not in err


def test_undecodable_argv_is_refused(capsysbinary: Capture) -> None:
    """argv bytes that were not UTF-8 reach Python as surrogates: refused with the fixed text, not a traceback."""
    assert main(["ls", "--dsn", "host=h \udcff password=PH-7f3a"]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "invalid connection string (not shown" in err
    assert "Traceback" not in err
    assert "PH-7f3a" not in err


def test_dsn_and_dsn_file_are_exclusive(tmp_path: Path, capsysbinary: Capture) -> None:
    path = tmp_path / "dsn"
    with pytest.raises(SystemExit) as info:
        main(["ls", "--dsn", "", "--dsn-file", str(path)])
    assert info.value.code == 2
    assert b"usage:" in capsysbinary.readouterr().err


def test_mask() -> None:
    mask = cli._mask  # pyright: ignore[reportPrivateUsage]
    forms = cli._forms  # pyright: ignore[reportPrivateUsage]

    assert mask("a PlaceHolder-7f3a b", forms("PlaceHolder-7f3a", "x")) == "a **** b"

    # The whole conninfo is masked as one ****, before the password inside it (longest secret first).
    dsn = "host=h password=PlaceHolder-7f3a"
    assert mask(f"failed with {dsn} at once", forms("PlaceHolder-7f3a", dsn)) == "failed with **** at once"

    # The percent-encoded form of the password is masked too, and a conninfo URI as a whole.
    assert mask("pw Place%20Holder-7f3a end", forms("Place Holder-7f3a", "x")) == "pw **** end"
    uri = "postgresql://u:Place%20Holder-7f3a@h/x"
    assert mask(f"connect to {uri} refused", forms("Place Holder-7f3a", uri)) == "connect to **** refused"

    # A secret shorter than _MIN_MASKED characters is left alone (see test_short_password_hides_the_error).
    assert mask("failed", ["a"]) == "failed"


def test_unreadable_dsn_file_is_not_named(capsysbinary: Capture) -> None:
    """A connection string given to --dsn-file by mistake isn't echoed back in the "no such file" error."""
    assert main(["ls", "--dsn-file", "host=h password=PH-7f3a"]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "can't read --dsn-file: No such file or directory" in err
    assert "PH-7f3a" not in err


def test_dsn_file_limits(tmp_path: Path, capsysbinary: Capture) -> None:
    """A NUL (libpq would stop reading there) and an oversized file (``/dev/zero``) are refused."""
    with_nul = tmp_path / "nul"
    with_nul.write_bytes(b"postgresql://127.0.0.1:PH-7f3a\0@127.0.0.1:1/x\n")
    assert main(["ls", "--dsn-file", str(with_nul)]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "invalid connection string (not shown" in err
    assert "7f3a" not in err

    big = tmp_path / "big"
    big.write_bytes(b"host=h " * 10_000)
    assert main(["ls", "--dsn-file", str(big)]) == 1
    assert "--dsn-file is over 65536 bytes" in capsysbinary.readouterr().err.decode()


def test_sslpassword_is_a_password(monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    """``sslpassword`` (the client key's passphrase) warns in --dsn and is masked like the password."""
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "2")
    assert main(["ls", "--dsn", "host=127.0.0.1 port=1 sslmode=PH-7f3a sslpassword=PH-7f3a"]) == 1
    err = capsysbinary.readouterr().err.decode()
    assert "warning: --dsn holds a password" in err
    assert 'invalid sslmode value: "****"' in err
    assert "PH-7f3a" not in err


# The quota command (#83)


def test_quota_show_set_remove(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(b"x" * 30))
    assert run(monkeypatch, db, "put", "-p", "/q/f") == 0
    capsysbinary.readouterr()
    assert run(monkeypatch, db, "quota", "/q") == 0
    assert capsysbinary.readouterr().out == b"no quota\n"
    assert run(monkeypatch, db, "quota", "/q", "--bytes", "100", "--nodes", "5") == 0
    assert capsysbinary.readouterr().out == b"max_bytes=100 max_nodes=5 used_bytes=30 used_nodes=1\n"
    assert run(monkeypatch, db, "quota", "/q") == 0  # the quota is still there
    assert capsysbinary.readouterr().out == b"max_bytes=100 max_nodes=5 used_bytes=30 used_nodes=1\n"
    assert run(monkeypatch, db, "quota", "/q", "--nodes", "9") == 0  # a flag omitted: no limit on that one
    assert capsysbinary.readouterr().out == b"max_bytes=none max_nodes=9 used_bytes=30 used_nodes=1\n"
    assert run(monkeypatch, db, "quota", "/q", "--remove") == 0
    assert capsysbinary.readouterr().out == b"no quota\n"
    assert run(monkeypatch, db, "quota", "/q") == 0
    assert capsysbinary.readouterr().out == b"no quota\n"
    # A missing directory and a file are the usual errors.
    assert run(monkeypatch, db, "quota", "/missing") == 1
    assert capsysbinary.readouterr().err.startswith(b"minifs quota: ENOENT: /missing")
    assert run(monkeypatch, db, "quota", "/q/f") == 1
    assert capsysbinary.readouterr().err.startswith(b"minifs quota: ENOTDIR: /q/f")


def test_quota_children(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    for name in ("a", "b"):
        monkeypatch.setattr(sys, "stdin", _Stdin(b"x"))
        assert run(monkeypatch, db, "put", "-p", f"/workspaces/{name}/f") == 0
    capsysbinary.readouterr()
    assert run(monkeypatch, db, "quota", "/workspaces", "--children", "--bytes", "100", "--nodes", "5") == 0
    assert capsysbinary.readouterr().out == b"2 existing directories got the quota\n"
    assert run(monkeypatch, db, "quota", "/workspaces/a") == 0
    assert capsysbinary.readouterr().out == b"max_bytes=100 max_nodes=5 used_bytes=1 used_nodes=1\n"
    assert run(monkeypatch, db, "quota", "/workspaces", "--children", "--remove") == 0
    assert capsysbinary.readouterr().out == b"no quota\n"


@pytest.mark.parametrize(
    "flags",
    [("--remove", "--bytes", "10"), ("--bytes", "10", "--remove"), ("--remove", "--nodes", "10"), ("--children",)],
)
def test_quota_flag_errors(
    db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture, flags: tuple[str, ...]
) -> None:
    with pytest.raises(SystemExit) as info:
        run(monkeypatch, db, "quota", "/q", *flags)
    assert info.value.code == 2
    assert b"usage:" in capsysbinary.readouterr().err


class _BigStdin:
    """A stdin whose ``buffer.read(n)`` returns exactly the ``n`` bytes asked for, recording each ``n``."""

    def __init__(self) -> None:
        self.asked: list[int] = []
        self.buffer = self

    def read(self, size: int = -1) -> bytes:
        self.asked.append(size)
        return b"x" * size


def test_put_over_the_file_cap_is_efbig(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    """#83: put reads at most MAX_FILE_SIZE + 1 bytes and refuses the excess before any database call."""
    stdin = _BigStdin()
    monkeypatch.setattr(sys, "stdin", stdin)
    assert run(monkeypatch, db, "put", "/big") == 1
    err = capsysbinary.readouterr().err.decode()
    assert f"minifs put: EFBIG: /big: over the {MAX_FILE_SIZE}-byte limit" in err
    assert stdin.asked == [MAX_FILE_SIZE + 1]  # the read is bounded
    assert run(monkeypatch, db, "ls") == 0  # and nothing was created
    assert capsysbinary.readouterr().out == b""
