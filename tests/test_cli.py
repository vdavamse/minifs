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
from minifs.vfs.core import ReadResult

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
