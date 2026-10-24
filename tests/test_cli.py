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


def run(db: str, *args: str) -> int:
    return main([args[0], "--dsn", db, *args[1:]])


def test_put_cat_ls(db: str, tmp_path: Path, capsysbinary: Capture) -> None:
    source = tmp_path / "a.txt"
    source.write_bytes(b"from a file\n")
    assert run(db, "put", "-p", "/docs/a.txt", str(source)) == 0
    assert run(db, "cat", "/docs/a.txt") == 0
    assert capsysbinary.readouterr().out == b"from a file\n"

    assert run(db, "put", "-a", "/docs/a.txt", str(source)) == 0
    assert run(db, "cat", "/docs//a.txt") == 0
    assert capsysbinary.readouterr().out == b"from a file\n" * 2

    assert run(db, "ls") == 0
    assert capsysbinary.readouterr().out == b"docs/\n"
    assert run(db, "ls", "-l", "/docs") == 0
    kind, size, _mtime, name = capsysbinary.readouterr().out.decode().split()
    assert (kind, size, name) == ("file", "24", "a.txt")
    assert run(db, "ls", "/docs/a.txt") == 0
    assert capsysbinary.readouterr().out == b"/docs/a.txt\n"


def test_put_from_stdin_replaces(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(b"one"))
    assert run(db, "put", "/f") == 0
    monkeypatch.setattr(sys, "stdin", _Stdin(b"two"))
    assert run(db, "put", "/f") == 0
    monkeypatch.setattr(sys, "stdin", _Stdin(b"!"))
    assert run(db, "put", "--append", "/g") == 0  # append creates a missing file
    assert run(db, "cat", "/f") == 0
    assert run(db, "cat", "/g") == 0
    assert capsysbinary.readouterr().out == b"two!"


def test_cat_large_file(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    data = bytes(range(256)) * 9000  # > 2 chunks
    monkeypatch.setattr(sys, "stdin", _Stdin(data))
    assert run(db, "put", "/big") == 0
    assert run(db, "cat", "/big") == 0
    assert capsysbinary.readouterr().out == data


def test_mv_rm_and_errors(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(b"x"))
    assert run(db, "put", "-p", "/a/f") == 0
    assert run(db, "mv", "/a/f", "/a/g") == 0
    assert run(db, "ls", "/a") == 0
    assert capsysbinary.readouterr().out == b"g\n"

    assert run(db, "rm", "/a") == 1
    assert capsysbinary.readouterr().err.decode().startswith("minifs rm: ENOTEMPTY: /a")
    assert run(db, "rm", "/a/g", "/a") == 0
    assert run(db, "cat", "/a/g") == 1
    err = capsysbinary.readouterr().err.decode()
    assert err.startswith("minifs cat: ENOENT: /a/g")
    assert run(db, "put", "/no/parent") == 1
    assert "ENOENT: /no/parent" in capsysbinary.readouterr().err.decode()
    assert run(db, "ls", "relative") == 1
    assert "EINVAL: relative" in capsysbinary.readouterr().err.decode()


def test_bad_password_fails_at_once(db: str, capsysbinary: Capture) -> None:
    params = conninfo_to_dict(db)
    params["password"] = "wrong"
    bad = make_conninfo("", **{k: str(v) for k, v in params.items() if v is not None})
    started = time.monotonic()
    assert main(["ls", "--dsn", bad]) == 1
    assert time.monotonic() - started < 10
    assert "password authentication failed" in capsysbinary.readouterr().err.decode()


def test_cat_refuses_a_file_replaced_midway(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(b"A" * 10))
    assert run(db, "put", "/f") == 0
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
    assert run(db, "cat", "/f") == 1
    captured = capsysbinary.readouterr()
    assert captured.out == b"AAAA"
    assert b"ESTALE: /f" in captured.err


def test_glob(db: str, monkeypatch: pytest.MonkeyPatch, capsysbinary: Capture) -> None:
    monkeypatch.setattr(sys, "stdin", _Stdin(b"x"))
    assert run(db, "put", "-p", "/w/a.md") == 0
    assert run(db, "glob", "*.md") == 0
    assert capsysbinary.readouterr().out == b"/w/a.md\n"
    assert run(db, "glob", "[", "/w") == 1
    assert capsysbinary.readouterr().err.startswith(b"minifs glob: ")
