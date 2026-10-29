"""Path normalisation and the value types in minifs.vfs.core."""

import psycopg
import pytest

from minifs.vfs.core import CONTROL_CHARACTERS, Entry, VFSError, basename, escape_controls, join, normalize, split


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/", "/"),
        ("//", "/"),
        ("/a", "/a"),
        ("/a/", "/a"),
        ("//a//b///c", "/a/b/c"),
        ("/a/./b/.", "/a/b"),
        ("/a/b/../c", "/a/c"),
        ("/a/b/../../c", "/c"),
        ("/..", "/"),
        ("/../../a", "/a"),
        ("/a/../..", "/"),
        ("/workspace/../docs//a.md", "/docs/a.md"),
        ("/a/.../b", "/a/.../b"),  # only . and .. are special
        ("/a/..b/c.", "/a/..b/c."),
        ("/ a /b ", "/ a /b "),  # spaces are part of names
        ("/naïve/文件", "/naïve/文件"),
    ],
)
def test_normalize(path: str, expected: str) -> None:
    assert normalize(path) == expected
    assert normalize(expected) == expected  # idempotent


def test_split() -> None:
    assert split("/") == ()
    assert split("/a//b/../c/") == ("a", "c")
    assert join(*split("/x/./y")) == "/x/y"


@pytest.mark.parametrize("path", ["", "a", "a/b", "./a", "../a", "~/a"])
def test_relative_paths_are_einval(path: str) -> None:
    with pytest.raises(VFSError) as info:
        normalize(path)
    assert info.value.errno == "EINVAL"
    assert info.value.path == path


@pytest.mark.parametrize(
    "path",
    [
        "/a\0b",
        "/a\nb",
        "/x\x1b[31m",
        "/tab\there/x",
        "/\x7f",
        "/c1\x85",
        "/\u202eevil",
        "/\u2066x\u2069",
        "/a/b\r",
        "/p\u2029s",
    ],
)
def test_control_characters_are_einval(path: str) -> None:
    """A name can't contain one (migration 016), and the path is refused before any round trip."""
    with pytest.raises(VFSError) as info:
        split(path)
    assert info.value.errno == "EINVAL"
    assert info.value.path == escape_controls(path)
    assert not CONTROL_CHARACTERS.search(str(info.value))  # the refused character isn't echoed
    assert info.value.message.startswith("path contains a control character U+")
    assert len(info.value.message) == len("path contains a control character U+0000")


@pytest.mark.parametrize("path", ["/ a b /<x>", "/é/😀", "/a\\b", "/\u200emark"])
def test_printable_characters_are_names(path: str) -> None:
    assert split(path) == tuple(path.split("/")[1:])


def test_escape_controls() -> None:
    assert escape_controls("/a\nb/x\x1b[31m/\u202eevil") == "/a\\u000ab/x\\u001b[31m/\\u202eevil"
    assert escape_controls("/plain/é") == "/plain/é"


def test_basename_and_entry_name() -> None:
    assert basename("/") == ""
    assert basename("/a/b.txt") == "b.txt"
    assert Entry("/a/b", None, "dir", 0, None).name == "b"
    assert Entry("/", None, "dir", 0, None).is_dir


def test_vfs_error_message() -> None:
    e = VFSError("ENOENT", "/a", "component 1 does not exist")
    assert (e.errno, e.path, e.message) == ("ENOENT", "/a", "component 1 does not exist")
    assert str(e) == "ENOENT: /a: component 1 does not exist"
    assert str(VFSError("EEXIST", "/")) == "EEXIST: /"
    # The server's DETAIL names real components and ids: kept apart, out of str().
    e = VFSError("ENOENT", "/a", detail="component 2 of '{workspace,secret}' does not exist")
    assert str(e) == "ENOENT: /a" and "secret" in e.detail


def test_from_db_ignores_other_errors() -> None:
    assert VFSError.from_db(psycopg.errors.DivisionByZero("x"), "/a") is None
