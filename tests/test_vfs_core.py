"""Path normalisation and the value types in minifs.vfs.core."""

import psycopg
import pytest

from minifs.vfs.core import Entry, VFSError, basename, join, normalize, split


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


def test_nul_is_einval() -> None:
    with pytest.raises(VFSError) as info:
        split("/a\0b")
    assert info.value.errno == "EINVAL"


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
