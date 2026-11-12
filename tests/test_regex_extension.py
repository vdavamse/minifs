import pytest
from packaging.version import Version

import minifs
from minifs import _regex
from minifs.vfs.glob import GlobError, compile_excludes, compile_glob


def test_extension_version_matches_package() -> None:
    # Cargo spells a pre-release 0.1.0-alpha.1, the package's metadata 0.1.0a1.
    assert Version(_regex.__version__) == Version(minifs.__version__)


def test_development_build_has_oracle() -> None:
    assert _regex.HAS_ORACLE
    assert _regex.oracle_is_match(r"fn \w+\(", "pub fn main() {")
    assert not _regex.oracle_is_match(r"^\d+$", "12a")


def test_oracle_rejects_invalid_pattern() -> None:
    with pytest.raises(ValueError):
        _regex.oracle_is_match("(", "")


# A Rust panic (#100). pyo3 would raise it as PanicException, a BaseException that no `except RegexError`,
# `except GlobError` or `except Exception` catches, so one pattern would end an agent's whole run.


def test_a_panic_in_the_extension_is_a_regex_error() -> None:
    with pytest.raises(_regex.RegexError) as raised:
        _regex.oracle_panic("boom")
    error = raised.value
    assert isinstance(error, ValueError)  # an Exception, not only a BaseException
    assert (error.message, error.span, error.syntax) == (
        "internal error: the pattern translator panicked (boom)",
        None,
        False,
    )
    assert str(error) == error.message


def test_a_panic_message_is_cut() -> None:
    with pytest.raises(_regex.RegexError) as raised:
        _regex.oracle_panic("x" * 10_000)  # a message can echo a pattern
    assert raised.value.message == f"internal error: the pattern translator panicked ({'x' * 200})"


def test_the_extension_works_after_a_panic() -> None:
    with pytest.raises(_regex.RegexError):
        _regex.oracle_panic("boom")
    assert _regex.translate("a+") == "a+"
    assert _regex.compile_glob("*.md")[0] == _regex.glob_to_are("*.md")
    assert _regex.escape("a.b") == r"a\.b"


def test_a_panic_while_compiling_a_glob_is_a_glob_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def panics(pattern: str, exclude: bool = False) -> None:
        _regex.oracle_panic("boom")

    monkeypatch.setattr(_regex, "compile_glob", panics)
    # A pattern never compiled before: compile_glob caches its results.
    with pytest.raises(GlobError) as raised:
        compile_glob("issue-100-*.md")
    assert raised.value.message == "internal error: the pattern translator panicked (boom)"
    with pytest.raises(GlobError) as raised:
        compile_excludes(["issue-100-*.txt"])
    assert raised.value.message == "internal error: the pattern translator panicked (boom)"
