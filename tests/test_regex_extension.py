import pytest

import minifs
from minifs import _regex


def test_extension_version_matches_package() -> None:
    assert _regex.__version__ == minifs.__version__


def test_development_build_has_oracle() -> None:
    assert _regex.HAS_ORACLE
    assert _regex.oracle_is_match(r"fn \w+\(", "pub fn main() {")
    assert not _regex.oracle_is_match(r"^\d+$", "12a")


def test_oracle_rejects_invalid_pattern() -> None:
    with pytest.raises(ValueError):
        _regex.oracle_is_match("(", "")
