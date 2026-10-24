"""The regex translator, checked against the Rust ``regex`` crate (the oracle) in Postgres, in both locales."""

from __future__ import annotations

import logging
import random
import subprocess
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import AsyncConnection, sql

from minifs import _regex
from minifs.vfs import regex
from minifs.vfs.regex import RegexError, RegexTooExpensive, RegexTranslationBug, Translation
from tests.conftest import PgServer
from tests.regex_corpus import LINES, PATTERNS

# The oracle and the random patterns exist only in development builds; test_regex_extension.py fails without them.
pytestmark = pytest.mark.skipif(not _regex.HAS_ORACLE, reason="minifs._regex was built without the oracle feature")

# Patterns regex-syntax rejects, so ripgrep must reject them too.
SYNTAX_ERRORS = [
    "(",
    ")",
    "[a",
    "a{2,1}",
    "*a",
    "foo(?=bar)",
    "foo(?!bar)",
    "(?<=a)b",
    "(?<!a)b",
    r"(a)\1",
    r"\Z",
    r"\Qa.b\E",
    r"\h",
    r"\p{Nope}",
    r"\pX",
    "[z-a]",
    "(?P<n>a)(?P<n>b)",
    "(?Q)a",
    r"\c",
    r"\x{110000}",
    "(?i",
    "\\",
    # rg wraps patterns in (?:...), so a (?x) comment swallows the closing paren.
    "(?x)a#c",
    r"(?x) \d+ # digits",
]

# Patterns ripgrep also rejects without -U, though regex-syntax parses them.
NEWLINE_ERRORS = [r"a\nb", "a\nb", r"[\n]", r"(?i)\n", r"\x0A"]

# Patterns ripgrep accepts but the translator can't express in ARE.
UNSUPPORTED = {
    r"a{256}": "at most 255",
    r"(?:ab){1,1000}": "at most 255",
    r"(?R)^a": "CRLF",
    r"(?-u:\xFF)": "UTF-8",
    r"(?-u:[^a])": "byte class",
    r"\b\w\b" * 20: "too large",
    r"(?:a{255}){255}": "expand to",
    r"(?:a{0,100}){100}": "expand to",
    r"(?-u:\B)": "use \\B",
}


def _matches(conn: psycopg.Connection, are: str, haystacks: list[str]) -> list[bool]:
    row = conn.execute(
        "SELECT array_agg(h ~ %s ORDER BY i) FROM unnest(%s::text[]) WITH ORDINALITY AS t(h, i)", (are, haystacks)
    ).fetchone()
    assert row is not None
    return row[0]


def _mismatches(conn: psycopg.Connection, pattern: str, lines: list[str], ignore_case: bool) -> list[str]:
    try:
        translation = regex.translate(pattern, ignore_case)
    except RegexError as e:
        return [f"{pattern!r} (ignore_case={ignore_case}) rejected: {e}"]
    actual = _matches(conn, translation.line_form, lines)
    return [
        f"{pattern!r} (ignore_case={ignore_case}) on {line!r}: oracle {expected}, Postgres {got}"
        for line, got in zip(lines, actual, strict=True)
        if got != (expected := _regex.oracle_is_match(pattern, line, ignore_case))
    ]


@pytest.mark.parametrize("ignore_case", [False, True], ids=["case", "ignore_case"])
def test_corpus_agrees_with_oracle(db_any_locale: str, ignore_case: bool) -> None:
    assert len(PATTERNS) >= 200
    with psycopg.connect(db_any_locale) as conn:
        problems = [p for pattern in PATTERNS for p in _mismatches(conn, pattern, LINES, ignore_case)]
    assert not problems, "\n".join(problems[:50])


def _random_line(rng: random.Random) -> str:
    return "".join(rng.choice(_regex.ORACLE_CHARS) for _ in range(rng.randrange(0, 12)))


def _random_patterns(count: int) -> list[str]:
    """Random HIR printed as Rust patterns, skipping the few that translate past the size cap."""
    patterns: list[str] = []
    for seed in range(count):
        pattern = _regex.oracle_random_pattern(seed)
        try:
            regex.translate(pattern)
        except RegexError as e:
            assert "too large" in e.message, (pattern, e.message)
            continue
        patterns.append(pattern)
    return patterns


def test_random_patterns_agree_with_oracle(db_any_locale: str) -> None:
    rng = random.Random(9)
    problems: list[str] = []
    with psycopg.connect(db_any_locale) as conn:
        for i, pattern in enumerate(_random_patterns(600)):
            lines = [_random_line(rng) for _ in range(40)]
            problems += _mismatches(conn, pattern, lines, ignore_case=i % 3 == 0)
    assert not problems, "\n".join(problems[:50])


def _split_lines(body: str) -> list[str]:
    """Lines as the search query splits them: the final newline dropped, then split on LF (CR stays in the line)."""
    return body.removesuffix("\n").split("\n")


def test_file_form_matches_every_file_with_a_matching_line(db_any_locale: str) -> None:
    rng = random.Random(18)
    problems: list[str] = []
    with psycopg.connect(db_any_locale) as conn:
        for pattern in _random_patterns(300) + PATTERNS:
            translation = regex.translate(pattern)
            files = [[_random_line(rng) for _ in range(rng.randrange(1, 6))] for _ in range(8)]
            files += [LINES[i : i + 5] for i in range(0, len(LINES), 25)]
            # As files are stored: with or without a final newline, and with LF or CRLF line ends.
            bodies = [body for f in files for body in ("\n".join(f), "\n".join(f) + "\n", "\r\n".join(f) + "\r\n")]
            file_hits = _matches(conn, translation.file_form, bodies)
            for body, file_hit in zip(bodies, file_hits, strict=True):
                lines = _split_lines(body)
                if not file_hit and any(_matches(conn, translation.line_form, lines)):
                    problems.append(f"{pattern!r}: file form misses {body!r}")
    assert not problems, "\n".join(problems[:50])


def test_word_boundaries_hold_in_the_c_locale(db_c: str) -> None:
    parse_config = regex.translate(r"\bparse_config\b").line_form
    deja = regex.translate(r"\bdéjà\b").line_form
    e_acute = regex.translate("é", ignore_case=True).line_form
    with psycopg.connect(db_c) as conn:
        assert _matches(conn, parse_config, ["def parse_config(", "parse_configs"]) == [True, False]
        assert _matches(conn, deja, ["x déjà y", "xdéjà", "déjàvu"]) == [True, False, False]
        assert _matches(conn, e_acute, ["É"]) == [True]


@pytest.mark.parametrize("pattern", SYNTAX_ERRORS + NEWLINE_ERRORS)
def test_rejections_are_rejected_by_rg(pattern: str, rg_path: str, tmp_path: Path) -> None:
    with pytest.raises(RegexError):
        regex.translate(pattern)
    haystack = tmp_path / "haystack.txt"
    haystack.write_text("a\n")
    result = subprocess.run([rg_path, "-e", pattern, "--", str(haystack)], capture_output=True, text=True)
    assert result.returncode == 2, (result.returncode, result.stdout, result.stderr)


@pytest.mark.parametrize("pattern", SYNTAX_ERRORS)
def test_syntax_errors_carry_rg_message(pattern: str) -> None:
    with pytest.raises(RegexError) as info:
        regex.translate(pattern)
    error = info.value
    assert error.syntax
    assert error.message.startswith("regex parse error:")
    assert str(error) == error.message
    assert error.span is None or 0 <= error.span[0] <= error.span[1] <= len(pattern)


def test_syntax_error_span_counts_characters() -> None:
    with pytest.raises(RegexError) as info:
        regex.translate("déjà(?=x)")
    assert info.value.span == (4, 7)
    assert "look-around" in info.value.message


@pytest.mark.parametrize("pattern", NEWLINE_ERRORS + list(UNSUPPORTED))
def test_unsupported_patterns_are_rejected_with_a_hint(pattern: str) -> None:
    with pytest.raises(RegexError) as info:
        regex.translate(pattern)
    error = info.value
    assert not error.syntax
    assert UNSUPPORTED.get(pattern, r"\n") in error.message
    assert isinstance(error, ValueError)


def test_translate_is_cached_and_file_form_is_newline_sensitive() -> None:
    first = regex.translate(r"\bfoo\b", ignore_case=True)
    assert regex.translate(r"\bfoo\b", ignore_case=True) is first
    assert first.file_form == "(?n)" + first.line_form
    assert regex.translate(r"\bfoo\b").line_form != first.line_form


def test_translate_fixed_escapes_metacharacters(db_any_locale: str) -> None:
    translation = regex.translate_fixed("a.b(c)*", ignore_case=True)
    long_text = "lorem ipsum " * 400
    with psycopg.connect(db_any_locale) as conn:
        assert _matches(conn, translation.line_form, ["x A.B(C)* y", "aXb(c)*", "a.b(c)"]) == [True, False, False]
        # Long literals aren't counted as repetitions.
        long = regex.translate_fixed(long_text, ignore_case=True)
        assert _matches(conn, long.line_form, [long_text.upper(), long_text[1:]]) == [True, False]


def test_patterns_are_parsed_as_rg_wraps_them() -> None:
    # rg parses (?:PATTERN), so a stray closing paren is accepted, as it is by rg.
    assert regex.translate("a)(b").line_form == "a(?:b)"
    assert regex.translate("a)|(b").line_form == "(?:a|(?:b))"


async def test_validate_accepts_translations_and_restores_the_timeout(conn: AsyncConnection) -> None:
    async with conn.transaction():
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        await regex.validate(conn, regex.translate(r"(?i)\b\w+\b"))
        row = await (await conn.execute("SELECT current_setting('statement_timeout')")).fetchone()
        assert row == ("5s",)


async def test_validate_reports_an_emitter_bug(conn: AsyncConnection, caplog: pytest.LogCaptureFixture) -> None:
    broken = Translation("x", False, "a{2,256}")
    with caplog.at_level(logging.ERROR, logger="minifs.vfs.regex"), pytest.raises(RegexTranslationBug):
        await regex.validate(conn, broken)
    assert "regex translator bug" in caplog.text
    # A failed translation isn't remembered as valid.
    with pytest.raises(RegexTranslationBug):
        await regex.validate(conn, broken)


async def test_validate_failure_leaves_the_outer_transaction_usable(db: str) -> None:
    async with await AsyncConnection.connect(db) as conn:  # not autocommit: validate runs in a savepoint
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        with pytest.raises(RegexTranslationBug):
            await regex.validate(conn, Translation("y", False, "a{2,256}"))
        row = await (await conn.execute("SELECT current_setting('statement_timeout')")).fetchone()
        assert row == ("5s",)


async def test_validate_reports_expensive_patterns_to_the_model(
    conn: AsyncConnection, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The translator refuses both of these; built by hand, they reach Postgres's own limits.
    too_complex = Translation("(?:a{255}){255}", False, "(?:a{255}){255}")
    with caplog.at_level(logging.ERROR, logger="minifs.vfs.regex"), pytest.raises(RegexTooExpensive) as info:
        await regex.validate(conn, too_complex)
    assert isinstance(info.value, RegexError) and "too complex" in info.value.message
    # A pattern the translator accepts, which Postgres still finds too complex.
    with caplog.at_level(logging.ERROR, logger="minifs.vfs.regex"), pytest.raises(RegexTooExpensive):
        await regex.validate(conn, regex.translate(r"(?:.*\b){20}"))
    monkeypatch.setattr(regex, "_VALIDATE_TIMEOUT", "1ms")
    slow = Translation("(?:[a-z]{0,255}){80}", False, "(?:[a-z]{0,255}){80}")
    with caplog.at_level(logging.ERROR, logger="minifs.vfs.regex"), pytest.raises(RegexTooExpensive) as info:
        await regex.validate(conn, slow)
    assert "too expensive" in info.value.message and info.value.span is None and not info.value.syntax
    assert "translator bug" not in caplog.text


async def test_validate_refuses_a_non_utf8_database(pg_server: PgServer) -> None:
    name = f"test_{uuid.uuid4().hex}"
    with psycopg.connect(pg_server.conninfo, autocommit=True) as admin:
        admin.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'SQL_ASCII' LOCALE 'C'").format(
                sql.Identifier(name)
            )
        )
    try:
        async with await AsyncConnection.connect(pg_server.database(name), autocommit=True) as conn:
            with pytest.raises(RuntimeError, match="UTF8"):
                await regex.validate(conn, regex.translate(r"\bnot_cached_elsewhere\b"))
    finally:
        with psycopg.connect(pg_server.conninfo, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.mark.parametrize("pattern", list(UNSUPPORTED))
def test_unsupported_patterns_are_accepted_by_rg(pattern: str, rg_path: str, tmp_path: Path) -> None:
    haystack = tmp_path / "haystack.txt"
    haystack.write_text("a\n")
    result = subprocess.run([rg_path, "-e", pattern, "--", str(haystack)], capture_output=True, text=True)
    assert result.returncode in (0, 1), (result.returncode, result.stderr)
