"""Search patterns: Rust regexes (ripgrep's dialect) translated into PostgreSQL ARE.

The translation itself is the Rust extension ``minifs._regex``. This module caches it per ``(pattern, ignore_case)``
and checks, once per translated pattern, that Postgres compiles it. Postgres rejecting a translation is a bug in the
emitter, never a problem with the model's pattern, so it is logged and raised as ``RegexTranslationBug``. The one
exception is cost: a pattern too expensive for Postgres to compile is the model's to simplify (``RegexTooExpensive``).
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass

from psycopg import AsyncConnection, errors

from minifs import _regex
from minifs._regex import RegexError

__all__ = [
    "RegexError",
    "RegexTooExpensive",
    "RegexTranslationBug",
    "Translation",
    "translate",
    "translate_fixed",
    "validate",
]

logger = logging.getLogger(__name__)

# Translations are cached up to a total size: one line form can be up to 256 KB, and each \b is about 23 KB.
_CACHE_BYTES = 32 * 1024 * 1024

# Inputs whose translation Postgres has compiled. Keys are small, so this is bounded by count.
_VALIDATED_SIZE = 4096

# The translator caps how far counted repetitions expand, so most patterns compile in milliseconds. A few, such as
# (?:.*\b){20}, take a few hundred milliseconds before Postgres reports them as too complex. The timeout leaves
# room for a loaded server.
_VALIDATE_TIMEOUT = "1s"

# Most recently used last.
_translations: OrderedDict[tuple[str, bool], Translation] = OrderedDict()
_translations_bytes = 0

# (pattern, ignore_case) of translations Postgres has compiled, most recently validated last. Compilation doesn't
# depend on the database, as long as it's UTF8, which validate() checks.
_validated: OrderedDict[tuple[str, bool], None] = OrderedDict()


class RegexTranslationBug(RuntimeError):
    """The translator emitted ARE that Postgres refuses: a bug in minifs, not in the pattern."""


class RegexTooExpensive(RegexError):
    """A pattern Postgres can't compile within its limits. Shown to the model, like any ``RegexError``."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
        self.span = None
        self.syntax = False


@dataclass(frozen=True, slots=True)
class Translation:
    pattern: str
    ignore_case: bool
    line_form: str
    """For ``line ~ line_form``, one line at a time."""

    @property
    def file_form(self) -> str:
        """``(?n)`` plus the line form: matches every file in which some line matches."""
        return _regex.FILE_FORM_PREFIX + self.line_form


def translate(pattern: str, ignore_case: bool = False) -> Translation:
    """Translate a ripgrep pattern into ARE. Raises ``RegexError`` with ripgrep's message if it's rejected."""
    global _translations_bytes
    key = (pattern, ignore_case)
    if (cached := _translations.get(key)) is not None:
        _translations.move_to_end(key)
        return cached
    translation = Translation(pattern, ignore_case, _regex.translate(pattern, ignore_case))
    _translations[key] = translation
    _translations_bytes += _size(translation)
    while _translations_bytes > _CACHE_BYTES:
        _, evicted = _translations.popitem(last=False)
        _translations_bytes -= _size(evicted)
    return translation


def _size(translation: Translation) -> int:
    return len(translation.pattern) + len(translation.line_form)


def translate_fixed(text: str, ignore_case: bool = False) -> Translation:
    """Translate a fixed string (rg ``-F``). Without ``ignore_case``, ``strpos`` is the cheaper test."""
    return translate(_regex.escape(text), ignore_case)


async def validate(conn: AsyncConnection, translation: Translation) -> None:
    """Check once that Postgres compiles both forms, under a timeout.

    Runs in a savepoint (or its own transaction) that is always rolled back, so the timeout doesn't outlive it.
    Raises ``RegexTooExpensive`` when Postgres finds the pattern too complex or too slow to compile, and
    ``RegexTranslationBug`` for any other rejection. Raises ``RuntimeError`` if the database isn't UTF8.
    """
    # Reported by the server at connect time, so this costs no round trip.
    encoding = conn.info.parameter_status("server_encoding")
    if encoding != "UTF8":
        # The translation spells characters as code points, which only means the same thing in a UTF8 database.
        raise RuntimeError(f"minifs needs a UTF8 database; this one's encoding is {encoding}")
    key = (translation.pattern, translation.ignore_case)
    if key in _validated:
        _validated.move_to_end(key)
        return
    try:
        async with conn.transaction(force_rollback=True):
            await conn.execute(f"SET LOCAL statement_timeout = '{_VALIDATE_TIMEOUT}'")
            await conn.execute("SELECT '' ~ %s, '' ~ %s", (translation.line_form, translation.file_form))
    except errors.QueryCanceled as e:
        if "statement timeout" not in (e.diag.message_primary or ""):
            raise  # cancelled by the client or an administrator, not by the pattern
        raise RegexTooExpensive(
            f"regex is too expensive for Postgres to compile (over {_VALIDATE_TIMEOUT}); "
            "use fewer or smaller counted repetitions"
        ) from e
    except errors.InvalidRegularExpression as e:
        if "too complex" in (e.diag.message_primary or ""):
            raise RegexTooExpensive(
                "regex is too complex for Postgres to compile; use fewer or smaller counted repetitions"
            ) from e
        logger.error(
            "regex translator bug: Postgres rejected the translation of %r (ignore_case=%s): %s; line form: %r",
            translation.pattern,
            translation.ignore_case,
            e,
            translation.line_form,
        )
        raise RegexTranslationBug(
            f"internal error: the search pattern {translation.pattern!r} was translated into a regex Postgres "
            f"rejects ({e})"
        ) from e
    _validated[key] = None
    if len(_validated) > _VALIDATED_SIZE:
        _validated.popitem(last=False)
