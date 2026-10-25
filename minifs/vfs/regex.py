"""Search patterns: Rust regexes (ripgrep's dialect) translated into PostgreSQL ARE.

The translation itself is the Rust extension ``minifs._regex``. This module caches it per ``(pattern, ignore_case)``
and checks, once per translated pattern, that Postgres compiles it. Postgres rejecting a translation is a bug in the
emitter, never a problem with the model's pattern, so it is logged and raised as ``RegexTranslationBug``. The one
exception is cost: a pattern too expensive for Postgres to compile is the model's to simplify (``RegexTooExpensive``).
"""

from __future__ import annotations

import logging
import math
import sys
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
    "compile_expense",
    "translate",
    "translate_fixed",
    "validate",
]

logger = logging.getLogger(__name__)

# Translations are cached up to a total size in memory. One line form can be up to 256 K characters, and the
# Unicode \w holds characters outside the BMP, so CPython stores these strings at 4 bytes per character: a \b is
# about 23 K characters, or 90 KB.
_CACHE_BYTES = 32 * 1024 * 1024

# Inputs whose translation Postgres has compiled. Patterns are at most 64 KB, so this is bounded by count.
_VALIDATED_SIZE = 4096

# The translator caps how far counted repetitions expand, so most patterns compile in milliseconds. A few, such as
# (?:.*\b){20}, take a few hundred milliseconds before Postgres reports them as too complex. The timeout leaves
# room for a loaded server.
_VALIDATE_TIMEOUT = "1s"
_VALIDATE_SECONDS = 1.0

# Most recently used last.
_translations: OrderedDict[tuple[str, bool], Translation] = OrderedDict()
_translations_bytes = 0

# (pattern, ignore_case) of translations Postgres has compiled, most recently validated last. Compilation doesn't
# depend on the database, as long as it's UTF8, which validate() checks.
_validated: OrderedDict[tuple[str, bool], None] = OrderedDict()

# (pattern, ignore_case) Postgres found too expensive whatever the load, with the message to repeat, most recent
# last.
_too_expensive: OrderedDict[tuple[str, bool], str] = OrderedDict()


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
    return sys.getsizeof(translation.pattern) + sys.getsizeof(translation.line_form)


def translate_fixed(text: str, ignore_case: bool = False) -> Translation:
    """Translate a fixed string (rg ``-F``). Without ``ignore_case``, ``strpos`` is the cheaper test."""
    return translate(_regex.escape(text), ignore_case)


async def validate(conn: AsyncConnection, translation: Translation, timeout: float | None = None) -> None:
    """Check once that Postgres compiles both forms, under a timeout.

    Runs in a savepoint (or its own transaction) that is always rolled back, so the timeout doesn't outlive it.
    Raises ``RegexTooExpensive`` when Postgres finds the pattern too complex or too slow to compile, and
    ``RegexTranslationBug`` for any other rejection. Raises ``RuntimeError`` if the database isn't UTF8.

    ``timeout`` (seconds) shortens the compile's own 1 s limit to the caller's; when that shorter limit is what
    cancels the compile, ``QueryCanceled`` propagates for the caller to report as its own timeout.
    """
    shortened = timeout is not None and timeout < _VALIDATE_SECONDS
    limit = f"{max(1, math.ceil(timeout * 1000))}ms" if timeout is not None and shortened else _VALIDATE_TIMEOUT
    # Reported by the server at connect time, so this costs no round trip.
    encoding = conn.info.parameter_status("server_encoding")
    if encoding != "UTF8":
        # The translation spells characters as code points, which only means the same thing in a UTF8 database.
        raise RuntimeError(f"minifs needs a UTF8 database; this one's encoding is {encoding}")
    key = (translation.pattern, translation.ignore_case)
    if key in _validated:
        _validated.move_to_end(key)
        return
    if (message := _too_expensive.get(key)) is not None:
        _too_expensive.move_to_end(key)
        raise RegexTooExpensive(message)
    try:
        async with conn.transaction(force_rollback=True):
            await conn.execute("SELECT set_config('statement_timeout', %s, true)", (limit,))
            await conn.execute("SELECT '' ~ %s, '' ~ %s", (translation.line_form, translation.file_form))
    except errors.DatabaseError as e:
        if isinstance(e, errors.QueryCanceled) and shortened:
            raise  # the caller's timeout, shorter than ours: not a verdict on the pattern
        if (expense := compile_expense(e)) is not None:
            reason, deterministic = expense
            message = f"{reason}; use a shorter pattern or fewer, smaller counted repetitions"
            if deterministic:
                # Retries of the same pattern don't cost the server another compile.
                _too_expensive[key] = message
                if len(_too_expensive) > _VALIDATED_SIZE:
                    _too_expensive.popitem(last=False)
            raise RegexTooExpensive(message) from e
        if not isinstance(e, errors.InvalidRegularExpression):
            raise  # not about the pattern: a cancel, a lost connection, ...
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


def compile_expense(error: errors.DatabaseError) -> tuple[str, bool] | None:
    """Whether a failure to compile a translated regex means it's too expensive for Postgres.

    Returns the reason and whether the outcome is deterministic (the same on an idle server), or ``None`` when the
    failure isn't about cost.
    """
    message = str(error)  # the server's primary message
    if isinstance(error, errors.InvalidRegularExpression):
        if "too complex" in message:
            return "regex is too complex for Postgres to compile", True
        if "out of memory" in message:
            # The regex compiler's own allocation failure: depends on the server's memory limits and load.
            return "regex is too large for Postgres to compile", False
        return None
    if isinstance(error, errors.QueryCanceled):
        if "statement timeout" in message:
            return f"regex is too expensive for Postgres to compile (over {_VALIDATE_TIMEOUT})", False
        return None
    if isinstance(error, errors.ProgramLimitExceeded):
        return "regex is too large for Postgres to compile", True
    if isinstance(error, errors.InsufficientResources):
        return "regex is too large for Postgres to compile", False
    if isinstance(error, errors.InternalError_) and "invalid memory alloc request size" in message:
        # A huge NFA makes Postgres refuse the allocation up front, whatever the load.
        return "regex is too large for Postgres to compile", True
    return None
