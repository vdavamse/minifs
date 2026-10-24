"""Type stubs for the Rust extension built from crates/minifs-regex."""

__version__: str
HAS_ORACLE: bool

class RegexError(ValueError):
    """A pattern the translator rejects.

    ``message`` is the text to show: regex-syntax's error formatted as ripgrep prints it for parse errors, otherwise
    a one-line reason with a hint. ``span`` holds the character offsets ``(start, end)`` of the offending part when
    known. ``syntax`` is true when regex-syntax rejected the pattern, so ripgrep rejects it too.
    """

    message: str
    span: tuple[int, int] | None
    syntax: bool

def translate(pattern: str, ignore_case: bool = False) -> tuple[str, str]:
    """Translate a Rust regex, parsed as ripgrep parses it, into PostgreSQL ARE: ``(line_form, file_form)``.

    Raises ``RegexError`` for a pattern ripgrep rejects or that has no ARE equivalent.
    """

def escape(text: str) -> str:
    """Escape regex metacharacters, so the result matches ``text`` literally."""

# Only present when the crate was built with the `oracle` feature (development builds).
ORACLE_CHARS: str

def oracle_is_match(pattern: str, haystack: str, ignore_case: bool = False) -> bool: ...
def oracle_random_pattern(seed: int) -> str: ...
