"""Type stubs for the Rust extension built from crates/minifs-regex."""

__version__: str
HAS_ORACLE: bool
FILE_FORM_PREFIX: str
"""``(?n)``: prepended to the line form, it gives the file form (ARE's newline-sensitive mode)."""

class RegexError(ValueError):
    """A pattern the translator rejects.

    ``message`` is the text to show: regex-syntax's error formatted as ripgrep prints it for parse errors, otherwise
    a one-line reason with a hint. ``span`` holds the character offsets ``(start, end)`` of the offending part when
    known. ``syntax`` is true when regex-syntax rejected the pattern, so ripgrep rejects it too. ``construct`` labels
    what was rejected, for metrics: ``look_around``, ``backreference`` or ``syntax`` for a parse error, ``newline``,
    ``repetition_count``, ``pattern_size``, ... for what has no ARE equivalent, ``glob`` for a glob.
    """

    message: str
    span: tuple[int, int] | None
    syntax: bool
    construct: str

def translate(pattern: str, ignore_case: bool = False) -> str:
    """Translate a Rust regex, parsed as ripgrep parses it, into PostgreSQL ARE (the line form).

    Raises ``RegexError`` for a pattern ripgrep rejects or that has no ARE equivalent.
    """

def compile_glob(pattern: str, exclude: bool = False) -> tuple[str, list[str], int | None, str | None, bool]:
    """Compile a glob as ``rg -g`` takes it (with ``exclude``, as what follows ``rg -g '!'``):
    ``(regex, prefix, max_depth, name_like, only_dir)``.

    ``regex`` is an anchored ARE over paths relative to the search root, where ``?`` and classes match one
    character and never ``/``. ``prefix`` holds the literal leading directories, ``max_depth`` the deepest level
    below them that can match (None: unbounded), ``name_like`` a LIKE pattern every matching file name satisfies
    (None: it would filter nothing), and ``only_dir`` is true for a pattern ending in ``/``.

    Raises ``RegexError`` with globset's message (``syntax`` true) for a glob rg rejects, and for an empty pattern,
    a NUL, braces nested over 64 deep, a glob over 4,096 characters and classes once translated, and, unless
    ``exclude``, a leading ``!`` or ``#`` (rg reads them as a negation and a comment).
    """

def glob_to_are(pattern: str) -> str:
    """The ``regex`` of ``compile_glob(pattern)``; also raises ``RegexError`` for a pattern ending in ``/``, which
    matches only directories."""

def escape(text: str) -> str:
    """Escape regex metacharacters, so the result matches ``text`` literally."""

# Only present when the crate was built with the `oracle` feature (development builds, where HAS_ORACLE is true).
ORACLE_CHARS: str

def oracle_is_match(pattern: str, haystack: str, ignore_case: bool = False) -> bool: ...
def oracle_random_pattern(seed: int) -> str: ...
def oracle_glob_is_match(pattern: str, path: str, is_dir: bool = False, exclude: bool = False) -> bool:
    """Whether ``rg -g pattern`` (with ``exclude``, ``rg -g '!pattern'``) matches ``path`` (a directory when
    ``is_dir``), by ripgrep's own matcher (bytes, not characters)."""
