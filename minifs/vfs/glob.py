"""Glob patterns for ``vfs_glob``: the translator to anchored Postgres AREs and the pruning hints (README: Glob).

Patterns follow ripgrep and gitignore. ``*`` and ``?`` stay within one path segment, ``**`` as a whole segment
spans directories (anywhere else it is a ``*``), ``[...]`` is a character class (``!`` or ``^`` negates, with
ASCII-only POSIX classes such as ``[[:alpha:]]``), ``{a,b}`` is an alternation that may nest, and a backslash
makes the next character literal. A pattern with no ``/`` matches the file name at any depth; a pattern with
one is anchored at the search root (a leading ``/`` only anchors). Paths are relative to the search root.

The translation uses no ARE classes, word boundaries or case folding, only literal characters and explicit
ranges, so a regex means the same under every database locale.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from uuid import UUID

from psycopg import AsyncConnection

# Brace alternatives are expanded before translation, so `{a,b}{c,d}` costs four; this caps the product.
MAX_ALTERNATIVES = 1024

# Characters with a meaning in an ARE outside brackets. A backslash before any other non-alphanumeric
# character is harmless, but before a letter it is an escape (`\y`, `\m`), so letters stay bare.
_ARE_SPECIAL = frozenset(".^$|()[]{}*+?\\")

_MAX_CODE_POINT = 0x10FFFF
_SLASH = ord("/")

# ASCII only, as in wcmatch and in the C locale.
_POSIX_CLASSES: dict[str, tuple[tuple[int, int], ...]] = {
    "alnum": ((0x30, 0x39), (0x41, 0x5A), (0x61, 0x7A)),
    "alpha": ((0x41, 0x5A), (0x61, 0x7A)),
    "ascii": ((0x00, 0x7F),),
    "blank": ((0x09, 0x09), (0x20, 0x20)),
    "cntrl": ((0x00, 0x1F), (0x7F, 0x7F)),
    "digit": ((0x30, 0x39),),
    "graph": ((0x21, 0x7E),),
    "lower": ((0x61, 0x7A),),
    "print": ((0x20, 0x7E),),
    "punct": ((0x21, 0x2F), (0x3A, 0x40), (0x5B, 0x60), (0x7B, 0x7E)),
    "space": ((0x09, 0x0D), (0x20, 0x20)),
    "upper": ((0x41, 0x5A),),
    "word": ((0x30, 0x39), (0x41, 0x5A), (0x5F, 0x5F), (0x61, 0x7A)),
    "xdigit": ((0x30, 0x39), (0x41, 0x46), (0x61, 0x66)),
}


class GlobError(ValueError):
    """A glob pattern that can't be translated: an unclosed ``[`` or ``{``, or too many alternatives."""

    def __init__(self, pattern: str, message: str) -> None:
        super().__init__(f"invalid glob {pattern!r}: {message}")
        self.pattern = pattern
        self.message = message


@dataclass(frozen=True)
class GlobPlan:
    """What ``vfs_glob`` needs for one pattern: the regex, and the hints that let it walk less of the tree."""

    regex: str  # anchored ARE over the path relative to the search root
    prefix: tuple[str, ...] = ()  # literal leading directories: walk only that subtree
    max_depth: int | None = None  # deepest level below the prefix that can match; None when the pattern has **
    name_like: str | None = None  # LIKE pattern every matching file name satisfies


@dataclass(frozen=True)
class Excludes:
    """Exclude patterns as ``vfs_glob`` takes them."""

    regexes: tuple[str, ...] = ()  # files and directories whose path matches one are skipped
    prune_dirs: tuple[str, ...] = ()  # names of directories never walked, at any depth (`**/<name>/**`)


@dataclass(frozen=True)
class GlobMatch:
    id: UUID
    path: str  # relative to the search root, no leading /
    size: int
    mtime: datetime


def glob_to_regex(pattern: str) -> str:
    """Translate a ripgrep/gitignore-style glob to an anchored Postgres ARE over relative paths."""
    return compile_glob(pattern).regex


@lru_cache(maxsize=1024)
def compile_glob(pattern: str) -> GlobPlan:
    """The regex and the pruning hints for ``pattern``. Raises GlobError."""
    alternatives = [_segments(_tokenize(pattern, alt)) for alt in _expand_braces(pattern)]
    anchored = any(len(segments) > 1 for segments in alternatives)
    if anchored:  # a leading '/' only anchors
        alternatives = [s[1:] if len(s) > 1 and not s[0] else s for s in alternatives]
    bodies = list(dict.fromkeys(_emit(segments) for segments in alternatives))
    body = bodies[0] if len(bodies) == 1 else "(?:" + "|".join(bodies) + ")"
    regex = f"^{body}$" if anchored else f"^(?:.*/)?{body}$"
    if not anchored:
        return GlobPlan(regex, name_like=_name_like(alternatives))
    prefix = os.path.commonprefix([_literal_prefix(segments) for segments in alternatives])
    max_depth = None
    if not any(_is_globstar(segment) for segments in alternatives for segment in segments):
        max_depth = max(len(segments) for segments in alternatives) - len(prefix)
    return GlobPlan(regex, tuple(prefix), max_depth, _name_like(alternatives))


def compile_excludes(patterns: Iterable[str], *, prune: bool = True) -> Excludes:
    """The regexes and pruned directory names for exclude patterns (rg's ``-g '!...'``). Raises GlobError.

    With ``prune``, a pattern ``**/<name>/**`` with a literal name becomes a pruned directory name instead of a
    regex, so the walk never enters those directories. The result is the same either way.
    """
    regexes: list[str] = []
    prune_dirs: list[str] = []
    for pattern in dict.fromkeys(patterns):
        name = _globstar_dir_name(pattern) if prune else None
        if name is not None:
            prune_dirs.append(name)
        else:
            regexes.append(compile_glob(pattern).regex)
    return Excludes(tuple(regexes), tuple(prune_dirs))


async def glob(
    conn: AsyncConnection,
    root: UUID | None,
    pattern: str,
    *,
    excludes: Sequence[str] = (),
    hidden: bool = False,
    limit: int | None = None,
    hints: bool = True,
) -> list[GlobMatch]:
    """The files below directory ``root`` (None: ``/``) that match ``pattern``, newest first, as ``vfs_glob``
    returns them. ``.git`` directories are never walked. Without ``hints`` it walks the whole tree (for tests).

    Raises GlobError for a bad pattern, and ENOENT / ENOTDIR (see ``minifs.db.errors``) for a bad ``root``.
    """
    plan = compile_glob(pattern)
    ex = compile_excludes(excludes, prune=hints)
    if not hints:
        plan = GlobPlan(plan.regex)
    cur = await conn.execute(
        "SELECT id, path, size, mtime FROM vfs_glob(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            root,
            plan.regex,
            plan.name_like,
            plan.max_depth,
            list(ex.regexes),
            hidden,
            limit,
            list(plan.prefix),
            list(ex.prune_dirs),
        ),
    )
    return [GlobMatch(*row) for row in await cur.fetchall()]


# Parsing. A pattern is first expanded into its brace alternatives, each alternative is tokenized, and the
# tokens are split into path segments.


@dataclass(frozen=True)
class _Literal:
    char: str


@dataclass(frozen=True)
class _Star:
    pass


@dataclass(frozen=True)
class _Any:  # ?
    pass


@dataclass(frozen=True)
class _Class:
    ranges: tuple[tuple[int, int], ...]  # sorted, disjoint, never containing '/'
    negated: bool  # a negated class never matches '/' either


@dataclass(frozen=True)
class _Separator:
    pass


type _Token = _Literal | _Star | _Any | _Class | _Separator
type _Segment = list[_Literal | _Star | _Any | _Class]

_STAR = _Star()
_ANY = _Any()
_SEPARATOR = _Separator()


def _expand_braces(pattern: str) -> list[str]:
    if pattern == "":
        raise GlobError(pattern, "empty pattern")
    if "\0" in pattern:
        raise GlobError(pattern, "NUL character")
    alternatives = _expand(pattern, pattern)
    if len(alternatives) > MAX_ALTERNATIVES:
        raise GlobError(pattern, f"more than {MAX_ALTERNATIVES} brace alternatives")
    return alternatives


def _expand(pattern: str, p: str) -> list[str]:
    """The alternatives of ``p``, a part of ``pattern``: the first top-level ``{...}`` and everything after it
    expanded, escapes and classes left as written."""
    i = 0
    while i < len(p):
        c = p[i]
        if c == "\\":
            i += 2
        elif c == "[":
            i = _class_end(pattern, p, i) + 1
        elif c == "{":
            j, parts = _brace_parts(pattern, p, i)
            heads = [alt for part in parts for alt in _expand(pattern, part)]
            tails = _expand(pattern, p[j + 1 :])
            if len(heads) * len(tails) > MAX_ALTERNATIVES:
                raise GlobError(pattern, f"more than {MAX_ALTERNATIVES} brace alternatives")
            return [p[:i] + head + tail for head in heads for tail in tails]
        else:
            i += 1
    return [p]


def _brace_parts(pattern: str, p: str, start: int) -> tuple[int, list[str]]:
    """The index of the ``}`` matching ``p[start]``, and the top-level comma-separated parts between them."""
    depth = 0
    parts: list[str] = []
    part_start = start + 1
    i = start + 1
    while i < len(p):
        c = p[i]
        if c == "\\":
            i += 2
            continue
        if c == "[":
            i = _class_end(pattern, p, i) + 1
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            if depth == 0:
                parts.append(p[part_start:i])
                return i, parts
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(p[part_start:i])
            part_start = i + 1
        i += 1
    raise GlobError(pattern, "unclosed '{'")


def _class_end(pattern: str, p: str, start: int) -> int:
    """The index of the ``]`` that closes the class opened at ``p[start]``."""
    return _parse_class(pattern, p, start)[1]


def _parse_class(pattern: str, p: str, start: int) -> tuple[_Class, int]:
    """The class opened at ``p[start]`` and the index of its closing ``]``."""
    i = start + 1
    negated = i < len(p) and p[i] in "!^"
    if negated:
        i += 1
    ranges: list[tuple[int, int]] = []
    first = True
    while True:
        if i >= len(p):
            raise GlobError(pattern, "unclosed '['")
        c = p[i]
        if c == "]" and not first:
            break
        first = False
        if p.startswith("[:", i):
            end = p.find(":]", i + 2)
            if end >= 0 and p[i + 2 : end] in _POSIX_CLASSES:
                ranges.extend(_POSIX_CLASSES[p[i + 2 : end]])
                i = end + 2
                continue
        lo, i = _class_char(p, i)
        if i + 1 < len(p) and p[i] == "-" and p[i + 1] != "]":
            hi, i = _class_char(p, i + 1)
            if ord(hi) >= ord(lo):
                ranges.append((ord(lo), ord(hi)))
            # A reversed range matches nothing.
        else:
            ranges.append((ord(lo), ord(lo)))
    return _Class(_without_slash(ranges), negated), i


def _class_char(p: str, i: int) -> tuple[str, int]:
    if p[i] == "\\" and i + 1 < len(p):
        return p[i + 1], i + 2
    return p[i], i + 1


def _without_slash(ranges: Iterable[tuple[int, int]]) -> tuple[tuple[int, int], ...]:
    """Sorted, merged ranges with '/' removed: a class never matches the separator."""
    merged: list[tuple[int, int]] = []
    for lo, hi in sorted(ranges):
        if merged and lo <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    out: list[tuple[int, int]] = []
    for lo, hi in merged:
        if lo <= _SLASH <= hi:
            if lo < _SLASH:
                out.append((lo, _SLASH - 1))
            if hi > _SLASH:
                out.append((_SLASH + 1, hi))
        else:
            out.append((lo, hi))
    return tuple(out)


def _tokenize(pattern: str, p: str) -> list[_Token]:
    tokens: list[_Token] = []
    i = 0
    while i < len(p):
        c = p[i]
        if c == "\\":
            # A trailing backslash is a literal backslash.
            char = p[i + 1] if i + 1 < len(p) else "\\"
            tokens.append(_SEPARATOR if char == "/" else _Literal(char))
            i += 2
        elif c == "*":
            tokens.append(_STAR)
            i += 1
        elif c == "?":
            tokens.append(_ANY)
            i += 1
        elif c == "[":
            cls, end = _parse_class(pattern, p, i)
            tokens.append(cls)
            i = end + 1
        elif c == "/":
            tokens.append(_SEPARATOR)
            i += 1
        else:
            tokens.append(_Literal(c))
            i += 1
    return tokens


def _segments(tokens: list[_Token]) -> list[_Segment]:
    segments: list[_Segment] = [[]]
    for token in tokens:
        if isinstance(token, _Separator):
            segments.append([])
        else:
            segments[-1].append(token)
    return segments


def _is_globstar(segment: _Segment) -> bool:
    return segment == [_STAR, _STAR]


# Emitting


def _emit(segments: list[_Segment]) -> str:
    out: list[str] = []
    for k, segment in enumerate(segments):
        last = k == len(segments) - 1
        if _is_globstar(segment):
            out.append(".*" if last else "(?:[^/]+/)*")
            continue
        previous: object = None
        for token in segment:
            if isinstance(token, _Star):
                if not isinstance(previous, _Star):  # ** inside a segment is a *
                    out.append("[^/]*")
            elif isinstance(token, _Any):
                out.append("[^/]")
            elif isinstance(token, _Class):
                out.append(_emit_class(token))
            else:
                out.append("\\" + token.char if token.char in _ARE_SPECIAL else token.char)
            previous = token
        if not last:
            out.append("/")
    return "".join(out)


def _emit_class(cls: _Class) -> str:
    body = "".join(_emit_range(lo, hi) for lo, hi in cls.ranges)
    if cls.negated:
        return "[^" + body + _class_escape(_SLASH) + "]"
    if not body:
        return "[^\\u0001-\\U0010ffff]"  # matches nothing: text never contains NUL
    return "[" + body + "]"


def _emit_range(lo: int, hi: int) -> str:
    if lo == hi:
        return _class_escape(lo)
    return _class_escape(lo) + "-" + _class_escape(hi)


def _class_escape(code: int) -> str:
    """A character inside a bracket expression: ASCII letters and digits bare, everything else as a
    character-entry escape, which bracket expressions accept and which never has a special meaning there."""
    char = chr(code)
    if char.isascii() and char.isalnum():
        return char
    return f"\\u{code:04x}" if code <= 0xFFFF else f"\\U{code:08x}"


# Hints


def _literal_prefix(segments: list[_Segment]) -> list[str]:
    """The leading segments, before the last one, that are all literal characters."""
    prefix: list[str] = []
    for segment in segments[:-1]:
        if not segment or not all(isinstance(token, _Literal) for token in segment):
            break
        prefix.append("".join(token.char for token in segment if isinstance(token, _Literal)))
    return prefix


def _name_like(alternatives: list[list[_Segment]]) -> str | None:
    """A LIKE pattern for the file name, when every alternative's last segment translates to the same one."""
    likes = {_segment_like(segments[-1]) for segments in alternatives}
    if len(likes) != 1:
        return None
    return likes.pop()


def _segment_like(segment: _Segment) -> str | None:
    if _is_globstar(segment):
        return None
    out: list[str] = []
    for token in segment:
        if isinstance(token, _Star):
            if not out or out[-1] != "%":
                out.append("%")
        elif isinstance(token, _Any):
            out.append("_")
        elif isinstance(token, _Class):
            return None
        else:
            out.append("\\" + token.char if token.char in "\\%_" else token.char)
    return "".join(out)


def _globstar_dir_name(pattern: str) -> str | None:
    """``name`` for an exclude of the form ``**/name/**`` with a literal name, else None."""
    if not (pattern.startswith("**/") and pattern.endswith("/**")):
        return None
    name = pattern[3:-3]
    if not name or any(c in name for c in "/\\*?[]{}") or name in (".", ".."):
        return None
    return name
