"""The fallback replacers of ``Edit`` give ladderframe's answers at a bounded cost (#80).

The ``reference`` section is ladderframe's matcher code as minifs shipped it before #80 (ladderframe's
``packages/harness/src/ladderframe/tools/edit.py``), kept here as the oracle: for random content and finds, each
rewritten replacer must yield the same candidates in the same order, and ``replace`` the same result or error. The
oracle is ladderframe's code with seven deliberate departures (#110, #122, #117, #127, #143), marked in the code.
Then the cost bounds: the cases that were quadratic finish in linear time, and a search that runs past ``timeout`` is a
retry, from inside a replacer's loop too."""

import math
import random
import re
import time
from bisect import bisect_left
from collections.abc import Callable, Iterator
from itertools import islice

import pytest

import minifs.tools.edit as new
from minifs.tools.deps import EditSettings, ToolSettings
from minifs.tools.edit import (
    BLOCK_LEAVES_OUT_LINES,
    EMPTY_OLD_STRING,
    MULTIPLE_MATCHES,
    Edit,
    ReplaceTimeout,
    timeout_message,
)
from tests.tools.conftest import Deps, retry_message

Replacer = Callable[[str, str], Iterator[str]]

# ---------------------------------------------------------------- reference: ladderframe's code before #80

SINGLE_CANDIDATE_SIMILARITY_THRESHOLD = 0.65
MULTIPLE_CANDIDATES_SIMILARITY_THRESHOLD = 0.65
MAX_SCORED_LINE_LENGTH = 200
"""Longer lines (minified code) are compared for equality only: `levenshtein` is quadratic in the line length."""


def levenshtein(a: str, b: str) -> int:
    if a == "" or b == "":
        return max(len(a), len(b))
    previous = list(range(len(b) + 1))
    for i in range(1, len(a) + 1):
        current = [i] + [0] * len(b)
        for j in range(1, len(b) + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            current[j] = min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost)
        previous = current
    return previous[len(b)]


def _span(lines: list[str], start: int, end: int) -> tuple[int, int]:
    """Character offsets of lines `start..end` (inclusive) in the text the lines were split from."""
    begin = sum(len(line) + 1 for line in lines[:start])
    finish = begin + sum(len(line) for line in lines[start : end + 1]) + (end - start)
    return begin, finish


class _LongerBlock(ValueError):
    # departs from ladderframe (#117): a block-anchored match that leaves out non-blank lines is refused with a
    # retry, kept pending by ``replace`` so the stricter replacers still look for the exact text.
    def __init__(self, start: int, end: int, extra: int) -> None:
        super().__init__(BLOCK_LEAVES_OUT_LINES.format(first=start + 1, last=end + 1, extra=extra))


def simple_replacer(content: str, find: str) -> Iterator[str]:
    yield find


def line_trimmed_replacer(content: str, find: str) -> Iterator[str]:
    original = content.split("\n")
    search = find.split("\n")
    if search[-1] == "":
        search.pop()
    for i in range(len(original) - len(search) + 1):
        if all(original[i + j].strip() == search[j].strip() for j in range(len(search))):
            begin, finish = _span(original, i, i + len(search) - 1)
            yield content[begin:finish]


def _middle_similarity(original: list[str], start: int, size: int, search: list[str], early_exit: bool) -> float:
    lines_to_check = min(len(search) - 2, size - 2)
    if lines_to_check <= 0:
        return 1.0  # no middle lines to compare: accept on the anchors
    similarity = 0.0
    for j in range(1, min(len(search) - 1, size - 1)):
        original_line = original[start + j].strip()
        search_line = search[j].strip()
        longest = max(len(original_line), len(search_line))
        if longest == 0:
            continue
        if original_line == search_line:
            score = 1.0
        elif longest > MAX_SCORED_LINE_LENGTH:
            score = 0.0
        else:
            score = 1 - levenshtein(original_line, search_line) / longest
        if early_exit:
            similarity += score / lines_to_check
            if similarity >= SINGLE_CANDIDATE_SIMILARITY_THRESHOLD:
                break
        else:
            similarity += score
    return similarity if early_exit else similarity / lines_to_check


def block_anchor_replacer(content: str, find: str) -> Iterator[str]:
    original = content.split("\n")
    search = find.split("\n")
    if search[-1] == "":
        search.pop()
    # departs from ladderframe (#110): check the length after the pop, so a two-line find with a trailing newline
    # (no middle line to score) can't match a block on its anchors alone and replace the line between them.
    if len(search) < 3:
        return
    first, last = search[0].strip(), search[-1].strip()
    max_delta = max(1, len(search) // 4)

    def leaves_out_lines(start: int, end: int) -> int | None:
        # departs from ladderframe (#117): how many non-blank lines of the block start..end pair with no line of
        # the find (the middle is scored by position over the shorter of the two, so in a longer block the lines
        # from the find's length up to the last anchor were never compared and the replacement deleted them), or
        # None. Blank ones are fine: the block is yielded as ladderframe did. Not a net count: a find line missing
        # from the file must not cancel out a block line nobody compared.
        extra = sum(1 for k in range(start + len(search) - 1, end) if original[k].strip())
        return extra or None

    blocks: list[tuple[int, int]] = []
    for i, line in enumerate(original):
        if line.strip() != first:
            continue
        for j in range(i + 2, len(original)):
            if original[j].strip() == last:
                if abs(j - i + 1 - len(search)) <= max_delta:
                    blocks.append((i, j))
                break  # only the first occurrence of the last line
    if not blocks:
        return

    if len(blocks) == 1:
        start, end = blocks[0]
        if _middle_similarity(original, start, end - start + 1, search, True) >= SINGLE_CANDIDATE_SIMILARITY_THRESHOLD:
            # departs from ladderframe (#117): a block that leaves out non-blank lines is refused with a retry
            # rather than yielded, and rather than dropped: dropping it let another block elsewhere win the
            # scoring.
            extra = leaves_out_lines(start, end)
            if extra is not None:
                raise _LongerBlock(start, end, extra)
            begin, finish = _span(original, start, end)
            yield content[begin:finish]
        return

    best: tuple[int, int] | None = None
    best_similarity = -1.0
    best_extra: int | None = None
    for start, end in blocks:
        similarity = _middle_similarity(original, start, end - start + 1, search, False)
        extra = leaves_out_lines(start, end)
        # departs from ladderframe (#117): on a tie, a block that doesn't leave out lines beats one that does
        if similarity > best_similarity or (similarity == best_similarity and best_extra is not None and extra is None):
            best_similarity, best, best_extra = similarity, (start, end), extra
    if best is not None and best_similarity >= MULTIPLE_CANDIDATES_SIMILARITY_THRESHOLD:
        start, end = best
        # departs from ladderframe (#117): as above, for the best of several candidates.
        if best_extra is not None:
            raise _LongerBlock(start, end, best_extra)
        begin, finish = _span(original, start, end)
        yield content[begin:finish]


def whitespace_normalized_replacer(content: str, find: str) -> Iterator[str]:
    def normalize(text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    normalized_find = normalize(find)
    lines = content.split("\n")
    for line in lines:
        normalized_line = normalize(line)
        if normalized_line == normalized_find:
            yield line
        elif normalized_find in normalized_line:
            words = find.strip().split()
            if words:
                match = re.search(r"\s+".join(re.escape(word) for word in words), line)
                if match:
                    yield match.group(0)

    find_lines = find.split("\n")
    if len(find_lines) > 1:
        for i in range(len(lines) - len(find_lines) + 1):
            block = "\n".join(lines[i : i + len(find_lines)])
            if normalize(block) == normalized_find:
                yield block


def indentation_flexible_replacer(content: str, find: str) -> Iterator[str]:
    def remove_indentation(text: str) -> str:
        lines = text.split("\n")
        non_empty = [line for line in lines if line.strip()]
        if not non_empty:
            return text
        least = min(len(line) - len(line.lstrip()) for line in non_empty)
        return "\n".join(line if not line.strip() else line[least:] for line in lines)

    normalized_find = remove_indentation(find)
    content_lines = content.split("\n")
    find_lines = find.split("\n")
    for i in range(len(content_lines) - len(find_lines) + 1):
        block = "\n".join(content_lines[i : i + len(find_lines)])
        if remove_indentation(block) == normalized_find:
            yield block


_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "'": "'", '"': '"', "`": "`", "\\": "\\", "\n": "\n", "$": "$"}
_ESCAPE = re.compile(r"\\(n|t|r|'|\"|`|\\|\n|\$)")


def _unescape(text: str) -> str:
    return _ESCAPE.sub(lambda m: _ESCAPES[m.group(1)], text)


def escape_normalized_replacer(content: str, find: str) -> Iterator[str]:
    unescaped_find = _unescape(find)
    if unescaped_find in content:
        yield unescaped_find
    lines = content.split("\n")
    find_lines = unescaped_find.split("\n")
    for i in range(len(lines) - len(find_lines) + 1):
        block = "\n".join(lines[i : i + len(find_lines)])
        if _unescape(block) == unescaped_find:
            yield block


def multi_occurrence_replacer(content: str, find: str) -> Iterator[str]:
    """Every exact match, so `replace` can decide between unique and `replaceAll`."""
    start = 0
    while (index := content.find(find, start)) != -1:
        yield find
        start = index + len(find)


def trimmed_boundary_replacer(content: str, find: str) -> Iterator[str]:
    trimmed = find.strip()
    if trimmed == find:
        return
    if trimmed in content:
        yield trimmed
    lines = content.split("\n")
    find_lines = find.split("\n")
    for i in range(len(lines) - len(find_lines) + 1):
        block = "\n".join(lines[i : i + len(find_lines)])
        if block.strip() == trimmed:
            yield block


def context_aware_replacer(content: str, find: str) -> Iterator[str]:
    find_lines = find.split("\n")
    if len(find_lines) < 3:
        return  # needs at least 3 lines to have meaningful context
    if find_lines[-1] == "":
        find_lines.pop()
    content_lines = content.split("\n")
    first, last = find_lines[0].strip(), find_lines[-1].strip()
    for i, line in enumerate(content_lines):
        if line.strip() != first:
            continue
        for j in range(i + 2, len(content_lines)):
            if content_lines[j].strip() != last:
                continue
            block_lines = content_lines[i : j + 1]
            if len(block_lines) == len(find_lines):
                # at least half of the non-empty middle lines must match when trimmed
                matching = total = 0
                for k in range(1, len(block_lines) - 1):
                    block_line, find_line = block_lines[k].strip(), find_lines[k].strip()
                    if block_line or find_line:
                        total += 1
                        matching += block_line == find_line
                if total == 0 or matching / total >= 0.5:
                    yield "\n".join(block_lines)
            break


REPLACERS: tuple[Replacer, ...] = (
    simple_replacer,
    line_trimmed_replacer,
    block_anchor_replacer,
    whitespace_normalized_replacer,
    indentation_flexible_replacer,
    escape_normalized_replacer,
    trimmed_boundary_replacer,
    context_aware_replacer,
    multi_occurrence_replacer,
)


def _lf_view(content: str) -> tuple[str, list[int]]:
    """The oracle's LF view of ``content`` (#143): each CRLF pair folded to its LF, a final lone CR dropped, and
    the view index of every LF that was a pair. What Read shows, so what the model's oldString can hold."""
    view = content.replace("\r\n", "\n")
    crlf = [m.start() - k for k, m in enumerate(re.finditer("\r\n", content))]
    if content.endswith("\r"):
        view = view[:-1]
        crlf.append(len(view))  # past the view's end: a span that reaches the end keeps that CR
    return view, crlf


def _raw_at(crlf: list[int], at: int) -> int:
    """The raw offset of a view index: itself plus the pairs the view hides before it."""
    return at + bisect_left(crlf, at)


def _splice(
    content: str, view: str, crlf: list[int], search: str, index: int, new_string: str, replace_all: bool
) -> str:
    """The oracle's substitution: the occurrence of ``search`` at the view's ``index``, or with ``replace_all``
    every one of them, each mapped back to its raw span (#143)."""
    size = len(search)
    starts: list[int] = []
    at: int = index
    while at != -1:
        starts.append(at)
        at = view.find(search, at + size) if replace_all else -1

    pieces: list[str] = []
    cursor = 0
    for s in starts:
        e = s + size
        pieces.append(content[cursor : _raw_at(crlf, s)])
        pieces.append(new_string)
        cursor = _raw_at(crlf, e)
    pieces.append(content[cursor:])
    return "".join(pieces)


def _changed(content: str, result: str, old_v: str) -> str:
    """The oracle's no-change check (#143): an edit that leaves the content as it was is refused."""
    if result == content:
        cr = (
            " A CR at the end of a line is the file's line ending, which Read doesn't show and oldString can't"
            " target; to change line endings, use Write."
        )
        raise ValueError(
            "No changes to apply: the text oldString matched already reads as newString."
            + (cr if "\r" in old_v else "")
        )
    return result


def replace(content: str, old_string: str, new_string: str, replace_all: bool = False) -> str:
    """Replace `old_string`, trying each replacer in turn until one finds a unique match (or any, with
    `replace_all`). Raises `ValueError` with the message for the model when nothing fits."""
    if old_string == new_string:
        raise ValueError("No changes to apply: oldString and newString are identical.")
    if old_string == "":
        raise ValueError(EMPTY_OLD_STRING)

    view, crlf = _lf_view(content)
    old_v = old_string.replace("\r\n", "\n")
    not_found = True
    # departs from ladderframe (#110): a whitespace-only oldString may match only its exact text; the fuzzy
    # matchers would only yield blank lines to throw away (a blank line of spaces for a tab rewrote every
    # indentation run).
    replacers = (simple_replacer,) if not old_v.strip() else REPLACERS
    refusal: _LongerBlock | None = None
    for replacer in replacers:
        # departs from ladderframe (#117): a refused block stays pending while the other replacers still look
        # for the exact text; the context-aware matcher is skipped once there is a refusal, as it would take a
        # looser sibling block.
        if refusal is not None and replacer is context_aware_replacer:
            continue
        try:
            for search in replacer(view, old_v):
                # departs from ladderframe (#110, #122): an empty candidate is never a match (it is "found" at 0
                # and with replace_all inserts new_string between every character); with replace_all, neither is
                # a whitespace candidate of a non-blank oldString (an escaped `\n` unescaped to a newline would
                # replace every newline in the file). Without replace_all it may still match once (an escaped
                # `\t` for a real tab).
                if not search or (replace_all and search != old_v and not search.strip()):
                    continue
                index = view.find(search)
                if index == -1:
                    continue
                not_found = False
                if _is_disproportionate_match(search, old_v):
                    raise ValueError(
                        "Refusing replacement because the matched span is much larger than oldString. "
                        "Re-read the file and provide the full exact oldString for the intended replacement."
                    )
                # departs from ladderframe (#127, #143): matches on an LF view and maps the span back, so a span
                # never splits a CRLF pair
                # departs from ladderframe (#143): an edit that leaves the content as it was is an error, not a
                # silent success (a fuzzy match can already read as new_string)
                if replace_all:
                    return _changed(content, _splice(content, view, crlf, search, index, new_string, True), old_v)
                if index != view.rfind(search):
                    if search == old_v:
                        # departs from ladderframe (#110): an ambiguous exact oldString is reported, not skipped
                        # in favour of a later fuzzy candidate elsewhere. A whole-line candidate can be the one
                        # meant, but it replaces text the model left out of oldString (its indentation, say): an
                        # error is safer.
                        raise ValueError(MULTIPLE_MATCHES)
                    continue
                return _changed(content, _splice(content, view, crlf, search, index, new_string, False), old_v)
        except _LongerBlock as e:
            # departs from ladderframe (#117): pending, not final: a later replacer may still find the text.
            refusal = e

    if not_found:
        # departs from ladderframe (#117): nothing found the text, so the refusal is the answer; when a replacer
        # found it more than once, the ambiguity is (the refused block is not what was meant).
        if refusal is not None:
            raise refusal
        raise ValueError(
            "Could not find oldString in the file. It must match exactly, including whitespace, indentation, "
            "and line endings."
        )
    raise ValueError(MULTIPLE_MATCHES)


def _is_disproportionate_match(search: str, old_string: str) -> bool:
    old_lines = old_string.count("\n") + 1
    search_lines = search.count("\n") + 1
    if search_lines >= max(old_lines + 3, old_lines * 2):
        return True
    if old_lines == 1:
        return False
    return len(search.strip()) > max(len(old_string.strip()) + 500, len(old_string.strip()) * 4)


# ---------------------------------------------------------------- the same answers

PAIRS: list[tuple[str, Replacer, Replacer]] = [
    ("line_trimmed", new.line_trimmed_replacer, line_trimmed_replacer),
    ("block_anchor", new.block_anchor_replacer, block_anchor_replacer),
    ("whitespace_normalized", new.whitespace_normalized_replacer, whitespace_normalized_replacer),
    ("indentation_flexible", new.indentation_flexible_replacer, indentation_flexible_replacer),
    ("escape_normalized", new.escape_normalized_replacer, escape_normalized_replacer),
    ("trimmed_boundary", new.trimmed_boundary_replacer, trimmed_boundary_replacer),
    ("context_aware", new.context_aware_replacer, context_aware_replacer),
]
"""Each rewritten replacer with its reference. ``simple_replacer`` and ``multi_occurrence_replacer`` are unchanged."""

PIECES = [
    "x",
    "y",
    "ab",
    "x",
    " ",
    "  ",
    "\t",
    "\x1c",
    "\xa0",
    "\\",
    "n",
    "$",
    '"',
    "'",
    "`",
    "\\n",
    "\\\\",
    "\\$",
    "\\\n",
    "\n",
    "\r",
    "a" * 201,
]
"""What random lines are made of: the characters the replacers treat specially (whitespace, backslash escapes, a
backslash before a newline, a ``\\r`` as a CRLF file's lines keep), few distinct letters, so lines repeat and
anchors recur, and now and then a line over ``MAX_SCORED_LINE_LENGTH``, which the block scorer doesn't score."""


def random_text(rng: random.Random, max_lines: int) -> str:
    lines = ["".join(rng.choice(PIECES) for _ in range(rng.randint(0, 4))) for _ in range(rng.randint(0, max_lines))]
    return "\n".join(lines)


def random_find(rng: random.Random, content: str) -> str:
    """Mostly a run of the content's lines, often altered the way a model alters them (indentation, stripped or
    added whitespace, a changed middle line, escapes, a trailing newline); otherwise random text."""
    lines = content.split("\n")
    if not content or rng.random() < 0.3:
        return random_text(rng, 4)
    i = rng.randrange(len(lines))
    taken = lines[i : i + rng.randint(1, 9)]  # up to 9 lines: the block anchors' max_delta reaches 2
    for _ in range(rng.randint(0, 2)):
        match rng.randint(0, 6):
            case 0:
                taken = [" " * rng.randint(1, 3) + line for line in taken]
            case 1:
                taken = [line.strip() for line in taken]
            case 2 if len(taken) > 2:
                k = rng.randrange(1, len(taken) - 1)
                taken[k] = random_text(rng, 1)
            case 3:
                taken = [line.replace('"', '\\"').replace("\n", "\\n") for line in taken]
            case 4:
                taken = [line.replace("  ", " ") for line in taken]
            case 5:
                taken = ["", *taken, ""]
            case _:
                taken = [*taken, ""]
    return "\n".join(taken)


def outcome(replace: Callable[..., str], content: str, find: str, new_string: str, replace_all: bool) -> object:
    try:
        return replace(content, find, new_string, replace_all)
    except ValueError as e:
        return ("error", str(e))


def candidates(replacer: Replacer, content: str, find: str) -> object:
    """A replacer's candidates, or the retry it raises instead (#117): both replacers must give the same."""
    try:
        return list(replacer(content, find))
    except ValueError as e:
        return ("error", str(e))


@pytest.mark.parametrize("seed", range(4))
def test_replacers_give_ladderframes_candidates(seed: int) -> None:
    rng = random.Random(seed)
    for _ in range(1500):
        content = random_text(rng, 10)
        find = random_find(rng, content) or "x"  # replace() rejects an empty find before any replacer runs
        for name, rewritten, reference in PAIRS:
            assert candidates(rewritten, content, find) == candidates(reference, content, find), (name, content, find)
        replace_all = rng.random() < 0.2
        assert outcome(new.replace, content, find, "R", replace_all) == outcome(
            replace, content, find, "R", replace_all
        ), (
            content,
            find,
        )


def test_words_within_a_line_are_found_as_the_regex_found_them() -> None:
    """The whitespace matcher's search within a line (a find whose words are in the line with other whitespace
    between them, starting and ending inside a token or not) no longer runs ladderframe's regex: the same text."""
    rng = random.Random(3)
    spaces = [" ", "  ", "\t", " \t ", "\xa0", "\x1c", "\r"]
    for _ in range(4000):
        tokens = [rng.choice(["a", "b", "ab", "ba", "a.b", "(a", "aa"]) for _ in range(rng.randint(1, 8))]
        line = rng.choice(["", " "]) + "".join(token + rng.choice(spaces) for token in tokens)
        i = rng.randrange(len(tokens))
        words = tokens[i : i + rng.randint(1, 4)]
        if rng.random() < 0.5:
            words[0] = words[0][rng.randint(0, len(words[0]) - 1) :]  # starts inside a token
        if rng.random() < 0.5:
            words[-1] = words[-1][: rng.randint(1, len(words[-1]))]  # ends inside one
        find = rng.choice(["", " "]) + rng.choice(spaces).join(words) + rng.choice(["", "\t"])
        content = "\n".join([line, "b a", line])
        assert list(new.whitespace_normalized_replacer(content, find)) == list(
            whitespace_normalized_replacer(content, find)
        ), (content, find)
        assert outcome(new.replace, content, find, "R", True) == outcome(replace, content, find, "R", True)


def test_forgetting_tried_candidates_changes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """``replace`` skips a candidate it already tried and forgets them past a byte budget: with the budget a few
    characters, every candidate of a repetitive file is tried again, and the answers are still the oracle's."""
    monkeypatch.setattr(new, "_SEEN_BYTES", 4)
    rng = random.Random(7)
    content = "\n".join("x" + " " * rng.randint(0, 3) for _ in range(300)) + "\n"
    for find in ("x", "x\nx", "x \nx\nx", "  x\n", "y"):
        for replace_all in (False, True):
            assert outcome(new.replace, content, find, "R", replace_all) == outcome(
                replace, content, find, "R", replace_all
            )


def test_replace_rejects_what_it_always_rejected() -> None:
    with pytest.raises(ValueError, match="identical"):
        new.replace("a", "a", "a")
    with pytest.raises(ValueError, match=re.escape(EMPTY_OLD_STRING)):
        new.replace("a", "", "b")


def test_a_blank_candidate_is_never_a_match() -> None:
    """A whitespace-only find made the fallback matchers yield blank lines (empty or spaces): an empty one was
    "found" at 0 and, with ``replace_all``, inserted the new string between every character, and a run of spaces
    rewrote every indentation run. Only the exact text of a whitespace-only find may match then; a non-blank find
    (an escaped tab) still matches whitespace through the escape matcher, but with ``replace_all`` a whitespace
    candidate of a non-blank find is refused too: an escaped ``\\n`` unescaped to a newline would replace every
    newline in the file (#122)."""
    blanks = [
        ("def f():\n    return 1\n\nx = 2\n", "\t", "    ", False),
        ("def f():\n    return 1\n\nx = 2\n", "\t", "    ", True),
        ("def f():\n    return 1\n    \nx = 2\n", "\t", "  ", False),
        ("def f():\n    return 1\n    \nx = 2\n", "\t", "  ", True),
        ("a\n\n    \n    b\n", "\t", "  ", False),
        ("a\n\n    \n    b\n", "\t", "  ", True),
        ("def f():\n\treturn 1\n   \nx = 2\n", "\t\t", "#", False),
    ]
    for content, find, new_string, replace_all in blanks:
        for search in (new.replace, replace):
            with pytest.raises(ValueError, match="Could not find oldString"):
                search(content, find, new_string, replace_all)
    # A whitespace-only oldString that is in the file exactly still matches: it is the find itself.
    for search in (new.replace, replace):
        assert search("a\n    \nb\n", "    \n", "") == "a\nb\n"
    # A non-blank find whose candidate is blank (an escaped tab) still matches through the escape matcher.
    for search in (new.replace, replace):
        assert search("a\tb\n", "\\t", " ") == "a b\n"
    # The same candidate with replace_all is refused (#122): an escaped newline, unescaped to a real one, would
    # replace every newline in the file. Without replace_all a single newline still matches once.
    for search in (new.replace, replace):
        with pytest.raises(ValueError, match="Could not find oldString"):
            search("a\nb\nc\n", "\\n", " ", True)
        assert search("a\nb", "\\n", " ") == "a b"


def test_a_whitespace_find_runs_only_the_exact_matcher(monkeypatch: pytest.MonkeyPatch) -> None:
    """A whitespace-only oldString may match only its exact text (#110), so the fuzzy matchers after the exact one
    never run: they would only yield blank lines to throw away, which on a large file costs seconds and can end in
    a timeout instead of "Could not find"."""

    def boom(content: str, find: str) -> Iterator[str]:
        raise AssertionError("a fuzzy matcher ran for a whitespace-only find")

    monkeypatch.setattr(new, "REPLACERS", (new.simple_replacer, boom))
    with pytest.raises(ValueError, match="Could not find oldString"):
        new.replace("x\n\n", "\t", " ")
    assert new.replace("x\n\t\n", "\t", " ") == "x\n \n"


def test_an_ambiguous_exact_match_is_reported() -> None:
    """An exact oldString that occurs more than once was skipped, and a later fuzzy candidate elsewhere in the
    file was silently replaced instead; the ambiguity is now reported (``replace_all`` still replaces every
    occurrence)."""
    content = "if a:\n    }\nelse:\n        }\n}\n"
    for search in (new.replace, replace):
        with pytest.raises(ValueError, match="Found multiple matches"):
            search(content, "}", "};")
    assert new.replace(content, "}", "};", True) == "if a:\n    };\nelse:\n        };\n};\n"


def test_block_anchors_alone_dont_take_the_line_between() -> None:
    """A two-line find with a trailing newline passed the length check before the pop of the trailing empty line,
    leaving no middle line to score: the anchors alone matched a three-line block and the unrelated line between
    them was replaced too."""
    content = "def f():\n    keep_me()\n    return 1\n"
    assert list(new.block_anchor_replacer(content, "def f():\nreturn 1\n")) == []
    with pytest.raises(ValueError, match="Could not find oldString"):
        new.replace(content, "def f():\nreturn 1\n", "def g():\n    return 2\n")
    # Positive control: with a middle line, slightly altered, the block anchors still find the block.
    assert list(new.block_anchor_replacer(content, "def f():\n    keep_me( )\n    return 1\n")) == [
        "def f():\n    keep_me()\n    return 1"
    ]


def test_block_anchors_refuse_a_longer_block() -> None:
    """A block-anchored match longer than the find has lines the find doesn't include: the
    middle is scored by position over the shorter of the two, so the block's lines past the find's length were
    never compared and the replacement deleted lines the model never wrote (#117). Such a block is refused with
    a retry rather than dropped: dropping it let another block elsewhere win the scoring. The refusal is pending
    in ``replace`` (the stricter replacers still run), but nothing here finds the text, so it is the answer. A
    shorter block is still taken, and a block that doesn't leave out lines wins a tie with one that does."""
    content = "def f():\n    keep_me()\n    delete_me()\n    return 1\n"
    find = "def f():\n    keep_me()\n    return 1\n"
    message = re.escape(BLOCK_LEAVES_OUT_LINES.format(first=1, last=4, extra=1))
    with pytest.raises(ValueError, match=message):
        next(islice(new.block_anchor_replacer(content, find), 1))
    for search in (new.replace, replace):
        with pytest.raises(ValueError, match=message):
            search(content, find, "def f():\n    keep_me()\n    return 2\n")
    # A longer block whose extra line sits at the end of the middle, where the positional scoring would still
    # accept the block (max_delta is 1 for 6 lines).
    with pytest.raises(ValueError, match=re.escape(BLOCK_LEAVES_OUT_LINES.format(first=1, last=7, extra=1))):
        next(
            islice(
                new.block_anchor_replacer(
                    "def f():\n    a()\n    b()\n    c()\n    d()\n    x()\n    return 1\n",
                    "def f():\n    a()\n    b()\n    c()\n    d()\n    return 1\n",
                ),
                1,
            )
        )
    # Dropping the longer block let the second block (b = 9) win and be overwritten; and the refusal stays
    # pending only for the stricter replacers: ``context_aware_replacer`` would take that looser sibling block
    # (its middle matches half of the time), so it is skipped once there is a refusal (#117).
    sibling = (
        "if ready:\n    a = 1\n    b = 2\n    extra()\n    return a\n\nif ready:\n    a = 1\n    b = 9\n    return a\n"
    )
    sibling_find = "if ready:\n    a = 1\n    b = 2\n    return a"
    sibling_message = re.escape(BLOCK_LEAVES_OUT_LINES.format(first=1, last=5, extra=1))
    for search in (new.replace, replace):
        with pytest.raises(ValueError, match=sibling_message):
            search(sibling, sibling_find, "if ready:\n    a = 1\n    b = 3\n    return a")
    # Positive control: a SHORTER block is still found.
    assert list(
        new.block_anchor_replacer(
            "def f():\n    a()\n    b()\n    c()\n    return 1\n",
            "def f():\n    a()\n    b()\n    c()\n    gone()\n    return 1\n",
        )
    ) == ["def f():\n    a()\n    b()\n    c()\n    return 1"]
    # A tie: both blocks score the same and the one that leaves out lines comes first. The block that doesn't
    # leave out lines wins, rather than a refusal that points at the wrong block. With the longer block twice,
    # ladderframe got there too: its pick was ambiguous, and whitespace_normalized_replacer found the right block.
    longer, same = "if x:\n  a = 1\n  b = 2\n  c = 3\nreturn\n", "if x:\n  a = 1\n  b = 2\nreturn\n"
    for content in (longer + same, longer + longer + same):
        for search in (new.replace, replace):
            assert search(content, "if x:\n  a  =  1\n  b = 2\nreturn", "NEW") == content.removesuffix(same) + "NEW\n"


def test_a_stricter_replacer_finds_the_text_the_block_anchor_refused() -> None:
    """Raising the refusal out of ``replace`` ended the whole search, so later replacers that would have found
    the exact text (modulo whitespace) never ran, and the retry pointed the model at the wrong block (#117). The
    refusal is pending now: here a strictly-higher-scoring longer block comes first, and the right block is
    found later."""
    content = "if x:\n  a = 1\n  b = 2\n  c = 3\nreturn\nif x:\n  a  = 1\n  b = 2\nreturn\n"
    for search in (new.replace, replace):
        assert search(content, "if x:\n  a = 1\n  b = 2\nreturn", "NEW") == (
            "if x:\n  a = 1\n  b = 2\n  c = 3\nreturn\nNEW\n"
        )


def test_a_single_refused_candidate_does_not_end_the_search() -> None:
    """A single block-anchor candidate whose extra line is non-blank is refused (#117), but the search goes on
    and the right block is replaced: the refusal never becomes the answer."""
    content = "f\n x\n }x\n y = 1\n w\n}\nf\n x\n}\n y  =  1\n}\n"
    for search in (new.replace, replace):
        assert search(content, "f\n x\n}\n y = 1\n}", "NEW") == "f\n x\n }x\n y = 1\n w\n}\nNEW\n"


def test_a_left_out_blank_line_is_accepted() -> None:
    """A longer block whose extra lines are all blank is yielded as ladderframe did (#117): a model often leaves
    out a blank line, and losing one is harmless, so it is not a refusal."""
    for search in (new.replace, replace):
        assert (
            search(
                "def f():\n    a()\n\n    return 1\n",
                "def f():\n    a()\n    return 1",
                "def f():\n    a()\n    return 2",
            )
            == "def f():\n    a()\n    return 2\n"
        )


def test_lines_missing_from_the_file_dont_cancel_out_a_left_out_line() -> None:
    """Only the block's lines that pair with no find line count (#117), not the block's non-blank lines against the
    find's: here the find's ``e()`` and ``g()`` aren't in the file, the block has blank lines in their place, and
    ``x()`` past the find's length was never compared. A net count (7 non-blank lines against 8) deleted it."""
    content = "def f():\n    a()\n    b()\n    c()\n    d()\n\n\n    x()\n    return 1\n"
    find = "def f():\n    a()\n    b()\n    c()\n    d()\n    e()\n    g()\n    return 1"
    message = re.escape(BLOCK_LEAVES_OUT_LINES.format(first=1, last=9, extra=1))
    for search in (new.replace, replace):
        with pytest.raises(ValueError, match=message):
            search(content, find, "NEW")


def test_an_ambiguous_match_outranks_a_refusal() -> None:
    """When a stricter replacer finds the text twice after the block anchors refused a block, the answer is the
    ambiguity: the refused block (lines 1-5 here) is not what was meant, and a retry naming it would send the model
    to edit it (#117)."""
    same = "if x:\n  a  = 1\n  b = 2\nreturn\n"
    content = "if x:\n  a = 1\n  b = 2\n  c = 3\nreturn\n" + same + same
    for search in (new.replace, replace):
        with pytest.raises(ValueError, match="Found multiple matches"):
            search(content, "if x:\n  a = 1\n  b = 2\nreturn", "NEW")


CRLF_BLOCK = "def f():\r\n    alpha = 1\r\n    beta  = 2\r\n    gamma = 3\r\nend\r\n"
"""A CRLF file for the #127 cases: the replacers split on ``\\n``, so every line keeps its ``\\r`` and a
whole-line candidate ends with it, the file's line ending."""


def test_a_fuzzy_match_in_a_crlf_file_keeps_the_line_ending() -> None:
    """A fuzzy candidate in a CRLF file ends with its last line's ``\\r``: that ``\\r`` is the file's line
    ending, not text the model wrote, and the CRLF-converted newString doesn't end in ``\\r``, so the
    replacement keeps it and the file is left without a bare LF (#127)."""
    for search in (new.replace, replace):
        assert search("alpha\r\nbeta\r\ngamma\r\ndelta\r\n", "alpha\r\nbetx\r\ngamma", "A\r\nB\r\nC") == (
            "A\r\nB\r\nC\r\ndelta\r\n"
        )


def test_each_line_based_replacers_candidate_keeps_its_trailing_cr() -> None:
    """Each line-based replacer's candidate in a CRLF file ends with its last line's ``\\r``, and the
    replacement keeps it, so the output has no bare LF (#127). Each find routes to the named replacer (the
    earlier ones find nothing). The indentation-flexible matcher isn't here: its candidate compares equal to the
    de-indented find, so it ends in ``\r`` only when the find does, and that CR is then the model's own, which
    the END rule leaves to the model. The context-aware find departs from the issue's
    example (a ``zzz = 9`` middle between two exact middles still scores 0.78 at the block anchors, which win):
    with one exact and one wholly different middle the block score is 0.5 (< 0.65) and the context match is
    1/2 (>= 0.5), so the context-aware matcher is the one that yields."""
    cases: list[tuple[Replacer, str, str, str]] = [
        (
            new.line_trimmed_replacer,
            "def f():\r\nalpha = 1\r\nbeta  = 2",
            "def f():\r\nalpha = ONE\r\nbeta  = 2",
            "def f():\r\nalpha = ONE\r\nbeta  = 2\r\n    gamma = 3\r\nend\r\n",
        ),
        (
            new.block_anchor_replacer,
            "def f():\r\n    alpha = 1\r\n    betx  = 2\r\n    gamma = 3\r\nend",
            "def f():\r\n    alpha = ONE\r\n    betx  = 2\r\n    gamma = 3\r\nend",
            "def f():\r\n    alpha = ONE\r\n    betx  = 2\r\n    gamma = 3\r\nend\r\n",
        ),
        (
            new.whitespace_normalized_replacer,
            "    alpha = 1\r\n    beta = 2",
            "    alpha = ONE\r\n    beta = 2",
            "def f():\r\n    alpha = ONE\r\n    beta = 2\r\n    gamma = 3\r\nend\r\n",
        ),
        (
            new.context_aware_replacer,
            "    alpha = 1\r\n    beta  = 2\r\n    qqqqqqqqq\r\nend",
            "    alpha = ONE\r\n    beta  = 2\r\n    qqqqqqqqq\r\nend",
            "def f():\r\n    alpha = ONE\r\n    beta  = 2\r\n    qqqqqqqqq\r\nend\r\n",
        ),
    ]
    for replacer, find, new_string, expected in cases:
        assert next(islice(replacer(CRLF_BLOCK, find), 1)).endswith("\r"), replacer.__name__
        for search in (new.replace, replace):
            out = search(CRLF_BLOCK, find, new_string)
            assert out == expected, (replacer.__name__, find)
            assert re.search(r"(?<!\r)\n", out) is None, (replacer.__name__, out)


def test_replace_all_keeps_the_crlf_ending_of_every_fuzzy_candidate() -> None:
    """``replace_all`` substitutes every occurrence of a fuzzy candidate; each keeps the file's line ending
    and no bare LF is left (#127)."""
    content = "alpha\r\nbeta\r\ngamma\r\nalpha\r\nbeta\r\ngamma\r\n"
    for search in (new.replace, replace):
        out = search(content, "alpha\r\nbetx\r\ngamma", "A\r\nB\r\nC", True)
        assert out == "A\r\nB\r\nC\r\nA\r\nB\r\nC\r\n"
        assert re.search(r"(?<!\r)\n", out) is None


def test_deleting_a_fuzzy_crlf_block_leaves_its_line_ending() -> None:
    """Deleting a block with an empty newString leaves its candidate's trailing ``\\r`` plus the ``\\n`` that
    follows it in the file, ``\\r\\n``, as an LF file leaves ``\\n`` (#127)."""
    for search in (new.replace, replace):
        assert search("alpha\r\nbeta\r\ngamma\r\ndelta\r\n", "alpha\r\nbetx\r\ngamma", "") == "\r\ndelta\r\n"
        assert search("alpha\nbeta\ngamma\ndelta\n", "alpha\nbetx\ngamma", "") == "\ndelta\n"


def test_the_crlf_guard_looks_at_the_file_not_at_oldstring() -> None:
    """A find whose last line ends with a ``\\r`` of its own still gets the file's line ending kept: the guard
    is the ``\\n`` right after the occurrence in the file, not what oldString ends with (#127), and a fuzzy
    replacer may match an oldString whose trailing ``\\r`` was whitespace it ignored. With an oldString guard
    the line-trimmed candidate's trailing CR was dropped here, leaving a bare LF after the replacement."""
    content = '  \r\n\x1c\r\n\r\n`xx\r\n\\\r\n\r\n\r\r\r\n\xa0\r\n\r\n\r\n\x1c"\r\n\\$ '
    find = "  \r\n   `xx\r\n \\\r\n \r\n  \r\r"
    for search in (new.replace, replace):
        out = search(content, find, "R\r\nS")
        assert out == '  \r\n\x1c\r\nR\r\nS\r\n\xa0\r\n\r\n\r\n\x1c"\r\n\\$ '
        assert re.search(r"(?<!\r)\n", out) is None


def test_a_cr_in_oldstring_matches_only_a_lone_cr_the_file_has() -> None:
    """A CR left in oldString once its CRLF pairs are folded is a lone CR, and it matches only a lone CR of the
    file — one Read shows the model, since Read drops a line ending's ``\\r`` and a final lone one too (#143). A
    line-ending CR is invisible in Read, so oldString can't target it: on a CRLF file the find isn't there and the
    answer is "Could not find", rather than #127's removal of line endings the model never wrote, which left bare
    LFs. A lone CR the file really has is still the model's to remove, exactly and with ``replaceAll``."""
    for search in (new.replace, replace):
        for content, find, new_string, replace_all in (
            ("a\r\nb\r\n", "\r", "", True),  # whitespace-only: the exact matcher alone runs (#110)
            ("a\r\nb\r\n", "\r", "", False),
            ("a\r\nb\r", "\r", "", False),  # the content's final lone CR is hidden by Read as well
        ):
            with pytest.raises(ValueError, match="Could not find"):
                search(content, find, new_string, replace_all)
        assert search("a\rb\r\n", "\r", "") == "ab\r\n"
        assert search("a\rb\r\n", "\r", "", True) == "ab\r\n"
        assert search("a\rb\rc\r\n", "\r", "", True) == "abc\r\n"


def test_an_edit_that_changes_nothing_is_reported() -> None:
    """A line matcher strips a trailing ``\\r`` of oldString as whitespace, so ``"foo\\r"`` -> ``"foo"`` on a CRLF
    file matches ``foo`` in the view and replaces it by ``foo``: before this check the edit changed nothing and
    reported success (#143). Now it is an error, which names the line-ending CR when oldString holds a CR. A fuzzy
    match that already reads as newString is refused the same way, without the CR hint."""
    for search in (new.replace, replace):
        for content, find, new_string in (
            ("foo\r\nbar\r\n", "foo\r", "foo"),
            ("x\r\nfoo\r\ny\r\n", "foo\r", "foo"),
            ("foo\r\nbar\nbaz\n", " foo\r", "foo"),
        ):
            with pytest.raises(ValueError, match="No changes to apply.*line ending"):
                search(content, find, new_string)
        with pytest.raises(ValueError, match="No changes to apply") as e:
            search("    foo\nbar\n", "foo  ", "    foo")  # line-trimmed: the candidate `    foo` is newString
        assert "line ending" not in str(e.value)


def test_a_fuzzy_cr_removal_needs_a_lone_cr_in_the_file() -> None:
    """The escape matcher unescapes a literal backslash-r of oldString to a CR, and a line matcher strips a real
    trailing ``\\r`` of it as whitespace: either way the candidate looks for a lone CR, and a line-ending CR is
    invisible in Read (#143). A CRLF file has no lone CR for it, so the answer is "Could not find", not #127's
    edit that took the file's line ending and left a bare LF. Where the file does have a lone CR the escape
    matcher's candidate removes it, through the fuzzy path."""
    view, _ = _lf_view("foo\rbar\r\n")
    assert next(islice(new.escape_normalized_replacer(view, "foo\\r"), 1)) == "foo\r"
    for search in (new.replace, replace):
        for find, new_string in (("\\r", ""), ("\\r", "X"), ("foo\\r", "foo")):
            with pytest.raises(ValueError, match="Could not find"):
                search("foo\r\nbar\r\nbaz\r\n", find, new_string)
        assert search("foo\rbar\r\n", "foo\\r", "foo") == "foobar\r\n"
        assert search("a\rb\r\n", "\\r", "X") == "aXb\r\n"


def test_a_literal_backslash_r_is_text_and_the_line_ending_stays_out_of_the_match() -> None:
    """Only the escape matcher reads oldString's escapes: to the others a literal backslash-r is two characters of
    text, matched against the same two in the file. The ``\\r`` after them in the file is a line ending, which is
    invisible in Read, so oldString can't target it and the view has no CR there for a candidate to end with
    (#143): the match stops at the model's text, its span maps back whole, and the file keeps its line ending —
    where counting the model's CRs on the unescaped oldString took that CR and left a bare LF (#127)."""
    cases = [
        (
            new.line_trimmed_replacer,
            "  alpha\r\n  sep = \\r\r\nend\r\n",
            "alpha\r\nsep = \\r",
            "ALPHA\r\nsep = \\r",
            "ALPHA\r\nsep = \\r\r\nend\r\n",
        ),
        (
            new.block_anchor_replacer,
            "x\r\ndef f():\r\n    a = 1\r\n    b = 2\r\n    sep = '\\r\r\ny\r\n",
            "def f():\r\n    a = 1\r\n    b = 9\r\n    sep = '\\r",
            "NEW",
            "x\r\nNEW\r\ny\r\n",
        ),
    ]
    for replacer, content, find, new_string, expected in cases:
        view, _ = _lf_view(content)
        candidate = next(islice(replacer(view, find.replace("\r\n", "\n")), 1))
        assert candidate.endswith("\\r") and not candidate.endswith("\r"), (replacer.__name__, candidate)
        for search in (new.replace, replace):
            out = search(content, find, new_string)
            assert out == expected, (replacer.__name__, out)
            assert re.search(r"(?<!\r)\n", out) is None


def test_replace_all_maps_every_occurrence_back_on_its_own() -> None:
    """With ``replaceAll`` every occurrence of the view is mapped back to its own raw span (#143), so the lone CR
    of ``"foo\\rbar"`` — one Read shows, so one oldString could name — is outside both matches and stays in the
    file, where #127's per-occurrence END rule took it at the first occurrence and left a bare LF. Without
    ``replaceAll`` the candidate is in the view twice, so the answer is the ambiguity (#110)."""
    content = "foo\rbar\r\nfoo\r\n"
    view, _ = _lf_view(content)
    assert next(islice(new.line_trimmed_replacer(view, "  foo"), 1)) == "foo"
    for search in (new.replace, replace):
        out = search(content, "  foo", "NEW", True)
        assert out == "NEW\rbar\r\nNEW\r\n"
        assert re.search(r"(?<!\r)\n", out) is None
        with pytest.raises(ValueError, match="Found multiple matches"):
            search(content, "  foo", "NEW")


def test_a_candidate_starting_with_lf_takes_the_cr_of_its_pair() -> None:
    """A candidate that starts with ``\\n`` right after a ``\\r`` holds the pair's ``\\n``, so the pair's
    ``\\r`` joins the span (START rule, #127). The old code split the pair and left a lone CR: the escape
    matcher unescapes the literal ``\\n`` of oldString to a real one, and the replacement of the candidate
    ``"\\nfoo"`` gave ``"x\\r\\r\\nFOO\\r\\ny\\r\\n"``."""
    content = "x\r\nfoo\r\ny\r\n"
    assert next(islice(new.escape_normalized_replacer(content, "\\nfoo"), 1)) == "\nfoo"
    for search in (new.replace, replace):
        assert search(content, "\\nfoo", "\r\nFOO") == "x\r\nFOO\r\ny\r\n"
        assert search(content, "\\nfoo", "FOO") == "xFOO\r\ny\r\n"


def test_an_escaped_cr_finds_no_match_in_a_crlf_file() -> None:
    """The escape matcher unescapes oldString's literal ``\\nfoo\\r`` to ``"\\nfoo\\r"``, whose CR is a lone one,
    and a line-ending CR is invisible in Read: a CRLF file has no lone CR for it to match, so the answer is
    "Could not find" (#143) rather than #127's ``"aXX\\n"``, whose per-occurrence rules split both pairs. Where
    the file does have that lone CR, the candidate holds its pair's ``\\n`` and so maps back over the pair's
    ``\\r`` too, and both go — no bare LF and no lone CR left."""
    lone = "a\r\nfoo\rfoo\r\n"
    assert next(islice(new.escape_normalized_replacer(_lf_view(lone)[0], "\\nfoo\\r"), 1)) == "\nfoo\r"
    for search in (new.replace, replace):
        with pytest.raises(ValueError, match="Could not find"):
            search("a\r\nfoo\r\nfoo\r\n", "\\nfoo\\r", "X", True)
        out = search(lone, "\\nfoo\\r", "X", True)
        assert out == "aXfoo\r\n"
        assert re.search(r"(?<!\r)\n", out) is None


def test_an_escaped_lf_cr_candidate_needs_a_lone_cr_in_the_file() -> None:
    """Opus pre-merge review of #127, now #143: oldString ``\\n\\r`` unescapes to a candidate ending in a lone CR,
    which a line-ending CR — invisible in Read — can't be, so a CRLF file gives "Could not find" rather than the
    old ``"\\\\r\\n"``, whose span covered both CRs and left the ``\\n`` bare. Where the file has the lone CR right
    after a line ending, the candidate holds that pair's ``\\n``, so its span takes the pair's ``\\r`` as well."""
    for search in (new.replace, replace):
        with pytest.raises(ValueError, match="Could not find"):
            search("\\r\r\n\r\n", "\\n\\r", "")
        with pytest.raises(ValueError, match="Could not find"):
            search("b\r\n\r\nfoo \r\na", "\\n\\r", "")
        out = search("b\r\n\rfoo \r\na", "\\n\\r", "")
        assert out == "bfoo \r\na"
        assert re.search(r"(?<!\r)\n", out) is None


def test_a_file_ending_in_a_lone_cr_keeps_it() -> None:
    """A candidate's trailing ``\\r`` at the very end of the file (no ``\\n`` after) is a line ending the model
    never saw a pair of: the END rule counts the end of the file like a following ``\\n`` and keeps the CR
    (#127); the old guard dropped it. The line-trimmed candidate for ``"  a\\n  b"`` in ``"a\\r\\nb\\r"`` is
    ``"a\\r\\nb\\r"``. The newString is the CRLF one ``_edit`` passes for this file; with a bare-LF one (a
    caller's own doing) the file's final CR is still kept."""
    assert next(islice(new.line_trimmed_replacer("a\r\nb\r", "  a\n  b"), 1)) == "a\r\nb\r"
    for search in (new.replace, replace):
        assert search("a\r\nb\r", "  a\n  b", "A\r\nB") == "A\r\nB\r"
        assert search("a\r\nb\r", "  a\n  b", "A\nB") == "A\nB\r"


def test_a_crlf_file_ending_in_a_lone_cr_keeps_it_too() -> None:
    """Read drops a final lone ``\\r`` as it drops a line ending's, so the view ends before it and no match can
    reach it (#143): an exact one, a fuzzy one, and ``replaceAll`` all leave the CR in the file. An oldString that
    names it matches the text before it only, and one that is nothing but that CR is not found at all."""
    for search in (new.replace, replace):
        assert search("a\r\nb\r", "b", "B") == "a\r\nB\r"
        assert search("a\r\nb\r", "b", "B", True) == "a\r\nB\r"
        assert search("a\r\nb\r", "a\r\nb\r", "X") == "X\r"
        with pytest.raises(ValueError, match="Could not find"):
            search("a\r\nb\r", "\r", "")


def test_a_mixed_endings_file_matches_both_endings_as_one() -> None:
    """``"foo\\r\\n"`` and ``"foo\\n"`` are the same ``"foo\\n"`` in the view, so on a file with both endings an
    oldString naming one of them matches twice and is reported ambiguous (#143): the model adds context rather
    than Edit picking the ending it happened to write. With ``replaceAll`` both occurrences go, each mapped back
    to its own raw span, and newString's ending is what replaces them."""
    for search in (new.replace, replace):
        with pytest.raises(ValueError, match="Found multiple matches"):
            search("foo\r\nfoo\n", "foo\r\n", "X\r\n")
        with pytest.raises(ValueError, match="Found multiple matches"):
            search("foo\r\nfoo\n", "foo\n", "X\n")
        assert search("foo\r\nfoo\n", "foo\r\n", "X\r\n", True) == "X\r\nX\r\n"
        # one occurrence: the span is the file's own bytes, whatever ending that line has
        assert search("one\r\ntwo\nthree\r\n", "two\n", "TWO\r\n") == "one\r\nTWO\r\nthree\r\n"


@pytest.mark.parametrize("seed", range(4))
def test_crlf_replacements_never_leave_a_bare_lf(seed: int) -> None:
    """On CRLF content (every ``\\n`` of the random text converted) with CRLF finds, whenever ``replace`` returns,
    the output has no bare LF — for every find, with no exemption (#143): the input has none, the newString has
    none, and the match is made on the LF view and mapped back, so a span never splits a CRLF pair. An exact find
    ending in a CR can't match such a file at all (its CR would have to be a lone one, and every CR here pairs
    with an LF), and a fuzzy candidate can only reach a lone CR, whose LF keeps its own CR. The oracle must give
    the same outcome in every case."""
    rng = random.Random(seed)
    for _ in range(500):
        content_lf = random_text(rng, 10)
        content = content_lf.replace("\n", "\r\n")
        find = random_find(rng, content_lf).replace("\n", "\r\n") or "x"
        replace_all = rng.random() < 0.2
        result = outcome(new.replace, content, find, "R\r\nS", replace_all)
        assert result == outcome(replace, content, find, "R\r\nS", replace_all), (content, find, replace_all)
        if isinstance(result, str):
            assert re.search(r"(?<!\r)\n", result) is None, (content, find, replace_all, result)


VIEW_PIECES = ["a", "b", "ab", "", "\r\n", "\n", "\r", "\r\r\n", "\r\n\r", "a\r\nb", "\r"]
"""What the random content of the mapping test is made of: CRLF pairs, bare LFs, lone CRs, and the two ways they
meet — ``\\r\\r\\n``, whose lone CR the view keeps before a pair, and a final lone CR, which the view drops."""

MARKER = "\x00m\x00"
"""A find no random content contains, so its one occurrence is the span being mapped back."""


@pytest.mark.parametrize("seed", range(4))
def test_a_view_span_maps_back_without_splitting_a_pair(seed: int) -> None:
    """A view span maps back to the raw content as ``raw(i) = i + bisect_left(crlf, i)`` (#143), so no span
    boundary — a start or an end — ever lands between a line ending's ``\\r`` and its ``\\n``, whatever the
    content mixes. minifs's view and mapping are the oracle's, the view is the content folded (minus a final lone
    CR), and replacing a unique marker changes nothing outside the marked span: the raw bytes on both sides of it
    are the content's own, through the exact matcher and through a fuzzy one."""
    rng = random.Random(seed)
    for _ in range(300):
        content = "".join(rng.choice(VIEW_PIECES) for _ in range(rng.randint(0, 8)))
        view, crlf = _lf_view(content)
        new_view, new_crlf = new._lf_view(content)  # pyright: ignore[reportPrivateUsage]
        assert (new_view, list(new_crlf)) == (view, crlf), content
        # the view is the content's own bytes up to its end, folded: a final lone CR is past the last span
        assert content[: _raw_at(crlf, len(view))].replace("\r\n", "\n") == view, content
        for i in range(len(view) + 1):
            at = _raw_at(crlf, i)
            assert new._raw_span(new_crlf, i, i) == (at, at), (content, i)  # pyright: ignore[reportPrivateUsage]
            assert content[at - 1 : at + 1] != "\r\n", (content, i, at)  # never between a CR and its LF

        k = rng.randrange(len(content) + 1)
        if k and k < len(content) and content[k - 1] == "\r" and content[k] == "\n":
            continue  # the marker itself would split a pair
        marked = content[:k] + MARKER + content[k:]
        if any(line.strip() == MARKER and line != MARKER for line in _lf_view(marked)[0].split("\n")):
            continue  # a line matcher's candidate would take the line's lone CR with the marker
        expected = content[:k] + "Q" + content[k:]
        for search in (new.replace, replace):
            # the marker exactly, and padded with whitespace a fuzzy matcher strips off it: both match the marker's
            # own span, so everything on either side of it must come back byte for byte
            for find in (MARKER, f" {MARKER} "):
                for replace_all in (False, True):
                    assert search(marked, find, "Q", replace_all) == expected, (content, k, find, replace_all)


def test_multi_occurrence_replacer_is_not_tried() -> None:
    """``multi_occurrence_replacer`` re-yields the oldString, which ``simple_replacer`` already tried, so it could
    never change ``replace``'s outcome: it is not in ``REPLACERS`` (kept for the parity tests)."""
    assert new.multi_occurrence_replacer not in new.REPLACERS


# ---------------------------------------------------------------- bounded cost


def test_anchor_scans_are_linear() -> None:
    """Every line equals the first anchor and none the last: the scan for the last line was O(n²) (2.85 s at
    8,000 lines, quadrupling per doubling, so minutes here); now the last line's positions are found once."""
    content = "x\n" * 40_000
    begin = time.monotonic()
    with pytest.raises(ValueError, match="Could not find oldString"):
        new.replace(content, "x\ny\nz", "q")
    assert time.monotonic() - begin < 20


def test_a_miss_costs_a_fraction_of_what_it_did() -> None:
    """A 20-line find that is nowhere in a file of varied lines: every replacer runs to the end. Against the
    reference on the same machine in the same test, so a slow runner slows both (measured: over ten times)."""
    rng = random.Random(0)
    content = "\n".join(
        f"{'  ' * rng.randint(0, 4)}line {rng.randint(0, 50)} = {rng.randint(0, 9)}" for _ in range(10_000)
    )
    find = "\n".join(f"other {k}" for k in range(20))
    took: list[float] = []
    for search in (new.replace, replace):
        begin = time.monotonic()
        with pytest.raises(ValueError, match="Could not find oldString"):
            search(content, find, "q")
        took.append(time.monotonic() - begin)
    assert took[0] < took[1] / 3


def test_the_uniqueness_check_is_linear() -> None:
    """An exact match of a long find in a repetitive file: ``str.rfind`` took 8 s for the check that it is the only
    one, in one call that no deadline interrupts; a forward search for a second occurrence takes milliseconds."""
    block = "xxxxx\n" * 4_000
    find = block + "u\n" + block
    content = find + "xxxxx\n" * 400_000
    begin = time.monotonic()
    assert new.replace(content, find, "q", timeout=30) == "q" + "xxxxx\n" * 400_000
    assert time.monotonic() - begin < 3


def test_a_million_replace_all_occurrences_are_substituted_in_one_pass() -> None:
    """A million occurrences of a fuzzy candidate (the line-trimmed ``"x\\r"`` for the find ``"  x"``): the
    ``starts`` and ``pieces`` lists of the old ``_substitute`` cost ~2.5 s and ~340 MB; one pass into an
    ``io.StringIO`` is linear and small. The bound is generous: it guards against a quadratic regression, not a
    benchmark."""
    content = "x\r\n" * 1_000_000
    begin = time.monotonic()
    assert new.replace(content, "  x", "y", True) == "y\r\n" * 1_000_000
    assert time.monotonic() - begin < 5


def test_words_in_a_long_line_are_found_in_one_pass() -> None:
    """A find of 10,000 words in a line of 40,000 with other whitespace between them: ladderframe's regex
    restarted at every character (20 s in one call); the answer is the same text."""
    line = "a " * 40_000 + "b"
    find = "a  " * 10_000 + "b"
    begin = time.monotonic()
    assert new.replace(line, find, "q", timeout=30) == "a " * 30_000 + "q"
    assert time.monotonic() - begin < 3


def test_a_one_line_find_in_a_long_periodic_line_is_not_searched_at_every_period() -> None:
    """A stripped find that recurs inside a 1 MB line, 500,000 times and never as a whole line: a search that
    stops at each of those took 20 s in the trimmed-lines matcher alone, where the reference took milliseconds."""
    content = "0," * 500_000 + "\n"
    find = " " + "0," * 2_000
    begin = time.monotonic()
    assert list(new.line_trimmed_replacer(content, find)) == []
    with pytest.raises(ValueError, match="Found multiple matches"):
        new.replace(content, find, "q", timeout=30)
    assert time.monotonic() - begin < 3


def slow_replacer(content: str, find: str) -> Iterator[str]:
    time.sleep(0.05)
    yield from ()


def test_replace_stops_between_replacers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(new, "REPLACERS", (slow_replacer, slow_replacer))
    begin = time.monotonic()
    with pytest.raises(ReplaceTimeout, match=re.escape(timeout_message(0.01))):
        new.replace("a\n", "b", "c", timeout=0.01)
    assert time.monotonic() - begin < 1
    # Without a timeout the search runs to the end, as the replacers' own tests call it.
    with pytest.raises(ValueError, match="Could not find"):
        new.replace("a\n", "b", "c")


def test_replace_stops_on_a_scan_that_would_take_tens_of_seconds() -> None:
    """Window scans whose cheap gates pass everywhere (every line equals the find's first and last lines) and
    whose block test fails late (the middle differs 1,000 lines in) are still O(n·m), tens of seconds here: the
    search ends soon after the timeout. (Where it stops depends on the machine; the tests below are the ones that
    hold each replacer to looking at the clock.)"""
    content = "x\n" * 200_000
    find = "x\n" * 1_000 + "y\n" + "x\n" * 999 + "x"
    begin = time.monotonic()
    with pytest.raises(ReplaceTimeout):
        new.replace(content, find, "q", timeout=0.2)
    assert 0.2 <= time.monotonic() - begin < 5


LOOPS: list[tuple[Replacer, str, str]] = [
    (new.line_trimmed_replacer, "a\n" * 10_000, "a"),  # a hit per line
    (new.block_anchor_replacer, "a\n" * 10_000, "a\nb\nc"),
    (new.whitespace_normalized_replacer, "a\n" * 10_000, "b\nc"),
    (new.indentation_flexible_replacer, "a\n" * 10_000, "b\nc"),
    (new.escape_normalized_replacer, "a\n" * 10_000, "b\nc"),
    (new.trimmed_boundary_replacer, "a\n" * 10_000, " b\nc "),
    (new.context_aware_replacer, "a\n" * 10_000, "a\nb\nc"),
    (new.multi_occurrence_replacer, "a" * 10_000, "a"),
]
"""A replacer, and an input whose lines all fail its cheap test (or, for the two that only loop over hits, all
hit), so nothing but the clock looked at in a pass over the lines can stop it: for the two anchor replacers that
is the pass that finds the last anchor's lines, which comes before their main loop."""

STEPS: list[tuple[str, Replacer, str, str]] = [
    ("block score", new.block_anchor_replacer, "a\nb\nc\n", "a\nq\nc"),
    ("block scores", new.block_anchor_replacer, "a\nb\nc\n" * 2, "a\nq\nc"),
    ("words in a line", new.whitespace_normalized_replacer, "a b  c d\n", "b c"),
    ("costly find", new.whitespace_normalized_replacer, "a" * 400 + "\n", "a" * 200 + "b"),
    ("whitespace block", new.whitespace_normalized_replacer, "x\n" * 100, "x\ny\nx"),
    ("indentation block", new.indentation_flexible_replacer, "x\n" * 100, " x\n y\n x"),
    ("escape block", new.escape_normalized_replacer, "x\n" * 100, "x\ny\nx"),
    ("escapes", new.escape_normalized_replacer, "\\t" * 5_000 + "\n", "zzz"),
    ("trimmed block", new.trimmed_boundary_replacer, "x\n" * 100, " x\ny\nx "),
    ("context block", new.context_aware_replacer, "a\nb\nc\n" * 30, "a\nq\nc"),
]
"""A replacer's step that costs more than a line (a distance, a block test, a search that can be quadratic, a
call per escape), and an input of under ``_TICK`` lines that reaches it: no tick fires, so only the look at the
clock before that step (or, for the escapes, the ticks inside it) can stop the replacer."""


@pytest.mark.parametrize(("replacer", "content", "find"), LOOPS, ids=[loop[0].__name__ for loop in LOOPS])
def test_each_replacer_looks_at_the_clock_in_its_loop(replacer: Replacer, content: str, find: str) -> None:
    """Under a deadline that has already passed, each replacer stops inside its own loop over the file (no
    ``replace`` around it to stop between replacers, and no timing: the deadline is in the past from the start);
    with none it runs to the end."""
    stops_when_late(replacer, content, find)


@pytest.mark.parametrize(("replacer", "content", "find"), [step[1:] for step in STEPS], ids=[s[0] for s in STEPS])
def test_each_replacer_looks_at_the_clock_before_a_costly_step(replacer: Replacer, content: str, find: str) -> None:
    assert content.count("\n") < new._TICK  # pyright: ignore[reportPrivateUsage]
    stops_when_late(replacer, content, find)


def stops_when_late(replacer: Replacer, content: str, find: str) -> None:
    for _ in replacer(content, find):  # runs to the end without a deadline
        pass
    late = new._Deadline(1.0)  # pyright: ignore[reportPrivateUsage]
    late.at = time.monotonic() - 1
    token = new._deadline.set(late)  # pyright: ignore[reportPrivateUsage]
    try:
        with pytest.raises(ReplaceTimeout, match=re.escape(timeout_message(1.0))):
            for _ in replacer(content, find):
                pass
    finally:
        new._deadline.reset(token)  # pyright: ignore[reportPrivateUsage]


def test_the_pass_over_a_long_lines_words_looks_at_the_clock() -> None:
    """A find whose words are at the end of one line of megabytes (``"a " * 5_000_000 + "b c"``, find ``"b  c"``):
    the pass over that line's tokens is seconds of work, which ran after the replacer's last look at the clock.
    Under a deadline already passed it stops inside the pass; with none it gives the words' text."""
    line = "a " * 10_000 + "b \t c"
    assert new._words_in_line(line, 20_000, ["b", "c"]) == "b \t c"  # pyright: ignore[reportPrivateUsage]
    late = new._Deadline(1.0)  # pyright: ignore[reportPrivateUsage]
    late.at = time.monotonic() - 1
    token = new._deadline.set(late)  # pyright: ignore[reportPrivateUsage]
    try:
        with pytest.raises(ReplaceTimeout, match=re.escape(timeout_message(1.0))):
            new._words_in_line(line, 20_000, ["b", "c"])  # pyright: ignore[reportPrivateUsage]
    finally:
        new._deadline.reset(token)  # pyright: ignore[reportPrivateUsage]


def test_multi_occurrence_replacer_gives_nothing_for_an_empty_find() -> None:
    """An empty find matches at every offset and the search never advanced past it, so the generator never ended
    (``replace`` rejects an empty oldString before any replacer; a direct caller looped forever). ``islice``, so a
    regression fails instead of hanging."""
    assert list(islice(new.multi_occurrence_replacer("abc", ""), 5)) == []
    assert list(islice(new.multi_occurrence_replacer("", ""), 5)) == []
    assert list(islice(new.multi_occurrence_replacer("abab", "ab"), 5)) == ["ab", "ab"]


def test_a_timeout_after_an_ambiguous_match_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    """A candidate is in the file twice and the deadline passes in a later replacer: the retry doesn't claim the
    text is missing. (An ambiguous *exact* oldString now fails fast with "Found multiple matches" (#110), so the
    ambiguity here comes from a fuzzy candidate that isn't the oldString.)"""

    def ambiguous(content: str, find: str) -> Iterator[str]:
        yield "a"  # twice in "a\na\n", and not the find itself

    monkeypatch.setattr(new, "REPLACERS", (ambiguous, slow_replacer, slow_replacer))
    with pytest.raises(ReplaceTimeout, match=re.escape(timeout_message(0.01, ambiguous=True))) as caught:
        new.replace("a\na\n", "b", "c", timeout=0.01)
    assert "Found multiple matches" in str(caught.value)


def test_an_ambiguous_exact_match_fails_before_a_slow_matcher(monkeypatch: pytest.MonkeyPatch) -> None:
    """An ambiguous exact oldString is reported at once, without trying the later matchers until the deadline: had
    the search continued past it, the slow matchers would run to the timeout, whose retry also says "Found
    multiple matches", so the error's type is what proves the raise happens at the ambiguity."""
    monkeypatch.setattr(new, "REPLACERS", (new.simple_replacer, slow_replacer, slow_replacer))
    with pytest.raises(ValueError, match="Found multiple matches") as caught:
        new.replace("a\na\n", "a", "c", timeout=0.01)
    assert not isinstance(caught.value, ReplaceTimeout)


def test_replace_stops_between_block_scores() -> None:
    """Hundreds of blocks with the find's anchors and six differing 200-character middle lines each: scoring one
    block is tens of milliseconds of Levenshtein, so the deadline is checked before each distance, not every
    4,096 lines (which let a 0.2 s timeout run for 44 s)."""
    rng = random.Random(0)
    letters = "abcdefghij"
    middle = ["".join(rng.choice(letters) for _ in range(200)) for _ in range(6)]
    content = "".join("{\n" + "\n".join(middle) + "\n}\n" for _ in range(600))
    find = "{\n" + "\n".join("".join(rng.choice(letters) for _ in range(200)) for _ in range(6)) + "\n}"
    begin = time.monotonic()
    with pytest.raises(ReplaceTimeout):
        new.replace(content, find, "q", timeout=0.2)
    assert time.monotonic() - begin < 2


async def test_a_slow_edit_is_a_retry(deps: Deps, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(new, "REPLACERS", (slow_replacer, slow_replacer))
    quick = Deps(deps.vfs, vfs_settings=ToolSettings(edit=EditSettings(timeout=0.01)))
    message = await retry_message(Edit, quick, filePath="notes.md", oldString="nope", newString="yes")
    assert message == timeout_message(0.01)


def test_edit_settings() -> None:
    assert (EditSettings().max_bytes, EditSettings().timeout) == (10 * 1024 * 1024, 10.0)
    assert EditSettings(timeout=1).timeout == 1
    assert EditSettings(timeout=None).timeout is None
    for timeout in (0, -1.0, math.inf, math.nan, 10**400):  # the int is past the largest float
        with pytest.raises(ValueError, match="edit timeout"):
            EditSettings(timeout=timeout)
    for wrong in (True, "10"):
        with pytest.raises(TypeError, match="edit timeout"):
            EditSettings(timeout=wrong)  # pyright: ignore[reportArgumentType]
