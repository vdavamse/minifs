"""The fallback replacers of ``Edit`` give ladderframe's answers at a bounded cost (#80).

The ``reference`` section is ladderframe's matcher code as minifs shipped it before #80 (ladderframe's
``packages/harness/src/ladderframe/tools/edit.py``), kept here as the oracle: for random content and finds, each
rewritten replacer must yield the same candidates in the same order, and ``replace`` the same result or error. Then
the cost bounds: the cases that were quadratic finish in linear time, and a search that runs past ``timeout`` is a
retry, from inside a replacer's loop too."""

import math
import random
import re
import time
from collections.abc import Callable, Iterator

import pytest

import minifs.tools.edit as new
from minifs.tools.deps import EditSettings, ToolSettings
from minifs.tools.edit import EMPTY_OLD_STRING, Edit, ReplaceTimeout, timeout_message
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
    if len(search) < 3:
        return
    if search[-1] == "":
        search.pop()
    first, last = search[0].strip(), search[-1].strip()
    max_delta = max(1, len(search) // 4)

    candidates: list[tuple[int, int]] = []
    for i, line in enumerate(original):
        if line.strip() != first:
            continue
        for j in range(i + 2, len(original)):
            if original[j].strip() == last:
                if abs((j - i + 1) - len(search)) <= max_delta:
                    candidates.append((i, j))
                break  # only the first occurrence of the last line
    if not candidates:
        return

    if len(candidates) == 1:
        start, end = candidates[0]
        if _middle_similarity(original, start, end - start + 1, search, True) >= SINGLE_CANDIDATE_SIMILARITY_THRESHOLD:
            begin, finish = _span(original, start, end)
            yield content[begin:finish]
        return

    best: tuple[int, int] | None = None
    best_similarity = -1.0
    for start, end in candidates:
        similarity = _middle_similarity(original, start, end - start + 1, search, False)
        if similarity > best_similarity:
            best_similarity, best = similarity, (start, end)
    if best is not None and best_similarity >= MULTIPLE_CANDIDATES_SIMILARITY_THRESHOLD:
        begin, finish = _span(original, *best)
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


def replace(content: str, old_string: str, new_string: str, replace_all: bool = False) -> str:
    """Replace `old_string`, trying each replacer in turn until one finds a unique match (or any, with
    `replace_all`). Raises `ValueError` with the message for the model when nothing fits."""
    if old_string == new_string:
        raise ValueError("No changes to apply: oldString and newString are identical.")
    if old_string == "":
        raise ValueError(EMPTY_OLD_STRING)

    not_found = True
    for replacer in REPLACERS:
        for search in replacer(content, old_string):
            index = content.find(search)
            if index == -1:
                continue
            not_found = False
            if _is_disproportionate_match(search, old_string):
                raise ValueError(
                    "Refusing replacement because the matched span is much larger than oldString. "
                    "Re-read the file and provide the full exact oldString for the intended replacement."
                )
            if replace_all:
                return content.replace(search, new_string)
            if index != content.rfind(search):
                continue
            return content[:index] + new_string + content[index + len(search) :]

    if not_found:
        raise ValueError(
            "Could not find oldString in the file. It must match exactly, including whitespace, indentation, "
            "and line endings."
        )
    raise ValueError("Found multiple matches for oldString. Provide more surrounding context to make the match unique.")


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


@pytest.mark.parametrize("seed", range(4))
def test_replacers_give_ladderframes_candidates(seed: int) -> None:
    rng = random.Random(seed)
    for _ in range(1500):
        content = random_text(rng, 10)
        find = random_find(rng, content) or "x"  # replace() rejects an empty find before any replacer runs
        for name, rewritten, reference in PAIRS:
            assert list(rewritten(content, find)) == list(reference(content, find)), (name, content, find)
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
    search ends soon after the timeout. (Where it stops depends on the machine; the test below is the one that
    holds each replacer to looking at the clock.)"""
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
hit), so nothing but the clock looked at inside its loop over lines can stop it."""


@pytest.mark.parametrize(("replacer", "content", "find"), LOOPS, ids=[loop[0].__name__ for loop in LOOPS])
def test_each_replacer_looks_at_the_clock_in_its_loop(replacer: Replacer, content: str, find: str) -> None:
    """Under a deadline that has already passed, each replacer stops inside its own loop over the file (no
    ``replace`` around it to stop between replacers, and no timing: the deadline is in the past from the start);
    with none it runs to the end."""
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


def test_a_timeout_after_an_ambiguous_match_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    """The find is in the file twice, exactly, and the deadline passes in a later replacer: the retry doesn't
    claim the text is missing."""
    monkeypatch.setattr(new, "REPLACERS", (new.simple_replacer, slow_replacer, slow_replacer))
    with pytest.raises(ReplaceTimeout, match=re.escape(timeout_message(0.01, ambiguous=True))) as caught:
        new.replace("a\na\n", "a", "c", timeout=0.01)
    assert "Found multiple matches" in str(caught.value)


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
