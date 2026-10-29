"""The fallback replacers of ``Edit`` give ladderframe's answers at a bounded cost (#80).

The ``reference`` section is ladderframe's matcher code as minifs shipped it before #80 (ladderframe's
``packages/harness/src/ladderframe/tools/edit.py``), kept here as the oracle: for random content and finds, each
rewritten replacer must yield the same candidates in the same order, and ``replace`` the same result or error. Then
the cost bounds: the cases that were quadratic finish in linear time, and a search that runs past ``timeout`` is a
retry, from inside a replacer's loop too."""

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
]
"""What random lines are made of: the characters the replacers treat specially (whitespace, backslash escapes, a
backslash before a newline), and few distinct letters, so lines repeat and anchors recur."""


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
    taken = lines[i : i + rng.randint(1, 5)]
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


def test_replace_rejects_what_it_always_rejected() -> None:
    with pytest.raises(ValueError, match="identical"):
        new.replace("a", "a", "a")
    with pytest.raises(ValueError, match=re.escape(EMPTY_OLD_STRING)):
        new.replace("a", "", "b")


# ---------------------------------------------------------------- bounded cost


def test_anchor_scans_are_linear() -> None:
    """Every line equals the first anchor and none the last: the scan for the last line was O(n²) (2.85 s at
    8,000 lines, quadrupling per doubling); now the last line's positions are found once."""
    content = "x\n" * 40_000
    begin = time.monotonic()
    with pytest.raises(ValueError, match="Could not find oldString"):
        new.replace(content, "x\ny\nz", "q")
    assert time.monotonic() - begin < 2


def test_a_miss_in_a_large_file_is_fast() -> None:
    """A 20-line find that is nowhere in a 1 MB file of varied lines: every replacer runs to the end."""
    rng = random.Random(0)
    content = "\n".join(
        f"{'  ' * rng.randint(0, 4)}line {rng.randint(0, 50)} = {rng.randint(0, 9)}" for _ in range(50_000)
    )
    find = "\n".join(f"other {k}" for k in range(20))
    begin = time.monotonic()
    with pytest.raises(ValueError, match="Could not find oldString"):
        new.replace(content, find, "q")
    assert time.monotonic() - begin < 2


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


def test_replace_stops_inside_a_replacer() -> None:
    """A window scan whose every cheap gate passes (every line is the find's first line) is still O(n·m); the
    deadline is checked inside the loop, so it ends within a tick of the timeout, not after the scan."""
    content = "x\n" * 200_000
    find = "x\n" * 2_000 + "y"
    begin = time.monotonic()
    with pytest.raises(ReplaceTimeout):
        new.replace(content, find, "q", timeout=0.2)
    assert time.monotonic() - begin < 3


async def test_a_slow_edit_is_a_retry(deps: Deps, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(new, "REPLACERS", (slow_replacer, slow_replacer))
    quick = Deps(deps.vfs, vfs_settings=ToolSettings(edit=EditSettings(timeout=0.01)))
    message = await retry_message(Edit, quick, filePath="notes.md", oldString="nope", newString="yes")
    assert message == timeout_message(0.01)


def test_edit_settings() -> None:
    assert (EditSettings().max_bytes, EditSettings().timeout) == (10 * 1024 * 1024, 10.0)
    assert EditSettings(timeout=1).timeout == 1
    for timeout in (0, -1.0, True):
        with pytest.raises(ValueError, match="edit timeout"):
            EditSettings(timeout=timeout)
