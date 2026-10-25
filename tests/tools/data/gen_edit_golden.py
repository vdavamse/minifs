"""Runs ladderframe's disk Edit over a corpus and prints the golden JSON for minifs's tests/tools/test_edit.py.
It needs ladderframe, which minifs doesn't depend on, so run it with ladderframe's own interpreter:

    <ladderframe>/.venv/bin/python -I tests/tools/data/gen_edit_golden.py > tests/tools/data/edit_golden.json
"""

# pyright: basic, reportMissingImports=false
import base64
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

from ladderframe.tools.edit import Edit
from pydantic_ai import ModelRetry

CASES = [
    ("unique", b"def f():\n    return 1\n", "return 1", "return 2", None),
    ("indented", b"    a = 1\n    b = 2\n", "b = 2", "b = 3", None),
    ("replace_all", b"x = 1\nx = 1\n", "x = 1", "x = 2", True),
    ("multiple", b"x = 1\nx = 1\n", "x = 1", "x = 2", None),
    ("not_found", b"x = 1\n", "y = 1", "y = 2", None),
    ("empty_old_existing", b"x\n", "", "y", None),
    ("create", None, "", "hello\n", None),
    ("create_bom", None, "", "\ufeffhello\n", None),
    ("crlf_bom", "\ufeffone\r\ntwo\r\nthree\r\n".encode(), "one\ntwo", "uno\ndos", None),
    ("crlf_given_crlf", b"one\r\ntwo\r\n", "one\r\ntwo", "uno\r\ndos", None),
    ("lf_given_crlf", b"one\ntwo\n", "one\r\ntwo", "uno\r\ndos", None),
    ("bom_in_new", b"a\nb\n", "a", "\ufeffz", None),
    ("non_utf8", b"caf\xe9\n", "caf", "bar", None),
    (
        "line_trimmed",
        b"def f():\n    if x:\n        return 1\n    return 2\n",
        "if x:\n    return 1",
        "    if y:\n        return 3",
        None,
    ),
    ("whitespace", b"a  =   1\n", "a = 1", "a = 2", None),
    ("escaped", b's = "x"\ny\n', 's = \\"x\\"', 's = "z"', None),
    ("block_anchor", b"start {\n    alpha_value = 1\n}\n", "start {\n    alpha_valu = 1\n}", "start {}", None),
    ("literal_dollar", b"a\n", "a", "$&", None),
    ("disproportionate", ("first\nm1\nm2\n" + "z" * 700 + "\nlast\n").encode(), "first\nm1\nm2\nlast", "x", None),
    ("no_final_newline", b"a\nb", "b", "c", None),
    ("delete_line", b"a\nb\nc\n", "b\n", "", None),
    ("unicode", "héllo wörld\n".encode(), "wörld", "\U0001f30d", None),
    (
        "multi_hunk_all",
        "".join(f"line {i % 7}\n" for i in range(40)).encode(),
        "line 3\n",
        "LINE 3\n",
        True,
    ),
]

out = []
with tempfile.TemporaryDirectory() as tmp:
    deps = SimpleNamespace(
        resolve_path=lambda p: Path(tmp) / p,
        permissions=SimpleNamespace(check=lambda *a: None),
    )
    ctx = SimpleNamespace(deps=deps, messages=[])
    for name, old, old_string, new_string, replace_all in CASES:
        path = Path(tmp) / f"{name}.txt"
        if old is not None:
            path.write_bytes(old)
        case = {
            "name": name,
            "old": None if old is None else base64.b64encode(old).decode(),
            "oldString": old_string,
            "newString": new_string,
            "replaceAll": replace_all,
        }
        try:
            result = Edit(ctx, f"{name}.txt", old_string, new_string, replace_all)
        except ModelRetry as e:
            case["retry"] = e.message.replace(str(path), "{path}")
        else:
            meta = dict(result.metadata)
            meta["filepath"] = meta["filepath"].replace(str(path), "{path}")
            meta["diff"] = meta["diff"].replace(str(path), "{path}")
            case["result"] = result.return_value
            case["metadata"] = meta
        case["written"] = base64.b64encode(path.read_bytes()).decode() if path.exists() else None
        out.append(case)
json.dump(out, sys.stdout, indent=1, ensure_ascii=True)
print()
