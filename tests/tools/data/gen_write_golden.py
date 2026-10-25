"""Runs ladderframe's disk Write over a corpus and prints the golden JSON for minifs's tests/tools/test_write.py.
It needs ladderframe, which minifs doesn't depend on, so run it with ladderframe's own interpreter:

    <ladderframe>/.venv/bin/python -I tests/tools/data/gen_write_golden.py > tests/tools/data/write_golden.json
"""

# pyright: basic, reportMissingImports=false
import base64
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

from ladderframe.tools.write import Write

CASES = [
    ("new", None, "a\nb\n"),
    ("overwrite", b"a\nb\n", "a\nc\n"),
    ("bom_kept", "﻿old".encode(), "new"),
    ("bom_in_content", None, "﻿x\n"),
    ("bom_both", "﻿old\n".encode(), "﻿new\n"),
    ("no_final_newline", b"x", "y"),
    ("add_final_newline", b"x", "x\n"),
    ("indented", b"    a = 1\n    b = 2\n", "    a = 1\n    b = 3\n"),
    ("crlf", b"one\r\ntwo\r\n", "one\r\nTWO\r\n"),
    ("non_utf8", b"caf\xe9\n", "cafe\n"),
    ("empty_to_empty", b"", ""),
    ("unchanged", b"same\n", "same\n"),
    ("form_feed", b"a\x0cb\nc\n", "a\x0cb\nd\n"),
    ("unicode", "héllo\n".encode(), "hallo \U0001f30d\n"),
    ("truncate", b"a\nb\nc\n", ""),
    ("headers_lookalike", b"x\n", "+++y\n--- z\n"),
    (
        "multi_hunk",
        "".join(f"line {i}\n" for i in range(40)).encode(),
        "".join(f"line {i}\n" if i not in (3, 30) else f"LINE {i}\n" for i in range(40)) + "tail\n",
    ),
]

out = []
with tempfile.TemporaryDirectory() as tmp:
    deps = SimpleNamespace(
        resolve_path=lambda p: Path(tmp) / p,
        permissions=SimpleNamespace(check=lambda *a: None),
    )
    ctx = SimpleNamespace(deps=deps, messages=[])
    for name, old, content in CASES:
        path = Path(tmp) / f"{name}.txt"
        if old is not None:
            path.write_bytes(old)
        result = Write(ctx, content, f"{name}.txt")
        meta = dict(result.metadata)
        meta["filepath"] = meta["filepath"].replace(str(path), "{path}")
        meta["diff"] = meta["diff"].replace(str(path), "{path}")
        out.append(
            {
                "name": name,
                "old": None if old is None else base64.b64encode(old).decode(),
                "content": content,
                "result": result.return_value,
                "metadata": meta,
                "written": base64.b64encode(path.read_bytes()).decode(),
            }
        )
json.dump(out, sys.stdout, indent=1, ensure_ascii=True)
print()
