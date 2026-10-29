# minifs

**A filesystem for LLM agents, stored entirely in PostgreSQL.**

An agent works on one file tree with five tools, `Read`, `Write`, `Edit`, `Glob` and `Grep`. They have the same names, parameters and output as the file tools of Claude Code and opencode, so models already know how to use them. Every byte lives in Postgres, so nothing is mounted and nothing is synced. It runs wherever FUSE and root aren't available: sandboxes, serverless containers, managed app platforms such as Databricks Apps.

```
pip install --pre "minifs-pg[pydantic-ai,binary]"
```

The distribution is `minifs-pg`; you import `minifs`. Python 3.12+, PostgreSQL 16 to 18 (tested in CI), no extensions and no superuser, so it runs on managed Postgres such as Databricks Lakebase.

## Highlights

- **Familiar agent tools.** The tools are pydantic-ai tools with the file-tool contracts models are trained on: line-numbered `Read`, whole-file `Write`, string-replacement `Edit`, ripgrep-style `Glob` and `Grep`.
- **ripgrep inside Postgres.** `Grep` takes Rust regex syntax, as rg does. Patterns are parsed with ripgrep's own parser and translated to Postgres regexes that mean the same in every database locale. A `\b` or `\w` never silently matches something else. There's no search index to keep in sync and no `rg` binary.
- **Cheap appends and line reads.** File bytes are stored as line-aligned extents. An append is an INSERT, and `Read` of lines 5000-5100 fetches only those lines, so logs and large files cost what you read.
- **Safe concurrent writes.** Every `Write` and `Edit` is a compare-and-swap against the version it read, so two agents or processes editing one file never silently overwrite each other.
- **One round trip per tool call, mostly.** Path resolution, permission checks and the read happen in one SQL statement. A per-process directory cache, kept fresh by `LISTEN/NOTIFY`, skips the path walk, and every cached entry is checked in that same statement.
- **A chroot per agent.** Each run sees its own `/workspace` plus shared read-only `/docs`, with per-actor path rules. An agent can't reach another workspace, and errors don't reveal what exists outside its view.
- **Watch and claim.** `LISTEN/NOTIFY` change events work like inotify, and an atomic rename gives exactly-once task claims (Maildir style).

## Quickstart

Migrate a database once, seed a workspace, and run an agent on it:

```sh
export MINIFS_DSN=postgresql://user@host/db
minifs db migrate
uv run python examples/provision.py                       # in a checkout: seeds a demo workspace, prints its id
MINIFS_MODEL=anthropic:claude-sonnet-5-5 uv run --with 'pydantic-ai-slim[anthropic]' \
    python examples/agent.py <workspace-id> "test_calc.py fails. Fix the bug."
```

This is the whole agent, [`examples/agent.py`](https://github.com/vdavamse/minifs/blob/main/examples/agent.py) (tested as pasted):

<!-- examples/agent.py -->
```python
import logging
from collections.abc import AsyncIterable
from dataclasses import dataclass

from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import (
    AgentStreamEvent,
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    PartEndEvent,
    TextPart,
    ThinkingPart,
)
from pydantic_ai.models import Model

from minifs.db.pool import Connector
from minifs.tools import Edit, Glob, Grep, Read, Write
from minifs.vfs.cache import DirEntryCache
from minifs.vfs.policy import DEFAULT_POLICY
from minifs.vfs.service import open_vfs
from minifs.vfs.view import VFSView, workspace_mounts


@dataclass
class Deps:
    vfs: VFSView  # all the tools need; vfs_workdir and vfs_settings are optional


# One agent per process, for every run and workspace: the workspace is in the deps.
agent = Agent(deps_type=Deps, tools=[Read, Write, Edit, Glob, Grep], retries=3)


async def show(ctx: RunContext[Deps], events: AsyncIterable[AgentStreamEvent]) -> None:
    """Logs the model's text and each tool call and result, in full, as they happen."""
    async for event in events:
        if isinstance(event, PartEndEvent) and isinstance(event.part, TextPart | ThinkingPart):
            logging.info("%s: %s", event.part.part_kind, event.part.content)
        elif isinstance(event, FunctionToolCallEvent):
            logging.info("call %s(%s)", event.part.tool_name, event.part.args_as_json_str())
        elif isinstance(event, FunctionToolResultEvent):
            logging.info("%s %s:\n%s", event.part.tool_name, event.part.outcome, event.part.model_response_str())


async def main(dsn: str, workspace_id: str, prompt: str, model: Model | str) -> str:
    # On Lakebase: Connector(conninfo, password=fetch_token), called for each new connection.
    cache = DirEntryCache()  # one client and cache per process, shared by every run
    async with open_vfs(Connector(dsn), cache=cache) as vfs:
        view = VFSView(vfs.client, workspace_mounts(workspace_id), DEFAULT_POLICY, "agent")  # one view per run
        result = await agent.run(prompt, deps=Deps(vfs=view), model=model, event_stream_handler=show)
    logging.info("directory cache: %s", cache.stats())
    return result.output
```

`open_vfs` opens the process's connection pools and, with a cache, one `LISTEN` connection that keeps the cache fresh. Each run gets its own `VFSView` over the shared client, so serving many workspaces costs one small object per run. Without `MINIFS_MODEL`, the example runs offline on pydantic-ai's `TestModel`.

## How it works

```
  agent tools            ingestion jobs        minifs CLI
 Read Write Edit         (write /docs)      (ls cat put grep)
    Glob Grep                  |                   |
        \                      |                   |
   VFSView: chroot + per-actor policy              |
               \               |                   /
     VFSClient: one SQL statement per operation, directory cache
                               |
                   PostgreSQL SQL functions
           vfs_nodes · vfs_extents · NOTIFY on channel vfs
```

### The data model, in two tables

```sql
vfs_nodes   (id uuid, parent_id uuid, name text, kind 'file'|'dir',
             size bigint, lines bigint, generation int, mtime timestamptz)
             UNIQUE NULLS NOT DISTINCT (parent_id, name)       -- the directory index

vfs_extents (node_id uuid, generation int, byte_offset bigint,
             line_start bigint, line_offsets int4[], data bytea)
             PRIMARY KEY (node_id, generation, byte_offset)
```

- **A node is an inode and its directory entry,** linked by `parent_id`, so a rename is a one-row update and a path walk is one index probe per component. Foreign keys enforce POSIX rules in the schema: no child under a file (`ENOTDIR`), no `rmdir` of a non-empty directory (`ENOTEMPTY`).
- **A file's bytes are extents** of about 64 KB, and each one starts at a line and knows its first line number (`line_start`) and where its lines begin (`line_offsets`). Line N is one index descent away, and a `Grep` hit has its line number for free.
- **Appending adds an extent; overwriting starts a new `generation`.** Bytes never change in place, so a reader can tell "grew" from "rewritten". Appends to one file take turns under a row lock, so offset order equals commit order, and a tailing reader never misses bytes.

Every operation (lookup, read, append, write, create, rename, unlink, glob, search) is a SQL function. Expected outcomes such as `ENOENT` or `EEXIST` are returned as values, not raised, so a guessed path costs no error log and doesn't abort a transaction.

### The tools

| Tool | What the model calls | Runs as |
| --- | --- | --- |
| `Read` | `Read(filePath, offset?, limit?)`: numbered lines, or a directory listing | one statement reads the page, a binary sniff and nearby `AGENTS.md` / `CLAUDE.md` |
| `Write` | `Write(content, filePath)`: create or replace a file | compare-and-swap write; parents created as needed |
| `Edit` | `Edit(filePath, oldString, newString, replaceAll?)` | read, replace in Python (opencode's fuzzy replacers), compare-and-swap write |
| `Glob` | `Glob(pattern, path?)`: matching files, newest first | a recursive walk over `vfs_nodes`, never touching content |
| `Grep` | `Grep(pattern, path?, include?)`: matching lines by file | scope from `vfs_nodes`, then a scan of the extents in Postgres |

What the model sees:

```
<path>/workspace/calc.py</path>
<type>file</type>
<content>
1: def add(a, b):
2:     return a - b
3: 
4: 
5: def mul(a, b):
6:     return a * b

(End of file - total 6 lines)
</content>
```

```
Found 1 matches
/workspace/calc.py:
  Line 2:     return a - b
```

Mistakes the model can fix (a missing path, an offset past the end, a bad regex) come back as pydantic-ai retries with a hint, such as `File not found` with similar names. A policy denial comes back as the tool's result, `Permission denied: write /docs/guide.md`, since retrying won't help.

### How `Grep` runs in Postgres

1. **Translate.** The pattern is parsed by `regex-syntax` (ripgrep's parser, in a small Rust extension) and emitted as a Postgres regex with every class spelled out as code-point ranges: `\w`, `\b` and `(?i)` mean the same under the `C` locale as under `en_US.utf8`. A construct with no equivalent, such as lookaround or backreferences, is rejected with ripgrep's own error message rather than mistranslated.
2. **Scope.** `Glob` picks the files and their total size from `vfs_nodes` before any content is read. A scope over 10,000 files or 50 MB is refused with a hint to narrow it.
3. **Scan.** Each extent is tested once against the pattern. Only the extents that match are split into lines, and the scan stops once it has enough hits. Every statement runs under a 2 s timeout.

Every write is visible to the next search at once, with no index lag. The translator is tested against Rust's `regex` crate on real and generated patterns, in both locales.

## Workspaces and permissions

`workspace_mounts(workspace_id)` gives an agent `/workspace` (its own `/workspaces/<id>`) and the shared `/docs`. `..` stops at `/`, and another workspace isn't reachable. `DEFAULT_POLICY` has three actors:

| Path | `agent` | `ingestion` | `operator` |
| --- | --- | --- | --- |
| `/workspace/log.md` | read, append | — | read |
| `/workspace/**` | read, write | — | read, write |
| `/docs/**` | read | read, write | read |

Rules are ordered and the first match wins. A rule with no actions hides a path, so `Policy({"agent": [Rule("/workspace/**/.env"), Rule("/workspace/**", "read", "write"), ...]})` makes `.env` files invisible to `Read`, `Glob` and `Grep`. The policy is the tools' only permission gate.

## Configuration

The tools read optional attributes from your deps:

```python
@dataclass
class Deps:
    vfs: VFSView
    vfs_workdir: str = "/workspace"                                             # where relative paths resolve
    vfs_settings: ToolSettings = ToolSettings(grep=GrepSettings(timeout=5.0))  # limits, below
```

| Setting | Default |
| --- | --- |
| `Read` | 2,000 lines, lines cut at 2,000 characters, output capped at 50 KB |
| `Glob`, `Grep` | 100 results; excludes `node_modules`, `.venv`, `__pycache__`, `.mypy_cache` (and always `.git`) |
| `Grep` | 2 s per statement; hit lines cut at 2,000 characters |
| `Edit` | files up to 10 MB; 10 s for the fuzzy matchers to find `oldString` |
| `Write` | diffs old files up to 1 MB (larger ones are replaced without a diff) |

**The directory cache.** Use one `DirEntryCache` per long-running process; leave it out for one-shot scripts. It maps `(parent id, name)` to a node, follows other processes' renames and deletes through `NOTIFY`, and expires entries after 60 s. Every use is checked in the statement that uses it, so a stale entry costs a slower lookup, never the wrong file. `cache.stats()` reports the hit rate.

**Lakebase and other token-auth Postgres.** `Connector(conninfo, password=fetch_token)` calls `fetch_token` (sync or async) for every new connection. `LISTEN` needs a direct connection: behind a transaction-mode pooler, pass `open_vfs(..., listen=Connector(direct_conninfo))`. `min_size=0` lets idle pools hold no connections.

## The library and the CLI

Below the tools, `VFSClient` and `VFSView` are a small async POSIX-like API. Use it for ingestion jobs, harnesses and tests:

```python
await view.create("/workspace/notes/today.md", b"# Today\n", parents=True)
await view.append("/workspace/notes/today.md", b"- started\n")     # returns the offset it landed at
page = await view.pread_lines("/workspace/notes/today.md", first_line=1, max_lines=50)
await vfs.client.claim("/workspaces/w/tasks/new/t1", "/workspaces/w/tasks/cur/t1")   # exactly one claimer wins
async with vfs.watcher.watch("/workspaces/w/tasks/new") as events:              # needs open_vfs(..., watch=True)
    async for event in events: ...                                              # create, modify, move, delete, rescan
```

The `minifs` command works on the same tree, using `$MINIFS_DSN` or the `PG*` variables:

```sh
minifs db migrate                                  # minifs db status lists the migrations
minifs ls -l /workspaces/<id>
minifs cat /docs/guide.md
echo hello | minifs put -p /docs/hello.md          # -a appends
minifs glob '**/*.py' /workspaces/<id>
minifs grep -i 'todo|fixme' /workspaces/<id> -g '*.py'
minifs mv /docs/a.md /docs/b.md && minifs rm /docs/b.md
```

## Limits

- Text first: files from kilobytes to a few megabytes. Images and PDFs can be read as attachments, other binaries are refused by `Read`, and non-UTF-8 files are skipped by `Grep`.
- Not full POSIX: no hard links, permission bits, symlinks or writes at an arbitrary offset (writes replace or append).
- `Grep` has no context lines (`-C`) or multiline mode, and `Glob` doesn't read `.gitignore`. It applies the exclude list above instead.
- No compaction job ships yet. A file appended to one line at a time gains an extent per append. `VFSClient.compact(path)` merges them, and nothing reads differently afterwards.

## More

- [docs/design.md](https://github.com/vdavamse/minifs/blob/main/docs/design.md): the design in depth (storage, reads, caching, glob, search, the regex translator, operations).
- [CONTRIBUTING.md](https://github.com/vdavamse/minifs/blob/main/CONTRIBUTING.md): setup, checks, migrations and the SQL conventions.
- [RELEASING.md](https://github.com/vdavamse/minifs/blob/main/RELEASING.md): cutting a release to PyPI.

MIT licensed.
