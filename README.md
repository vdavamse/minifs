# minifs: Everything Is a File for Agents

A small POSIX-like virtual filesystem stored entirely in PostgreSQL, built for coding assistants and other LLM agents.

## Overview

An agent sees one filesystem tree, stored entirely in Postgres, and works on it with four tools: `Read`, `Write`, `Glob` and `Grep`. `Read` on a directory lists it, so there is no separate `ls`. Source files, notes, logs, working memory and ingested documents are all ordinary files in that tree. Nothing is mounted, so it runs where FUSE and root are unavailable: sandboxes, serverless containers and managed app platforms.

**Goals**

- One storage model and one set of operations for every kind of data. There are no special tables for logs, memory or task queues.
- The tools follow ladderframe's tool contracts (`Read` with line-based `offset` and `limit`, `Glob`, `Grep`), which track opencode's and Claude Code's, so models already use them well.
- Incremental reads: a reader can tail a growing file and get only what is new since its last read, by byte offset in the library and by line number in the tools.
- Search and glob run inside Postgres, so there's no local mirror, no sync and no `rg` binary.
- PostgreSQL is the single source of truth, and every read reflects the latest committed write.
- Low latency: a tool call is one or two SQL round trips.

**Non-goals**

- Kernel mounts (FUSE or NFS). A later TigerFS integration can provide these where they're allowed (see Roadmap).
- Full POSIX: no hard links, permission bits, xattrs or random-offset writes in v1.
- Large binary files. The design targets text (source code, markdown, logs) from kilobytes to a few megabytes per file.

**In one paragraph:** files are inodes in `vfs_nodes`, and their bytes are append-friendly extents in `vfs_extents`. SQL functions implement append, overwrite, positional read, rename, compaction and watch (via NOTIFY). Search scans the current bytes of the files in scope inside Postgres, the way ripgrep scans a disk, so there is no line table or search index to keep in step with writes. A recursive walk over the parent pointers powers `Glob`. A Python VFS library wraps these functions and caches what can't go stale: directory entries, and file bytes keyed by generation. Agent tools with ladderframe's contracts (for example PydanticAI tools) wrap the library and number lines on the client.

## Context

**Why files?** Models are heavily trained on shell and file work. A small, uniform tool set over one tree is easy to discover by reading directories, needs no new tools when a data source is added, and gives humans the same view when debugging. Every agent, ingestion job and human operator follows one UNIX-style model instead of a set of purpose-built tables.

**Why not a real filesystem?** Many places agents run don't allow one. Managed platforms run apps as a non-privileged user with no system packages and no FUSE ([Databricks Apps best practices](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices)), and AWS Fargate allows only `SYS_PTRACE` to be added ([ECS KernelCapabilities](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_KernelCapabilities.html)). Every kernel-mount approach needs `CAP_SYS_ADMIN` or `/dev/fuse`, so the filesystem lives in the tool layer, not the kernel. Postgres also gives durability, transactions, concurrent access from many agents and change notifications for free.

**What we take from TigerFS.** TigerFS exposes PostgreSQL as a mountable filesystem. Its file-first workspaces already use a parent-pointer directory model (ADR-017: `parent_id`, `filename`, `UNIQUE NULLS NOT DISTINCT`, a one-round-trip `resolve_path`, and a 2-second path cache). Its consistency rule is: never cache content, cache only metadata. This design adopts the directory model and refines that rule: content may be cached because bytes never change within a generation, and every read still checks the database (see Caching). It adds extent-based content for appends and offset reads, and replaces kernel mounts with tools.

## Architecture

Any number of clients share one VFS library and one set of SQL functions. Postgres holds every byte, and NOTIFY at commit is the only wake-up signal.

```
  agent tools        ingestion jobs       CLI / humans
 (Read Write          (write docs)        (debugging)
  Glob Grep)
        \                  |                  /
         \                 |                 /
          +-------- VFS library (Python) ---+
                           |
                    SQL functions
                           |
                      PostgreSQL
        vfs_nodes · vfs_extents · NOTIFY vfs
```

| Component | Role |
| --- | --- |
| Agent tools | `Read`, `Write`, `Glob`, `Grep` exposed to the model, with ladderframe's signatures, output formats and errors |
| VFS library | Path resolution, the chroot view, write policy, glob and regex translation, caching, the line map, cursors and watch |
| SQL functions | Append, overwrite, positional read, resolve, readdir, create, rename, unlink, search, compact |
| PostgreSQL | Tables, the NOTIFY trigger, and routine compaction |

## Filesystem tree and conventions

All clients share one tree. A file's purpose comes from where it sits and how it's used, never from how it's stored. An example layout:

```
/workspaces/<workspace_id>/
    src/...              the code or documents the agent works on
    notes.md             agent overwrites: its working memory across runs
    log.md               append-only activity log
    tasks/
        new/             tasks waiting to be picked up      = pending
        cur/             renamed here when claimed          = in progress
        done/            renamed here when finished         = done
/docs/                   ingestion writes; everyone reads
    guides/style.md
    api/reference.md
```

**The agent's view (a chroot).** An agent sees `/workspace` mapped to `/workspaces/<its workspace_id>`, plus shared read-only trees such as `/docs`. It can't reach another workspace's data. The mapping lives in the tool layer, like a bind mount.

**Write policy, per actor.** Instead of permission bits, each actor has a list of path rules, like mount options. For example:

| Path | Agent | Ingestion | Operator |
| --- | --- | --- | --- |
| `/workspace/**` | read, write | — | read, write |
| `/workspace/log.md` | read, append | — | read |
| `/docs/**` | read | write | read |

The most specific rule wins. The agent's tools have no append (`Write` replaces whole files and the policy refuses it on `log.md`), so the harness appends the agent's log entries through `vfs.append`, acting as the agent.

**Coordination through rename (Maildir).** When several agents or workers share work, a file's status can be the directory it sits in. Claiming a file is an atomic rename (see Core operations), so exactly one worker wins.

## Storage model

The model has two tables: `vfs_nodes` holds inodes and directory entries, and `vfs_extents` holds file bytes as variable-length extents at byte offsets. An append is one INSERT, an overwrite is a new generation, and a read is a range scan.

```sql
CREATE TABLE vfs_nodes (                         -- inode + directory entry (no hard links)
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    parent_id   UUID,                            -- NULL for root entries
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('file','dir')),   -- never changes after create
    size        BIGINT NOT NULL DEFAULT 0,       -- exact; maintained under the inode lock; 0 for dirs
    generation  INT    NOT NULL DEFAULT 0,       -- +1 on overwrite/truncate; appends keep it
    ctime       TIMESTAMPTZ NOT NULL DEFAULT now(),
    mtime       TIMESTAMPTZ NOT NULL DEFAULT now(),
    parent_kind TEXT NOT NULL DEFAULT 'dir' CHECK (parent_kind = 'dir'),
    UNIQUE NULLS NOT DISTINCT (parent_id, name),
    UNIQUE (id, kind),                           -- target of the two kind-checking foreign keys
    FOREIGN KEY (parent_id, parent_kind)         -- parent must be a dir -> ENOTDIR
        REFERENCES vfs_nodes (id, kind) ON DELETE RESTRICT   -- rmdir of non-empty dir -> ENOTEMPTY
) WITH (fillfactor = 80);                        -- room for HOT updates of size/mtime on append

CREATE TABLE vfs_extents (                       -- data blocks
    node_id     UUID   NOT NULL,
    generation  INT    NOT NULL,
    byte_offset BIGINT NOT NULL,
    data        BYTEA  NOT NULL,
    node_kind   TEXT   NOT NULL DEFAULT 'file' CHECK (node_kind = 'file'),
    PRIMARY KEY (node_id, generation, byte_offset),
    FOREIGN KEY (node_id, node_kind)             -- only files have extents
        REFERENCES vfs_nodes (id, kind) ON DELETE CASCADE
);
ALTER TABLE vfs_extents ALTER COLUMN data SET STORAGE EXTERNAL;   -- slice reads without decompressing
```

Each column choice, and why:

| Choice | Why | Alternative rejected |
| --- | --- | --- |
| `UNIQUE NULLS NOT DISTINCT (parent_id, name)` | Root entries have `parent_id IS NULL`. A plain `UNIQUE` treats NULLs as distinct, so it would allow two `/docs` entries. Needs PostgreSQL 15+. | Plain `UNIQUE (parent_id, name)` |
| The parent foreign key uses `RESTRICT` | POSIX `rmdir` on a non-empty directory returns `ENOTEMPTY`. `CASCADE` would silently delete a whole subtree. | `ON DELETE CASCADE` |
| Foreign keys on `(id, kind)` | The schema itself rejects a child under a file and an extent on a directory. The constant columns `parent_kind = 'dir'` and `node_kind = 'file'` make that a plain foreign key. A root's `parent_id` is NULL, so the check skips it. | A trigger, or trusting every SQL function to check |
| `fillfactor = 80` on `vfs_nodes` | Every append updates `size` and `mtime`, which aren't indexed. With free space in the page, Postgres makes that a HOT update: no new index entries, and the dead version is pruned in place. | The default 100, where a full page forces a non-HOT update |
| `uuidv7()` ids | Time-ordered: inserts stay local in the B-tree, and the id records creation time. Built in from PostgreSQL 18. On PG 16 or 17, use the pg\_uuidv7 extension where available, or generate the ids in the app. | `gen_random_uuid()` |
| Variable-length extents | An append is one INSERT and never rewrites earlier bytes. A positional read selects only the overlapping extents. | Inline `body TEXT` (each append rewrites the whole value) or fixed-size chunks (each append rewrites the last block) |
| `generation` | Lets readers and cursors tell an append (same generation, larger size) from a rewrite (new generation). | Comparing content hashes |
| `size` stored on the inode | `stat` without scanning extents. It's always updated in the same transaction as the extent, so it can't drift. | Computing `sum(length(data))` on every stat |
| `BYTEA` content | True byte offsets, and safe for binary content. Python decodes UTF-8. | `TEXT` (offsets would be in characters) |
| `STORAGE EXTERNAL` on `data` | TOAST stores values over about 2 KB in roughly 2 KB chunks. Uncompressed, a `substring` fetches only the chunks it covers, so a 16 KB read of a 5 MB extent touches about 8 chunks. | The default `EXTENDED`: text compresses 3 to 4 times, but a slice read may decompress from the start of the value |

**Directory entry and inode in one row.** Without hard links, every inode has exactly one name, so splitting them gains nothing. If hard links are ever needed, split this into `inodes(id, size, generation, nlink, ...)` and `dentries(parent_id, name, inode_id)`.

**Files and directories.** As in ext4, both kinds share one inode format, and `kind` plays the role of the file-type bits in `i_mode`. They differ in what holds their contents.

| | File | Directory |
| --- | --- | --- |
| Contents | Extents in `vfs_extents`, keyed by its id | Its children: the `vfs_nodes` rows whose `parent_id` is its id. ext4 stores the entries in the directory's data blocks (linear or HTree); here each entry is stored on the child, and the `(parent_id, name)` index is the directory index. |
| `size` | Exact byte count | 0 (ext4 reports the size of its entry blocks, which means nothing here) |
| `generation` | Bumped by overwrite | Unused |
| `mtime` | Set by append and overwrite | Set at creation only (see below) |
| Allowed operations | `vfs_append`, `vfs_write`, `vfs_pread` (`EISDIR` on a directory) | `vfs_readdir`, being a parent (`ENOTDIR` on a file), `rmdir` only when empty |
| Enforced by | The `(node_id, node_kind)` foreign key: no extents on a directory | The `(parent_id, parent_kind)` foreign key: no children under a file |

- **Kind never changes.** A file can't become a directory in place; replacing one with the other is unlink plus create, as in POSIX.
- **Resolution returns the kind.** `vfs_resolve` returns `(id, kind)`, so the library reports `ENOTDIR` and `EISDIR` before calling an operation, and the directory cache stores the kind with the id. The foreign keys are the backstop.
- **Directory `mtime` isn't maintained.** POSIX updates it on every create, unlink and rename inside the directory. Here that would lock and update the parent row on every create: a hot spot for shared directories such as `tasks/new/`, and an extra NOTIFY each time. Watchers get those events from NOTIFY by parent id instead.

**Inode numbers and handles.** If a protocol needs a 64-bit inode number, take the low 64 bits of the UUID (the random part in v7). A stateful protocol can use the 16-byte UUID itself as the file handle. Unlike TigerFS's path-encoded NFS handles, it survives renames.

**Moves must not create cycles.** `rename` of a directory checks that the destination isn't inside the source. A recursive CTE walks up from the destination's parent within the same transaction, so a move like `mv /a /a/b/c` returns `EINVAL`.

**Compaction.** Extent size isn't a tuning knob: an extent is exactly as big as the write that made it, and TOAST already splits large values into chunks. What grows is the extent count: a log appended one line at a time gains one extent per append. `vfs_compact(node)` merges a file's extents into pieces of about 64 KB under the inode lock.

- It changes none of the bytes, `size`, `generation` or `mtime`, so readers, cursors and caches never notice. It doesn't update `vfs_nodes`, so it emits no NOTIFY.
- It runs as routine maintenance: a periodic job compacts files with more than 1,000 extents. `vfs_append` can also compact opportunistically when a file crosses that threshold.
- The target size is forgiving. Anything from 16 KB to 1 MB behaves about the same, so it is set once, not tuned.

## Core operations

Every operation is a SQL function, so every client (agent tools, ingestion, a future TigerFS front end) gets the same semantics from the database. The Python library is a thin wrapper.

| Operation | Function | POSIX analogue | Locking |
| --- | --- | --- | --- |
| Append | `vfs_append(node, data) -> offset` | `write` with `O_APPEND` | Inode `FOR UPDATE` until commit |
| Overwrite | `vfs_write(node, data, expect_gen, expect_size, expect_parent, expect_name) -> (generation, size)` | `open` with `O_TRUNC`, then `write` | Inode `FOR UPDATE`; optional compare-and-swap |
| Positional read | `vfs_pread(node, offset, limit)` | `pread` | None (MVCC snapshot) |
| Resolve path | `vfs_resolve(path) -> id` | `namei` | None |
| List directory | `vfs_readdir(dir)` | `readdir` | None |
| Create, mkdir | `vfs_create(parent, name, kind, data DEFAULT NULL) -> (id, generation, size)` | `open` with `O_CREAT\|O_EXCL`, `mkdir` | Unique constraint |
| Rename, move | `vfs_rename(node, new_parent, new_name, expect_parent)` | `rename` | Row lock; checks for cycles |
| Unlink, rmdir | `vfs_unlink(node)` | `unlink`, `rmdir` | Child check plus FK |
| Watch | `LISTEN vfs` | `inotify` | — |

### Append

```sql
CREATE FUNCTION vfs_append(p_node UUID, p_data BYTEA) RETURNS BIGINT AS $$
DECLARE v_size BIGINT; v_gen INT;
BEGIN
    SELECT size, generation INTO v_size, v_gen
      FROM vfs_nodes WHERE id = p_node AND kind = 'file' FOR UPDATE;
    IF NOT FOUND THEN PERFORM vfs_raise('ENOENT', p_node::text); END IF;   -- or EISDIR
    IF length(p_data) = 0 THEN RETURN v_size; END IF;   -- extents are never empty
    INSERT INTO vfs_extents VALUES (p_node, v_gen, v_size, p_data);
    UPDATE vfs_nodes SET size = v_size + length(p_data),
                         mtime = greatest(mtime, clock_timestamp())   -- never backwards
     WHERE id = p_node;
    RETURN v_size;                       -- the offset the data landed at
END $$ LANGUAGE plpgsql;
```

The inode lock is held until commit, so concurrent appenders take turns and **offset order always equals commit order**. A reader at `offset > cursor` can never miss bytes that commit late at a lower offset. This is the guarantee that makes incremental reads correct. Because it comes from the lock, it holds for any number of writers.

### Overwrite

```sql
CREATE FUNCTION vfs_write(p_node UUID, p_data BYTEA,
                          p_expect_gen INT DEFAULT NULL, p_expect_size BIGINT DEFAULT NULL,
                          p_expect_parent UUID DEFAULT NULL, p_expect_name TEXT DEFAULT NULL)
RETURNS TABLE (generation INT, size BIGINT) AS $$
DECLARE v_gen INT; v_size BIGINT; v_parent UUID; v_name TEXT;
BEGIN
    SELECT n.generation, n.size, n.parent_id, n.name INTO v_gen, v_size, v_parent, v_name
      FROM vfs_nodes n WHERE n.id = p_node AND n.kind = 'file' FOR UPDATE;
    IF NOT FOUND THEN PERFORM vfs_raise('ENOENT', p_node::text); END IF;   -- or EISDIR
    IF num_nulls(p_expect_gen, p_expect_size, p_expect_name) NOT IN (0, 3)
       OR (p_expect_gen IS NULL AND p_expect_parent IS NOT NULL) THEN
        PERFORM vfs_raise('EINVAL', 'partial expectation');   -- a compare-and-swap is all or nothing
    END IF;
    IF p_expect_gen IS NOT NULL                     -- compare-and-swap for the Write tool
       AND ((v_gen, v_size, v_name) IS DISTINCT FROM (p_expect_gen, p_expect_size, p_expect_name)
            OR v_parent IS DISTINCT FROM p_expect_parent) THEN
        PERFORM vfs_raise('ESTALE', p_node::text);  -- SQLSTATE MF116
    END IF;
    DELETE FROM vfs_extents WHERE node_id = p_node;
    IF length(p_data) > 0 THEN
        INSERT INTO vfs_extents VALUES (p_node, v_gen + 1, 0, p_data);
    END IF;
    UPDATE vfs_nodes SET generation = v_gen + 1, size = length(p_data),
                         mtime = greatest(mtime, clock_timestamp())
     WHERE id = p_node;
    RETURN QUERY SELECT v_gen + 1, length(p_data)::BIGINT;
END $$ LANGUAGE plpgsql;
```

### Rename as an exactly-once claim

A rename is a single-row update. When it also states the expected current parent, it works as a compare-and-swap:

```sql
UPDATE vfs_nodes SET parent_id = $cur_dir, mtime = now()
 WHERE id = $file AND parent_id = $new_dir;     -- 0 rows = already claimed by someone else
```

This is how a worker claims a file from `tasks/new/` to `tasks/cur/`. If two workers race, exactly one wins.

### Path resolution

`vfs_resolve(segments TEXT[]) -> (id, kind)` walks parent pointers on the server in one round trip, the same idea as TigerFS's `resolve_path`. The library caches `(parent_id, name) -> id` and evicts entries when NOTIFY reports a move or delete (see Caching).

### Watch (inotify)

A trigger on `vfs_nodes` (migration `005_vfs_notify`) sends one NOTIFY on channel `vfs` per changed row, with a small JSON payload. Every event has `op`, `id` and `parent` (the node's directory after the change, `null` for the root):

| `op` | Extra fields | Emitted by |
| --- | --- | --- |
| `create` | `name`, `kind` | create, mkdir |
| `modify` | `size`, `gen` | append, overwrite |
| `move` | `name`, `old_parent`, `old_name`, `kind` | rename, a Maildir claim |
| `delete` | `name`, `kind` | unlink, rmdir |

- An append updates `size`, so it emits `modify`. A Maildir-style claim (`tasks/new` → `tasks/cur`) emits one `move`, which watchers of both directories see through `parent` and `old_parent`.
- Notifications are delivered **at commit**, so a watcher is never woken for data it can't read yet, and a rolled-back write emits nothing.
- An `UPDATE` that changes no column emits nothing, and compaction doesn't touch `vfs_nodes`, so it emits nothing either. `TRUNCATE` emits one `{"op": "rescan"}`.
- Events are hints. PostgreSQL drops a payload identical to one already sent in the same transaction, and any role that can connect can NOTIFY on `vfs`, so a watcher reads the database before it acts (a Maildir claim already does, through `vfs_rename`'s compare-and-swap).

In Python, `minifs.vfs.watch.Watcher` holds the process's one LISTEN connection, which it gets from a connection factory (so each reconnect has fresh credentials):

```python
async with Watcher(connect) as watcher:
    async with watcher.watch("tasks/new") as events:
        async for event in events:      # Event(op, id, parent, name, kind, size, generation, old_parent, old_name)
            if event.op == "rescan":
                ...                     # events may have been lost: re-check the directory
```

- `watch(path)` filters the single channel by node id (a file) or by parent id (a directory, plus the directory's own events). It subscribes before resolving the path, so nothing committed after it returns is missed; a change committed just before may be delivered too.
- Like inotify, a watch follows the node the path named when it started: through renames, until the node is deleted. The `delete` is its last event, and the watch ends. To follow whatever a path names now, watch it again.
- When the connection drops, the watcher reconnects with jittered backoff, listens again, and only then sends every subscriber a `rescan` event. A watch then checks that its node still exists, and ends after the `rescan` if it doesn't. A subscriber that falls more than `maxsize` events behind, or whose predicate raises, also gets a `rescan`.
- Every `keepalive` seconds (30 by default) the watcher probes the connection with `SELECT 1`, so a connection that died silently is noticed within about `keepalive + probe_timeout` plus psycopg's 10-second cancel wait.
- A watch's own queries (resolving the path, the check after a `rescan`) go through `pool` if one is given, else through a short-lived connection from the factory.
- `watcher.subscribe(predicate)` is the raw feed. The library's directory cache uses it on the same connection (see Caching).

## Read contract

Reads come in two layers. The **library** addresses every file by byte offset, so a source file, a 200 KB doc, `notes.md` and a growing log all page and tail identically. The **tool** is ladderframe's line-numbered `Read`, and derives line numbers from the bytes through the client line map (see Caching).

### Library contract: byte offsets

```
vfs.pread(path, offset, limit=16_000) -> {content, offset, next_offset, size, generation, eof}
```

| Input | Meaning | UNIX equivalent |
| --- | --- | --- |
| `offset = N`, N ≥ 0 | Start at byte N | `pread(fd, buf, limit, N)` |
| `offset = -N` | Start N bytes before the end | `tail -c N` |
| `offset > size` | Empty content, `eof=true`, not an error | `pread` returning 0 |
| `limit` | Maximum bytes returned | The `count` argument of `read(2)` |

Library reads return exact bytes, never adjusted to line or character boundaries. Ingestion, other services, harnesses and tests use them.

### One snapshot for the bytes, size and generation

```sql
CREATE FUNCTION vfs_pread(p_node UUID, p_offset BIGINT, p_limit INT)
RETURNS TABLE (size BIGINT, generation INT, byte_offset BIGINT, data BYTEA) AS $$
    WITH n AS (
        SELECT id, size, generation,
               CASE WHEN p_offset < 0 THEN greatest(size + p_offset, 0)
                    ELSE least(p_offset, size) END AS off   -- clamped, so off + p_limit can't overflow
          FROM vfs_nodes WHERE id = p_node AND kind = 'file'
    ), s AS (                                       -- step 1: the extent that contains off
        SELECT n.*, coalesce((SELECT e.byte_offset FROM vfs_extents e
                               WHERE e.node_id = n.id AND e.generation = n.generation
                                 AND e.byte_offset <= n.off
                               ORDER BY e.byte_offset DESC LIMIT 1), 0) AS start
          FROM n
    )
    SELECT s.size, s.generation,
           greatest(e.byte_offset, s.off),
           substring(e.data                         -- substring(bytea) takes int, not bigint
                     FROM (greatest(s.off - e.byte_offset, 0) + 1)::int
                     FOR  (least(e.byte_offset + length(e.data), s.off + p_limit)
                         - greatest(e.byte_offset, s.off))::int)
      FROM s
      LEFT JOIN vfs_extents e                       -- step 2: range scan forward from there
             ON e.node_id = s.id AND e.generation = s.generation
            AND e.byte_offset >= s.start
            AND e.byte_offset <  s.off + p_limit
            AND e.byte_offset + length(e.data) > s.off
            AND p_limit > 0
     ORDER BY e.byte_offset
$$ LANGUAGE sql STABLE;
```

- **Two index steps, like walking ext4's extent tree.** Only `byte_offset < off + limit` can bound a scan of the primary key; the overlap test `byte_offset + length(data) > off` can't. A single range scan would therefore read every extent from the start of the file. Step 1 descends backward to the extent that contains `off`, and step 2 scans forward from it, so a read costs O(log E + extents returned) for a file with E extents. On a 20,000-extent file, a read at the end touches 3 index pages.
- **Trimming happens in Postgres.** A 16 KB read of a 5 MB doc sends 16 KB. `length(data)` reads the size from the TOAST pointer without fetching the value.
- **It's one statement, so one MVCC snapshot.** `size` and `generation` describe exactly the bytes returned, even while another client is appending.
- **The `LEFT JOIN`** returns a single row with NULL data when nothing overlaps, so a read past EOF or of an empty file still returns `size` and `generation`, and `byte_offset` is the resolved start offset.
- **The real function** (migration 004, like the real `vfs_append` and `vfs_write`) is this query inside a PL/pgSQL `STABLE` function, still one snapshot, which raises `ENOENT` or `EISDIR` when the query returns no row.

### Per-path cursors and generations

A library caller that tails files (a harness, an ingestion job, a watcher) keeps a `Cursor`, `path -> (generation, next_offset)`, and can save it to a `vfs_cursors` table for crash recovery. `vfs.read_from(cursor)` continues where the cursor left off:

| Since the last read | `generation` | What `vfs.read_from(cursor)` does |
| --- | --- | --- |
| Bytes appended | Unchanged | Returns only the new bytes |
| Nothing changed | Unchanged | Empty content, `eof=true` |
| File overwritten (a doc re-ingested, `notes.md` rewritten) | Changed | Restarts at 0 and flags the result as rewritten |

### Tool contract: numbered lines

```
Read(filePath, offset=None, limit=None) -> str
```

The tool takes ladderframe's `Read` parameters and returns its output format.

| Input | Meaning |
| --- | --- |
| `offset = N`, N ≥ 1 (default 1) | Start at line N (1-indexed). `offset=0` means 1 |
| A negative `offset` or `limit` | `ModelRetry: offset and limit must be non-negative integers` |
| `limit = K` (default 2000) | At most K lines |
| `offset` past the last line | `ModelRetry`: `Offset 120 is out of range for this file (80 lines)` |
| `filePath` is a directory | Its entries, hidden ones included, one per line and with `/` after subdirectories, inside an `<entries>` block after `<type>directory</type>`, with the footer last inside the block. Sorted case-insensitively. `offset` and `limit` page through the entries; the footer is `(N entries)` or `(Showing X of Y entries. Use 'offset' parameter to read beyond entry Z)` |

```
<path>/workspace/src/config.py</path>
<type>file</type>
<content>
17: def parse_config(path: str) -> Config:
18:     with open(path) as f:

(Showing lines 17-18 of 240. Use offset=19 to continue.)
</content>
```

- **Lines come from the line map.** The map turns line N into a byte offset, then `vfs_pread` reads forward through the content cache until K lines or the byte cap. Lines split on `\n` only and drop a trailing `\r`, so line numbers agree with `Grep` hits.
- **Bounded output, as in ladderframe.** A line longer than 2,000 characters is cut and gets `... (line truncated to 2000 chars)`, and output stops at 50 KB with `(Output capped at 50 KB. Showing lines a-b. Use offset=b+1 to continue.)`. The last page ends with `(End of file - total n lines)`.
- **Text only.** Bytes are decoded as UTF-8 with replacement characters. A file that looks binary (PNG, JPEG, GIF, WEBP or `%PDF-` magic bytes, a `.png`, `.jpg`, `.jpeg`, `.gif`, `.webp` or `.pdf` extension, which ladderframe would attach, or another known binary extension, a NUL byte in the first 4 KB, or more than 30% non-printable bytes there) gets `ModelRetry: Cannot read binary file: <path>`. ladderframe's `Read` attaches images and PDFs; minifs's `Read` doesn't, so they get the same binary-file error (see Differences from ladderframe).
- **Nearby instructions.** As in ladderframe: for a file below `/workspace`, each directory from the file's parent up to, but not including, `/workspace` is checked for `AGENTS.md`, then `CLAUDE.md`, taking the first that exists, with no fallback. That file is then omitted if it's the file being read, was already shown in this conversation, or is blank. Each remaining instruction file is appended, headed `Instructions from: <path>`, in one `<system-reminder>` block. Files outside `/workspace`, such as `/docs/**`, get none. The ancestor ids are already known from path resolution, so one query finds every candidate (`WHERE parent_id = ANY($ancestors) AND kind = 'file' AND name IN ('AGENTS.md', 'CLAUDE.md')`, returning each candidate's `vfs_text`), sent in the same round trip as the read. The client applies the per-directory choice and the omissions above.
- **Stateless.** There is no `offset=None` auto-continue and no per-agent cursor in the tool. To tail a log, the agent calls `Read` again with the `offset` its last footer gave. Generation-aware tailing is a library feature.

### Line numbers in the tools, byte offsets in the library

- `Grep` hits carry line numbers (`Line 42: ...`), so the agent follows up with `Read(filePath, offset=32, limit=40)`.
- Library search results also carry each hit's byte offset, in the same currency as `vfs.pread`. A cursor is just a saved `next_offset`.

## Caching

PostgreSQL stays the single source of truth: every read through a cache still reflects the latest committed write. Two properties make that cheap.

- **File bytes never change in place.** An append adds bytes after the end, an overwrite starts a new generation, and compaction changes neither. Bytes cached under `(node_id, generation)` are valid forever. The only question is whether the file has grown or been rewritten.
- **Directory entries are keyed by parent id, not by path.** Renaming `/a` to `/b` changes only the `(root, "a")` entry. Every entry below it is keyed by its parent's id, which didn't change, so it stays valid.

| Cache | Key → value | Kept fresh by | Used by |
| --- | --- | --- | --- |
| Directory entries | `(parent_id, name) -> (id, kind)` | NOTIFY `move` and `delete` evict; a 2 s TTL as a fallback | Every path resolution |
| File content | `(node_id, generation) -> bytes [0, cached_size)` | The read itself (below) | `Read`, `vfs.pread`, the line map |
| Line map | `(node_id, generation) ->` byte offset of every 1,000th line start, and the line count | Extended with the content | `Read` and its line numbers |

The caches live in each process's memory and are never shared between processes. Search and glob results aren't cached, because they depend on the whole tree. Saved cursors aren't a cache: they're per-caller state (see Per-path cursors and generations).

### Directory entries

- Only positive entries are cached. A missing name is never cached, so a file another client creates is visible at once.
- The cache also keeps `id -> (parent_id, name)`. A `move` or `delete` notification carries the node id, so it evicts the right entry without knowing the old name.
- The TTL only covers notifications lost while the LISTEN connection reconnects. After a reconnect, the whole cache is cleared.

### File content

A cached read is one statement, the same as an uncached one:

1. Call `vfs_pread(node, cached_size, limit)`. It returns the file's current `size` and `generation`, along with any bytes past the cached end.
2. If the generation is unchanged, add the returned bytes (often none) to the entry and serve the request from memory.
3. If the generation changed, drop the entry and read again from offset 0.

The saving is bytes on the wire and the work of re-splitting lines, not round trips. Tailing a growing log fetches only the new tail on each read. Entries are evicted least-recently-used under a per-process byte budget. Files above a size cap keep only their line map, and their bytes are read by range as before.

### Line map

Line numbers are derived on the client and never stored in Postgres.

- The map holds the byte offset where every 1,000th line starts, plus the total line count. Reading lines N to N+k jumps to the nearest checkpoint at or before N, reads forward from that byte offset and splits on `\n`.
- It is built from the content on first use and extended on append by scanning only the new bytes. A cold start on a large file reads that file once.
- Lines split on `\n` only, as in ripgrep and ladderframe's `Read`, so line numbers agree with `Grep` hits.

## Glob

`Glob` and `Grep` share one walker and one glob matcher, as they do inside ripgrep (whose matcher is the `globset` crate). The walker is `vfs_glob`, a recursive CTE over the parent pointers that reads only `vfs_nodes`, never content.

### Semantics

| Pattern | Matches | Translated regex |
| --- | --- | --- |
| `*` | Any characters within one path segment | `[^/]*` |
| `?` | One character other than `/` | `[^/]` |
| `**/` | Zero or more directories | `(?:[^/]+/)*` |
| `**` at the end | Everything below | `.*` |
| `[abc]`, `[a-z]`, `[!x]` | A character class (`!` negates) | `[abc]`, `[a-z]`, `[^x]` |
| `{md,txt}` | Alternatives (may nest) | `(?:md\|txt)` |
| `\*` | A literal `*` | `\*` |
| A pattern with no `/`, e.g. `*.md` | The file name at any depth (the gitignore/rg rule) | `^(?:.*/)?[^/]*\.md$` |
| A pattern with `/`, e.g. `src/*.py` | Anchored at the search root | `^src/[^/]*\.py$` |
| `exclude=["**/node_modules/**"]` (library only) | Like rg's `-g '!...'` | `AND NOT path ~ ANY(...)` |

The defaults follow rg. Only files are returned (directories are walked, not listed). For `Glob`, hidden directories (names starting with `.`) are never descended into, and a hidden file is returned whenever the pattern matches it, as `rg --files` does: `*.yml` finds `.foo.yml`, but `.github/**` finds nothing. `Grep` searches hidden files and directories, as ladderframe's `Grep` runs `rg --hidden`. Both skip `.git/`. `Glob` results are sorted by `mtime`, newest first (see Differences from ladderframe).

### Translator

```python
ARE_SPECIAL = set(r".^$|()[]{}*+?\\")

def glob_to_regex(pat: str) -> str:
    """Translate a ripgrep/gitignore-style glob to an anchored Postgres ARE."""
    body = _translate(pat)
    return f"^(?:.*/)?{body}$" if "/" not in pat else f"^{body}$"

def _translate(p: str) -> str:
    out, i = [], 0
    while i < len(p):
        if p.startswith("**/", i):
            out.append("(?:[^/]+/)*"); i += 3
        elif p.startswith("**", i):
            out.append(".*"); i += 2
        elif p[i] == "*":
            out.append("[^/]*"); i += 1
        elif p[i] == "?":
            out.append("[^/]"); i += 1
        elif p[i] == "[":
            j = p.find("]", i + 2)
            if j < 0:
                raise GlobError(p, "unclosed '['")
            cls = p[i + 1:j]
            out.append("[^" + cls[1:] + "]" if cls.startswith("!") else "[" + cls + "]"); i = j + 1
        elif p[i] == "{":
            j = _matching_brace(p, i)                    # depth-aware
            alts = _split_top_level(p[i + 1:j], ",")
            out.append("(?:" + "|".join(_translate(a) for a in alts) + ")"); i = j + 1
        elif p[i] == "\\" and i + 1 < len(p):
            out.append("\\" + p[i + 1] if p[i + 1] in ARE_SPECIAL else p[i + 1]); i += 2
        else:
            out.append("\\" + p[i] if p[i] in ARE_SPECIAL else p[i]); i += 1
    return "".join(out)
```

Only the special characters are escaped. In Postgres ARE, a backslash before a letter is a meaningful escape (`\y`, `\m`), so letters must stay bare.

### Pruning hints

| Hint | Example | Effect |
| --- | --- | --- |
| Literal prefix | `docs/guides/**/*.md` gives `docs/guides` | Resolve that directory once and walk only its subtree. If it doesn't exist, return an empty result, not an error. A prefix through a hidden directory (for `Glob`) or an excluded one also gives an empty result, as in rg. The walk's paths are seeded with the prefix (`p_prefix`), so the pattern and excludes still match paths relative to the tool's `path`, and results don't depend on the hint. |
| Max depth | `src/*/*.py` gives 3 | Stop the recursion at that depth. Only when the pattern has no `**`. |
| Name filter | Last segment `*.md` gives `LIKE '%.md'` | A cheap filter on the name before the full regex. Only when the last segment has no `[` or `{`. |

### The SQL

```sql
CREATE FUNCTION vfs_glob(
    p_root UUID, p_regex TEXT, p_name_like TEXT, p_max_depth INT,
    p_excludes TEXT[], p_hidden BOOLEAN, p_limit INT,
    p_prefix TEXT DEFAULT NULL, p_prune_dirs TEXT[] DEFAULT '{.git}'
) RETURNS TABLE (id UUID, path TEXT, size BIGINT, mtime TIMESTAMPTZ) AS $$
    WITH RECURSIVE t(id, path, kind, size, mtime, depth) AS (
        SELECT id, concat_ws('/', p_prefix, name), kind, size, mtime, 1   -- paths stay relative to the tool's path
          FROM vfs_nodes
         WHERE parent_id IS NOT DISTINCT FROM p_root
           AND (p_hidden OR kind = 'file' OR name NOT LIKE '.%')   -- rg: hidden dirs aren't walked
           AND NOT (kind = 'dir' AND name = ANY (p_prune_dirs))     -- excluded dirs aren't walked
        UNION ALL
        SELECT c.id, t.path || '/' || c.name, c.kind, c.size, c.mtime, t.depth + 1
          FROM vfs_nodes c JOIN t ON c.parent_id = t.id
         WHERE t.kind = 'dir'
           AND (p_max_depth IS NULL OR t.depth < p_max_depth)
           AND (p_hidden OR c.kind = 'file' OR c.name NOT LIKE '.%')
           AND NOT (c.kind = 'dir' AND c.name = ANY (p_prune_dirs))
    )
    SELECT id, path, size, mtime
      FROM t
     WHERE kind = 'file'
       AND (p_name_like IS NULL OR path LIKE '%/' || p_name_like OR path LIKE p_name_like)
       AND path ~ p_regex
       AND NOT (path ~ ANY (p_excludes))
     ORDER BY mtime DESC, path
     LIMIT p_limit
$$ LANGUAGE sql STABLE;
```

Excludes of the form `**/<name>/**`, which covers all the defaults, are also passed as `p_prune_dirs`, so those directories are never walked; `.git` is always among them. Other exclude patterns are tested on each path at the end. Each directory level is an index lookup on `UNIQUE (parent_id, name)`. A few thousand nodes take milliseconds. For trees of 100k+ nodes, the literal-prefix and depth hints keep the walk small, and an optional trigram index on `vfs_nodes.name` (`pg_trgm`) speeds up `*foo*` patterns.

## Search (rg in Postgres)

`Grep` runs entirely in Postgres and scans the current bytes of the files in scope, the way ripgrep scans a disk. Nothing is derived at write time, so an append stays one INSERT, there's no index to keep in step, and a search sees every committed write, even one from a millisecond ago. There's no local mirror, no sync and no `rg` binary.

### Why there's no line index

An earlier draft kept every line in a `vfs_lines` table with a trigram index. It paid for that on every write: storage of 2 to 4 times the text, a trigger on each append, a full re-index on each overwrite, and a rule that writers never split a UTF-8 character across appends. The only gain was index lookups, which matter once the bytes in scope get large. The target is workspaces of files from kilobytes to a few megabytes, where a scan is enough. If measurements say otherwise, an index can be added without changing the storage model (see Open decisions).

### Lines exist only inside the query

ripgrep matches line by line, and models write patterns that assume it: `^def `, `foo$`, `.*`. The search keeps those semantics without storing lines.

1. **Scope.** `vfs_glob` picks the files (see Glob) and reads their sizes from `vfs_nodes`, before any content is touched.
2. **File filter.** Each file's current bytes are joined in offset order, decoded as UTF-8 and tested once against the pattern in newline-sensitive mode (`(?n)`). In that mode `^` and `$` match at line breaks, and `.` and `[^...]` don't cross them. Every file with a matching line passes, plus the occasional file without one (for example when `\s` matches a newline).
3. **Line check.** Only the files that pass are split on `\n`, and each line is tested on its own. This step is exact and decides the results.

```sql
CREATE FUNCTION vfs_text(p_node UUID) RETURNS TEXT AS $$
BEGIN
    RETURN (SELECT convert_from(string_agg(e.data, ''::bytea ORDER BY e.byte_offset), 'UTF8')
              FROM vfs_nodes n
              JOIN vfs_extents e ON e.node_id = n.id AND e.generation = n.generation
             WHERE n.id = p_node);
EXCEPTION WHEN character_not_in_repertoire THEN
    RETURN NULL;                                    -- binary content: readable, not searchable
END $$ LANGUAGE plpgsql STABLE;
```

The bytes are decoded only after they're joined, so a UTF-8 character split across two appends decodes correctly, and writers need no rule about where an append ends.

### The search query

```sql
-- in one REPEATABLE READ transaction, so both steps see one snapshot:
SET LOCAL statement_timeout = '2s';

-- step 1: the scope, from vfs_nodes only; the client refuses it if it's over either cap
SELECT count(*) AS files, coalesce(sum(size), 0) AS bytes,
       array_agg(id) AS ids, array_agg(path) AS paths
  FROM vfs_glob($root, $glob_regex, $name_like, $depth, $excludes, true, $file_cap + 1, $prefix, $prune_dirs);  -- hidden included

WITH files AS (
    SELECT g.id, g.path, vfs_text(g.id) AS body
      FROM unnest($ids::uuid[], $paths::text[]) AS g(id, path)
), hits AS (                                        -- step 2: one regex test per file
    SELECT id, path,
           CASE WHEN right(body, 1) = E'\n' THEN left(body, -1) ELSE body END AS body
      FROM files
     WHERE body ~ $file_pattern                     -- '(?n)' plus the translated pattern
), lines AS (                                       -- step 3: lines exist only here
    SELECT h.path, l.line_no, l.text,
           coalesce(sum(octet_length(l.text) + 1) OVER (
               PARTITION BY h.id ORDER BY l.line_no
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS byte_offset
      FROM hits h,
           regexp_split_to_table(h.body, E'\n') WITH ORDINALITY AS l(text, line_no)
)
SELECT path, line_no, byte_offset, text
  FROM lines
 WHERE text ~ $line_pattern                         -- always ~: case folding is in the pattern
 ORDER BY path, line_no
 LIMIT $max_plus_one;
```

- **Trailing newline.** The final `\n` is dropped before splitting, so a file ending in a newline doesn't gain an empty phantom line that `^$` would match.
- **Byte offsets.** Each hit's offset is the running byte length of the lines before it, in the same currency as `vfs.pread(offset=...)`. The `Grep` tool shows only line numbers.
- **One snapshot.** The two statements run in one `REPEATABLE READ` transaction and `vfs_text` is `STABLE`, so the scope and every file are read as of the same moment.
- **Caps before content.** Step 1 reads only `vfs_nodes`. The client checks the file and byte caps on its result, so an oversized scope is refused before any extent is read. The search costs two round trips.

The `Grep` tool takes only ladderframe's parameters: `pattern`, `path` (a directory, or a single file, which searches just that file) and `include`. The other rg features are options of the library's `vfs.search`; a model gets case-insensitive search by writing `(?i)` in the pattern.

| rg feature | Implementation |
| --- | --- |
| `-i` | The translator case-folds the pattern itself (`[Ee]`), so both steps still use `~`. ARE's `~*` would depend on the database's locale (see Regex dialect). |
| `-F` (fixed strings) | `strpos(body, $s) > 0` for the file filter and `strpos(text, $s) > 0` for lines. With `-i` too, the escaped literal goes through the translator instead. |
| `-C n` (context) | Keep the lines `line_no BETWEEN hit - n AND hit + n` from the same split of the matching files |
| `-g` / `--glob` (the tool's `include`) | Through the `vfs_glob` scope |
| `--max-count` | `row_number() OVER (PARTITION BY path)` plus a filter |
| Byte offset per hit | The running sum above, which `vfs.pread(offset=...)` accepts directly |
| `-U` (multiline) | Not supported in v1. A literal `\n` in the pattern is rejected, as rg does without `-U`. |

### Cost and limits

- Work grows with the bytes in scope, not with the number of matches. The file filter costs one regex pass per file, and only matching files are split into lines.
- The total size of the scope comes from `vfs_nodes` before any content is read. A search over more than a configured cap (for example 50 MB) is refused with `ModelRetry: Search scope is 120 MB; narrow the path or include pattern`.
- Every search runs under `SET LOCAL statement_timeout`.
- Files that aren't valid UTF-8 are skipped. They're still readable with `vfs.pread`, and with `Read` unless they look binary.

### Output format for the agent

ladderframe's `Grep` format: a count, then hits grouped by file, at most 100 by default.

```
Found 100 matches (more matches available)
/docs/guides/style.md:
  Line 42: Prefer early returns over nested conditionals.

/workspace/src/config.py:
  Line 17: def parse_config(path: str) -> Config:
  ...

(Results truncated. Consider using a more specific path or pattern.)
```

The example is a `Grep` with `path="/"`; the default path is `/workspace`. A search with no hits returns `No files found`. A trailing `\r` is dropped from each hit line, as in ladderframe, and lines longer than 2,000 characters are cut, with `...` appended.

`Glob` output is ladderframe's too: one absolute path per line, `No files found` when nothing matches, and `(Results are truncated: showing first 100 results. Consider using a more specific path or pattern.)` past the limit.

## Regex dialect and translation

`Grep` patterns are Rust regexes, as in rg, and Postgres speaks ARE (Advanced Regular Expressions). We parse each pattern with [`regex-syntax`](https://docs.rs/regex-syntax), the parser behind Rust's `regex` crate and therefore behind ripgrep, and emit ARE from its output. No open-source translator in that direction exists, and the extensions that bring PCRE or RE2 into Postgres aren't available on most managed services. The translator is the riskiest part of search: when it gets something wrong, it fails silently.

### Why it's needed: patterns that silently change meaning

Postgres documents these incompatibilities with Perl: "\\b, \\B, the lack of special treatment for a trailing newline, ... the restrictions on parentheses and back references in lookahead/lookbehind constraints, and the longest/shortest-match (rather than first-match) matching semantics" ([PostgreSQL 9.7.3.6](https://www.postgresql.org/docs/current/functions-matching.html)). Two kinds of mismatch hurt a model most.

- **Different syntax.** In ARE, `\b` means **backspace**, so `\bparse_config\b` silently matches nothing. The model concludes the code isn't there and acts on that false belief.
- **Different classes, depending on the database's locale.** ARE's `\w`, `[[:alpha:]]`, `\y` and `~*` classify characters by the database's ctype. Under the `C` locale, which some managed services use, they're ASCII-only. Measured on PostgreSQL 18: `'é' ~ '^\w$'` and `'É' ~* 'é'` are false under `C` and true under `en_US.utf8`, and `\ydéjà\y` finds nothing in `x déjà y` under `C`. ARE's `\d` matches only ASCII digits in every locale, while Rust's `\d` also matches digits like `٣`.

So the translator never relies on ARE's classes, word boundaries or case folding. It spells each one out as explicit character ranges, which mean the same in every locale.

### Why regex-syntax

The patterns come from models trained on ripgrep, so the reference is ripgrep's own parser. It produces two levels of output:

- **The AST** is the pattern as written, with a position for every node. Parse errors print exactly as ripgrep prints them:

  ```
  regex parse error:
      foo(?=bar)
         ^^^
  error: look-around, including look-ahead and look-behind, is not supported
  ```

- **The HIR** (high-level intermediate representation) is what the pattern means, with everything resolved: flags are applied, `(?i:ab)` has become `[Aa][Bb]`, `\pL`, `\w` and `[a-z&&[^aeiou]]` are explicit sets of code-point ranges, `(?x)` whitespace and comments are gone, and `\x{1F600}` is the literal character. The HIR has only eight node kinds, so the emitter is one small function per kind.

We emit from the HIR. It already contains the exact Rust semantics, so the emitter has nothing to interpret, only to spell.

### Mapping

| The model writes | HIR | Emitted ARE |
| --- | --- | --- |
| Literals, including `\x{1F600}` and `\t` | `Literal` | The characters, with ARE metacharacters escaped and control characters as `\uXXXX` |
| `\w`, `\d`, `\s`, `.`, `[...]`, `\pL`, `\p{Greek}`, `[^\D]`, `[a-z&&[^aeiou]]` | `Class`: a set of ranges | A bracket expression listing the ranges, with `\n` removed as rg removes it (a line never contains one) and NUL excluded (Postgres text can't hold it) |
| `(?i)` anywhere, `(?i:...)`, or `ignore_case=true` | Case-folded `Class` | Ranges such as `[Ee]`, so case-insensitive search uses `~`, never `~*` |
| `^`, `$`, `\A`, `\z`, and `(?m)` forms | `Look::Start`, `End`, `StartLF`, `EndLF` | `^`, `$`. rg's text is the line. |
| `\b`, `\B` | `Look::WordUnicode`, `WordUnicodeNegate` | Lookaround over the explicit `\w` class: `(?:(?<=W)(?!W)\|(?<!W)(?=W))` and its complement |
| `\<`, `\>`, `\b{start}`, `\b{end}`, `\b{start-half}`, `\b{end-half}` | `Look::WordStart...`, `WordEnd...` | `(?<!W)(?=W)`, `(?<=W)(?!W)`, `(?<!W)`, `(?!W)` |
| `(?-u:\b)` | `Look::WordAscii` | The same forms with `W = [0-9A-Za-z_]` |
| `*`, `+`, `?`, `{n,m}`, lazy `+?` | `Repetition` | The same quantifier. Counts above 255 are rejected, because ARE's limit is 255. |
| `(...)`, `(?P<n>...)`, `(?<n>...)`, `(?:...)` | `Capture`, or nothing | `(?:...)`. Captures are never used, and non-capturing groups compile smaller. |
| Alternation, concatenation, empty | `Alternation`, `Concat`, `Empty` | `(?:a\|b)`, juxtaposition, `(?:)` |

Rejected, each with a message in ripgrep's format:

| Construct | Why |
| --- | --- |
| Lookaround `(?=...)`, `(?<=...)`, backreferences `\1` | regex-syntax rejects them, as rg does without `-P` |
| `\Z`, `\Q...\E`, `\h`, unknown `\p{...}` names | regex-syntax rejects them, as rg does |
| A literal `\n` | rg rejects it without `-U`; no single line contains one |
| Byte classes from `(?-u)` outside ASCII | Lines are text, not bytes |
| A repetition count above 255 | ARE's limit |
| A translated pattern over the size cap (for example 256 KB) | Keeps compilation bounded. One `\w` is about 10 KB; one `\b` is four of them. |

**Longest-match versus first-match** semantics don't matter here. Search only asks whether a line matches and returns the whole line; it never extracts match spans.

### Cost of explicit classes

Measured on PostgreSQL 18 over 200,000 lines (about 12 MB):

| Pattern | Time |
| --- | --- |
| `config\(\w+\)` with ARE's `\w` | 138 ms |
| The same with the 796-range Unicode `\w` | 150 ms |
| `PARSE_CONFIG` with `~*` | 153 ms |
| `[Pp][Aa][Rr][Ss][Ee]_...` with `~` | 154 ms |
| `\yparse_config\y` (wrong under `C` for non-ASCII) | 190 ms |
| The lookaround word boundary | 302 ms |

Compiling the 796-range class takes about 0.7 ms, against 0.15 ms for ARE's `\w`. Correct semantics in every locale cost little, except word boundaries, which cost about 1.6 times as much. If that matters, a later optimization can probe the database's ctype once at startup and use `\y` where it's known to agree.

### What exists: survey

| Project | What it is | Usable here? |
| --- | --- | --- |
| [regex-syntax](https://docs.rs/regex-syntax) | The parser of Rust's `regex` crate, used by ripgrep. Produces an AST with positions and a fully resolved HIR. | **Chosen.** Exact rg dialect and rg's error messages. |
| [regex](https://docs.rs/regex) | Rust's regex engine | As the test oracle: same parser, real matching |
| [pgpcre](https://github.com/petere/pgpcre) | Postgres extension that adds PCRE as a `pcre` type with `~` and `=~` operators | Rarely: not offered by most managed services, and it has no index support for pattern matching. |
| [re2 (PGXN)](https://api.pgxn.org/src/re2/re2-0.4.0/doc/re2.md) | Postgres extension with ClickHouse-compatible RE2 functions; an `@~` operator with a `gin_re2_ops` trigram prefilter | Rarely available. Its GIN trigram prefilter is the model for an optional index (see Open decisions). |
| [omni\_regex (Omnigres)](https://docs.omnigres.org/omni_regex/regex/) | PCRE2-based match and capture functions | Rarely available |
| [PL/Rust](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.Using.PL_Rust.html) | Postgres functions written in Rust, with the `regex` crate allowed | RDS only |
| [PCRE2 `pcre2_convert`](https://git.stg.centos.org/source-git/pcre2/blob/c8s/f/src/pcre2_convert.c) | Converts POSIX patterns and globs into PCRE2 | Wrong direction |
| [RegexBuddy](https://www.regexbuddy.com/postgresql.html) | Commercial GUI that converts between regex flavors, including PostgreSQL | No: a GUI tool, not a library |
| Python `re._parser` (formerly `sre_parse`) | CPython's internal regex parser | No: an internal API, and Python's dialect, not Rust's |

Postgres's built-in ARE engine is the only regex engine available everywhere, so we translate to ARE. `pg_trgm`, which ships with Postgres and is offered on most managed services such as Databricks Lakebase ([Lakebase extensions](https://docs.databricks.com/gcp/en/oltp/projects/extensions)), stays in reserve for an index if scans prove too slow.

### Translator design

1. **A Rust extension module.** The translator is a small Rust crate, `minifs-regex`, built with PyO3 and maturin as the Python module `minifs._regex`. It's built as abi3 wheels (one per platform for every Python from 3.12) for Linux x86\_64 and aarch64 and macOS arm64. The rest of minifs stays Python.
2. **Parse as rg does.** One call: `translate(pattern, ignore_case) -> (line_form, file_form)`. It configures regex-syntax the way ripgrep does (Unicode on, case-insensitivity from `ignore_case`), parses to the HIR and emits ARE using the mapping above. Errors raise `RegexError` carrying ripgrep's formatted message and the position.
3. **Two forms.** The line form is for the line check. The file form is the same pattern with `(?n)` in front, for the file filter. Since classes never contain `\n`, `(?n)` only affects `^` and `$`.
4. **Reject, don't guess.** Anything the mapping doesn't cover is an error with a hint. An error is always better than a silent zero-match.
5. **Validate in Postgres.** Compile the translated pattern once with `SELECT '' ~ $1` under a 100 ms timeout. An error here is an emitter bug, reported as such, never shown to the model as a pattern problem.
6. **Cache.** `vfs/regex.py` caches translations per `(pattern, ignore_case)`.
7. **Safety.** Postgres warns that "regular expressions can be contrived that take arbitrary amounts of time and memory" and advises a statement timeout for untrusted patterns. Backreferences, the main cause of slow ARE matches, are rejected. Every search still runs under `SET LOCAL statement_timeout`.

### Differential testing (how we know it's right)

- **The oracle is Rust's `regex` crate,** exposed by the same extension behind a test-only cargo feature. It uses the same parser and real Rust matching, so for every pattern and line, `regex.is_match(line)` must equal Postgres `line ~ line_form`. Every disagreement is an emitter bug: there is no "shared subset" to exclude.
- **Two databases:** every comparison runs against a `C`-locale database and a UTF-8 one. Results must agree with the oracle, and therefore with each other.
- **A corpus of real patterns:** start with 200+ patterns collected from model transcripts (`\bparse\b`, `TODO|FIXME`, `(?i)error`, `\d{2}:\d{2}`, and so on), with lines that include non-ASCII text, and add every new pattern seen in production logs.
- **Property tests:** generate random HIR, print it as a Rust pattern (regex-syntax has a printer), and compare on random lines.
- **Rejections match rg:** every pattern regex-syntax rejects is also rejected by the real `rg` binary, checked in CI.
- **The file filter never drops a match:** whenever some line matches the line form, the whole file matches the file form. A failure here would be a silent missed hit, so it gets its own property test.

## Agent integration

The agent tools follow ladderframe's tool contracts (`packages/harness/src/ladderframe/tools`): the same names, parameters, output formats and error handling as its `Read`, `Glob` and `Grep`, and as its upcoming `Write`. ladderframe's tools track opencode's, which mirror Claude Code's, so models apply what they already practice, and an agent moves between a ladderframe workspace on disk and minifs without prompt changes. The examples use PydanticAI, but the tools are plain async functions over the VFS library and fit any agent framework or an MCP server.

| Tool | Signature | Backed by |
| --- | --- | --- |
| Read | `Read(filePath, offset, limit)`: numbered lines of a file, or the entries of a directory | The line map and `vfs_pread`, through the content cache; `vfs_readdir` for a directory |
| Glob | `Glob(pattern, path)`: absolute paths, at most 100 | `vfs_glob` over `vfs_nodes` |
| Grep | `Grep(pattern, path, include)`: hits grouped by file, at most 100 | `vfs_glob` and a scan of `vfs_extents` |
| Write | `Write(filePath, content)` | `vfs_write` or `vfs_create`, subject to the write policy |

Paths are absolute in the agent's chroot view; a relative path resolves against `/workspace`, the agent's working directory. Options without a ladderframe parameter (Glob's `exclude`, Grep's `ignore_case`, `fixed_strings`, `context` and `max_count`, byte offsets) stay in the library.

**Write.** ladderframe has no `Write` yet. Until it lands, minifs follows opencode's `write`, which ladderframe's tools track: `filePath` and `content`, creating the file and any missing parent directories, or replacing the whole file as a new generation. Details, from opencode:

- **Read before overwrite.** Overwriting an existing file requires a `Read` of it earlier in the run, and the file must be unchanged since. The tool records each path's `(node_id, generation, size)` at its last `Read`. It writes by that `node_id` and passes the recorded generation and size, plus the parent id and name it resolves `filePath` to, to `vfs_write`, which compares all four under the inode lock (`ESTALE` on a mismatch). The check and the write are one atomic step. The check applies only when `filePath` resolves to an existing file. If nothing is there now (the read file was deleted, or renamed away with nothing left behind), the tool drops the record and creates the file, as opencode does. If another file is at the path (a different node), or the read file was renamed away while `vfs_write` ran (it fails on its parent or name), the agent gets the "modified" message; its next `Read` records the current file, so the retry succeeds. The parent and name check is per directory entry: it keeps a `Write` from landing on the read file at its new path, but a concurrent rename of an ancestor directory isn't detected. Messages: `ModelRetry: You must Read <path> before overwriting it.` when there is no record, and `ModelRetry: File has been modified since it was last read. Read it again before overwriting.` on a mismatch. The same check stops two agents from silently clobbering each other's `notes.md`.
- **Writes refresh the record.** A successful create or overwrite stores the `(node_id, generation, size)` that `vfs_create` (which takes the initial content) or `vfs_write` returned, so an agent can write the same file twice without reading it in between.
- **Create races.** If `vfs_create` hits the `UNIQUE` constraint because another client created the file first, the call becomes the overwrite case above and fails the read check. A missing parent directory that another client creates at the same moment is not an error: the tool resolves it and continues. `vfs_create` with `data` for a directory raises `EINVAL`.
- **Other errors.** `filePath` naming a directory, or content over the file size cap, raises `ModelRetry`. A write the policy denies (for example to `/docs/**`, or a `Write` to the append-only `/workspace/log.md`) is a returned string.
- **Success** returns `Wrote file successfully.`

When ladderframe's `Write` lands, minifs adopts its signature and messages.

### Differences from ladderframe

ladderframe's tools run over a disk, `Glob` and `Grep` through ripgrep; minifs implements the same contracts in Postgres. These differences remain, by design:

- **No `.gitignore`.** ladderframe's `Glob` and `Grep` both honour `.gitignore`, `.ignore` and git's excludes, as rg does by default. minifs has no ignore files. In their place, `Glob` and `Grep` apply a default exclude list limited to unambiguous caches (`**/node_modules/**`, `**/.venv/**`, `**/__pycache__/**`, `**/.mypy_cache/**`), configurable per workspace. As in rg, excludes match each path relative to the search root: a `path` inside an excluded directory is searched, and excluded directories below `path` are still skipped. Excluded directories are pruned from the walk (see Glob), so they cost nothing. `**/.git/**` is always excluded, outside the configurable list, as ladderframe hard-codes it. Hidden files follow ladderframe (see Glob). Honouring `.gitignore` files stored in the tree can come later.
- **ARE underneath.** Patterns are parsed as rg parses them and translated to Postgres ARE (see Regex dialect). A few constructs rg accepts are refused with a `ModelRetry` that names them: byte classes outside ASCII, repetition counts above 255, and patterns whose translation exceeds the size cap.
- **mtime order.** `Glob` returns paths newest first, as opencode does; ladderframe's `Glob` and `Grep` return them in rg's walk order, which varies between runs because rg searches in parallel. `Grep` orders hits by path, then line. Above the result limit, minifs returns the first 100 in its order, where ladderframe returns an arbitrary 100.
- **No attachments.** `Read` doesn't attach images or PDFs (see Read contract).
- **Non-UTF-8 files aren't searched.** rg searches them and ladderframe decodes hits with replacement characters; minifs's `Grep` skips them (see Search).
- **Guard rails.** minifs refuses searches over the file cap or byte cap, and stops each of a search's two statements after 2 seconds. ladderframe's `Grep` has no such limits.

### Package layout

```
minifs/
  vfs/
    core.py      Entry, ReadResult, VFSError, path normalization, chroot mapping
    client.py    thin async wrappers over the SQL functions
    policy.py    per-actor write rules
    glob.py      glob translator and pruning hints
    regex.py     thin wrapper over minifs._regex, with the translation cache
    watch.py     LISTEN vfs and async iterators per path
    cache.py     directory-entry cache, content and line-map cache, NOTIFY eviction
    cursor.py    Cursor and read_from: generation-aware tailing for library callers
  tools/
    tools.py     Read / Write / Glob / Grep, with ladderframe's contracts
  db/
    pool.py      psycopg pool and credential refresh
    schema.sql
crates/
  minifs-regex/  Rust: regex-syntax HIR to ARE, built as minifs._regex (PyO3, maturin)
```

### Deps and tools

```python
@dataclass
class Deps:
    vfs: VFSClient
    workspace_id: UUID
    actor: str = "agent"
    versions: dict[str, tuple[UUID, int, int]] = field(default_factory=dict)  # path -> (node_id, generation, size) at last Read or Write

agent = Agent("<model>", deps_type=Deps, instructions=AGENT_PROMPT, retries=3)   # ladderframe's tool_retries

@agent.tool
async def Read(ctx: RunContext[Deps], filePath: str, offset: int | None = None, limit: int | None = None) -> str:
    """Read a file or directory.

    Args:
        filePath: The absolute path to the file or directory to read
        offset: The line number to start reading from (1-indexed)
        limit: The maximum number of lines to read (defaults to 2000)
    """
    ...

@agent.tool
async def Glob(ctx: RunContext[Deps], pattern: str, path: str | None = None) -> str:
    """Find files by glob pattern."""
    ...

@agent.tool
async def Grep(ctx: RunContext[Deps], pattern: str, path: str | None = None, include: str | None = None) -> str:
    """Search file contents with a regular expression."""
    ...

@agent.tool
async def Write(ctx: RunContext[Deps], filePath: str, content: str) -> str:
    """Write a file, creating it or replacing its contents."""
    ...
```

The tool descriptions start from ladderframe's text, so a model sees the same instructions on disk and in minifs. minifs drops the lines that point to tools or features it doesn't have (Bash with `rg`, the Agent tool, image and PDF attachments, "the local filesystem"). It adds one line to `Grep`'s description: use `(?i)` for case-insensitive search; lookaround and backreferences aren't supported.

**Errors follow ladderframe.** A mistake the model can fix by calling again raises `ModelRetry`, with a message that says what to change: a missing path (`File not found: /workspace/notes.md`, with up to three similar names), an offset past the end, a binary file, a `Glob` path that isn't a directory (`glob path must be a directory: ...`, `No such directory: ...`), a missing `Grep` path (`No such file or directory: ...`) or empty pattern (`pattern is required`), a pattern the translator rejects (ripgrep's message and position), a search scope over the cap, or a timeout. A path outside the agent's view gets each tool's not-found error (`File not found` for `Read` and `Write`, `No such directory` for `Glob`, `No such file or directory` for `Grep`), and `Read` never suggests similar names from outside the view, so errors don't reveal what exists beyond the chroot. A permission denial (the write policy) is returned as a string, as ladderframe's permission guard does: retrying won't help, and the model reads the refusal and moves on. Each tool maps `VFSError` to one of the two at its boundary, and nothing else escapes.

**Pre-fill reads that happen every time.** Each tool call costs a full extra LLM inference: the model stops, waits for the result, then reprocesses its whole context. The database query takes milliseconds; the extra inference takes roughly 0.5 to 1.5 s. If a harness always reads the same file at the start of a run (for example `notes.md`), it can run that `Read` itself, through the same tool function so the version is recorded for a later `Write`, and put the result in the prompt as if the agent had run it. A read that depends on what the agent is thinking stays a tool call.

**Reacting to changes.** `vfs.watch(path)` lets a harness start an agent run when a file changes, for example when a new file appears in `tasks/new/` or a log grows.

## Operations

Three things matter most in production: credential handling, LISTEN connections that reconnect cleanly, and keeping heavy search load away from latency-sensitive writes.

### Postgres specifics

| Topic | Guidance |
| --- | --- |
| Credentials | Managed services often use short-lived tokens as the Postgres password. Give the psycopg pool a connection factory that fetches a fresh credential, not a fixed DSN. |
| LISTEN | Use a direct, dedicated connection for LISTEN, never one through a transaction-mode pooler. Its keepalive probe keeps a Lakebase compute from scaling to zero while a watcher runs. A listener the server still thinks is alive but that no longer reads holds back the shared NOTIFY queue; if the queue fills (8 GB), commits that notify fail. The server's `tcp_keepalives_*` settings decide how soon it drops a client that vanished; `keepalives` and `tcp_user_timeout` in the LISTEN conninfo make the client notice a dead server sooner. |
| Reconnect | When the LISTEN connection drops: reconnect, LISTEN again, clear the directory cache, then re-check watched paths so nothing missed is skipped. Saved cursors make this safe. |
| NOTIFY volume | A transaction that called NOTIFY takes a database-wide lock at commit, so every append commits through that lock. Cache eviction only needs `create`, `move` and `delete`; `modify` exists for watchers. If a hot log makes the lock a bottleneck, stop emitting `modify` for appends to it and have its watchers poll `size`. |
| TOAST | `SET STORAGE EXTERNAL` on `vfs_extents.data`, so range reads fetch only the chunks they cover |
| Vacuum | Appends leave a dead `vfs_nodes` row version each; overwrites and compaction leave dead extents. `fillfactor = 80` keeps the inode updates HOT, so they're pruned in place. Watch `n_dead_tup` on both tables, and lower autovacuum's scale factor on `vfs_extents` if overwrites are frequent. |
| Compaction | A periodic job runs `vfs_compact` on files with more than 1,000 extents. It holds each inode lock only while merging that file. |
| Postgres version | PG 15+ for `UNIQUE NULLS NOT DISTINCT`. PG 18 has a built-in `uuidv7()`; on older versions use `pg_uuidv7` where available, or generate UUIDv7 in the app. |
| Extensions | None required. `pg_trgm` only if the optional search index is added, and `vector` (pgvector) for future semantic search. |
| Pools | Per process: one LISTEN connection (shared by watch and the cache), and a small pool (about 5) for the tools. |

### Protecting the database

- **Every search runs under `SET LOCAL statement_timeout = '2s'`,** which applies to each of its two statements. On timeout, raise `ModelRetry: Search timed out; narrow the pattern or path`.
- **Cap the files in scope.** The scope query (step 1 of the search) asks for one file more than the cap (for example 10,000). If it gets it, the search is refused with a `ModelRetry` to narrow the `path` or `include`, rather than silently dropping the oldest files.
- **Cap the bytes scanned.** Refuse a search whose scope is larger than the configured cap, with a hint to narrow the `path` or `include`. The size comes from `vfs_nodes`, so the check reads no content.
- **Cap output:** `Grep` and `Glob` return at most 100 results by default, as in ladderframe, each with a "narrow it" hint.
- **Separate pools for search and writes.** If CPU contention appears at scale, consider sending search to a read replica. That gives up read-your-latest-write consistency, so only do it if you need to.

### Capacity, rough estimates

| Item | Estimate |
| --- | --- |
| Append cost | One inode row lock, one extent insert, one NOTIFY |
| Storage | The text itself, uncompressed (`STORAGE EXTERNAL`), plus about 30 bytes of row overhead per extent |
| Search cost | One regex pass over the bytes in scope, plus a line split of the files that match |
| `Glob` over a few thousand nodes | Milliseconds |

### Observability

Metrics to track:

- Tool call latency per operation, and tool calls per agent run.
- Search duration, bytes scanned, timeouts, scope-cap refusals, and translator rejections by construct. Rejections show which constructs are worth supporting next. Bytes scanned per search shows when an index starts to pay off.
- Cache hit rate and bytes fetched per read.
- Extents per file, which shows whether compaction keeps up.
- Write-policy denials, which show whether agents try to write outside their allowed paths.

## Testing strategy

Most of the correctness rests on four properties, and each gets a test that compares two independent answers: joined chunked reads equal the full file, cached reads equal uncached ones, pruned globs equal full walks, and translated regexes agree with an oracle. Everything runs against real Postgres in testcontainers; there are no SQL mocks.

| Area | Test | Kind |
| --- | --- | --- |
| Append ordering | Two connections appending concurrently produce gap-free offsets, and offset order equals commit order | Concurrency |
| Read | For random files and random `(offset, limit)` sequences, joining the `pread` chunks equals the full content, including while another task keeps appending | Property |
| Read edges | Offsets `0`, `size`, `size+1`, `-1`, `-size`, `-(size+1)`; empty file; one extent larger than `limit`; a read spanning extents | Unit |
| Read plan | On a file with 10,000+ extents, `EXPLAIN` of a read near the end shows a backward index descent and a bounded forward scan, not a scan from offset 0 | Unit |
| Kinds | Creating a child under a file gives `ENOTDIR`; an extent on a directory is rejected by the foreign key; `vfs.pread` of a directory gives `EISDIR`, and `Read` of one lists its entries | Unit |
| Read tool | Output equals ladderframe's `Read` on the same file written to disk, after mapping the disk path prefix to the VFS path and with no instruction files present, for random `offset` and `limit`, CRLF files, lines over 2,000 characters, the 50 KB cap, no trailing newline and directories | Differential |
| Generations | An overwrite between two `vfs.read_from(cursor)` calls flags the restart; an append doesn't | Unit |
| Tree | `UNIQUE NULLS NOT DISTINCT` rejects duplicate root names; `rmdir` on a non-empty directory gives `ENOTEMPTY`; a move into its own subtree gives `EINVAL` | Unit |
| Claim | Two claimers racing for the same file: exactly one wins | Concurrency |
| Watch | Events arrive only after commit; `move` versus `modify` is classified correctly | Integration |
| Search | Hits match a per-line search in Python over the same files; a trailing newline adds no phantom line; a UTF-8 character split across two appends still matches; non-UTF-8 files are skipped; the scope cap refuses before reading content | Integration |
| File filter | Whenever a line matches the line form, the file matches the file form | Property |
| Compaction | Bytes, `size`, `generation` and `mtime` are unchanged; appends running during compaction keep gap-free offsets | Concurrency |
| Content cache | Reads through the cache equal uncached `pread` while another client appends, overwrites and compacts | Property |
| Directory cache | A rename or delete by another client is seen once its NOTIFY arrives; a dropped LISTEN connection clears the cache; a missing name is never cached | Integration |
| Line map | Line N read through the map equals line N from splitting the whole file, across appends that end mid-line | Property |
| Glob translator | Compared with `PurePath.full_match` (Python 3.13+) or `wcmatch` (GLOBSTAR and BRACE flags) on generated patterns and paths | Differential |
| Glob pruning | Results with all hints enabled equal results with none | Differential |
| Regex translator | A corpus of real model patterns plus generated ones, compared with Rust's `regex` crate, in a `C`-locale and a UTF-8 database | Differential |
| Regex rejections | Every pattern the translator rejects at parse time is also rejected by the `rg` binary | Differential |
| Search scope | `vfs.search(p, glob=X)` hits exactly the files that `vfs.glob(X)` returns | Integration |
| Glob and Grep tools | Output equals ladderframe's `Glob` and `Grep` on the same tree written to disk, apart from the documented differences. The disk copy gets a `.gitignore` built from the exclude list as directory patterns (`node_modules/`), path prefixes are mapped, and results are compared as sets, and only when they're under the limit | Differential |
| Agent tools | Retryable mistakes raise `ModelRetry`; permission denials come back as strings; the chroot blocks `..` escapes; the write policy blocks writes outside allowed paths; `Write` over a file changed since its last `Read` (including a concurrent create, or a recreated file of the same size) raises `ModelRetry`; a second `Write` without a `Read` in between succeeds | Unit |
| Write check | `vfs_write` with a stale expected generation, size, parent or name raises `ESTALE` and changes nothing; a partial expectation raises `EINVAL`; after the read file was renamed away, a `Write` raises `ModelRetry` if another file is now at the path and creates a new file if the path is empty, and in both cases leaves the moved file unchanged | Unit |
| Evals | A set of recorded coding tasks run against the VFS tools: track task success and tool calls per task | Eval |

## Roadmap

The tool-based VFS ships first. A TigerFS front end follows once the tree and tools are stable.

**What stays frozen.** The ladderframe-style tools: `Read`, `Write`, `Glob` and `Grep`, with ladderframe's signatures, output formats and error handling. The TigerFS work can then replace the VFS client with a thin MCP client, and prompts, tools and evals carry over unchanged.

**What the TigerFS integration adds.** Three pieces of work in the TigerFS repo:

1. A `tigerfs mcp` front end over `fs.Operations`, which agent frameworks connect to with their MCP clients.
2. An extent-backed file workspace format, `vfs_nodes` plus `vfs_extents`. TigerFS's current synth workspaces store each file's body in a single row, so they can't do cheap appends or offset reads. With extents, a FUSE `Write` at an offset maps straight onto extents.
3. `Glob` and `Grep` exposed through the same front end, so every client gets the same search results.

The payoff is that the same tree mounts through FUSE on a laptop or NFS on Kubernetes for debugging, alongside tool-based access where mounts aren't allowed.

## Open decisions

| Decision | Options | Recommendation |
| --- | --- | --- |
| Client access | Through the VFS library, or direct SQL | The VFS library, for one model end to end. Revisit only if profiling shows overhead. |
| Workspaces per process | One, or many | Many, with one LISTEN connection filtered by payload |
| Postgres version | PG 15–17 (UUIDv7 from an extension or the app), or PG 18 (built-in `uuidv7()`) | PG 18 where available |
| Line-numbered reads | A stored line table, or derived on the client | **Decided for v1:** derived on the client from the line map (see Caching), and used by `Read`. If cold reads of large logs prove slow, store each extent's count of preceding newlines: one integer per extent, set by `vfs_append` and kept by compaction. |
| Search index | None (scan), or a trigram GIN index | Scan in v1. If bytes scanned per search grow too large, index whole-file text refreshed by compaction, and keep scanning the newer appends. An index on raw extents would be unsound, because a match can cross an extent boundary. |
| Write contract | opencode's `write`, or ladderframe's `Write` | opencode's until ladderframe's `Write` lands, then ladderframe's |
| Semantic search | pgvector over windows of file text | After the core tools are stable |

## Sources

- [PostgreSQL 9.7 Pattern Matching](https://www.postgresql.org/docs/current/functions-matching.html): ARE syntax, Perl incompatibilities, and the statement-timeout advice
- [PostgreSQL pg\_trgm](https://www.postgresql.org/docs/current/pgtrgm.html): regex index support, for the optional search index
- [Databricks Apps best practices](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices) and [ECS KernelCapabilities](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_KernelCapabilities.html): examples of platforms without FUSE or root
- [Lakebase extensions](https://docs.databricks.com/gcp/en/oltp/projects/extensions): an example of a managed Postgres extension list
- [regex-syntax](https://docs.rs/regex-syntax) and [regex](https://docs.rs/regex): the parser the translator uses, and the test oracle
- [pgpcre](https://github.com/petere/pgpcre), [re2 for PostgreSQL (PGXN)](https://api.pgxn.org/src/re2/re2-0.4.0/doc/re2.md), [omni\_regex](https://docs.omnigres.org/omni_regex/regex/), [PL/Rust on RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.Using.PL_Rust.html), [PCRE2 pcre2\_convert](https://git.stg.centos.org/source-git/pcre2/blob/c8s/f/src/pcre2_convert.c), [RegexBuddy PostgreSQL](https://www.regexbuddy.com/postgresql.html): the translator survey
- TigerFS repository: `docs/adr/017-relational-directory-structure.md` and `internal/tigerfs/fs/synth/build.go`
