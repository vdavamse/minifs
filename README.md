# minifs: Everything Is a File for Agents

A small POSIX-like virtual filesystem stored entirely in PostgreSQL, built for coding assistants and other LLM agents.

## Overview

An agent sees one filesystem tree, stored entirely in Postgres, and works on it with five tools: `Read`, `Write`, `Edit`, `Glob` and `Grep`. `Read` on a directory lists it, so there is no separate `ls`. Source files, notes, logs, working memory and ingested documents are all ordinary files in that tree. Nothing is mounted, so it runs where FUSE and root are unavailable: sandboxes, serverless containers and managed app platforms.

**Goals**

- One storage model and one set of operations for every kind of data. There are no special tables for logs, memory or task queues.
- The tools are pydantic-ai tools with ladderframe's (opencode's) contracts (`Read` with line-based `offset` and `limit`, `Write`, `Edit`, `Glob`, `Grep`), which track Claude Code's too, so models already use them well. Any pydantic-ai agent can use them.
- Incremental reads: a reader can tail a growing file and get only what is new since its last read, by byte offset in the library and by line number in the tools.
- Search and glob run inside Postgres, so there's no local mirror, no sync and no `rg` binary.
- PostgreSQL is the single source of truth, and every read reflects the latest committed write.
- Low latency: a tool call is one or two SQL round trips (`Read` adds about one per directory below `/workspace` for its instruction-file lookups, until they become one query).

**Non-goals**

- Kernel mounts (FUSE or NFS). A later TigerFS integration can provide these where they're allowed (see Roadmap).
- Full POSIX: no hard links, permission bits, xattrs or random-offset writes in v1.
- Large binary files. The design targets text (source code, markdown, logs) from kilobytes to a few megabytes per file.

**In one paragraph:** files are inodes in `vfs_nodes`, and their bytes are append-friendly, line-aligned extents in `vfs_extents`: each starts at a line start and knows its first line number and where its lines start. SQL functions implement append, overwrite, positional and line reads, rename, compaction and watch (via NOTIFY). Search scans the current bytes of the files in scope inside Postgres, extent by extent, the way ripgrep scans a disk, so there is no line table or search index to keep in step with writes. A recursive walk over the parent pointers powers `Glob`. A Python VFS library wraps these functions and caches what can't go stale: directory entries. pydantic-ai tools with ladderframe's (opencode's) contracts wrap the library's chroot view; `Read` and `Grep` get their line numbers from Postgres.

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
| Agent tools | `Read`, `Write`, `Edit`, `Glob`, `Grep`: pydantic-ai tools with ladderframe's signatures, output formats and errors |
| VFS library | Path resolution, the chroot view, write policy, glob and regex translation, caching, cursors and watch |
| SQL functions | Append, overwrite, positional and line reads, resolve, readdir, create, rename, unlink, search, compact |
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
| `/docs/**` | read | read, write | read |

Rules are ordered and the first match wins, so specific rules come first (`/workspace/log.md` before `/workspace/**`). A path no rule matches may be read but not written. The view enforces the policy on visible paths, the tools' only permission gate; a denial is `EACCES`, counted per actor and action. The agent's tools have no append (`Write` replaces whole files and the policy refuses it on `log.md`), so the harness appends the agent's log entries through `vfs.append`, acting as the agent.

**Coordination through rename (Maildir).** When several agents or workers share work, a file's status can be the directory it sits in. Claiming a file is an atomic rename (see Core operations), so exactly one worker wins.

## Storage model

The model has two tables: `vfs_nodes` holds inodes and directory entries, and `vfs_extents` holds file bytes as variable-length extents at byte offsets. Every extent starts at a line start. An append is an INSERT (plus, when the file ends mid-line, a rewrite of its last extent), an overwrite is a new generation, and a read, by bytes or by lines, is a range scan.

```sql
CREATE TABLE vfs_nodes (                         -- inode + directory entry (no hard links)
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    parent_id   UUID,                            -- NULL for root entries
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('file','dir')),   -- never changes after create
    size        BIGINT NOT NULL DEFAULT 0,       -- exact; maintained under the inode lock; 0 for dirs
    lines       BIGINT NOT NULL DEFAULT 0,       -- line count, updated with size; 0 for dirs
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
    byte_offset BIGINT NOT NULL,                 -- always at a line start
    line_start  BIGINT NOT NULL,                 -- the number of \n before byte_offset
    data        BYTEA  NOT NULL,                 -- whole lines; only the last extent may end mid-line
    line_offsets INT4[] NOT NULL CHECK (line_offsets[1] = 0),   -- where each of its lines starts, in data
    node_kind   TEXT   NOT NULL DEFAULT 'file' CHECK (node_kind = 'file'),
    PRIMARY KEY (node_id, generation, byte_offset),
    UNIQUE (node_id, generation, line_start),    -- line lookups; each extent starts a different line
    FOREIGN KEY (node_id, node_kind)             -- only files have extents
        REFERENCES vfs_nodes (id, kind) ON DELETE CASCADE
);
ALTER TABLE vfs_extents ALTER COLUMN data SET STORAGE EXTENDED;   -- compressed: extents are small, read whole
```

Each column choice, and why:

| Choice | Why | Alternative rejected |
| --- | --- | --- |
| `UNIQUE NULLS NOT DISTINCT (parent_id, name)` | Root entries have `parent_id IS NULL`. A plain `UNIQUE` treats NULLs as distinct, so it would allow two `/docs` entries. Needs PostgreSQL 15+. | Plain `UNIQUE (parent_id, name)` |
| The parent foreign key uses `RESTRICT` | POSIX `rmdir` on a non-empty directory returns `ENOTEMPTY`. `CASCADE` would silently delete a whole subtree. | `ON DELETE CASCADE` |
| Foreign keys on `(id, kind)` | The schema itself rejects a child under a file and an extent on a directory. The constant columns `parent_kind = 'dir'` and `node_kind = 'file'` make that a plain foreign key. A root's `parent_id` is NULL, so the check skips it. | A trigger, or trusting every SQL function to check |
| `fillfactor = 80` on `vfs_nodes` | Every append updates `size`, `lines` and `mtime`, which aren't indexed. With free space in the page, Postgres makes that a HOT update: no new index entries, and the dead version is pruned in place. | The default 100, where a full page forces a non-HOT update |
| `uuidv7()` ids | Time-ordered: inserts stay local in the B-tree, and the id records creation time. Built in from PostgreSQL 18. On PG 16 or 17, use the pg\_uuidv7 extension where available, or generate the ids in the app. | `gen_random_uuid()` |
| Variable-length extents | An append that follows a complete line is one INSERT. A positional read selects only the overlapping extents. | Inline `body TEXT` (each append rewrites the whole value) or fixed-size chunks (each append rewrites the last block) |
| Line-aligned extents | Every extent starts at a line start, and every extent but a file's last ends with `\n`. So line N is found with one index descent on `line_start` plus `line_offsets[N - line_start]`, search can decode and split each extent on its own, and no extent splits a UTF-8 character (`\n` never occurs inside one). The price: an append to a file that ends mid-line rewrites the last extent, at most about 64 KB unless its last line is longer. A file with no newlines at all is one line, so each append rewrites all of it, and a line can't grow past bytea's 1 GB limit. The workload is read-heavy and its files are text with lines, so work moves from reads to writes. | A line table (a row per line), a `text[]` of lines per file, or line numbers derived on the client from the bytes |
| `line_start` and `line_offsets` | The extent's first line number and the start of each line in it: an 8-byte integer and 4 bytes per line, computed by `vfs_cut` as the content is written. Compaction concatenates them; nothing else maintains them. | Counting newlines at read time, from the start of the file |
| `lines` stored on the inode | The total for `Read`'s footers and its out-of-range message, without scanning. Split on `\n`, and a trailing `\n` adds no empty last line, so `a\nb\n` and `a\nb` both have 2 lines. | Computing it from the last extent on every read |
| `generation` | Lets readers and cursors tell an append (same generation, larger size) from a rewrite (new generation). | Comparing content hashes |
| `size` stored on the inode | `stat` without scanning extents. It's always updated in the same transaction as the extent, so it can't drift. | Computing `sum(length(data))` on every stat |
| `BYTEA` content | True byte offsets, and safe for binary content. Python decodes UTF-8. | `TEXT` (offsets would be in characters) |
| `STORAGE EXTENDED` on `data` (the default) | Writers cut extents of about 64 KB (a longer one is a single line), and compaction merges up to its target, 64 KB by default. Extents that size are read whole, so decompressing one costs little, and text compresses 3 to 4 times. | `EXTERNAL` (uncompressed), whose slice reads fetch only the TOAST chunks they cover: that paid off for multi-megabyte extents, which line alignment now makes only for a single line longer than the target. Reading the tail of such a line decompresses it from the start (in one review measurement on PG 16, about 5 ms for a 16 KB tail of an 18 MB line, against 0.6 ms uncompressed) |

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
- **Resolution returns the kind.** `vfs_lookup` returns `(id, kind)` (with `chain, err, err_detail`), so the directory cache stores the kind with the id. The operations report `ENOTDIR` and `EISDIR` themselves, and the foreign keys are the backstop.
- **Directory `mtime` isn't maintained.** POSIX updates it on every create, unlink and rename inside the directory. Here that would lock and update the parent row on every create: a hot spot for shared directories such as `tasks/new/`, and an extra NOTIFY each time. Watchers get those events from NOTIFY by parent id instead.

**Inode numbers and handles.** If a protocol needs a 64-bit inode number, take the low 64 bits of the UUID (the random part in v7). A stateful protocol can use the 16-byte UUID itself as the file handle. Unlike TigerFS's path-encoded NFS handles, it survives renames.

**Moves must not create cycles.** `rename` of a directory checks that the destination isn't inside the source. A recursive CTE walks up from the destination's parent within the same transaction, so a move like `mv /a /a/b/c` returns `EINVAL`.

**Cutting.** Every writer cuts its content with `vfs_cut(data, target)` into consecutive whole lines of about 64 KB, always just after a `\n`. A line longer than the target is one extent of its own; a line is never cut. An overwrite or a create with content cuts all of it. An append cuts what it adds, together with the file's last extent when that ends mid-line (deleted and cut again), so the alignment holds whatever the appends contain: partial lines, a UTF-8 character split across two appends, or no newline at all.

**Compaction.** Extent size isn't a tuning knob: an extent is as big as the write that made it, up to the cut's target. What grows is the extent count: a log appended one line at a time gains one extent per append. `vfs_compact(node)` merges a file's small neighbouring extents into pieces of about 64 KB under the inode lock.

- It merges whole extents, so pieces stay line-aligned: a piece keeps its first extent's `line_start`, and its `line_offsets` are its extents' offsets, shifted.
- It changes none of the bytes, `size`, `lines`, `generation` or `mtime`, so readers, cursors and caches never notice. It doesn't update `vfs_nodes`, so it emits no NOTIFY.
- It runs as routine maintenance: a periodic job compacts files with more than 1,000 extents. `vfs_append` can also compact opportunistically when a file crosses that threshold.
- The target size is forgiving. Anything from 16 KB to 1 MB behaves about the same, so it is set once, not tuned.

## Core operations

Every operation is a SQL function, so every client (agent tools, ingestion, a future TigerFS front end) gets the same semantics from the database. The Python library is a thin wrapper.

Expected outcomes (`ENOENT`, `ENOTDIR`, `EISDIR`, `EEXIST`, `ESTALE`, `ENOTEMPTY`) are returned, not raised: each function has `err` and `err_detail` result columns, and on an outcome changes nothing and returns one row carrying the error. Only faults (`EINVAL`, `ENAMETOOLONG`, broken invariants) raise, so a missing path an agent guessed costs no server log entry and doesn't abort the caller's transaction (migration 009, CONTRIBUTING: SQL functions and errors). The sketches below raise for brevity.

| Operation | Function | POSIX analogue | Locking |
| --- | --- | --- | --- |
| Append | `vfs_append(node, data) -> offset` | `write` with `O_APPEND` | Inode `FOR UPDATE` until commit |
| Overwrite | `vfs_write(node, data, expect_gen, expect_size, expect_parent, expect_name) -> (generation, size)` | `open` with `O_TRUNC`, then `write` | Inode `FOR UPDATE`; optional compare-and-swap |
| Positional read | `vfs_pread(node, offset, limit)` | `pread` | None (MVCC snapshot) |
| Line read | `vfs_pread_lines(node, first_line, max_lines, max_bytes)` | — | None (MVCC snapshot) |
| Resolve path | `vfs_lookup(path) -> (id, kind, chain, err, err_detail)` | `namei` | None |
| List directory | `vfs_readdir(dir)` | `readdir` | None |
| Create, mkdir | `vfs_create(parent, name, kind, data DEFAULT NULL) -> (id, generation, size)` | `open` with `O_CREAT\|O_EXCL`, `mkdir` | Unique constraint |
| Rename, move | `vfs_rename(node, new_parent, new_name, expect_parent)` | `rename` | Row lock; checks for cycles |
| Unlink, rmdir | `vfs_unlink(node)` | `unlink`, `rmdir` | Child check plus FK |
| Watch | `LISTEN vfs` | `inotify` | — |

Every function in the table that can produce an outcome also returns `err, err_detail` after the columns shown (migration 009).

### Append

```sql
CREATE FUNCTION vfs_append(p_node UUID, p_data BYTEA) RETURNS BIGINT AS $$
DECLARE v_size BIGINT; v_gen INT; v_lines BIGINT; v_off BIGINT; v_line BIGINT; v_last BYTEA;
BEGIN
    SELECT size, generation, lines INTO v_size, v_gen, v_lines
      FROM vfs_nodes WHERE id = p_node AND kind = 'file' FOR UPDATE;
    IF NOT FOUND THEN PERFORM vfs_raise('ENOENT', p_node::text); END IF;   -- or EISDIR
    IF length(p_data) = 0 THEN RETURN v_size; END IF;   -- extents are never empty
    SELECT byte_offset, line_start, data INTO v_off, v_line, v_last
      FROM vfs_extents WHERE node_id = p_node AND generation = v_gen
     ORDER BY byte_offset DESC LIMIT 1;                  -- the last extent
    IF NOT FOUND OR get_byte(v_last, length(v_last) - 1) = 10 THEN
        v_off := v_size; v_line := v_lines; v_last := '';   -- the file ends with \n (or is empty)
    ELSE                                                 -- it ends mid-line: cut the last extent again
        DELETE FROM vfs_extents WHERE node_id = p_node AND generation = v_gen AND byte_offset = v_off;
    END IF;
    INSERT INTO vfs_extents (node_id, generation, byte_offset, line_start, data, line_offsets)
    SELECT p_node, v_gen, v_off + rel_offset, v_line + rel_line, data, line_offsets
      FROM vfs_cut(v_last || p_data);
    UPDATE vfs_nodes SET size = v_size + length(p_data),
                         lines = (SELECT line_start + cardinality(line_offsets) FROM vfs_extents
                                   WHERE node_id = p_node AND generation = v_gen
                                   ORDER BY byte_offset DESC LIMIT 1),
                         mtime = greatest(mtime, clock_timestamp())   -- never backwards
     WHERE id = p_node;
    RETURN v_size;                       -- the offset the data landed at
END $$ LANGUAGE plpgsql;
```

Appends keep extents line-aligned, so an append to a file that ends mid-line rewrites the file's last extent: those bytes are unchanged, but their row is replaced. A trailing partial line goes in an extent of its own, so a writer whose appends end mid-line (stdout copied in chunks, a token stream) rewrites only that partial line each time, and a log appended a line at a time never rewrites anything. Overwrites and creates split it off too, so only the first such append after a compaction rewrites the whole last extent once, up to the compaction's target. A file with no newlines at all is the worst case: every append rewrites the whole file.

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
    INSERT INTO vfs_extents (node_id, generation, byte_offset, line_start, data, line_offsets)
    SELECT p_node, v_gen + 1, rel_offset, rel_line, data, line_offsets
      FROM vfs_cut(p_data);                         -- whole lines of about 64 KB each; none if empty
    UPDATE vfs_nodes SET generation = v_gen + 1, size = length(p_data),
                         lines = (SELECT coalesce(max(line_start + cardinality(line_offsets)), 0)
                                    FROM vfs_extents WHERE node_id = p_node AND generation = v_gen + 1),
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

`vfs_lookup(segments TEXT[]) -> (id, kind, chain, err, err_detail)` walks parent pointers on the server in one round trip, the same idea as TigerFS's `resolve_path`. `chain` is the id of every component it found, in order: the whole path on success, the prefix that resolved on `ENOENT` or `ENOTDIR` (on `ENOTDIR` it ends with the file). It is the one column an outcome sets besides `err` and `err_detail`. The library caches `(parent_id, name) -> id` for every link of the chain, and evicts entries when NOTIFY reports a move or delete (see Caching).

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

- `watch(path)` filters the single channel by node id (a file) or by parent id (a directory, plus the directory's own events). It subscribes before resolving the path, so a change committed after it returns is delivered, or covered by a later `rescan`; a change committed just before may be delivered too.
- Like inotify, a watch follows the node the path named when it started: through renames, until the node is deleted. The `delete` (confirmed against the database, since events are hints) is its last event, and the watch ends. To follow whatever a path names now, watch it again.
- When the connection drops, the watcher reconnects with jittered backoff, listens again, and only then sends every subscriber a `rescan` event. A watch then checks that its node still exists, and ends after the `rescan` if it doesn't, or if the check fails: it is never left on a node it can't vouch for. Consecutive `rescan`s are folded into one. A subscriber that falls more than `maxsize` events behind, or whose predicate raises, also gets a `rescan`.
- Every `keepalive` seconds (30 by default) the watcher probes the connection with `SELECT 1`, so a connection that died silently is noticed within `keepalive + probe_timeout`. A probe that times out closes the connection without waiting for the server.
- A watch's own queries (resolving the path, the check after a `rescan`) go through `pool` if one is given, else through a short-lived connection from the factory. With many watches per process, pass a pool, so a reconnect doesn't open one connection per watch at once.
- `watcher.subscribe(predicate)` is the raw feed. The library's directory cache uses it on the same connection (see Caching).

## Read contract

Reads come in two layers. The **library** addresses every file by byte offset, so a source file, a 200 KB doc, `notes.md` and a growing log all page and tail identically, and also by line (`vfs_pread_lines`). The **tool** is ladderframe's line-numbered `Read`, and reads its lines with `vfs_pread_lines`: Postgres finds line N from the extents' line numbers, so there's no client line map and no full-file read.

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
DECLARE v_size BIGINT; v_gen INT; v_off BIGINT; v_start BIGINT;
BEGIN
    SELECT n.size, n.generation INTO v_size, v_gen
      FROM vfs_nodes n WHERE n.id = p_node AND n.kind = 'file';   -- ENOENT or EISDIR if none
    v_off := CASE WHEN p_offset < 0 THEN greatest(v_size + p_offset, 0)
                  ELSE least(p_offset, v_size) END;  -- clamped, so v_off + p_limit can't overflow
    SELECT e.byte_offset INTO v_start FROM vfs_extents e   -- step 1: the extent that contains v_off
     WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset <= v_off
     ORDER BY e.byte_offset DESC LIMIT 1;
    RETURN QUERY                                    -- step 2: range scan forward from there
    SELECT v_size, v_gen, greatest(e.byte_offset, v_off),
           substring(e.data                         -- substring(bytea) takes int, not bigint
                     FROM (greatest(v_off - e.byte_offset, 0) + 1)::int
                     FOR  (least(e.byte_offset + length(e.data), v_off + p_limit)
                         - greatest(e.byte_offset, v_off))::int)
      FROM vfs_extents e
     WHERE e.node_id = p_node AND e.generation = v_gen
       AND e.byte_offset >= v_start
       AND e.byte_offset <  v_off + p_limit
       AND e.byte_offset + length(e.data) > v_off
       AND p_limit > 0
     ORDER BY e.byte_offset;
    IF NOT FOUND THEN                               -- nothing in range: still report size and generation
        RETURN QUERY SELECT v_size, v_gen, v_off, NULL::BYTEA;
    END IF;
END $$ LANGUAGE plpgsql STABLE;
```

- **Two index steps, like walking ext4's extent tree.** Only `byte_offset < off + limit` can bound a scan of the primary key; the overlap test `byte_offset + length(data) > off` can't. A single range scan would therefore read every extent from the start of the file. Step 1 descends backward to the extent that contains `off`, and step 2 scans forward from it, so a read costs O(log E + extents returned) for a file with E extents. On a 20,000-extent file, a read at the end touches 3 index pages.
- **Each step is its own statement**, so the planner sees step 2's bounds as values. Written as one query, with the bounds computed in it, it can't tell how narrow they are, and may scan all of a file's extents instead (it did once the extents gained their line columns).
- **Trimming happens in Postgres.** A 16 KB read of a 5 MB doc sends 16 KB. `length(data)` reads the size from the value's header without decompressing it.
- **It's `STABLE`, so it reads one MVCC snapshot.** Every statement in a `STABLE` function runs on the snapshot of the statement that called it, so `size` and `generation` describe exactly the bytes returned, even while another client is appending.
- **When nothing overlaps** it returns a single row with NULL data, so a read past EOF or of an empty file still returns `size` and `generation`, and `byte_offset` is the resolved start offset.
- **The real function** (migration 007, like the real `vfs_append` and `vfs_write`) also validates its arguments and raises `ENOENT` or `EISDIR`.

### Line reads

```
vfs_pread_lines(node, first_line, max_lines, max_bytes)
    -> (size, generation, lines, first_line, byte_offset, data)
```

It returns the bytes from the start of line `first_line` (1-based) through at most `max_lines` whole lines, each with its `\n`, capped at `max_bytes` (so the last line may come back cut), in one row. With them come the file's `size`, `generation` and total `lines`, and the `byte_offset` where `first_line` starts, so a caller can switch between line and byte reads. A `first_line` past the last line returns no bytes, `byte_offset = size` and the total, which is what `Read` needs to report `Offset N is out of range for this file (T lines)`.

- **Three index steps.** The extent with the largest `line_start < first_line` holds the start of `first_line`, at `byte_offset + line_offsets[first_line - line_start]`: one backward descent of the `(node_id, generation, line_start)` index, like `vfs_pread`'s step 1. A second descent finds where line `first_line + max_lines` starts, unless that is past the end, and a forward range scan of the primary key between the two reads the bytes. On a 20,000-line file a read at the start, the middle or the end touches a few pages.
- **One snapshot**, as in `vfs_pread`: it is `STABLE`, and the total, the bytes and their offset come from the same moment.
- **Lines split on `\n` only**, and a trailing `\n` adds no empty last line, so the line numbers are `Grep`'s, and `lines` is the count `Read` reports.

### Per-path cursors and generations

A library caller that tails files (a harness, an ingestion job, a watcher) keeps a `Cursor`, `path -> (generation, next_offset)`, and can save it to a `vfs_cursors` table for crash recovery. `vfs.read_from(cursor)` continues where the cursor left off:

| Since the last read | `generation` | What `vfs.read_from(cursor)` does |
| --- | --- | --- |
| Bytes appended | Unchanged | Returns only the new bytes |
| Nothing changed | Unchanged | Empty content, `eof=true` |
| File overwritten (a doc re-ingested, `notes.md` rewritten) | Changed | Restarts at 0 and flags the result as rewritten |

### Tool contract: numbered lines

```
Read(filePath, offset=None, limit=None) -> str | ToolReturn
```

The tool takes ladderframe's `Read` parameters and returns its output format.

| Input | Meaning |
| --- | --- |
| `offset = N`, N ≥ 1 (default 1) | Start at line N (1-indexed). `offset=0` means 1 |
| A negative `offset` or `limit` | `ModelRetry: offset and limit must be non-negative integers` |
| `limit = K` (default 2000) | At most K lines |
| `offset` past the last line | `ModelRetry`: `Offset 120 is out of range for this file (80 lines)` |
| `filePath` is a directory | Its entries the actor may read, hidden ones included, one per line and with `/` after subdirectories, inside an `<entries>` block after `<type>directory</type>`, with the footer last inside the block. Sorted case-insensitively. `offset` and `limit` page through the entries; the footer is `(N entries)` or `(Showing X of Y entries. Use 'offset' parameter to read beyond entry Z)` |

```
<path>/workspace/src/config.py</path>
<type>file</type>
<content>
17: def parse_config(path: str) -> Config:
18:     with open(path) as f:

(Showing lines 17-18 of 240. Use offset=19 to continue.)
</content>
```

- **One round trip for most reads.** The 4 KB sample that decides whether the file is binary (below) and the page's first `vfs_pread_lines` call go out together, so a Read of a file directly in `/workspace` or outside it holds two connections for one round trip; for an image or other binary, the page's bytes are fetched and dropped. The read starts again, for up to three attempts, when the sample, the page or the attachment come from different versions of the file, or the path vanishes between them.
- **Lines come from Postgres.** One `vfs_pread_lines(node, N, K, 50 KB)` call returns lines N to N+K-1, the total line count and the snapshot's generation, with no full-file read. The 50 KB cap applies to the raw bytes, before the tool cuts each line to 2,000 characters, so a page of long lines can need more than one call: a line the cap cut is used as is when it's the first of its batch (the batch holds more than 2,000 characters of it, so it's truncated to the same text), and is otherwise read again as the first line of the next call, until the page is full as ladderframe fills it. A page whose file changes between its calls (a new generation, or another file renamed over it) is read again, up to three times. Lines split on `\n` only, and the tool drops a trailing `\r`, so line numbers agree with `Grep` hits.
- **Bounded output, as in ladderframe.** A line longer than 2,000 characters is cut and gets `... (line truncated to 2000 chars)`, and output stops at 50 KB with `(Output capped at 50 KB. Showing lines a-b. Use offset=b+1 to continue.)`. The last page ends with `(End of file - total n lines)`.
- **Images and PDFs are attached, other binaries refused.** The first 4 KB (`vfs.pread(path, 0, 4096)`) decide, as in ladderframe. PNG, JPEG, GIF, WEBP or `%PDF-` magic bytes, or an extension `mimetypes` maps to one of those types (`.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`, `.pdf` and the like), return `Image read successfully` or `PDF read successfully` with the file as a `BinaryContent` attachment, up to 3.75 MB (`ModelRetry: <path> is too large to attach (N bytes, the limit is 3750000)` above). Another known binary extension, a NUL byte in the sample, or more than 30% non-printable bytes there gets `ModelRetry: Cannot read binary file: <path>`. Anything else is text, decoded as UTF-8 with replacement characters.
- **Nearby instructions.** As in ladderframe: for a file below `/workspace`, each directory from the file's parent up to, but not including, `/workspace` is checked for `AGENTS.md`, then `CLAUDE.md`, taking the first that is a file the actor may read, with no fallback. That file is then omitted if it's the file being read, was already shown in this conversation, or is blank. Each remaining instruction file is appended, headed `Instructions from: <path>`, in one `<system-reminder>` block. Files outside `/workspace`, such as `/docs/**`, get none. Once the sample shows a text file, the tool stats the candidates through the view while the rest of the page is read, two at a time so a Read holds at most three connections, then reads the chosen files, so a file below `/workspace` costs a round trip per directory or so. A lookup that fails otherwise than missing or denied (EIO) skips that directory rather than fall back to `CLAUDE.md`. (One query for every candidate, sent with the read, would save those round trips.)
- **Stateless.** There is no `offset=None` auto-continue and no per-agent cursor in the tool. To tail a log, the agent calls `Read` again with the `offset` its last footer gave. Generation-aware tailing is a library feature.

### Line numbers in the tools, byte offsets in the library

- `Grep` hits carry line numbers (`Line 42: ...`), so the agent follows up with `Read(filePath, offset=32, limit=40)`.
- Library search results also carry each hit's byte offset, in the same currency as `vfs.pread`. A cursor is just a saved `next_offset`.

## Caching

PostgreSQL stays the single source of truth: every read through a cache still reflects the latest committed write. Two properties make that cheap.

- **File bytes never change in place.** An append adds bytes after the end, an overwrite starts a new generation, and compaction changes neither. (An append that completes a partial line rewrites the last extent's row, but not its bytes.) Bytes cached under `(node_id, generation)` are valid forever. The only question is whether the file has grown or been rewritten.
- **Directory entries are keyed by parent id, not by path.** Renaming `/a` to `/b` changes only the `(root, "a")` entry. Every entry below it is keyed by its parent's id, which didn't change, so it stays valid.

| Cache | Key → value | Kept fresh by | Used by |
| --- | --- | --- | --- |
| Directory entries | `(parent_id, name) -> (id, kind)` | NOTIFY `move` and `delete` evict; a 2 s TTL as a fallback | Every path resolution |
| File content (deferred) | `(node_id, generation) -> bytes [0, cached_size)` | The read itself (below) | `vfs.pread` |

The caches live in each process's memory and are never shared between processes. Search and glob results aren't cached, because they depend on the whole tree. Saved cursors aren't a cache: they're per-caller state (see Per-path cursors and generations). There's no line map: line numbers live in the extents, and `Read` gets its lines from `vfs_pread_lines` (see Line reads).

### Directory entries

The cache fills the way the kernel's dcache does: every path the server resolves adds an entry per component. A statement that resolves a path with `vfs_lookup` gets the chain of ids it walked back in the same row (no extra round trip), and the client stores each `(parent_id, name) -> (id, kind)`: directories for the intermediate components, the returned kind for the last. That covers every operation that looks a path up: `stat`, reads, writes, unlink, the source and the destination's directory of a rename, the parent of a create, and the root of a glob or grep. The exceptions walk nothing that comes back: `mkdir_p` and a create with `parents` resolve through `vfs_mkdir_p`, which reports no chain (only the created node is stored), and a rename stores nothing under the destination name (the next use of it walks). So after `Glob` or `Grep` hands an agent a list of paths, the second `Read` or `Edit` under the same directory hits every shared component, though no directory was ever listed. Listings and creates still store what they return. A statement that used a fully cached chain stores nothing: it learned nothing new, and if a link was stale, its fallback walk is left to the next lookup.

Compared with the kernel's dcache:

| | Linux dcache | minifs |
| --- | --- | --- |
| Filled by | Each component of a path walk | Each component of a server lookup, listings, creates |
| Negative entries | Yes (a dentry with no inode) | No: a name another client creates must be visible at once |
| A failed walk | Caches the prefix, and the missing name as negative | Caches the prefix only |
| Revalidation | Local filesystems trust it; network ones call `d_revalidate` | Every use checks every link against `vfs_nodes` in the same statement, falling back to a walk |
| Invalidation | The VFS sees every rename and unlink | NOTIFY `move`/`delete`, the client's own changes, and a 2 s TTL |

- Only positive entries are cached. A missing name is never cached, so a file another client creates is visible at once.
- The cache also keeps `id -> (parent_id, name)`. A `move` or `delete` notification carries the node id, so it evicts the right entry without knowing the old name.
- The TTL only covers what notifications can't: those lost while the LISTEN connection reconnects (after a reconnect, the whole cache is cleared), and a listing or lookup whose snapshot predates a move or delete but which is stored after that change's notification was handled. Either way, a stale entry costs a slower resolution, never the wrong node.

In the library this is `minifs.vfs.cache.DirEntryCache`, kept fresh by `async with cache.follow(watcher)` and passed to `VFSClient(pool, cache=cache)`. Its `stats()` gives the hit rate. Concurrent operations that find the same cached path stale share one eviction query, as long as it hasn't been sent yet. Each entry costs about 500 bytes; `max_entries` (default 10,000) caps the total.

### File content

Deferred: line reads come from the server, which sends only the lines asked for, so a content cache would save little. Revisit it if the metrics show bytes per read are a problem. If it's built, a cached read is one statement, the same as an uncached one:

1. Call `vfs_pread(node, cached_size, limit)`. It returns the file's current `size` and `generation`, along with any bytes past the cached end.
2. If the generation is unchanged, add the returned bytes (often none) to the entry and serve the request from memory.
3. If the generation changed, drop the entry and read again from offset 0.

The saving is bytes on the wire, not round trips. Tailing a growing log fetches only the new tail on each read. Entries are evicted least-recently-used under a per-process byte budget, and files above a size cap are read by range as before.

## Glob

`Glob` and `Grep` share one walker and one glob matcher, as they do inside ripgrep (whose matcher is the `globset` crate). The walker is `vfs_glob`, a recursive CTE over the parent pointers that reads only `vfs_nodes`, never content.

### Semantics

Patterns are rg's `-g` globs, compiled by rg's own code (see Translator). In the regexes below, `[^/]` stands for the explicit ranges actually emitted.

| Pattern | Matches | Regex (globset's) |
| --- | --- | --- |
| `*` | Any characters within one path segment | `[^/]*` |
| `?` | One character other than `/` | `[^/]` |
| `**/` | Zero or more directories | `(?:/?\|.*/)` at the start, `(?:/\|/.*/)` in the middle |
| `dir/**` | Everything below `dir` (rg reads it as `dir/**/*`) | `dir(?:/\|/.*/)[^/]*` |
| `**` alone | Every file | `^.*$` |
| `**` elsewhere, e.g. `a**b` | A `*` | `a[^/]*[^/]*b` |
| `[abc]`, `[a-z]`, `[!x]` | A character class (`!` or `^` negates), never matching `/` | `[a-c]`, `[a-z]`, `[^x/]` |
| `*.{md,txt}` | Alternatives, which may nest; an empty one is dropped (`a{b,}` is `ab`) | `[^/]*\.(?:md\|txt)` |
| `\*` | A literal `*` | `\*` |
| A pattern with no `/`, e.g. `*.md` | The file name at any depth (rg prepends `**/`) | `^(?:/?\|.*/)[^/]*\.md$` |
| A pattern with `/` anywhere, e.g. `src/*.py` | Anchored at the search root (a leading `/` only anchors) | `^src/[^/]*\.py$` |
| `exclude=["**/node_modules/**"]` (library only) | Like rg's `-g '!...'`: matching files are dropped, matching directories not walked | `NOT path ~ ANY(...)` |

The defaults follow rg. Only files are returned (directories are walked, not listed). For `Glob`, hidden directories (names starting with `.`) are never descended into, and a hidden file is returned whenever the pattern matches it, as `rg --files` does: `*.yml` finds `.foo.yml`, but `.github/**` finds nothing. `Grep` searches hidden files and directories, as ladderframe's `Grep` runs `rg --hidden`. Both skip `.git/`. `Glob` results are sorted by `mtime`, newest first (see Differences from ladderframe).

### Translator

Globs are compiled in the `minifs._regex` extension (`crates/minifs-regex/src/glob.rs`) by ripgrep's own code, then emitted as ARE by the regex translator (see Regex dialect and translation):

1. **rg's gitignore rules**, as `ignore::gitignore::GitignoreBuilder::add_line` applies them to each `-g`: trailing unescaped whitespace is dropped, a leading `/` is removed and anchors, a trailing `/` (escaped or not) means directories only, a pattern without `/` gets `**/` in front, and one ending in `/**` gets `/*` after.
2. **`globset`** (rg's matcher, with rg's options: `literal_separator`, `backslash_escape`, no unclosed classes) parses the result and writes a Rust regex. Its errors are rg's: `GlobError` prints `error parsing glob '<pattern>': <message>` as rg does (rg echoes the pattern after dropping trailing whitespace).
3. **Bytes to characters.** globset's regex is byte-mode (`(?-u)`) and writes every non-ASCII character as its UTF-8 bytes. They are decoded back into characters, so `?` and classes match one character and `[é-ö]` is a range of characters; each class also loses `/` (`[...]` becomes `[...&&[^/]]`, `[^...]` becomes `[^.../]`).
4. **regex-syntax and the ARE emitter.** The rewritten regex is parsed into a HIR (with `.` matching newlines, as globset's does) and emitted as ARE, with every class spelled out as code-point ranges. Unlike for Grep, a newline is an ordinary character here (names may contain one). The emitter's size limits apply.

The pruning hints are computed from the same HIR (below), and `minifs.vfs.glob` only wraps the result. Besides globset's errors, `GlobError` is raised for an empty pattern, a NUL, braces nested more than 64 deep (globset builds its regex recursively, so deeper nesting could overflow a thread's stack), a glob over 4,096 characters and classes once translated (`vfs_glob` matches the regex against every path it walks; a real glob has a few dozen), a leading `!` or `#` in an include (rg would read a negation, which belongs in the excludes, or a comment, which it ignores; write `\!` or `\#`), a pattern ending in `/` (it would match only directories, and only files are listed) and a pattern over 64 KB. In excludes, a trailing `/` limits the pattern to directories without anchoring it, as in gitignore: `build/` skips every directory named `build`, and `vfs_glob` tests each directory as its path plus `/`, and an exclude regex ends in `(?<!/)/?$` so that this matches what the pattern matches on the bare directory path, as rg tests it (`{a/,b}` must not exclude the directory `a`).

### Divergences from rg

The first two are checked against the `rg` binary in `tests/test_glob.py` (`test_divergences_from_rg`), so a change in rg shows up:

- **`?` and classes match one character; rg's match one byte.** `rg --files -g '?.md'` doesn't list `é.md`, minifs does; `[!a]` likewise matches `é` whole. Postgres matches characters, so this is deliberate.
- **A class never matches `/`.** rg's classes can, even in an unanchored pattern: negated ones (`rg --files -g 'a[!x]b'` lists `a/b`), ranges spanning `/` (`-g '/a[.-0]b'`) and an explicit `/` (`[/]`, `[x/]`; in minifs `[/]` matches nothing). As in rg, a `/` inside a class still anchors the pattern. Keeping classes inside one segment keeps them consistent with `*` and `?`, and keeps the pruning hints simple.
- **A leading `!` or `#` in an include is an error** rather than a negation or a comment: rg's `-g '#x'` is ignored, so it lists every file. In an exclude, which is what follows rg's `-g '!'`, they are ordinary characters, as in rg.
- **An empty or whitespace-only pattern is an error**, and so is `/`. As an include, rg ignores such a pattern and lists every file. As an exclude, `-g '!'` and `-g '! '` exclude everything and `-g '!/'` changes nothing.
- **For `Glob`, a pattern ending in `/` is an error**: in rg it matches only directories, so it lists nothing.
- **globset version.** minifs uses globset 0.4.18 or later, which accepts nested braces (`{a,{b,c}}`) and rejects a stray `}`. rg 14.1 (what apt installs) bundles an older globset and does the reverse: it rejects nested braces (`nested alternate groups are not allowed`) and drops a stray `}` (`rg --files -g 'a}'` lists `a`, not `a}`). Both are checked against the binary when it is rg 14.

POSIX classes such as `[[:alpha:]]` aren't supported, as in rg: that is a class of `[`, `:`, `a`, `l`, `p`, `h` followed by a literal `]`.

### Pruning hints

| Hint | Example | Effect |
| --- | --- | --- |
| Literal prefix | `docs/guides/**/*.md` gives `docs/guides` | `vfs_glob` resolves that directory and walks only its subtree. If it doesn't exist or is a file, the result is empty, not an error. A prefix through a hidden directory (for `Glob`) or an excluded one also gives an empty result, as in rg. Paths stay relative to the search root, so results don't depend on the hint. The complete directories of the longest literal every match starts with (regex-syntax's prefix extractor), so for alternatives, the prefix they share. |
| Max depth | `src/*/*.py` gives 2 below `src` | Stop the recursion at that depth: the most `/` a match can contain, plus one, minus the prefix. Only when that is bounded (no `**` segment; a `**` inside a segment is a `*`). |
| Name filter | Last segment `*.md` gives `LIKE '%.md'`, `*.{py,md}` gives `'%.%'` | A cheap filter on the name before the full regex: the regex after the last place a `/` can match, with a class as `_` and a repetition or an alternation as `%` (and a `%` in front when the name can start earlier). |

### The SQL

```sql
vfs_glob(
    p_root uuid,                    -- the search root (NULL: "/"); paths are relative to it
    p_regex text,                   -- glob_to_regex(pattern)
    p_name_like text DEFAULT NULL,  -- hint
    p_max_depth int DEFAULT NULL,   -- hint, counted below the prefix
    p_excludes text[] DEFAULT '{}', -- exclude regexes: matching files are dropped, matching directories not walked
    p_hidden boolean DEFAULT false, -- walk directories whose names start with '.'
    p_limit int DEFAULT NULL,
    p_prefix text[] DEFAULT '{}',   -- hint: literal leading directories
    p_prune_dirs text[] DEFAULT '{}'  -- directory names never walked; '.git' always is
) RETURNS TABLE (id uuid, path text, size bigint, mtime timestamptz)   -- files only, ORDER BY mtime DESC, path
```

It is a recursive CTE over the parent pointers (migration `006_vfs_glob.sql`). Each directory level is an index lookup on `UNIQUE (parent_id, name)`: the walk seeds from the root's entries or the start directory's in two branches, never `parent_id IS NOT DISTINCT FROM`, which can't use the index. Directory rules (pruned name, hidden, excluded path) and the name filter apply inside the recursion, so skipped directories are never expanded; the regex and file excludes apply once at the end. Paths are `COLLATE "C"`, like `name`, so ordering and matching don't depend on the locale. ENOENT if `p_root` doesn't exist, ENOTDIR if it is a file.

In Python, `minifs.vfs.glob.glob(conn, root, pattern, excludes=..., hidden=..., limit=...)` compiles the pattern and excludes and calls `vfs_glob`. Excludes of the form `**/<name>/**` or `<name>/`, which covers all the defaults, become `p_prune_dirs` instead of regexes, so those directories are never walked. A smoke benchmark finds `src/**/*.py` in a 10k-node tree in about 15 ms. For trees of 100k+ nodes, the literal-prefix and depth hints keep the walk small, and an optional trigram index on `vfs_nodes.name` (`pg_trgm`) speeds up `*foo*` patterns.

The tests compare the translator with rg's own matcher (the `ignore` crate's overrides, behind the extension's `oracle` feature) on generated patterns and paths, and its rejections with globset's; check that oracle against the `rg` binary on a real directory tree, and the rejection messages and the divergences above against `rg` itself; check that every path a regex matches satisfies its hints; compare Postgres's matching with Python's on the same regexes in every locale; and compare `vfs_glob` with all hints against no hints.

## Search (rg in Postgres)

`Grep` runs entirely in Postgres and scans the current bytes of the files in scope, extent by extent, the way ripgrep scans a disk. No search index is kept in step with writes, so a search sees every committed write, even one from a millisecond ago. There's no local mirror, no sync and no `rg` binary.

### Why there's no line index

An earlier draft kept every line in a `vfs_lines` table with a trigram index. It paid for that on every write: storage of 2 to 4 times the text, a trigger on each append, a full re-index on each overwrite, and a rule that writers never split a UTF-8 character across appends. The only gain was index lookups, which matter once the bytes in scope get large. The target is workspaces of files from kilobytes to a few megabytes, where a scan is enough. If measurements say otherwise, an index can be added without changing the storage model (see Open decisions).

Line-aware extents aren't a line table either. There's no row per line and no index on content: each extent carries its first line number (`line_start`) and where its lines start (`line_offsets`), an 8-byte integer plus 4 bytes per line, about a tenth of typical source text before compression. The writing statement computes them with `vfs_cut` as it cuts the content, so there's no trigger, and compaction only concatenates them. Writers needn't care where an append ends: the cut keeps every extent on whole lines.

### Lines exist only inside the query

ripgrep matches line by line, and models write patterns that assume it: `^def `, `foo$`, `.*`. The search keeps those semantics without storing lines.

1. **Scope.** `vfs_glob` picks the files (see Glob) and reads their sizes from `vfs_nodes`, before any content is touched.
2. **Extent filter.** Each extent of a file's current generation holds whole lines and never splits a UTF-8 character, so it is decoded on its own and tested once against the pattern in newline-sensitive mode (`(?n)`). In that mode `^` and `$` match at line breaks, and `.` and `[^...]` don't cross them. An extent passes exactly when one of its lines matches, since a line never spans two extents and the translator keeps `\n` out of every class.
3. **Line check.** Only the extents that pass are split on `\n`, and each line is tested on its own. This step is exact and decides the results. Line k of an extent is file line `line_start + k`, starting at file byte `byte_offset + line_offsets[k]`.

### The search query

```sql
-- in one REPEATABLE READ transaction, so both steps see one snapshot:
SET LOCAL statement_timeout = '2s';

-- step 1: the scope, from vfs_nodes only; the client refuses it if it's over either cap
SELECT count(*) AS files, coalesce(sum(size), 0) AS bytes,
       array_agg(id) AS ids, array_agg(path) AS paths
  FROM vfs_glob($root, $glob_regex, $name_like, $depth, $excludes, true, $file_cap + 1, $prefix, $prune_dirs);  -- hidden included

-- steps 2 and 3: vfs_search($ids, $paths, $file_pattern, $line_pattern, $fixed, $max_plus_one, 2000), which does,
-- for each file in path order, until it has the hits it needs:
--   skip the file if any extent fails convert_from(data, 'UTF8')   (one subtransaction and one decode per file)
--   for each extent in line_start order:                            (FETCHed one at a time from a cursor)
--     body := convert_from(data, 'UTF8')
--     continue unless body (without its final \n) ~ $file_pattern  -- step 2: '(?n)' plus the translated pattern
SELECT $path, x.line_start + l.k AS line_no, x.byte_offset + l.off AS byte_offset, l.text
  FROM ROWS FROM (string_to_table(x.body, E'\n'), unnest(x.line_offsets))  -- step 3: lines exist only here,
       WITH ORDINALITY AS l(text, off, k)                                  -- zipped with their offsets
 WHERE l.off IS NOT NULL                            -- no empty piece after the final \n
   AND l.text ~ $line_pattern                       -- always ~: case folding is in the pattern
 ORDER BY l.k
 LIMIT $hits_still_needed;
```

- **Trailing newline.** Each extent yields exactly its own `cardinality(line_offsets)` lines: the empty piece after its final `\n` is dropped, so a file ending in a newline doesn't gain an empty phantom line that `^$` would match. The filter tests the extent without its final `\n`, for the same reason.
- **Cut lines.** `vfs_search` also cuts each hit's line to 2,000 characters by default and reports whether it did.
- **Byte offsets.** Each hit's offset comes from `line_offsets`, in the same currency as `vfs.pread(offset=...)`. The `Grep` tool shows only line numbers.
- **One snapshot.** The statements run in one `REPEATABLE READ` transaction, so the scope and every extent are read as of the same moment.
- **Caps before content.** Step 1 reads only `vfs_nodes`. The client checks the file and byte caps on its result, so an oversized scope is refused before any extent is read. The search costs two round trips.

The `Grep` tool takes only ladderframe's parameters: `pattern`, `path` (a directory, or a single file, which searches just that file) and `include`. The other rg features are options of the library's `VFSClient.grep` (`minifs.vfs.search`); a model gets case-insensitive search by writing `(?i)` in the pattern.

| rg feature | Implementation |
| --- | --- |
| `-i` | The translator case-folds the pattern itself (`[Ee]`), so both steps still use `~`. ARE's `~*` would depend on the database's locale (see Regex dialect). |
| `-F` (fixed strings) | `strpos(body, $s) > 0` for the file filter and `strpos(text, $s) > 0` for lines. With `-i` too, the escaped literal goes through the translator instead. |
| `-C n` (context) | Not in v1. Would keep the lines `line_no BETWEEN hit - n AND hit + n`, reading neighbouring extents for context that crosses one |
| `-g` / `--glob` (the tool's `include`) | Through the `vfs_glob` scope |
| `--max-count` | Not in v1. Would be `row_number() OVER (PARTITION BY path)` plus a filter |
| Byte offset per hit | `byte_offset + line_offsets[k]` (see above), which `vfs.pread(offset=...)` accepts directly |
| `-U` (multiline) | Not supported in v1. A literal `\n` in the pattern is rejected, as rg does without `-U`. |

### Cost and limits

- Work grows with the bytes scanned, not with the number of matches. The extent filter costs one regex pass over the bytes, and only matching extents (about 64 KB each, or one longer line) are split into lines. Files are searched in path order, each in line order, and the scan stops once it has the hits it needs, so a common pattern reads only the first extents, while one with few hits reads the whole scope (about 0.7 s for 45 MB on a laptop).
- The total size of the scope comes from `vfs_nodes` before any content is read. A search over more than a configured cap (for example 50 MB) is refused with `ModelRetry: Search scope is 120 MB; narrow the path or include pattern`.
- Every search runs under `SET LOCAL statement_timeout`.
- A file with content that isn't valid UTF-8 is skipped whole, so the result doesn't depend on how its bytes are cut into extents. Each file is checked once before its extents are searched, which costs one pass over its bytes. As in rg, a UTF-8 byte order mark at the start of a file isn't part of its first line (that line's byte offset is then 3). It's still readable with `vfs.pread`, and with `Read` unless it looks binary.

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

The example is a `Grep` with `path="/"`; the default path is `/workspace`. A search with no hits returns `No files found`. A trailing `\r` is dropped from each hit line, as in ladderframe, and lines longer than 2,000 characters are cut, with `...` appended. Files the agent's policy doesn't let it read (a `/workspace/**/.env` rule without `read`, say) are left out of the search before it runs, so they never match and count toward neither the limit nor the byte cap; they still count toward the file cap and the walk's time, so a search refused over the cap, or timed out while listing, can tell that unreadable files exist. A `path` the agent may not read returns `Permission denied: read <path>`; a rule on a directory alone (`/workspace/secret`, no `/**`) doesn't cover the files below it, as for `Glob` and `Read`.

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

## Agent tools

minifs ships pydantic-ai tools with ladderframe's (opencode's) contracts, in `minifs.tools` behind the `minifs[pydantic-ai]` extra: the same names, parameters, descriptions, output formats and error handling as ladderframe's `Read`, `Write`, `Edit`, `Glob` and `Grep` (`packages/harness/src/ladderframe/tools`). ladderframe's tools track opencode's, which mirror Claude Code's, so models apply what they already practice, and an agent moves between a workspace on disk and minifs without prompt changes. Nothing in minifs imports ladderframe: ladderframe is a pydantic-ai harness, so it uses these tools like any other pydantic-ai project. The tools are plain `async def` functions over a `VFSView`, each also wrapped in a `pydantic_ai.Tool`.

| Tool | Signature | Backed by |
| --- | --- | --- |
| Read | `Read(filePath, offset, limit)`: numbered lines of a file, or the entries of a directory | `vfs_pread_lines`, `vfs_pread` (the binary sample, attachments); `vfs_readdir` for a directory; a stat (`vfs_nodes`) for instruction files |
| Glob | `Glob(pattern, path)`: absolute paths, at most 100 | `vfs_glob` over `vfs_nodes` |
| Grep | `Grep(pattern, path, include)`: hits grouped by file, at most 100 | `vfs_glob` and a scan of `vfs_extents` |
| Write | `Write(content, filePath)` | `vfs_create`, or `vfs_pread` and a compare-and-swap `vfs_write`, subject to the write policy |
| Edit | `Edit(filePath, oldString, newString, replaceAll)` | `vfs_pread` and a compare-and-swap `vfs_write`, subject to the write policy |

Paths are absolute in the agent's chroot view; a relative path resolves against the deps' `vfs_workdir`, `/workspace` (the agent's working directory) by default. Options without a ladderframe parameter (Glob's `exclude`, Grep's `ignore_case`, `fixed_strings`, `context` and `max_count`, byte offsets) stay in the library.

**Write.** ladderframe's `Write(content, filePath)`, which follows opencode's `write`: it creates the file and any missing parent directories, or replaces the whole file as a new generation. Details:

- **No read-before-write check.** The description tells the model to `Read` an existing file first, as ladderframe's does, but neither checks it. What the VFS adds is a compare-and-swap: `Write` stats the file, reads its old text for the diff (decoded as UTF-8 with replacement characters, so a file that isn't UTF-8 is still overwritten), then writes with `VFSView.write(expect=<the version read>)`, which `vfs_write` checks under the inode lock (`ESTALE` if another writer got in between). ladderframe holds a per-path thread lock instead, which doesn't reach other processes.
- **Lost races are tried once more.** Another writer getting in between two steps (the file changed after its stat, a create that finds the file just created by someone else, a file removed or replaced by a directory before its overwrite) starts the call over from the stat, once: `Write` replaces the whole file, so starting over is safe, and the diff is against what was actually overwritten. A second loss, of whatever kind, is the `ModelRetry` `<path> was modified since it was read. Read it again before writing.`
- **New files.** `VFSView.create` with the content, without `parents` first and with it only if that is ENOENT (the parent is missing, or was just removed): the view checks `write` on every directory a `parents` create could make, existing or not, so passing it always would refuse files below a directory the policy only lets the agent write into.
- **BOM and metadata.** An existing file's UTF-8 BOM is kept (as is one at the start of `content`). The result is `Wrote file successfully.`, with ladderframe's metadata: `filepath`, `exists`, `diff` (a jsdiff-style patch, its common indentation trimmed), `additions` and `deletions`.
- **Errors.** A path outside the view, or below a file, is the `ModelRetry` `File not found: <path>`. Inside it, as in ladderframe, the write permission comes before the directory check and before the old text is read: a write the policy denies (to `/docs/**`, to `/` or `/docs` themselves, or to the append-only `/workspace/log.md`) is the returned string `Permission denied: write <path>`, and reads nothing. When the file is allowed but `parents` would create a directory the agent may not write, `<path>` is the first directory refused between the mount point and the file. A directory the agent may write is the `ModelRetry` `Path is a directory, not a file: <path>`. A rule granting `write` without `read` lets `Write` create files, but an existing one is `Permission denied: read <path>`, since its old text can't be read for the diff.
- **Sequential.** `Write` is a `sequential` tool, as is `Edit`: pydantic-ai runs no other tool call of the same model response alongside it.

**Edit.** ladderframe's `Edit(filePath, oldString, newString, replaceAll)` (`minifs/tools/edit.py`), which follows opencode's `edit`: the same description, opencode's fallback replacers ported unchanged from ladderframe (`simple`, `line_trimmed`, `block_anchor`, `whitespace_normalized`, `indentation_flexible`, `escape_normalized`, `trimmed_boundary`, `context_aware`, `multi_occurrence`, with `replace()` and `_is_disproportionate_match`), and the same messages. Details:

- **Read, replace, compare-and-swap.** `Edit` stats the file, reads all of it with its version (`VFSView.pread` in pages, `ESTALE` if it changes between pages), replaces in Python, then writes with `VFSView.write(expect=<the version read>)`. A lost race (the file changed, removed or replaced by a directory after the read, or an empty-`oldString` create beaten to the file) redoes the whole read-replace-write once, since the old string may have moved: if it's gone, the result is the replacer's `Could not find oldString` retry. A second loss is the `ModelRetry` `<path> was modified since it was read. Read it again before writing.` ladderframe holds a per-path thread lock instead.
- **Text.** The file is decoded as strict UTF-8: anything else is `ModelRetry: Cannot edit <path>: it is not a UTF-8 text file`, as in ladderframe, since a rewrite would corrupt it. The file's line endings (CRLF if it has any) are applied to `oldString` and `newString`, and its BOM is kept. An empty `oldString` creates the file (with missing parents, as `Write` does); on an existing file it is the `EMPTY_OLD_STRING` retry. `oldString == newString` is `No changes to apply: oldString and newString are identical.`
- **Size cap.** A file over `EditSettings.max_bytes` (10 MB) is `ModelRetry: Cannot edit <path>: it is <n> bytes, over the <max>-byte limit for Edit. Use the Write tool to replace the whole file instead.`, since `Edit` holds the whole file in memory. A file that grew past it since the stat is refused after the first page. The replacers and the diff run in a worker thread (`asyncio.to_thread`), as pydantic-ai runs ladderframe's sync `Edit`: a miss on a file of a few MB takes seconds, which would otherwise stall the event loop.
- **Result.** `Edit applied successfully.`, with ladderframe's metadata: `filepath`, `diff` (over the LF-normalized text), `additions` and `deletions`.
- **Errors.** A missing file, a path below a file, and a path outside the view are `ModelRetry: File <path> not found`, ladderframe's wording. A missing file is not found even where the policy would deny the write (`/docs/missing.md`): there is nothing to edit. Otherwise, as in ladderframe, the permission comes first: a denied write (`/docs/**`, the append-only `/workspace/log.md`) is the returned `Permission denied: write <path>`, and reads nothing; then an empty `oldString` on an existing path; then `Path is a directory, not a file: <path>`. A rule granting `write` without `read` lets an empty `oldString` create a file, but without `read` a non-empty `oldString` is `Permission denied: read <path>`, whether or not the file exists (nothing is statted, so existence isn't revealed).

### Glob

`Glob(pattern, path)` (`minifs/tools/glob.py`) is ladderframe's `Glob` over `VFSView.glob`: the same description (less its line about the Agent tool), parameters and output. `path` defaults to the working directory and resolves against it. The result is the matching files' visible absolute paths, one per line, newest first, or `No files found`. Past `GlobSettings.limit` (100) matches, it keeps the first 100 and adds, after a blank line, `(Results are truncated: showing first 100 results. Consider using a more specific path or pattern.)` with the actual limit. Exactly `limit` matches get no footer.

- **Errors.** A `path` that is a file is `ModelRetry: glob path must be a directory: <path>`. A missing one, one below a file and one outside the view are all `ModelRetry: No such directory: <path>`. A bad pattern is a `ModelRetry` with rg's message, `error parsing glob '<pattern>': <reason>`; inside the view it is reported before a bad `path`. From a virtual directory such as `/`, a pattern that can't be restated in each mount is `ModelRetry: <path>: '<pattern>' spans mount points here: glob from inside a mount` (see `VFSView.glob`). A bad `GlobSettings` limit or exclude fails when the settings are built; an exclude must also avoid the forms that can't be restated in each mount (see `GlobSettings.excludes`), or every glob from `/` is that retry. A `path` the policy doesn't let the agent read returns `Permission denied: read <path>`.
- **What is skipped.** As `rg --files`: hidden directories are never descended into, and a hidden file is listed when the pattern matches it. `**/.git/**` (`ALWAYS_EXCLUDED`) is always skipped, and the caches in `GlobSettings.excludes` (`DEFAULT_EXCLUDES`: `node_modules`, `.venv`, `__pycache__`, `.mypy_cache`) by default, in place of ladderframe's `.gitignore` handling (see Differences from ladderframe). Files the policy hides are left out and don't count toward the limit.
- **Patterns differ from rg's** in a few corners, all listed in Glob, Divergences from rg: `?` matches a character, not a byte; a class never matches `/`; a leading `!` or `#`, an empty pattern, `/` and a trailing `/` are errors rather than a negation, a comment, every file or nothing; and globset 0.4.18 accepts nested braces and rejects a stray `}`, where rg 14.1 does the reverse. As in rg, POSIX classes aren't supported and an empty brace alternative is dropped (`a{b,}` is `ab`). None of these is in the tool's description: a model rarely writes them, and the error says what's wrong.
- **A truncation the tool can't see.** When the policy hides files, the view looks a bounded distance past the limit (8 times it) for one more readable match. If hidden files fill that whole window, the result has no footer, though readable matches may lie past it (see `VFSView.glob`).

### Differences from ladderframe

ladderframe's tools run over a disk, `Glob` and `Grep` through ripgrep; minifs implements the same contracts in Postgres. Each tool's differences, by design, gathered here (the tool subsections above and the Read contract give the details):

**Every tool**

- **No ladderframe imports, so none of its machinery.** The tools are plain pydantic-ai `Tool`s: no `@tool` metadata, `HarnessDeps`, YAML `tool_settings` or entry points. Settings come from the deps' `vfs_settings`, the working directory from `vfs_workdir`.
- **The policy is the only permission gate.** ladderframe asks its permission guard per call; minifs checks the view's write policy (see Actor and policy). A denial is the tool's result, not a retry, as with ladderframe's guard, but worded differently: `Permission denied: <action> <path>`, where ladderframe says `Permission denied: <Tool> for '<path>' is not allowed by the agent's permission rules.`
- **Paths are the view's.** A path outside the chroot is each tool's not-found error, `..` stops at `/`, `/` is a virtual directory listing the mounts, and there is no `~` expansion.
- **Descriptions.** ladderframe's text, less what minifs doesn't have: "the local filesystem" becomes "the filesystem", and the lines about Bash with `rg` and the Agent tool are dropped. `Grep`'s adds one line: use `(?i)` for case-insensitive search; lookaround and backreferences aren't supported.

**Read**

- **Read sees the view.** A path the policy refuses is `Permission denied: read <path>` whether or not anything is there, so a denial doesn't tell what exists. Directory listings, `Did you mean` suggestions and instruction files leave out what the actor may not read (an unreadable `AGENTS.md` falls back to `CLAUDE.md`). ladderframe also carries already-shown instruction files across a Temporal activity; minifs has no Temporal integration, so a run context without `messages` repeats them on every `Read`.

**Write and Edit**

- **A compare-and-swap, not a lock.** ladderframe serialises writers of a path with a per-path thread lock, which doesn't reach other processes. minifs writes with `VFSView.write(expect=<the version read>)`, checked under the inode lock, so a writer anywhere is detected; a lost race is redone once from the stat, and a second loss is the retry `<path> was modified since it was read. Read it again before writing.` ladderframe never sees that message.
- **`Edit` has a size cap.** A file over `EditSettings.max_bytes` (10 MB) is refused with a retry pointing at `Write`, since `Edit` holds the whole file in memory. ladderframe has no cap.
- Neither checks that the model read a file before overwriting it, as ladderframe doesn't.

**Glob and Grep**

- **No `.gitignore`.** ladderframe's `Glob` and `Grep` both honour `.gitignore`, `.ignore` and git's excludes, as rg does by default. minifs has no ignore files. In their place, `Glob` and `Grep` apply a default exclude list limited to unambiguous caches (`**/node_modules/**`, `**/.venv/**`, `**/__pycache__/**`, `**/.mypy_cache/**`), configurable per workspace. As in rg, excludes match each path relative to the search root: a `path` inside an excluded directory is searched, and excluded directories below `path` are still skipped. Excluded directories are pruned from the walk (see Glob), so they cost nothing. `**/.git/**` is always excluded, outside the configurable list, as ladderframe hard-codes it. Hidden files follow ladderframe (see Glob). Honouring `.gitignore` files stored in the tree can come later.
- **ARE underneath.** Patterns are parsed as rg parses them and translated to Postgres ARE (see Regex dialect). A few constructs rg accepts are refused with a `ModelRetry` that names them: byte classes outside ASCII, repetition counts above 255, and patterns whose translation exceeds the size cap.
- **mtime order.** `Glob` returns paths newest first, as opencode does; ladderframe's `Glob` and `Grep` return them in rg's walk order, which varies between runs because rg searches in parallel. `Grep` orders hits by path, then line. Above the result limit, minifs returns the first 100 in its order, where ladderframe returns an arbitrary 100.
- **Non-UTF-8 files aren't searched.** rg searches them and ladderframe decodes hits with replacement characters; minifs's `Grep` skips them (see Search).
- **Guard rails.** minifs refuses searches over the file cap or byte cap, and stops each of a search's two statements after 2 seconds. ladderframe's `Grep` has no such limits.

### Package layout

```
minifs/
  vfs/
    core.py      Entry, ReadResult, VFSError, path normalization
    view.py      VFSView: the chroot (mounts) with the write policy enforced
    client.py    thin async wrappers over the SQL functions
    policy.py    per-actor write rules
    glob.py      glob translator and pruning hints
    regex.py     thin wrapper over minifs._regex, with the translation cache
    watch.py     LISTEN vfs and async iterators per path
    cache.py     directory-entry cache, content and line-map cache, NOTIFY eviction
    service.py   open_vfs: a process's pools, Watcher and directory cache, opened and closed in order
    cursor.py    Cursor and read_from: generation-aware tailing for library callers
  tools/         pydantic-ai tools (the pydantic-ai extra), with ladderframe's contracts
    deps.py      HasVFS (the deps protocol), ToolSettings, resolve
    errors.py    VFSError -> ModelRetry, or the returned denial
    base.py      vfs_tool: a function as a pydantic_ai.Tool with ladderframe's description
    read.py, write.py, edit.py, glob.py, grep.py
  db/
    pool.py      psycopg pool and credential refresh
    schema.sql
crates/
  minifs-regex/  Rust: regex-syntax HIR to ARE, built as minifs._regex (PyO3, maturin)
```

### Using the tools with pydantic-ai

**Install.** `pip install 'minifs[pydantic-ai]'` adds `pydantic-ai-slim`, which `minifs.tools` needs (`minifs.vfs` and `minifs.db` work without it, and importing `minifs.tools` without it raises an `ImportError` that names the extra). Add `minifs[binary]` for a bundled libpq, and your model provider's extra (`pydantic-ai-slim[anthropic]`, say). Run `minifs db migrate` once per database.

**A minimal agent.** This is `examples/agent.py` below its docstring, which `tests/tools/test_example.py` runs, and the snippet runs as pasted. `examples/provision.py` migrates a database and seeds a demo workspace for it.

<!-- examples/agent.py -->
```python
import logging
from dataclasses import dataclass

from pydantic_ai import Agent
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


async def main(dsn: str, workspace_id: str, prompt: str, model: Model | str) -> str:
    # On Lakebase: Connector(conninfo, password=fetch_token), called for each new connection.
    cache = DirEntryCache()  # one client and cache per process, shared by every run
    async with open_vfs(Connector(dsn), cache=cache) as client:
        view = VFSView(client, workspace_mounts(workspace_id), DEFAULT_POLICY, "agent")  # one view per run
        result = await agent.run(prompt, deps=Deps(vfs=view), model=model)
    logging.info("directory cache: %s", cache.stats())
    return result.output
```

- **One agent, one client and cache per process, a view per run.** The `Agent` holds no workspace: each run's deps carry a `VFSView` of its own, over the process's one client, and the model can be given per run. `open_vfs` opens the write and search pools (see Operations) and, given a cache, the process's `Watcher` (its one LISTEN connection) that the cache follows, and closes them in order on exit.
- **Lakebase.** `Connector(conninfo, password=fetch_token)` calls `fetch_token` (sync or async) for every new connection, the pools' and the watcher's reconnects alike, since its OAuth tokens expire after an hour. LISTEN needs a direct connection: if `conninfo` goes through a transaction-mode pooler, pass a direct `listen=Connector(...)` to `open_vfs`, or notifications stop without an error and the cache falls back to its TTL. A following cache keeps that connection open, and its keepalive probe keeps the compute awake; `open_vfs(..., min_size=0)` lets idle pools hold no connections.
- **Running it.** `uv run python examples/agent.py <workspace-id> "<prompt>"` uses `$MINIFS_MODEL` (any pydantic-ai model name, `openrouter:<slug>` with `OPENROUTER_API_KEY` for instance), and otherwise pydantic-ai's `TestModel`, offline. It logs `cache.stats()` at the end, and with logfire installed (`--with 'pydantic-ai-slim[logfire]'`) prints every model and tool call to the console (`logfire.configure(send_to_logfire=False)`, `logfire.instrument_pydantic_ai()`).

**Deps.** The tools take `RunContext[HasVFS]`, where `HasVFS` is a protocol whose only member is `vfs: VFSView`, so the caller's own deps dataclass works as long as it has a `vfs`. Two optional attributes are read with defaults: `vfs_workdir` (`/workspace`), where relative paths resolve, and `vfs_settings`, a `ToolSettings` with ladderframe's limits (`Read` 2,000 lines of at most 2,000 characters and 50 KB, `Glob` and `Grep` 100 results, `Grep` lines cut at 2,000 characters and a 2 s timeout, `Edit` files up to 10 MB):

```python
@dataclass
class Deps:
    vfs: VFSView
    vfs_workdir: str = "/workspace"                                             # optional
    vfs_settings: ToolSettings = ToolSettings(grep=GrepSettings(timeout=5.0))  # optional
```

**Tools or a toolset.** Pass the tools, `Agent(model, deps_type=Deps, tools=[Read, Write, Edit, Glob, Grep])` (`minifs.tools.TOOLS` is the same tuple), or the toolset, `Agent(model, deps_type=Deps, toolsets=[toolset()])`. `retries=3` matches ladderframe's `tool_retries`.

**The directory cache: on in a service, off for one-shot scripts.** The cache maps `(parent_id, name) -> (id, kind)` (see Caching). A path this process has already looked up, listed or created resolves from it on its next use, every link checked against `vfs_nodes` in the statement that uses it, so a stale entry costs a slower lookup, never the wrong file. A path seen for the first time, from a `Glob` or `Grep` result say, is one server walk, which then caches every component. NOTIFY evicts another process's renames and deletes as they commit, and a 2 s TTL covers what notifications can't. Leave it off (`open_vfs(connector)` with no `cache`) where nothing lives long enough to reuse it: a one-shot script such as `examples/provision.py`, a test, the `minifs` CLI. There the Watcher's extra connection, and the clear on entering `follow`, buy nothing.

`CacheStats` counts path components, not paths, and a path skips its walk only when all of its components hit. With a real model an agent's calls are seconds apart, one inference each, so entries also expire between calls: a run of the example on a hosted model hit 17 of 32 component lookups, with 4 misses expired, where a scripted run with no inference between calls hits 40 of 42. A longer `ttl` (`DirEntryCache(ttl=...)`) keeps more without risking correctness, since every use is checked.

**Actor and policy: agents vs. ingestion.** The view's `actor` picks its rules in the policy (see Filesystem tree and conventions). Under `DEFAULT_POLICY`, `"agent"` reads and writes its own `/workspace`, except `log.md`, which it may read and append but not overwrite (the tools never append; the harness appends the agent's log entries with `vfs.append`, acting as the agent), and only reads `/docs`. Ingestion jobs run as `"ingestion"`, which writes `/docs` and has no access to any workspace. The policy is the tools' only permission gate: a denied call returns `Permission denied: <action> <path>` as the tool's result rather than a retry. For other rules, build a `Policy({"agent": [Rule("/workspace/**/.env"), Rule("/workspace/**", "read", "write"), ...]})`: rules are ordered, the first match wins, and a rule with no actions hides the path. Seeding a workspace, as `examples/provision.py` does, is an operator's job, through the client or an `"operator"` view, not through the agent's.

**Multi-workspace servers.** `workspace_mounts(workspace_id)` mounts one workspace at `/workspace` and the shared `/docs`; a view never reaches another workspace (`..` stops at `/`, another workspace's real path is `File not found`). The views of concurrent runs share the client, its pools and its cache, so the cost per run is a `VFSView` object; size the pools (`open_vfs(..., write_size=, search_size=)`) for the runs a process serves at once, since a tool call holds one to three connections while it runs.

Each tool is a plain function and a prebuilt `Tool` carrying ladderframe's description:

```python
async def read(ctx: RunContext[HasVFS], filePath: str, offset: int | None = None, limit: int | None = None) -> str:
    """Read a file or directory.

    Args:
        filePath: The absolute path to the file or directory to read
        offset: The line number to start reading from (1-indexed)
        limit: The maximum number of lines to read (defaults to 2000)
    """
    ...

Read = vfs_tool(read, "Read", DESCRIPTION)
```

The tool descriptions start from ladderframe's text, so a model sees the same instructions on disk and in minifs. minifs drops the lines that point to tools or features it doesn't have (Bash with `rg`, the Agent tool, "the local filesystem"). It adds one line to `Grep`'s description: use `(?i)` for case-insensitive search; lookaround and backreferences aren't supported.

**Errors follow ladderframe.** A mistake the model can fix by calling again raises `ModelRetry`, with a message that says what to change: a missing path (`File not found: /workspace/notes.md`, with up to three similar names), an offset past the end, a binary file, a `Glob` path that isn't a directory (`glob path must be a directory: ...`, `No such directory: ...`), a missing `Grep` path (`No such file or directory: ...`) or empty pattern (`pattern is required`), a pattern the translator rejects (ripgrep's message and position), a search scope over the cap, or a timeout. A path outside the agent's view gets each tool's not-found error (`File not found` for `Read` and `Write`, `No such directory` for `Glob`, `No such file or directory` for `Grep`), and `Read` never suggests similar names from outside the view, so errors don't reveal what exists beyond the chroot. A permission denial (the write policy) is returned as a string, as ladderframe's permission guard does: retrying won't help, and the model reads the refusal and moves on. Each tool maps `VFSError` to one of the two at its boundary with `minifs.tools.errors.tool_error`, and nothing else escapes: `EACCES` becomes the result `Permission denied: <action> <path>`, `ESTALE` the retry `<path> was modified since it was read. Read it again before writing.`, a database error `Internal error reading <path>`, and a refused search its own message.

**Pre-fill reads that happen every time.** Each tool call costs a full extra LLM inference: the model stops, waits for the result, then reprocesses its whole context. The database query takes milliseconds; the extra inference takes roughly 0.5 to 1.5 s. If a harness always reads the same file at the start of a run (for example `notes.md`), it can run that `Read` itself, through the same tool function, and put the result in the prompt as if the agent had run it. A read that depends on what the agent is thinking stays a tool call.

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
| TOAST | `vfs_extents.data` returns to the default `STORAGE EXTENDED` (migration 004 had set `EXTERNAL`): extents are about 64 KB unless one line is longer, and are read whole, so they're stored compressed. Keep `vfs_compact`'s target near the default for the same reason. |
| Vacuum | Appends leave a dead `vfs_nodes` row version each; overwrites and compaction leave dead extents. `fillfactor = 80` keeps the inode updates HOT, so they're pruned in place. Pruning only removes versions no open transaction still needs: a long or idle-in-transaction session in the database, a prepared transaction, or a standby with `hot_standby_feedback`. While one is open (a slow search, an ANALYZE), a hot file's appends use up that free space, and the next ones are non-HOT updates to another page. Watch `n_dead_tup` on both tables, and lower autovacuum's scale factor on `vfs_extents` if overwrites are frequent. |
| Compaction | A periodic job runs `vfs_compact` on files with more than 1,000 extents. It holds each inode lock only while merging that file. |
| Postgres version | PG 15+ for `UNIQUE NULLS NOT DISTINCT`. PG 18 has a built-in `uuidv7()`; on older versions use `pg_uuidv7` where available, or generate UUIDv7 in the app. |
| Extensions | None required. `pg_trgm` only if the optional search index is added, and `vector` (pgvector) for future semantic search. |
| Pools | Per process: one LISTEN connection (shared by watch and the cache), and a small pool (about 5) for the tools. |

### Protecting the database

- **Every search runs under `SET LOCAL statement_timeout = '2s'`,** which applies to each of its statements (resolving the path, the scope, the scan). On timeout, raise `ModelRetry: Search timed out; narrow the pattern or path`.
- **Cap the files in scope.** The scope query (step 1 of the search) asks for one file more than the cap (for example 10,000). If it gets it, the search is refused with a `ModelRetry` to narrow the `path` or `include`, rather than silently dropping the oldest files.
- **Cap the bytes scanned.** Refuse a search whose scope is larger than the configured cap, with a hint to narrow the `path` or `include`. The size comes from `vfs_nodes`, so the check reads no content.
- **Cap output:** `Grep` and `Glob` return at most 100 results by default, as in ladderframe, each with a "narrow it" hint.
- **Separate pools for search and writes.** If CPU contention appears at scale, consider sending search to a read replica. That gives up read-your-latest-write consistency, so only do it if you need to.

### Capacity, rough estimates

| Item | Estimate |
| --- | --- |
| Append cost | One inode row lock, one extent insert (plus a rewrite of the last extent when the file ended mid-line), one NOTIFY |
| Storage | The text, compressed 3 to 4 times when an extent is over about 2 KB, plus about 40 bytes of row overhead per extent and 4 bytes per line in `line_offsets` |
| Search cost | One regex pass over the bytes in scope, plus a line split of the extents that match |
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
| Read plan | On a file with 10,000+ extents, `EXPLAIN` of a read near the end shows a backward index descent and a bounded forward scan, not a scan from offset 0; on a 20,000-line file, line reads at the start, middle and end touch a few pages | Unit |
| Kinds | Creating a child under a file gives `ENOTDIR`; an extent on a directory is rejected by the foreign key; `vfs.pread` of a directory gives `EISDIR`, and `Read` of one lists its entries | Unit |
| Read tool | Output equals ladderframe's `Read` on the same file written to disk, after mapping the disk path prefix to the VFS path and with no instruction files present, for random `offset` and `limit`, CRLF files, lines over 2,000 characters, the 50 KB cap, no trailing newline and directories | Differential |
| Generations | An overwrite between two `vfs.read_from(cursor)` calls flags the restart; an append doesn't | Unit |
| Tree | `UNIQUE NULLS NOT DISTINCT` rejects duplicate root names; `rmdir` on a non-empty directory gives `ENOTEMPTY`; a move into its own subtree gives `EINVAL` | Unit |
| Claim | Two claimers racing for the same file: exactly one wins | Concurrency |
| Watch | Events arrive only after commit; `move` versus `modify` is classified correctly | Integration |
| Search | Hits match a per-line search in Python over the same files; a trailing newline adds no phantom line; a UTF-8 character split across two appends still matches; files that aren't UTF-8 are skipped; the scope cap refuses before reading content | Integration |
| File filter | Whenever a line matches the line form, the file matches the file form | Property |
| Compaction | Bytes, `size`, `lines`, `generation` and `mtime` are unchanged; appends running during compaction keep gap-free offsets | Concurrency |
| Content cache (deferred) | Reads through the cache equal uncached `pread` while another client appends, overwrites and compacts | Property |
| Directory cache | A rename or delete by another client is seen once its NOTIFY arrives; a dropped LISTEN connection clears the cache; a missing name is never cached | Integration |
| Line-aware extents | After random appends (partial lines, a UTF-8 character split across appends, empty data, lines over 64 KB, no newline at all), overwrites and compactions: the extents tile the file, each non-final one ends with `\n`, `line_start` and `line_offsets` are exact, `lines` is right, and `vfs_pread_lines` from every line equals splitting the content in Python; concurrent appenders stay gap-free and line-aligned | Property |
| Glob translator | Compared with rg's own matcher (the `ignore` crate) on generated patterns and paths, that matcher with the `rg` binary, and the rejections and their messages with rg's | Differential |
| Glob pruning | Results with all hints enabled equal results with none | Differential |
| Regex translator | A corpus of real model patterns plus generated ones, compared with Rust's `regex` crate, in a `C`-locale and a UTF-8 database | Differential |
| Regex rejections | Every pattern the translator rejects at parse time is also rejected by the `rg` binary | Differential |
| Search scope | `vfs.grep(p, include=X)` hits only files that `vfs.glob(X)` returns | Integration |
| Glob and Grep tools | Output equals ladderframe's `Glob` and `Grep` on the same tree written to disk, apart from the documented differences. The disk copy gets a `.gitignore` built from the exclude list as directory patterns (`node_modules/`), path prefixes are mapped, and results are compared as sets, and only when they're under the limit | Differential |
| Agent tools | Retryable mistakes raise `ModelRetry`; permission denials come back as strings; the chroot blocks `..` escapes; the write policy blocks writes outside allowed paths; `Write` loses a compare-and-swap or create race once and succeeds, twice and raises `ModelRetry`; `Write` gives ladderframe's result, metadata and bytes on a corpus | Unit |
| Write check | `vfs_write` with a stale expected generation, size, parent or name raises `ESTALE` and changes nothing; a partial expectation raises `EINVAL` | Unit |
| Evals | A set of recorded coding tasks run against the VFS tools: track task success and tool calls per task | Eval |

## Roadmap

The tool-based VFS ships first. A TigerFS front end follows once the tree and tools are stable.

**What stays frozen.** The ladderframe-style tools: `Read`, `Write`, `Edit`, `Glob` and `Grep`, with ladderframe's signatures, output formats and error handling. The TigerFS work can then replace the VFS client with a thin MCP client, and prompts, tools and evals carry over unchanged.

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
| Line-numbered reads | A `text[]` of lines per file, a row per line, or line-aware extents | **Decided (2026-10-24):** line-aware extents (see Storage model and Line reads). The workload is read-heavy, so lines are found in Postgres at read time from numbers computed at write time, with no client line map. |
| Search index | None (scan), or a trigram GIN index | Scan in v1. If bytes scanned per search grow too large, index the extents' text: they hold whole lines, so a line match never crosses an extent boundary. |
| Write contract | opencode's `write`, or ladderframe's `Write` | **Decided (2026-10-25):** ladderframe's (which follows opencode's), with a compare-and-swap in place of its per-path lock |
| Semantic search | pgvector over windows of file text | After the core tools are stable |

## Sources

- [PostgreSQL 9.7 Pattern Matching](https://www.postgresql.org/docs/current/functions-matching.html): ARE syntax, Perl incompatibilities, and the statement-timeout advice
- [PostgreSQL pg\_trgm](https://www.postgresql.org/docs/current/pgtrgm.html): regex index support, for the optional search index
- [Databricks Apps best practices](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices) and [ECS KernelCapabilities](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_KernelCapabilities.html): examples of platforms without FUSE or root
- [Lakebase extensions](https://docs.databricks.com/gcp/en/oltp/projects/extensions): an example of a managed Postgres extension list
- [regex-syntax](https://docs.rs/regex-syntax) and [regex](https://docs.rs/regex): the parser the translator uses, and the test oracle
- [pgpcre](https://github.com/petere/pgpcre), [re2 for PostgreSQL (PGXN)](https://api.pgxn.org/src/re2/re2-0.4.0/doc/re2.md), [omni\_regex](https://docs.omnigres.org/omni_regex/regex/), [PL/Rust on RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.Using.PL_Rust.html), [PCRE2 pcre2\_convert](https://git.stg.centos.org/source-git/pcre2/blob/c8s/f/src/pcre2_convert.c), [RegexBuddy PostgreSQL](https://www.regexbuddy.com/postgresql.html): the translator survey
- TigerFS repository: `docs/adr/017-relational-directory-structure.md` and `internal/tigerfs/fs/synth/build.go`
