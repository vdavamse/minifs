# minifs: Everything Is a File for Agents

A small POSIX-like virtual filesystem stored entirely in PostgreSQL, built for coding assistants and other LLM agents.

## Overview

An agent sees one filesystem tree, stored entirely in Postgres, and works on it with five tools: `ls`, `read`, `write`, `glob` and `rg`. Source files, notes, logs, working memory and ingested documents are all ordinary files in that tree. Nothing is mounted, so it runs where FUSE and root are unavailable: sandboxes, serverless containers and managed app platforms.

**Goals**

- One storage model and one set of operations for every kind of data. There are no special tables for logs, memory or task queues.
- Claude-Code-style tool semantics (Read with offset and limit, Glob, Grep), which models already use well.
- Incremental reads: an agent can tail a growing file and get only what is new since its last read, using byte offsets.
- Search and glob run inside Postgres, so there's no local mirror, no sync and no `rg` binary.
- PostgreSQL is the single source of truth, and every read reflects the latest committed write.
- Low latency: a tool call is one or two SQL round trips.

**Non-goals**

- Kernel mounts (FUSE or NFS). A later TigerFS integration can provide these where they're allowed (see Roadmap).
- Full POSIX: no hard links, permission bits, xattrs or random-offset writes in v1.
- Large binary files. The design targets text (source code, markdown, logs) from kilobytes to a few megabytes per file.

**In one paragraph:** files are inodes in `vfs_nodes`, and their bytes are append-friendly extents in `vfs_extents`. SQL functions implement append, overwrite, positional read, rename and watch (via NOTIFY). A trigger keeps a line index (`vfs_lines`, with a trigram index) in step with every write, which powers `rg`. A recursive walk over the parent pointers powers `glob`. A Python VFS library wraps these functions, and agent tools (for example PydanticAI tools) wrap the library.

## Context

**Why files?** Models are heavily trained on shell and file work. A small, uniform tool set over one tree is easy to discover with `ls`, needs no new tools when a data source is added, and gives humans the same view when debugging. Every agent, ingestion job and human operator follows one UNIX-style model instead of a set of purpose-built tables.

**Why not a real filesystem?** Many places agents run don't allow one. Managed platforms run apps as a non-privileged user with no system packages and no FUSE ([Databricks Apps best practices](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices)), and AWS Fargate allows only `SYS_PTRACE` to be added ([ECS KernelCapabilities](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_KernelCapabilities.html)). Every kernel-mount approach needs `CAP_SYS_ADMIN` or `/dev/fuse`, so the filesystem lives in the tool layer, not the kernel. Postgres also gives durability, transactions, concurrent access from many agents and change notifications for free.

**What we take from TigerFS.** TigerFS exposes PostgreSQL as a mountable filesystem. Its file-first workspaces already use a parent-pointer directory model (ADR-017: `parent_id`, `filename`, `UNIQUE NULLS NOT DISTINCT`, a one-round-trip `resolve_path`, and a 2-second path cache). Its consistency rule is: never cache content, cache only metadata. This design adopts all of that. It adds extent-based content for appends and offset reads, and replaces kernel mounts with tools.

## Architecture

Any number of clients share one VFS library and one set of SQL functions. Postgres holds every byte, and NOTIFY at commit is the only wake-up signal.

```
  agent tools        ingestion jobs       CLI / humans
 (ls read write        (write docs)        (debugging)
   glob rg)
        \                  |                  /
         \                 |                 /
          +-------- VFS library (Python) ---+
                           |
                    SQL functions
                           |
                      PostgreSQL
        vfs_nodes · vfs_extents · vfs_lines · NOTIFY vfs
```

| Component | Role |
| --- | --- |
| Agent tools | `ls`, `read`, `write`, `glob`, `rg` exposed to the model, with shell-style errors |
| VFS library | Path resolution, the chroot view, write policy, glob and regex translation, and watch |
| SQL functions | Append, overwrite, positional read, resolve, readdir, create, rename, unlink |
| PostgreSQL | Tables, triggers for the line index and NOTIFY |

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
| `/workspace/log.md` | append | — | read |
| `/docs/**` | read | write | read |

**Coordination through rename (Maildir).** When several agents or workers share work, a file's status can be the directory it sits in. Claiming a file is an atomic rename (see Core operations), so exactly one worker wins.

## Storage model

The model has two tables: `vfs_nodes` holds inodes and directory entries, and `vfs_extents` holds file bytes as variable-length extents at byte offsets. An append is one INSERT, an overwrite is a new generation, and a read is a range scan.

```sql
CREATE TABLE vfs_nodes (                         -- inode + directory entry (no hard links)
    id         UUID PRIMARY KEY DEFAULT uuidv7(),
    parent_id  UUID REFERENCES vfs_nodes(id),    -- RESTRICT: rmdir of non-empty dir -> ENOTEMPTY
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('file','dir')),
    size       BIGINT NOT NULL DEFAULT 0,        -- exact; maintained under the inode lock
    generation INT    NOT NULL DEFAULT 0,        -- +1 on overwrite/truncate; appends keep it
    ctime      TIMESTAMPTZ NOT NULL DEFAULT now(),
    mtime      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (parent_id, name)
);

CREATE TABLE vfs_extents (                       -- data blocks
    node_id     UUID   NOT NULL REFERENCES vfs_nodes(id) ON DELETE CASCADE,
    generation  INT    NOT NULL,
    byte_offset BIGINT NOT NULL,
    data        BYTEA  NOT NULL,
    PRIMARY KEY (node_id, generation, byte_offset)
);
```

Each column choice, and why:

| Choice | Why | Alternative rejected |
| --- | --- | --- |
| `UNIQUE NULLS NOT DISTINCT (parent_id, name)` | Root entries have `parent_id IS NULL`. A plain `UNIQUE` treats NULLs as distinct, so it would allow two `/docs` entries. Needs PostgreSQL 15+. | Plain `UNIQUE (parent_id, name)` |
| `parent_id` uses `RESTRICT` | POSIX `rmdir` on a non-empty directory returns `ENOTEMPTY`. `CASCADE` would silently delete a whole subtree. | `ON DELETE CASCADE` |
| `uuidv7()` ids | Time-ordered: inserts stay local in the B-tree, and the id records creation time. Built in from PostgreSQL 18. On PG 16 or 17, use the pg\_uuidv7 extension where available, or generate the ids in the app. | `gen_random_uuid()` |
| Variable-length extents | An append is one INSERT and never rewrites earlier bytes. A positional read selects only the overlapping extents. | Inline `body TEXT` (each append rewrites the whole value) or fixed-size chunks (each append rewrites the last block) |
| `generation` | Lets readers and cursors tell an append (same generation, larger size) from a rewrite (new generation). | Comparing content hashes |
| `size` stored on the inode | `stat` without scanning extents. It's always updated in the same transaction as the extent, so it can't drift. | Computing `sum(length(data))` on every stat |
| `BYTEA` content | True byte offsets, and safe for binary content. Python decodes UTF-8. | `TEXT` (offsets would be in characters) |

**Directory entry and inode in one row.** Without hard links, every inode has exactly one name, so splitting them gains nothing. If hard links are ever needed, split this into `inodes(id, size, generation, nlink, ...)` and `dentries(parent_id, name, inode_id)`.

**Inode numbers and handles.** If a protocol needs a 64-bit inode number, take the low 64 bits of the UUID (the random part in v7). A stateful protocol can use the 16-byte UUID itself as the file handle. Unlike TigerFS's path-encoded NFS handles, it survives renames.

**Moves must not create cycles.** `rename` of a directory checks that the destination isn't inside the source. A recursive CTE walks up from the destination's parent within the same transaction, so a move like `mv /a /a/b/c` returns `EINVAL`.

**Extent count.** A log appended one line at a time gains one extent per append. A few thousand extents still read in a single indexed range scan. If it ever matters, an optional compaction job can merge extents into about 64 KB pieces under the inode lock, without changing the bytes or the generation.

## Core operations

Every operation is a SQL function, so every client (agent tools, ingestion, a future TigerFS front end) gets the same semantics from the database. The Python library is a thin wrapper.

| Operation | Function | POSIX analogue | Locking |
| --- | --- | --- | --- |
| Append | `vfs_append(node, data) -> offset` | `write` with `O_APPEND` | Inode `FOR UPDATE` until commit |
| Overwrite | `vfs_write(node, data)` | `open` with `O_TRUNC`, then `write` | Inode `FOR UPDATE` |
| Positional read | `vfs_pread(node, offset, limit)` | `pread` | None (MVCC snapshot) |
| Resolve path | `vfs_resolve(path) -> id` | `namei` | None |
| List directory | `vfs_readdir(dir)` | `readdir` | None |
| Create, mkdir | `vfs_create(parent, name, kind)` | `open` with `O_CREAT\|O_EXCL`, `mkdir` | Unique constraint |
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
    IF NOT FOUND THEN RAISE EXCEPTION 'ENOENT' USING ERRCODE = 'P0002'; END IF;
    INSERT INTO vfs_extents VALUES (p_node, v_gen, v_size, p_data);
    UPDATE vfs_nodes SET size = v_size + length(p_data), mtime = now() WHERE id = p_node;
    RETURN v_size;                       -- the offset the data landed at
END $$ LANGUAGE plpgsql;
```

The inode lock is held until commit, so concurrent appenders take turns and **offset order always equals commit order**. A reader at `offset > cursor` can never miss bytes that commit late at a lower offset. This is the guarantee that makes incremental reads correct. Because it comes from the lock, it holds for any number of writers.

### Overwrite

```sql
CREATE FUNCTION vfs_write(p_node UUID, p_data BYTEA) RETURNS VOID AS $$
DECLARE v_gen INT;
BEGIN
    SELECT generation INTO v_gen FROM vfs_nodes WHERE id = p_node AND kind = 'file' FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'ENOENT' USING ERRCODE = 'P0002'; END IF;
    DELETE FROM vfs_extents WHERE node_id = p_node;
    DELETE FROM vfs_lines   WHERE node_id = p_node;      -- search index, see Search
    INSERT INTO vfs_extents VALUES (p_node, v_gen + 1, 0, p_data);
    UPDATE vfs_nodes SET generation = v_gen + 1, size = length(p_data), mtime = now()
     WHERE id = p_node;
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

`vfs_resolve(segments TEXT[])` walks parent pointers on the server in one round trip, the same idea as TigerFS's `resolve_path`. The library caches `(parent_id, name) -> id` for 2 seconds, which is metadata only. Renames and unlinks clear the affected entries.

### Watch (inotify)

```sql
CREATE FUNCTION vfs_notify() RETURNS trigger AS $$
DECLARE r vfs_nodes := coalesce(NEW, OLD);
BEGIN
    PERFORM pg_notify('vfs', json_build_object(
        'id', r.id, 'parent', r.parent_id, 'size', r.size, 'gen', r.generation,
        'op', CASE TG_OP WHEN 'INSERT' THEN 'create' WHEN 'DELETE' THEN 'delete'
                 ELSE CASE WHEN OLD.parent_id IS DISTINCT FROM NEW.parent_id
                             OR OLD.name <> NEW.name THEN 'move' ELSE 'modify' END END)::text);
    RETURN NULL;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_vfs_notify AFTER INSERT OR UPDATE OR DELETE ON vfs_nodes
    FOR EACH ROW EXECUTE FUNCTION vfs_notify();
```

- An append updates `size`, so it emits `modify`. A Maildir-style claim emits `move`.
- Notifications are delivered **at commit**, so a watcher is never woken for data it can't read yet.
- In Python, `vfs.watch(path)` is an async iterator that filters the single `vfs` channel by node id (a file) or parent id (a directory).

## Read contract

Every file is read the same way, by byte offset. A source file, a 200 KB doc, `notes.md` and a growing log all page and tail identically.

```
read(path, offset=None, limit=16_000) -> {content, offset, next_offset, size, generation, eof}
```

| Input | Meaning | UNIX equivalent |
| --- | --- | --- |
| `offset = N`, N ≥ 0 | Start at byte N | `pread(fd, buf, limit, N)` |
| `offset = -N` | Start N bytes before the end | `tail -c N` |
| `offset > size` | Empty content, `eof=true`, not an error | `pread` returning 0 |
| `offset = None` (agent tool only) | Continue from where this agent last read this path | An open fd with its own file position |
| `limit` | Maximum bytes returned | The `count` argument of `read(2)` |

### One snapshot for the bytes, size and generation

```sql
CREATE FUNCTION vfs_pread(p_node UUID, p_offset BIGINT, p_limit INT)
RETURNS TABLE (size BIGINT, generation INT, byte_offset BIGINT, data BYTEA) AS $$
    WITH n AS (
        SELECT id, size, generation,
               CASE WHEN p_offset < 0 THEN greatest(size + p_offset, 0) ELSE p_offset END AS off
          FROM vfs_nodes WHERE id = p_node AND kind = 'file'
    )
    SELECT n.size, n.generation,
           greatest(e.byte_offset, n.off),
           substring(e.data
                     FROM greatest(n.off - e.byte_offset, 0) + 1
                     FOR  least(e.byte_offset + length(e.data), n.off + p_limit)
                        - greatest(e.byte_offset, n.off))
      FROM n
      LEFT JOIN vfs_extents e
             ON e.node_id = n.id AND e.generation = n.generation
            AND e.byte_offset + length(e.data) > n.off
            AND e.byte_offset < n.off + p_limit
     ORDER BY e.byte_offset
$$ LANGUAGE sql STABLE;
```

- **Trimming happens in Postgres.** A 16 KB read of a 5 MB doc sends 16 KB.
- **It's one statement, so one MVCC snapshot.** `size` and `generation` describe exactly the bytes returned, even while another client is appending.
- **The `LEFT JOIN`** returns a single row with NULL data when nothing overlaps, so a read past EOF or of an empty file still returns `size` and `generation`.

### Two layers, two precisions

| Layer | Users | Behavior |
| --- | --- | --- |
| Library `vfs.pread` | Ingestion, other services, tests | Exact bytes, never adjusted |
| Agent tool `read` | The model | Snaps to whole lines. The start moves back to the beginning of its line, the end is cut at the last newline within `limit`, and at least one line is always returned. A single line longer than `limit` is cut at a UTF-8 boundary. |

Line snapping reads up to 4 KB before the requested offset to find the line start. The model never sees half a line or a split multi-byte character.

### Per-path cursors and generations

The tool keeps `path -> (generation, next_offset)` in the agent's run state and can save it to an `agent_cursors` table for crash recovery.

| Since the last read | `generation` | What `read(path)` with no offset does |
| --- | --- | --- |
| Bytes appended | Unchanged | Returns only the new bytes |
| Nothing changed | Unchanged | Empty content, `eof=true` |
| File overwritten (a doc re-ingested, `notes.md` rewritten) | Changed | Restarts at 0 and says "file was rewritten since your last read" |

### Offsets as one currency

- `rg` hits carry byte offsets (`style.md @18234`), so the agent can follow up with `read(path, offset=18234 - 500, limit=2000)`.
- A cursor is just a saved `next_offset`, so tailing a log is a `read` with no offset.

## Glob

`glob` and `rg` share one walker and one glob matcher, as they do inside ripgrep (whose matcher is the `globset` crate). The walker is `vfs_glob`, a recursive CTE over the parent pointers that reads only `vfs_nodes`, never content.

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
| `exclude=["**/node_modules/**"]` | Like rg's `-g '!...'` | `AND NOT path ~ ANY(...)` |

The defaults follow rg. Only files are returned (directories are walked, not listed). Entries starting with `.` are skipped unless the pattern segment itself starts with `.`. Results are sorted by `mtime`, newest first, as in Claude Code's Glob tool.

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
| Literal prefix | `docs/guides/**/*.md` gives `docs/guides` | Resolve that directory once and walk only its subtree. If it doesn't exist, return an empty result, not an error. |
| Max depth | `src/*/*.py` gives 3 | Stop the recursion at that depth. Only when the pattern has no `**`. |
| Name filter | Last segment `*.md` gives `LIKE '%.md'` | A cheap filter on the name before the full regex. Only when the last segment has no `[` or `{`. |

### The SQL

```sql
CREATE FUNCTION vfs_glob(
    p_root UUID, p_regex TEXT, p_name_like TEXT, p_max_depth INT,
    p_excludes TEXT[], p_hidden BOOLEAN, p_limit INT
) RETURNS TABLE (id UUID, path TEXT, size BIGINT, mtime TIMESTAMPTZ) AS $$
    WITH RECURSIVE t(id, path, kind, size, mtime, depth) AS (
        SELECT id, name, kind, size, mtime, 1
          FROM vfs_nodes
         WHERE parent_id IS NOT DISTINCT FROM p_root
           AND (p_hidden OR name NOT LIKE '.%')
        UNION ALL
        SELECT c.id, t.path || '/' || c.name, c.kind, c.size, c.mtime, t.depth + 1
          FROM vfs_nodes c JOIN t ON c.parent_id = t.id
         WHERE t.kind = 'dir'
           AND (p_max_depth IS NULL OR t.depth < p_max_depth)
           AND (p_hidden OR c.name NOT LIKE '.%')
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

Each directory level is an index lookup on `UNIQUE (parent_id, name)`. A few thousand nodes take milliseconds. For trees of 100k+ nodes, the literal-prefix and depth hints keep the walk small, and a trigram index on `vfs_nodes.name` speeds up `*foo*` patterns.

## Search (rg in Postgres)

`rg` runs entirely in Postgres. A line index (`vfs_lines`) is kept in step with every write by a trigger, and a `pg_trgm` GIN index speeds up regex matching on it. There's no local mirror, no sync and no `rg` binary, and a search sees every committed write, even one from a millisecond ago.

### Why not search the extents directly

Extents are append fragments: a match can cross an extent boundary, extents have no line numbers, and a trigram index on fragments would miss matches. ripgrep works on lines, so the index stores lines. It's derived data, like a filesystem's search index, never a second source of truth.

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE vfs_lines (
    node_id     UUID    NOT NULL REFERENCES vfs_nodes(id) ON DELETE CASCADE,
    generation  INT     NOT NULL,
    line_no     INT     NOT NULL,                  -- 1-based, like rg -n
    byte_offset BIGINT  NOT NULL,                  -- same currency as read(offset=...)
    text        TEXT    NOT NULL,                  -- without the trailing \n
    complete    BOOLEAN NOT NULL,                  -- false = last line, no \n yet
    PRIMARY KEY (node_id, generation, line_no)
);
CREATE INDEX vfs_lines_trgm ON vfs_lines USING gin (text gin_trgm_ops);
```

### The indexing trigger

```sql
-- AFTER INSERT ON vfs_extents: split the new bytes into lines, merging a trailing partial line.
CREATE FUNCTION vfs_index_extent() RETURNS trigger AS $$
DECLARE
    v_prev vfs_lines; v_text TEXT; v_parts TEXT[]; v_no INT; v_off BIGINT; n INT;
BEGIN
    BEGIN
        v_text := convert_from(NEW.data, 'UTF8');
    EXCEPTION WHEN character_not_in_repertoire THEN
        RETURN NULL;                                -- binary content: readable, not searchable
    END;

    SELECT * INTO v_prev FROM vfs_lines
     WHERE node_id = NEW.node_id AND generation = NEW.generation
     ORDER BY line_no DESC LIMIT 1;

    IF FOUND AND NOT v_prev.complete THEN           -- previous append ended mid-line
        DELETE FROM vfs_lines
         WHERE node_id = NEW.node_id AND generation = NEW.generation AND line_no = v_prev.line_no;
        v_text := v_prev.text || v_text;
        v_no := v_prev.line_no;  v_off := v_prev.byte_offset;
    ELSE
        v_no := coalesce(v_prev.line_no, 0) + 1;  v_off := NEW.byte_offset;
    END IF;

    v_parts := string_to_array(v_text, E'\n');
    n := array_length(v_parts, 1);
    FOR i IN 1 .. n LOOP
        EXIT WHEN i = n AND v_parts[i] = '';        -- text ended with \n
        INSERT INTO vfs_lines VALUES (NEW.node_id, NEW.generation, v_no, v_off, v_parts[i], i < n);
        v_off := v_off + octet_length(v_parts[i]) + 1;
        v_no  := v_no + 1;
    END LOOP;
    RETURN NULL;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_vfs_index AFTER INSERT ON vfs_extents
    FOR EACH ROW EXECUTE FUNCTION vfs_index_extent();
```

- **Appends are cheap.** An append of one whole line adds one index row and touches nothing earlier.
- **Overwrites re-index.** `vfs_write` deletes the node's lines, and the new generation's single extent indexes it again.
- **Writers must append whole UTF-8 strings.** A multi-byte character split across two extents would make `convert_from` fail on both halves. The library guarantees this, and a test enforces it.

### The search query

The file scope comes from `vfs_glob` (see Glob), so `rg` has ripgrep's structure: walk and filter by glob, then search the matching files.

```sql
-- in one transaction:
SET LOCAL statement_timeout = '2s';

SELECT g.path, l.line_no, l.byte_offset, l.text
  FROM vfs_glob($root, $glob_regex, $name_like, $depth, $excludes, false, 10000) g
  JOIN vfs_nodes n ON n.id = g.id
  JOIN vfs_lines l ON l.node_id = n.id AND l.generation = n.generation
 WHERE l.text ~ $pattern                 -- or ~* for ignore_case; built in Python, never a CASE
 ORDER BY g.path, l.line_no
 LIMIT $max_plus_one;
```

| rg feature | Implementation |
| --- | --- |
| `-i` | `~*` instead of `~`. Build one predicate form in Python, because a `CASE` around the operators stops the planner from using the trigram index. |
| `-F` (fixed strings) | `strpos(l.text, $s) > 0`, or `LIKE` with `%` and `_` escaped. Models search literal words most often, and trigrams speed those up the most. |
| `-C n` (context) | A second query for `line_no BETWEEN hit - n AND hit + n` on the files that matched |
| `-g` / `--glob` | Through the `vfs_glob` scope |
| `--max-count` | `row_number() OVER (PARTITION BY node_id)` plus a filter |
| Byte offset per hit | `l.byte_offset`, the line's start, which `read(offset=...)` accepts directly |
| `-U` (multiline) | Not supported in v1. Lines are rows, as in rg's default mode. |

### Index behavior

The `pg_trgm` GIN and GiST indexes support `~` and `~*`. They extract trigrams from the regex, and "a pattern with no extractable trigrams will degenerate to a full-index scan" ([pg\_trgm docs](https://www.postgresql.org/docs/current/pgtrgm.html)). Patterns with a literal run of 3+ characters (`refund`, `parse.*config`) are fast. For patterns like `\d+` or `.`, require a `path` or `glob` scope.

### Output format for the agent

```
/docs/guides/style.md:42 @18234: Prefer early returns over nested conditionals.
/workspace/src/config.py:17 @2210: def parse_config(path: str) -> Config:
... 38 more matches; narrow the pattern or path
```

## Regex dialect and translation

We need our own translator from rg-style (Rust/PCRE) patterns to Postgres ARE. No open-source translator in that direction exists, and the extensions that bring PCRE or RE2 into Postgres aren't available on most managed Postgres services. The translator is small, but it is the riskiest part of search: when it gets something wrong, it fails silently.

### Why it's needed: patterns that silently change meaning

Postgres documents these incompatibilities with Perl: "\\b, \\B, the lack of special treatment for a trailing newline, ... the restrictions on parentheses and back references in lookahead/lookbehind constraints, and the longest/shortest-match (rather than first-match) matching semantics" ([PostgreSQL 9.7.3.6](https://www.postgresql.org/docs/current/functions-matching.html)). For a model writing patterns, the dangerous case is `\b`: in ARE it means **backspace**, so `\bparse_config\b` silently matches nothing. The model concludes the code isn't there and acts on that false belief.

| The model writes | Rust/PCRE meaning | Postgres ARE | Translator action |
| --- | --- | --- | --- |
| `\b` | Word boundary | Backspace | Rewrite to `\y` |
| `\B` | Not a word boundary | Different meaning | Rewrite to `\Y` |
| `\<`, `\>` (GNU grep) | Word start or end | Not supported | Rewrite to `\m`, `\M` |
| `\z` | End of text | Not documented | Rewrite to `$`. Each line is its own row. |
| `\A`, `\Z` | Start or end of text | Supported | Pass through |
| `(?P<n>...)`, `(?<n>...)` | Named group | Not documented | Remove the name, keep the group |
| `\p{L}`, `\pL`, `\P{...}` | Unicode property class | Not supported | Reject, with the hint: use `[[:alpha:]]` |
| `(?i)` at the start | Ignore case | Supported only at the start | Pass through |
| `(?i)` mid-pattern, `(?i:...)` | Scoped flags | Not supported | Reject, with the hint: use `ignore_case=true` |
| `\D`, `\S`, `\W` inside `[...]` | Negated class inside a class | Illegal | Reject, with the hint: use a POSIX class |
| `\x{1F600}` | Code point | `\x` takes plain hex; `\U` takes 8 digits | Rewrite to `\U0001F600` |
| `\Q...\E` (PCRE) | Literal span | Not supported | Escape the span |
| `\h` (PCRE) | Horizontal whitespace | Not supported | Rewrite to `[ \t]` |
| A literal `{` | Must be escaped | Ambiguous | Always emit `\{` |
| `\d \w \s`, `[...]`, `(?:...)`, `+?`, `{n,m}`, `(?=...)`, `(?<=...)`, `\1` | — | Supported | Pass through |

**Longest-match versus first-match** semantics don't matter here. `rg` only asks whether a line matches and returns the whole line; it never extracts match spans.

### What exists: survey

| Project | What it is | Usable here? |
| --- | --- | --- |
| [pgpcre](https://github.com/petere/pgpcre) | Postgres extension that adds PCRE as a `pcre` type with `~` and `=~` operators | Rarely: not offered by most managed services, and it has no index support for pattern matching. |
| [re2 (PGXN)](https://api.pgxn.org/src/re2/re2-0.4.0/doc/re2.md) | Postgres extension with ClickHouse-compatible RE2 functions; an `@~` operator with a `gin_re2_ops` trigram prefilter | Rarely available, but the best design reference: RE2 syntax is close to Rust's, and its GIN trigram prefilter is the same strategy we use with `pg_trgm`. |
| [omni\_regex (Omnigres)](https://docs.omnigres.org/omni_regex/regex/) | PCRE2-based match and capture functions | Rarely available |
| [PL/Rust](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.Using.PL_Rust.html) | Postgres functions written in Rust, with the `regex` crate allowed | RDS only |
| [PCRE2 `pcre2_convert`](https://git.stg.centos.org/source-git/pcre2/blob/c8s/f/src/pcre2_convert.c) | Converts POSIX patterns and globs into PCRE2 | Wrong direction. Useful as a reference for edge cases only. |
| [RegexBuddy](https://www.regexbuddy.com/postgresql.html) | Commercial GUI that converts between regex flavors, including PostgreSQL | No: a GUI tool, not a library |
| Python `re._parser` (formerly `sre_parse`) | CPython's internal regex parser, which produces an AST | It's a possible base for a translator, but it's an internal API that can change between Python versions. |

`pg_trgm` ships with Postgres and is available almost everywhere, including managed services such as Databricks Lakebase ([Lakebase extensions](https://docs.databricks.com/gcp/en/oltp/projects/extensions)). So we translate to ARE and use `pg_trgm`.

### Translator design

1. **Accepted dialect:** Rust regex syntax (what `rg` uses by default), plus lookaround and backreferences, since ARE supports both. The tool description says "Rust-style regex".
2. **Tokenizer, then emitter.** Write a small hand-written tokenizer that understands escapes, classes, groups, quantifiers and flags, rather than depending on `re._parser`. Then emit ARE tokens using the table above. The amount of code is small; correctness comes from the tests.
3. **Reject, don't guess.** Unsupported constructs return a shell-style error with a hint, such as `rg: \p{..} is not supported; use [[:alpha:]]`. An error is always better than a silent zero-match.
4. **Validate in Postgres.** Compile the translated pattern once with `SELECT '' ~ $1` under a 100 ms timeout, so syntax errors come back as `rg: regex parse error: ...` before the real search runs. Cache translations per pattern.
5. **Safety.** Postgres warns that "regular expressions can be contrived that take arbitrary amounts of time and memory" and advises a statement timeout for untrusted patterns. Every search runs under `SET LOCAL statement_timeout`.

### Differential testing (how we know it's right)

- **A corpus of real patterns:** start with 200+ patterns collected from model transcripts (`\bparse\b`, `TODO|FIXME`, `(?i)error`, `\d{2}:\d{2}`, and so on), and add every new pattern seen in production logs.
- **An oracle:** for each pattern and each test line, compare Python `re.search(original)` with Postgres `line ~ translated`, for the subset where Python and Rust semantics agree. Every disagreement is either a translator bug or a construct that should be rejected.
- **Property tests:** generate random patterns from the supported grammar and random lines, then compare the same way.

## Agent integration

Agents get Claude Code's file toolset with the same semantics, so models apply what they already practice. The examples use PydanticAI, but the tools are plain async functions over the VFS library and fit any agent framework or an MCP server.

| Claude Code tool | VFS tool | Backed by |
| --- | --- | --- |
| Read (`offset`, `limit`) | `read(path, offset, limit)` | `vfs_pread` over `vfs_extents` |
| Glob | `glob(pattern, path, exclude)` | `vfs_glob` over `vfs_nodes` |
| Grep (ripgrep) | `rg(pattern, path, glob, ignore_case, fixed_strings, context)` | `vfs_glob` and `vfs_lines` |
| LS | `ls(path)` | `vfs_readdir` |
| Write | `write(path, content)` | `vfs_write` or `vfs_create`, subject to the write policy |

### Package layout

```
minifs/
  vfs/
    core.py      Entry, ReadResult, VFSError, path normalization, chroot mapping
    client.py    thin async wrappers over the SQL functions
    policy.py    per-actor write rules
    glob.py      glob translator and pruning hints
    regex.py     rg-pattern to ARE translator
    watch.py     LISTEN vfs and async iterators per path
  tools/
    tools.py     ls / read / write / glob / rg
  db/
    pool.py      psycopg pool and credential refresh
    schema.sql
```

### Deps and tools

```python
@dataclass
class Deps:
    vfs: VFSClient
    workspace_id: UUID
    actor: str = "agent"
    cursors: dict[str, Cursor] = field(default_factory=dict)   # path -> (generation, next_offset)

agent = Agent("<model>", deps_type=Deps, instructions=AGENT_PROMPT)

@agent.tool
async def read(ctx: RunContext[Deps], path: str, offset: int | None = None, limit: int = 16_000) -> str:
    """Read any file by byte offset. Omit offset to continue where you last read this path;
    offset=0 rereads from the start; negative offsets count from the end. Output is whole lines."""
    ...

@agent.tool
async def glob(ctx: RunContext[Deps], pattern: str, path: str = "/",
               exclude: list[str] | None = None, max_results: int = 100) -> str:
    """Find files by name pattern ("**/*.py", "src/*.md"). Newest first."""
    ...

@agent.tool
async def rg(ctx: RunContext[Deps], pattern: str, path: str = "/", glob: str | None = None,
             ignore_case: bool = False, fixed_strings: bool = False, context: int = 0) -> str:
    """Search file contents (Rust-style regex). Hits show path:line @byte_offset."""
    ...
```

**Errors are returned, not raised.** Each tool catches `VFSError` and returns a shell-style message, such as `read: /workspace/foo.md: No such file or directory (hint: ls /workspace)`. Models recover from these naturally. Raising an exception the framework treats as a retry spends the retry budget and can fail the run.

**Pre-fill reads that happen every time.** Each tool call costs a full extra LLM inference: the model stops, waits for the result, then reprocesses its whole context. The database query takes milliseconds; the extra inference takes roughly 0.5 to 1.5 s. If a harness always reads the same file at the start of a run (for example `notes.md`), it can run that `read` itself and put the result in the prompt as if the agent had run it. A read that depends on what the agent is thinking stays a tool call.

**Reacting to changes.** `vfs.watch(path)` lets a harness start an agent run when a file changes, for example when a new file appears in `tasks/new/` or a log grows.

## Operations

Three things matter most in production: credential handling, LISTEN connections that reconnect cleanly, and keeping heavy search load away from latency-sensitive writes.

### Postgres specifics

| Topic | Guidance |
| --- | --- |
| Credentials | Managed services often use short-lived tokens as the Postgres password. Give the psycopg pool a connection factory that fetches a fresh credential, not a fixed DSN. |
| LISTEN | Use a direct, dedicated connection for LISTEN, never one through a transaction-mode pooler. |
| Reconnect | When the LISTEN connection drops: reconnect, LISTEN again, then re-check watched paths so nothing missed is skipped. Saved cursors make this safe. |
| Postgres version | PG 15+ for `UNIQUE NULLS NOT DISTINCT`. PG 18 has a built-in `uuidv7()`; on older versions use `pg_uuidv7` where available, or generate UUIDv7 in the app. |
| Extensions | `pg_trgm` (required). `vector` (pgvector) is an option for future semantic search over `vfs_lines`. |
| Pools | Per process: one LISTEN connection, and a small pool (about 5) for the tools. |

### Protecting the database

- **Every search runs under `SET LOCAL statement_timeout = '2s'`.** On timeout, return `rg: search timed out; narrow the pattern or path`.
- **Guide patterns toward the index.** Require a `path` or `glob` scope when the pattern has no literal run of 3+ characters.
- **Cap output:** at most 50 matches by default, and `glob` at most 100 results, each with a "narrow it" hint.
- **Separate pools for search and writes.** If CPU contention appears at scale, consider sending search to a read replica. That gives up read-your-latest-write consistency, so only do it if you need to.

### Capacity, rough estimates

| Item | Estimate |
| --- | --- |
| Append cost | One inode row lock, one extent insert, one or two line-index rows, one NOTIFY |
| Line index plus trigram GIN | Very roughly 2 to 4 times the text size |
| `glob` over a few thousand nodes | Milliseconds |

### Observability

Metrics to track:

- Tool call latency per operation, and tool calls per agent run.
- Search duration, timeouts, and translator rejections by construct. Rejections show which constructs are worth supporting next.
- Write-policy denials, which show whether agents try to write outside their allowed paths.

## Testing strategy

Most of the correctness rests on three properties, and each gets a test that compares two independent answers: joined chunked reads equal the full file, pruned globs equal full walks, and translated regexes agree with an oracle. Everything runs against real Postgres in testcontainers; there are no SQL mocks.

| Area | Test | Kind |
| --- | --- | --- |
| Append ordering | Two connections appending concurrently produce gap-free offsets, and offset order equals commit order | Concurrency |
| Read | For random files and random `(offset, limit)` sequences, joining the `pread` chunks equals the full content, including while another task keeps appending | Property |
| Read edges | Offsets `0`, `size`, `size+1`, `-1`, `-size`, `-(size+1)`; empty file; one extent larger than `limit`; a read spanning extents | Unit |
| Line snapping | Offset in the middle of a line or of a multi-byte character; a line longer than `limit`; no trailing newline | Unit |
| Generations | An overwrite between two cursor reads produces the restart note; an append doesn't | Unit |
| Tree | `UNIQUE NULLS NOT DISTINCT` rejects duplicate root names; `rmdir` on a non-empty directory gives `ENOTEMPTY`; a move into its own subtree gives `EINVAL` | Unit |
| Claim | Two claimers racing for the same file: exactly one wins | Concurrency |
| Watch | Events arrive only after commit; `move` versus `modify` is classified correctly | Integration |
| Line index | Partial-line merges across appends; overwrite re-indexes; non-UTF-8 content is skipped | Unit |
| Glob translator | Compared with `PurePath.full_match` (Python 3.13+) or `wcmatch` (GLOBSTAR and BRACE flags) on generated patterns and paths | Differential |
| Glob pruning | Results with all hints enabled equal results with none | Differential |
| Regex translator | A corpus of real model patterns plus generated ones, compared with Python `re` on the shared subset | Differential |
| rg scope | `rg(p, glob=X)` hits exactly the files that `glob(X)` returns | Integration |
| Agent tools | Errors come back as shell-style strings; the chroot blocks `..` escapes; the write policy blocks writes outside allowed paths | Unit |
| Evals | A set of recorded coding tasks run against the VFS tools: track task success and tool calls per task | Eval |

## Roadmap

The tool-based VFS ships first. A TigerFS front end follows once the tree and tools are stable.

**What stays frozen.** The five tool names and signatures, and the shell-style error format. The TigerFS work can then replace the VFS client with a thin MCP client, and prompts, tools and evals carry over unchanged.

**What the TigerFS integration adds.** Three pieces of work in the TigerFS repo:

1. A `tigerfs mcp` front end over `fs.Operations`, which agent frameworks connect to with their MCP clients.
2. An extent-backed file workspace format, `vfs_nodes` plus `vfs_extents`. TigerFS's current synth workspaces store each file's body in a single row, so they can't do cheap appends or offset reads. With extents, a FUSE `Write` at an offset maps straight onto extents.
3. `glob` and `rg` exposed through the same front end, so every client gets the same search results.

The payoff is that the same tree mounts through FUSE on a laptop or NFS on Kubernetes for debugging, alongside tool-based access where mounts aren't allowed.

## Open decisions

| Decision | Options | Recommendation |
| --- | --- | --- |
| Client access | Through the VFS library, or direct SQL | The VFS library, for one model end to end. Revisit only if profiling shows overhead. |
| Workspaces per process | One, or many | Many, with one LISTEN connection filtered by payload |
| Postgres version | PG 15–17 (UUIDv7 from an extension or the app), or PG 18 (built-in `uuidv7()`) | PG 18 where available |
| Line-based addressing | Add `read(path, line=, lines=)` | Not in v1. Byte offsets are the only addressing scheme, since `rg` hits already carry offsets. |
| Semantic search | pgvector over windows of `vfs_lines` | After the core tools are stable |

## Sources

- [PostgreSQL 9.7 Pattern Matching](https://www.postgresql.org/docs/current/functions-matching.html): ARE syntax, Perl incompatibilities, and the statement-timeout advice
- [PostgreSQL pg\_trgm](https://www.postgresql.org/docs/current/pgtrgm.html): regex index support and how it degrades
- [Databricks Apps best practices](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices) and [ECS KernelCapabilities](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_KernelCapabilities.html): examples of platforms without FUSE or root
- [Lakebase extensions](https://docs.databricks.com/gcp/en/oltp/projects/extensions): an example of a managed Postgres extension list
- [pgpcre](https://github.com/petere/pgpcre), [re2 for PostgreSQL (PGXN)](https://api.pgxn.org/src/re2/re2-0.4.0/doc/re2.md), [omni\_regex](https://docs.omnigres.org/omni_regex/regex/), [PL/Rust on RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.Using.PL_Rust.html), [PCRE2 pcre2\_convert](https://git.stg.centos.org/source-git/pcre2/blob/c8s/f/src/pcre2_convert.c), [RegexBuddy PostgreSQL](https://www.regexbuddy.com/postgresql.html): the translator survey
- TigerFS repository: `docs/adr/017-relational-directory-structure.md` and `internal/tigerfs/fs/synth/build.go`
