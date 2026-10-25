# minifs design

How minifs works and why, for contributors and for anyone who wants to check its claims. The [README](../README.md) is the user guide. The SQL functions' header comments in `minifs/db/migrations/` are the reference for each function's arguments, results and errors.

- [Why a filesystem in Postgres](#why-a-filesystem-in-postgres)
- [Layers](#layers)
- [Storage model](#storage-model)
- [Operations and errors](#operations-and-errors)
- [Reads](#reads)
- [Writes](#writes)
- [Path resolution and the directory cache](#path-resolution-and-the-directory-cache)
- [Watch](#watch)
- [Glob](#glob)
- [Search](#search)
- [Regex translation](#regex-translation)
- [The agent tools in detail](#the-agent-tools-in-detail)
- [Running it in production](#running-it-in-production)
- [Testing](#testing)
- [Not built yet](#not-built-yet)

## Why a filesystem in Postgres

**Why files?** Models are heavily trained on shell and file work. A small, uniform tool set over one tree is easy to discover by reading directories, needs no new tools when a data source is added, and gives humans the same view when debugging. Source files, notes, logs, working memory and ingested documents are all ordinary files. There are no special tables for any of them.

**Why not a real filesystem?** Many places agents run don't allow one. Managed platforms run apps as a non-privileged user with no FUSE ([Databricks Apps best practices](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices)), and AWS Fargate allows only `SYS_PTRACE` to be added ([ECS KernelCapabilities](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_KernelCapabilities.html)). Every kernel mount needs `CAP_SYS_ADMIN` or `/dev/fuse`, so the filesystem lives in the tool layer. Postgres adds durability, transactions, concurrent access from many agents, and change notifications.

**Prior art.** [TigerFS](https://github.com/timescale/tigerfs) exposes Postgres as a mountable filesystem. Its file workspaces use the same parent-pointer directory model (`parent_id`, `filename`, `UNIQUE NULLS NOT DISTINCT`, a one-round-trip path resolution, a short-lived path cache). minifs adds line-aligned extents for cheap appends and line reads, search and glob in SQL, and tools in place of kernel mounts.

**Goals.** One storage model for every kind of data; the file tools models already use; incremental reads (tail a growing file by byte offset or line); search and glob inside Postgres; every read reflects the latest committed write; about one round trip per tool call. **Non-goals:** kernel mounts, full POSIX (hard links, permission bits, xattrs, writes at an arbitrary offset), large binary files.

## Layers

| Layer | Module | Role |
| --- | --- | --- |
| Agent tools | `minifs/tools/` | `Read`, `Write`, `Edit`, `Glob`, `Grep` as pydantic-ai tools over a `VFSView` |
| View | `minifs/vfs/view.py`, `policy.py` | The agent's chroot (mounts) and the per-actor path rules |
| Client | `minifs/vfs/client.py` | One SQL statement per operation; path resolution, with the directory cache |
| Cache, watch | `minifs/vfs/cache.py`, `watch.py` | The directory-entry cache; `LISTEN vfs` and per-path watches |
| Glob, search, regex | `minifs/vfs/glob.py`, `search.py`, `regex.py`, `crates/minifs-regex/` | Pattern compilation (Rust, `minifs._regex`) and the search statements |
| Service | `minifs/vfs/service.py`, `minifs/db/pool.py` | `open_vfs`: pools, watcher and cache, opened and closed in order; credentials per connection |
| SQL | `minifs/db/migrations/` | Tables, functions, the NOTIFY trigger |

Every operation is a SQL function, so every client (the tools, ingestion jobs, the CLI) gets the same semantics from the database, and the Python layers stay thin.

## Storage model

Two tables. `vfs_nodes` holds inodes and directory entries; `vfs_extents` holds file bytes as variable-length, line-aligned extents (migrations 003, 004 and 007; abridged):

```sql
CREATE TABLE vfs_nodes (                         -- inode + directory entry (no hard links)
    id          uuid PRIMARY KEY DEFAULT uuidv7(),
    parent_id   uuid,                            -- NULL for root entries
    name        text NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('file','dir')),   -- never changes after create
    size        bigint NOT NULL DEFAULT 0,       -- exact, maintained under the inode lock; 0 for dirs
    lines       bigint NOT NULL DEFAULT 0,       -- line count, updated with size
    generation  int    NOT NULL DEFAULT 0,       -- +1 on overwrite; appends keep it
    ctime, mtime timestamptz NOT NULL DEFAULT now(),
    parent_kind text NOT NULL DEFAULT 'dir' CHECK (parent_kind = 'dir'),
    UNIQUE NULLS NOT DISTINCT (parent_id, name),
    UNIQUE (id, kind),                           -- target of the two kind-checking foreign keys
    FOREIGN KEY (parent_id, parent_kind) REFERENCES vfs_nodes (id, kind) ON DELETE RESTRICT
) WITH (fillfactor = 80);

CREATE TABLE vfs_extents (
    node_id      uuid   NOT NULL,
    generation   int    NOT NULL,
    byte_offset  bigint NOT NULL,                -- always at a line start
    line_start   bigint NOT NULL,                -- the number of \n before byte_offset
    line_offsets int4[] NOT NULL,                -- where each of its lines starts, in data
    data         bytea  NOT NULL,                -- whole lines; only a file's last extent may end mid-line
    node_kind    text   NOT NULL DEFAULT 'file' CHECK (node_kind = 'file'),
    utf8         boolean NOT NULL GENERATED ALWAYS AS (vfs_utf8(data)) STORED,   -- migration 015
    PRIMARY KEY (node_id, generation, byte_offset),
    UNIQUE (node_id, generation, line_start),
    FOREIGN KEY (node_id, node_kind) REFERENCES vfs_nodes (id, kind) ON DELETE CASCADE
);
ALTER TABLE vfs_extents ALTER COLUMN data SET COMPRESSION pglz;   -- migration 013
CREATE INDEX vfs_extents_not_utf8_idx ON vfs_extents (node_id, generation) WHERE NOT utf8;   -- migration 015
```

| Choice | Why | Rejected |
| --- | --- | --- |
| `UNIQUE NULLS NOT DISTINCT (parent_id, name)` | Root entries have `parent_id IS NULL`; a plain `UNIQUE` would allow two `/docs`. It is also the directory index: a path walk is one probe per component. | Plain `UNIQUE` |
| Parent foreign key `RESTRICT` | `rmdir` of a non-empty directory is `ENOTEMPTY`, as in POSIX; `CASCADE` would delete a subtree silently. | `ON DELETE CASCADE` |
| Foreign keys on `(id, kind)` | The schema rejects a child under a file and an extent on a directory. The constant columns `parent_kind` and `node_kind` make that a plain foreign key. | A trigger, or trusting every function |
| `fillfactor = 80` | Every append updates `size`, `lines` and `mtime`, none indexed, so free page space makes it a HOT update. | The default 100 |
| `uuidv7()` ids | Time-ordered, so inserts stay local in the B-tree. Built in from PG 18; on older versions migration 002 defines it, over `pg_uuidv7` when installed, else in SQL (Lakebase has no `pg_uuidv7`). | `gen_random_uuid()` |
| Variable-length extents | An append after a complete line is one INSERT; a read selects only the overlapping extents. | A `text` body (each append rewrites it), fixed-size blocks |
| Line-aligned extents | Line N is one index descent plus `line_offsets[N - line_start]`; search decodes and splits each extent on its own; no extent splits a UTF-8 character. The price: an append to a file ending mid-line rewrites the last extent. | A row per line, a `text[]` of lines, line numbers computed on the client |
| `generation` | Tells an append (same generation, larger size) from a rewrite. Bytes cached under `(id, generation)` stay valid. | Content hashes |
| `bytea` content | True byte offsets, and binary-safe. | `text` (offsets in characters) |
| `pglz` compression | Extents are about 64 KB and compress 2.5 to 4 times. A slice of a pglz value fetches only the TOAST chunks up to the slice's end, so a `Read` page of a minified 10 MB line fetches 2 TOAST blocks, where lz4 fetches all 512. PG 19 makes lz4 the default, hence the explicit setting. | `EXTERNAL` (uncompressed), lz4 |

**Files and directories** share one row format, as ext4's inodes do; `kind` plays the role of the file-type bits. A directory's contents are the rows whose `parent_id` is its id. Kind never changes: replacing a file with a directory is unlink plus create. Directory `mtime` isn't maintained (POSIX bumps it on every entry change, which would make shared directories a hot spot); watchers get those events from NOTIFY instead.

**Cutting.** Every writer cuts content with `vfs_cut(data, 64 KB)` into consecutive whole lines, always just after a `\n`. A line longer than the target is an extent of its own; a line is never cut. An append cuts what it adds together with the file's last extent when that ends mid-line, so alignment holds whatever the appends contain: partial lines, a UTF-8 character split across appends, no newline at all. A trailing partial line goes in an extent of its own, so a writer that streams partial lines rewrites only that line each time. The worst case is a file with no newlines, where every append rewrites the whole file.

**Compaction.** An extent is as big as the write that made it, so a log appended one line at a time gains one extent per append. `vfs_compact(node, target)` merges neighbouring extents into pieces of about 64 KB under the inode lock. It merges whole extents, so pieces stay line-aligned, and it changes no bytes, `size`, `lines`, `generation` or `mtime`. Readers and caches never notice, and it emits no NOTIFY. No scheduled job runs it yet.

**Moves never create cycles.** Moving a directory locks the destination's ancestors while it checks that the source isn't among them, so `mv /a /a/b/c` is `EINVAL`, even under concurrent moves.

## Operations and errors

| Operation | Function | POSIX analogue | Locking |
| --- | --- | --- | --- |
| Resolve a path | `vfs_lookup(segments, cached)` | `namei` | None |
| List a directory | `vfs_readdir(dir)` | `readdir` | None |
| Byte read | `vfs_pread(node, offset, limit)` | `pread` | None (one snapshot) |
| Line read | `vfs_pread_lines(node, first_line, max_lines, max_bytes, max_line_bytes)` | — | None (one snapshot) |
| Append | `vfs_append(node, data)` | `write` with `O_APPEND` | Inode `FOR UPDATE` |
| Overwrite | `vfs_write(node, data, [expected generation, size, parent, name])` | `open(O_TRUNC)` + `write` | Inode `FOR UPDATE`; optional compare-and-swap |
| Create, mkdir | `vfs_create(parent, name, kind, data)`, `vfs_mkdir_p(segments)` | `open(O_CREAT\|O_EXCL)`, `mkdir -p` | Unique constraint |
| Rename | `vfs_rename(node, new_parent, new_name, expect_parent)` | `rename` with `RENAME_NOREPLACE` | Row locks; cycle check |
| Unlink, rmdir | `vfs_unlink(node)` | `unlink`, `rmdir` | Foreign key |
| Glob, search | `vfs_glob(...)`, `vfs_search(...)` | `find`, `rg` | None |
| Compact | `vfs_compact(node, target)` | — | Inode `FOR UPDATE` |

**Outcomes are returned, faults raised** (migration 009). Expected outcomes (`ENOENT`, `ENOTDIR`, `EISDIR`, `EEXIST`, `ESTALE`, `ENOTEMPTY`) come back in `err` and `err_detail` result columns, with nothing changed. A missing path an agent guessed then costs no server log entry and doesn't abort the caller's transaction. Only faults (`EINVAL`, `ENAMETOOLONG`, broken invariants) raise, with a custom SQLSTATE that maps to the errno.

**One statement per operation.** The client resolves an operation's paths inside the statement that operates on them: each path is a `vfs_lookup` subquery, and the function runs only if every lookup succeeded. So a `stat`, a read or a write is one round trip, and it never sees the NULL id of a failed lookup, which for a directory would mean the root.

## Reads

**Byte reads.** `vfs_pread(node, offset, limit)` returns exact bytes, never adjusted to lines or characters, with the file's `size` and `generation`. A negative offset counts from the end (`tail -c`), and an offset past the end is empty with `eof`, not an error. It runs in two index steps, like walking ext4's extent tree: step 1 descends backward to the extent that contains `offset`, and step 2 scans forward from it. A read costs O(log E + extents returned); on a 20,000-extent file, a read at the end touches 3 index pages. Each step is its own statement, so the planner sees step 2's bounds as values, and trimming happens in Postgres: a 16 KB read of a 5 MB file sends 16 KB.

**Line reads.** `vfs_pread_lines(node, first_line, max_lines, max_bytes, max_line_bytes)` returns lines `first_line` (1-based) onwards, each with its `\n`, capped at `max_bytes`, together with `size`, `generation`, the total `lines` and the byte offset where `first_line` starts. The extent holding a line is found by a backward descent of the `(node_id, generation, line_start)` index, and the line's offset is `byte_offset + line_offsets[...]`. A first line past the end returns no bytes and the total, which is what `Read` needs for its out-of-range message. With `max_line_bytes`, each longer line comes back cut to that prefix and its number goes in `cut`, so a page of very long lines is still one call. The scan stops at the line where the result reaches `max_bytes` (migration 012), so a page's slice ends at most one line past the cap, even inside a 64 MB compacted extent.

**One snapshot.** Both read functions are `STABLE`, so all their statements share the caller's snapshot: `size`, `generation`, `lines` and the bytes describe the same moment, even while another client appends. Lines split on `\n` only, and a trailing `\n` adds no empty last line, so `a\nb\n` and `a\nb` both have 2 lines, and `Read`'s numbers are `Grep`'s.

**Reading several files at once.** `VFSClient.pread_many(reads, groups=)` resolves and reads several paths in one statement and one snapshot. `Read` uses it to fetch the nearby `AGENTS.md` / `CLAUDE.md` files with the page. With `groups`, only the first existing file of each group is read: `AGENTS.md`, else `CLAUDE.md`.

## Writes

**Appends serialize on the inode.** `vfs_append` locks the file's row until commit, so concurrent appenders take turns, and **offset order always equals commit order**. A reader that has seen up to offset N can never miss bytes that commit late below N. That is what makes incremental reads correct, for any number of writers.

**Overwrites start a new generation.** `vfs_write` replaces all extents under the inode lock and bumps `generation`. A reader holding `(generation, offset)` can tell "grew" from "rewritten" (a doc re-ingested, `notes.md` rewritten) and restart from 0.

**Compare-and-swap.** `vfs_write` can be given the generation, size, parent and name it expects. Under the inode lock it raises `ESTALE` if any differs, and changes nothing. `Write` and `Edit` use this, so a writer anywhere (another agent, another process) is detected, where a per-path lock in one process wouldn't reach the others.

**Rename as an exactly-once claim.** `vfs_rename(node, new_parent, new_name, expect_parent)` moves the node only if it is still in `expect_parent`, else `ESTALE`. `VFSClient.claim(src, dst)` uses that for a Maildir-style claim from `tasks/new/` to `tasks/cur/`: of workers racing for one file, exactly one gets True. Without `expect_parent`, a rename is `RENAME_NOREPLACE` (`EEXIST` if the destination exists).

## Path resolution and the directory cache

**Resolution on the server.** `vfs_lookup(segments)` walks the parent pointers in one round trip, one index probe per component, and returns `(id, kind, chain)`. `chain` is the id of every component it found: the whole path on success, the prefix that resolved on `ENOENT`, and up to the file on `ENOTDIR`. It is the only column an outcome sets besides the error. `vfs_lookup(segments, cached)` takes the ids of the path's first components as a client's cache has them. If every one is still the directory of that name in the one before it (checked in one statement), the walk starts below the last; otherwise it starts from the root (migration 014). The answer is the same either way.

**The cache** (`minifs.vfs.cache.DirEntryCache`) maps `(parent id, name) -> (id, kind)`, per process, in memory. Entries are keyed by parent id, not path, so renaming `/a` changes only the `(root, "a")` entry, and everything below stays valid. It fills the way the kernel's dcache does: every statement that walks a path returns the chain in the same row, and the client stores each component (directories for the intermediate ones, the returned kind for the last). That covers `stat`, reads, writes, unlink, both sides of a rename (which also stores the moved node under its new name), creates (`vfs_mkdir_p` returns its chain too), the root of a glob or grep, and `pread_many`. So after `Glob` hands an agent a list of paths, the next `Read` under the same directory hits every shared component, though no directory was ever listed.

**Every use is checked.** When the cache holds a whole path, the statement checks every cached `(id, parent, name)` link against `vfs_nodes` instead of walking, and falls back to `vfs_lookup` if any link no longer holds. The fallback's chain then replaces the stale links. A path whose first components are cached is walked from the deepest of them; if that prefix no longer holds, the chain the server returns shows the first stale link, which the client evicts with no extra query. So **a stale entry costs a slower resolution, never the wrong node**.

| | Linux dcache | minifs |
| --- | --- | --- |
| Filled by | Each component of a path walk | Each component of a server walk, listings, creates, renames |
| Negative entries | Yes | No: a name another client creates is visible at once |
| Revalidation | Local filesystems trust it; network ones call `d_revalidate` | Every use, in the same statement |
| Invalidation | The VFS sees every rename and unlink | NOTIFY `move`/`delete` (by node id, through a reverse map), the client's own changes, a TTL |

**The TTL** (60 s by default) covers what notifications can't: events lost while the `LISTEN` connection reconnects (the cache is cleared when it is back), and a lookup whose snapshot predates a move but whose result is stored after the move's notification was handled. Since every use is checked, the TTL can be long. It counts from the store, and a fully cached hit stores nothing, so every path is walked again once per TTL however often it hits. Replaying a 15-call session of the example agent, with the cache's clock moved on by a simulated inference gap at each model response, gave these component hit rates (#55):

| TTL | no gap | 3 s | 8 s | 15 s |
| --- | --- | --- | --- | --- |
| 2 s | 0.87 | 0.38 | 0.38 | 0.38 |
| 10 s | 0.87 | 0.82 | 0.78 | 0.38 |
| 30 s | 0.87 | 0.85 | 0.82 | 0.78 |
| 60 s (default) | 0.87 | 0.87 | 0.85 | 0.82 |

The remaining misses are first uses. An entry costs about 500 bytes, and `max_entries` (10,000) caps the total. Concurrent operations that find the same cached path stale share one eviction query.

**No content cache.** Line reads already send only the lines asked for, so caching bytes would save little. If metrics ever show otherwise, bytes cached under `(id, generation)` can be revalidated by the read itself: `vfs_pread(node, cached_size, limit)` returns the current generation with anything new past the cached end.

## Watch

A trigger on `vfs_nodes` (migration 005) sends one NOTIFY on channel `vfs` per changed row, as a small JSON payload. Every event has `op`, `id` and `parent`:

| `op` | Extra fields | Emitted by |
| --- | --- | --- |
| `create` | `name`, `kind` | create, mkdir |
| `modify` | `size`, `gen` | append, overwrite |
| `move` | `name`, `old_parent`, `old_name`, `kind` | rename, claim |
| `delete` | `name`, `kind` | unlink, rmdir |
| `rescan` | | `TRUNCATE`; also sent by the watcher after a reconnect or overflow |

Notifications are delivered at commit, so a watcher is never woken for data it can't read yet, and a rolled-back write emits nothing. Compaction emits nothing. Events are hints: Postgres drops a payload identical to one already sent in the same transaction, and any role that can connect can NOTIFY, so a watcher reads the database before acting.

`minifs.vfs.watch.Watcher` holds the process's one `LISTEN` connection, from a connection factory, so each reconnect has a fresh credential. `watcher.watch(path)` filters it by node id (a file) or parent id (a directory). It subscribes before resolving the path, so nothing committed after it returns is missed. Like inotify, a watch follows the node through renames until it is deleted. After a reconnect, every subscriber gets a `rescan`, and a watch whose node is gone ends. A keepalive probe (every 30 s) notices a connection that died silently. The directory cache subscribes to the same connection.

## Glob

`Glob` and `Grep` share one walker and one matcher, as they do inside ripgrep. The walker is `vfs_glob`, a recursive CTE over the parent pointers that reads only `vfs_nodes`. Each directory level is an index lookup on `(parent_id, name)`.

**Semantics.** Patterns are ripgrep's `-g` globs, compiled by ripgrep's own code (`globset` and the `ignore` crate's gitignore rules, in `crates/minifs-regex/src/glob.rs`). A pattern without `/` matches the file name at any depth (`*.md`). One with `/` is anchored at the search root (`src/*.py`). `**/` is zero or more directories, `dir/**` is everything below `dir`, and `{a,b}` alternatives may nest. Only files are returned. For `Glob`, hidden directories are never descended into and a hidden file is returned when the pattern matches it, as `rg --files` does. `Grep` searches hidden files. Both always skip `.git`. Excludes (`**/node_modules/**`, `build/`) drop matching files and never walk matching directories.

**Translation.** The gitignore rules rewrite the pattern as rg does (a leading `/` anchors, no `/` gets `**/` in front), `globset` writes a regex, and the regex translator (below) emits it as a Postgres regex. On the way, non-ASCII bytes become characters again and classes lose `/`. Errors carry rg's message: `error parsing glob '<pattern>': <reason>`.

**Divergences from rg**, checked against the `rg` binary in `tests/test_glob.py`:

- `?` and classes match one character; rg's match one byte. `?.md` finds `é.md`.
- A class never matches `/`, where rg's negated classes and ranges spanning `/` can.
- A leading `!` or `#`, an empty pattern, `/`, and (for `Glob`) a trailing `/` are errors, rather than a negation, a comment, every file or nothing.
- globset 0.4.18+ accepts nested braces and rejects a stray `}`; rg 14.1 bundles an older globset that does the reverse.

**Pruning hints**, computed from the same parse:

| Hint | Example | Effect |
| --- | --- | --- |
| Literal prefix | `docs/guides/**/*.md` | Walk only `docs/guides` |
| Max depth | `src/*/*.py` | Stop the recursion 2 levels below `src` |
| Name filter | `*.md` | `name LIKE '%.md'` before the full regex |
| Pruned directories | `**/node_modules/**` | Never expanded |

The tests check that results with every hint equal results with none. A smoke benchmark finds `src/**/*.py` in a 10,000-node tree in about 15 ms. Results are sorted newest first, then by path.

## Search

`Grep` runs entirely in Postgres and scans the current bytes of the files in scope, extent by extent, the way ripgrep scans a disk. No index is kept in step with writes, so a search sees every committed write.

**Why no index.** An earlier draft kept every line in a table with a trigram index. It paid on every write (2 to 4 times the text in storage, a trigger on each append, a full re-index on each overwrite) to speed up lookups that matter only once the bytes in scope get large. The target is workspaces of files from kilobytes to a few megabytes, where a scan is enough. Line-aware extents aren't a line table: there is no row per line and no index on content.

**How a search runs**, in one `REPEATABLE READ` transaction, every statement under `statement_timeout` (2 s by default):

1. **Scope.** `vfs_glob` picks the files and reads their sizes from `vfs_nodes`, before any content. The client refuses a scope over 10,000 files or 50 MB with a hint to narrow `path` or `include`.
2. **Extent filter.** Each extent holds whole lines and no split UTF-8 character, so it is decoded on its own and tested once against the pattern in newline-sensitive mode (`(?n)`: `^` and `$` match at line breaks, and classes never contain `\n`). An extent passes exactly when one of its lines matches.
3. **Lines.** Only passing extents are split on `\n`, each line zipped with its offset from `line_offsets`. Line k of an extent is file line `line_start + k`, at byte `byte_offset + line_offsets[k]`. Files are searched in path order, each in line order, and the scan stops once it has the hits it needs.

The cost grows with the bytes scanned, not the matches: a common pattern reads only the first extents, a rare one the whole scope (about 0.7 s for 45 MB on a laptop). A file that isn't valid UTF-8 is skipped whole, so results don't depend on how its bytes are cut. Validity is decided when an extent is written and stored with it (`vfs_extents.utf8`), so the search reads flags rather than decoding each file twice. That costs writes 2 to 7%, halves a full-scope scan, and makes a search that stops at its limit early about 7 times faster, since it no longer decodes the whole first file. A UTF-8 BOM isn't part of the first line, as in rg.

**Options.** The tool takes `pattern`, `path` (a directory or one file) and `include`. A model gets case-insensitive search with `(?i)`. The library's `VFSClient.grep` adds `ignore_case`, `fixed` (literal strings, via `strpos`), `excludes`, `hidden`, the caps, the timeout and `keep` (the view's policy, so unreadable files are left out before content is read). Each hit carries its byte offset as well as its line number. Context lines (`-C`), `--max-count` and multiline (`-U`) aren't supported.

## Regex translation

`Grep` patterns are Rust regexes, as in rg, and Postgres speaks ARE (Advanced Regular Expressions). minifs parses each pattern with [`regex-syntax`](https://docs.rs/regex-syntax), the parser behind ripgrep, and emits ARE from it. No open-source translator in that direction exists, and the extensions that bring PCRE or RE2 into Postgres aren't offered by most managed services. This is the riskiest part of search, since a translation error fails silently.

**Why it's needed.** Some patterns mean something else in ARE:

- In ARE, `\b` is **backspace**, so `\bparse_config\b` silently matches nothing. The model concludes the code isn't there.
- ARE's `\w`, `[[:alpha:]]`, `\y` and `~*` classify characters by the database's locale. Under `C`, which some managed services use, they're ASCII-only: `'é' ~ '^\w$'` is false under `C` and true under `en_US.utf8` (measured on PG 18). Rust's `\d` matches `٣`, ARE's doesn't.

So the translator never relies on ARE's classes, word boundaries or case folding. It spells each one out as explicit code-point ranges, which mean the same in every locale.

**Emitting from the HIR.** regex-syntax produces an AST (the pattern as written, used for rg's error messages, with a caret under the problem) and a HIR (what it means: flags applied, `(?i:ab)` already `[Aa][Bb]`, `\pL` and `[a-z&&[^aeiou]]` resolved to ranges). The HIR has eight node kinds, so the emitter is one small function per kind:

| The model writes | Emitted ARE |
| --- | --- |
| Literals, `\x{1F600}`, `\t` | The characters, metacharacters escaped |
| `\w`, `\d`, `\s`, `.`, `[...]`, `\pL`, set operations | A bracket expression of ranges, without `\n` and NUL |
| `(?i)` | Case-folded ranges such as `[Ee]`, so search always uses `~` |
| `^`, `$`, `\A`, `\z` | `^`, `$` (rg's text is the line) |
| `\b`, `\B`, `\<`, `\>`, `\b{start}`... | Lookaround over the explicit `\w` class |
| `*`, `+`, `?`, `{n,m}`, lazy forms | The same quantifier (counts up to 255, ARE's limit) |
| Groups, alternation | `(?:...)`, `|` |

**Rejected, with an rg-style message:** lookaround and backreferences (as rg without `-P`), a literal `\n` (as rg without `-U`), byte classes outside ASCII, counts above 255, and a translation over the size cap. Rejecting is always better than a silent zero-match. Each translation is also compiled once in Postgres (`SELECT '' ~ $1`); a failure there is reported as a translator bug, never as the model's mistake.

**Cost of explicit classes**, on PG 18 over 200,000 lines:

| Pattern | Time |
| --- | --- |
| `config\(\w+\)` with ARE's `\w` | 138 ms |
| The same with the 796-range Unicode `\w` | 150 ms |
| `PARSE_CONFIG` with `~*` | 153 ms |
| `[Pp][Aa][Rr][Ss][Ee]_...` with `~` | 154 ms |
| The lookaround word boundary | 302 ms |

Correct semantics in every locale cost little, except word boundaries, which cost about 1.6 times as much.

**How we know it's right.** The oracle is Rust's `regex` crate, built into the extension behind a test-only feature: for every pattern and line, `regex.is_match(line)` must equal Postgres's `line ~ translation`, in a `C`-locale and a UTF-8 database. The corpus is real model patterns plus generated ones (random HIR printed back as a pattern). Separate property tests check that every pattern regex-syntax rejects is rejected by the `rg` binary too, and that the extent filter never drops a line match.

## The agent tools in detail

The tools follow opencode's file-tool contracts (which Claude Code's resemble): names, parameters, descriptions, output formats and errors, so a model moves between a workspace on disk and minifs without prompt changes. They are plain `async def` functions over a `VFSView`, each wrapped in a `pydantic_ai.Tool` (`minifs.tools.base.vfs_tool`). Paths are absolute in the view; a relative one resolves against `vfs_workdir`.

**Read.**
- **Usually one round trip.** A 4 KB sample (to tell binary from text), the page's first `vfs_pread_lines` call and the instruction-file lookups go out together, so a `Read` costs about three statements however deep the file is. If the sample, the page and the attachments come from different versions of the file, the read starts again, up to three times.
- **Pages.** Lines are cut at 2,000 characters (`... (line truncated to 2000 chars)`), and output stops at 50 KB with a footer giving the next `offset`. The last page ends `(End of file - total N lines)`.
- **Binaries.** Images and PDFs, recognised by magic bytes or extension, are attached as `BinaryContent` up to 3.75 MB. Other binaries are refused (`Cannot read binary file`).
- **Directories** list the readable entries, `/` after subdirectories, sorted case-insensitively and paged by `offset` and `limit`.
- **Nearby instructions.** For a file below `/workspace`, each directory from the file's parent up to `/workspace` is checked for `AGENTS.md`, else `CLAUDE.md`. Those not yet shown in the conversation are appended in a `<system-reminder>`.
- **Stateless.** There's no per-agent cursor: to tail a log, the agent reads again from the offset its last footer gave.

**Write** creates the file (and missing parents) or replaces it as a new generation. It stats the file and reads the old text for the diff, then writes with a compare-and-swap. A lost race (the file changed, appeared or vanished in between) starts over once; a second loss is the retry `<path> was modified since it was read. Read it again before writing.` An existing UTF-8 BOM is kept. The metadata carries a unified diff and the added and deleted line counts. Old files over 1 MB aren't read or diffed. The diff runs in a worker thread so a large one doesn't stall the event loop.

**Edit** reads the whole file with its version, replaces in Python with opencode's fallback replacers (exact, line-trimmed, block-anchored, whitespace- and indentation-flexible, and so on), and writes with a compare-and-swap. A lost race redoes the whole read-replace-write once. Files must be strict UTF-8 and at most 10 MB. The file's line endings and BOM are kept. An empty `oldString` creates a file. `Write` and `Edit` are `sequential` tools, so pydantic-ai runs nothing alongside them in the same response.

**Glob and Grep** are described above. Output: one absolute path per line for `Glob`, newest first; `Found N matches`, then hits grouped by file as `  Line N: text`, for `Grep`. Both return `No files found` when nothing matches, and a truncation footer past 100 results.

**Errors.** A mistake the model can fix by calling again raises `ModelRetry` with a message that says what to change: `File not found` (with up to three similar names from inside the view), an offset past the end, a bad pattern (rg's message), a scope over the cap, a timeout. A path outside the view gets each tool's not-found error, so errors never reveal what exists beyond the chroot. A policy denial is returned as the result, `Permission denied: <action> <path>`, since retrying won't help. `minifs.tools.errors.tool_error` does the mapping at each tool's boundary.

**Where the tools differ from disk-based implementations of the same contracts:**
- The view's policy is the only permission gate.
- Writes are compare-and-swap checked in the database, not a per-path lock in one process.
- `Edit` has a 10 MB cap, and `Write` skips the diff of an old file over 1 MB.
- There's no `.gitignore`: a default exclude list of unambiguous caches applies instead, configurable in `ToolSettings`.
- `Glob` returns newest first, `Grep` by path then line, so results don't vary between runs.
- Files that aren't UTF-8 aren't searched.
- Searches have file, byte and time caps.

## Running it in production

| Topic | Guidance |
| --- | --- |
| Credentials | Managed services use short-lived tokens as the password. `Connector(conninfo, password=fetch_token)` fetches one for every new connection: pools and watcher reconnects alike. |
| LISTEN | Use a direct connection, never a transaction-mode pooler (`open_vfs(..., listen=...)`). Its keepalive probe keeps a Lakebase compute from scaling to zero while a watcher runs. A listener that stops reading holds back the shared NOTIFY queue. |
| Pools | Per process: one `LISTEN` connection, a write pool (about 5) and a smaller search pool, so searches don't starve writes. A tool call holds one to three connections while it runs. |
| NOTIFY volume | A transaction that notifies takes a database-wide lock at commit, so every append commits through it. If a hot log makes that a bottleneck, consider not emitting `modify` for it. |
| Vacuum | Appends leave a dead `vfs_nodes` version each, mostly pruned in place thanks to HOT; overwrites and compaction leave dead extents. Watch `n_dead_tup` on both tables. Long-open transactions block pruning. |
| Compaction | Run `vfs_compact` periodically on files with many extents (say over 1,000). It holds each inode lock only while merging that file. |
| Versions and extensions | PG 16 to 18 are tested (the schema needs 15+ for `UNIQUE NULLS NOT DISTINCT`). No extensions are required. |

**Protecting the database.** Every search statement runs under a timeout. Scope caps (files and bytes, from `vfs_nodes`) refuse an oversized search before any content is read. Output is capped at 100 results with a hint to narrow. Search has its own pool. A read replica would also isolate search load, at the price of read-your-writes consistency.

**What to watch:** tool latency and calls per run; search duration, bytes scanned, timeouts, refusals and translator rejections by construct; the directory cache's hit rate (`cache.stats()`); extents per file; policy denials (`Policy.denials`, by actor and action).

## Testing

Everything runs against real Postgres (16, 17 and 18 in CI) in testcontainers; there are no SQL mocks. Most of the correctness rests on comparing two independent answers:

| Property | How it's checked |
| --- | --- |
| Reads | Joined `pread` chunks equal the whole file, for random offsets, also while another task appends; `vfs_pread_lines` from every line equals splitting the content in Python |
| Line-aware extents | After random appends (partial lines, split UTF-8 characters, long lines, no newlines), overwrites and compactions, the extents tile the file and every line column is exact |
| Concurrency | Concurrent appenders give gap-free offsets in commit order; racing claimers have exactly one winner; concurrent directory moves can't make a cycle |
| Cache | Cached and uncached clients agree; a stale chain never reaches the wrong node; another client's renames are seen once their NOTIFY arrives |
| Glob | The translator agrees with rg's own matcher (the `ignore` crate) on generated patterns, and that matcher with the `rg` binary; pruned walks equal full walks |
| Regex | Translations agree with Rust's `regex` crate in a `C` and a UTF-8 database; rejections match rg's |
| Search | Hits equal a line-by-line search in Python; `include` scopes equal `glob` |
| Tools | Through a real pydantic-ai `Agent` with a scripted `FunctionModel`; `Write` and `Edit` output is compared with a golden corpus from a disk-based implementation of the same contracts |
| Example | `examples/agent.py` and the README's snippet run end to end |

## Not built yet

- A scheduled compaction job, and metrics export.
- `.gitignore` support, `Grep` context lines and `--max-count`.
- A trigram index for search, if bytes scanned per search grow too large. The extents hold whole lines, so a line match never crosses an extent boundary, and an index on their text would stay exact.
- A content cache (see the directory cache section).
- Semantic search over file text (pgvector).
- Kernel mounts where they're allowed, through a TigerFS front end over the same tables.

## Sources

- [PostgreSQL pattern matching](https://www.postgresql.org/docs/current/functions-matching.html): ARE syntax, its incompatibilities with Perl, and the advice to time-limit untrusted patterns.
- [regex-syntax](https://docs.rs/regex-syntax) and [regex](https://docs.rs/regex): the parser the translator uses, and the test oracle.
- [pgpcre](https://github.com/petere/pgpcre), [re2 for PostgreSQL](https://api.pgxn.org/src/re2/re2-0.4.0/doc/re2.md), [omni_regex](https://docs.omnigres.org/omni_regex/regex/), [PL/Rust on RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.Using.PL_Rust.html): the Postgres regex extensions considered, none widely available on managed services.
- [Databricks Apps best practices](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/best-practices), [ECS KernelCapabilities](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_KernelCapabilities.html), [Lakebase extensions](https://docs.databricks.com/gcp/en/oltp/projects/extensions): the platform constraints.
- [TigerFS](https://github.com/timescale/tigerfs): the parent-pointer directory model (ADR-017).
