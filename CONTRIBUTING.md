# Contributing

## Setup

You need [uv](https://docs.astral.sh/uv/), a Rust toolchain (stable, via rustup) and Docker (tests start
PostgreSQL in testcontainers).

```sh
uv sync            # creates .venv and builds minifs._regex in place, with the `oracle` feature
```

`uv sync` rebuilds the extension whenever `crates/**` changes. To force a rebuild: `uv sync --reinstall-package
minifs`.

## Checks

```sh
uv run pytest                      # tests against postgres:18 in testcontainers
MINIFS_TEST_PG_IMAGE=postgres:16 uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run pyright                     # strict, over minifs/ and tests/
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --features oracle
```

CI runs all of these, with the tests on PG 16, 17 and 18: the versions Databricks Lakebase, the production
target, offers. Install ripgrep (`rg`) locally to run the tests that
cross-check against it; without it they skip, while in CI they fail.

## Layout

| Path | Contents |
| --- | --- |
| `minifs/vfs/`, `minifs/tools/` | VFS library and agent tools |
| `minifs/db/migrate.py` | Migration runner, also `minifs db migrate` / `minifs db status` |
| `minifs/db/migrations/NNN_name.sql` | Schema migrations, applied in order, each once |
| `crates/minifs-regex/` | Rust regex translator, imported as `minifs._regex` (stubs in `minifs/_regex.pyi`) |
| `tests/conftest.py` | Postgres fixtures |

## Migrations

Add a file `minifs/db/migrations/NNN_lower_snake.sql` with the next free number. Each file runs in one
transaction with its row in `vfs_schema_version`, so don't use statements that can't run in a transaction
(`CREATE INDEX CONCURRENTLY`), and don't use `BEGIN`/`COMMIT` (the runner refuses a migration that does).
`migrate()` needs an autocommit connection. Never edit a migration once it's merged: the runner stores a checksum and refuses
a changed file. Add a new migration instead. A pending migration numbered below one already applied is refused
too, so renumber a branch's migration after the latest one on `main` before merging.

The version table and the objects go in the connection's `current_schema()`. Each migration starts with the
caller's `search_path`, so prefer `SET LOCAL` inside one. Cluster-wide objects (roles) outlive a test database,
so create them idempotently (`DO $$ ... IF NOT EXISTS ...`).

`minifs db status` prints each migration as `applied`, `pending` or `modified` (changed since it was applied),
and versions the database has but this checkout doesn't as `unknown`.

## SQL functions and errors

The tree lives in `vfs_nodes` (migration 003). Every operation is a SQL function (`vfs_lookup`, `vfs_readdir`,
`vfs_create`, `vfs_mkdir_p`, `vfs_rename`, `vfs_unlink`); the header comment of each says what it does, returns
and raises. A NULL directory id stands for the virtual root `/`.

File bytes live in `vfs_extents` (migration 004): `vfs_append`, `vfs_write`, `vfs_pread` and `vfs_compact`, and
`vfs_create` takes optional initial content. The extents of a file's current generation tile `[0, size)` with no
gaps. Every function that changes an existing file's content keeps that invariant under the inode lock
(`vfs_lock_file`); `vfs_create` needs no lock, since nobody else can see the new file yet. Readers take no locks.

Extents are line-aware (migration 007): every extent starts at a line start, and all but a file's last end with
`\n`. Each carries `line_start` (the number of `\n` before it) and `line_offsets` (where each of its lines starts),
and `vfs_nodes.lines` holds the line count. Every writer cuts content with `vfs_cut`, an append re-cuts the last
extent when the file ends mid-line, and compaction merges whole extents, so the alignment holds. `vfs_pread_lines`
reads by line, optionally cutting each line to a prefix (migration 011). Readers are `STABLE`, so all their statements share the caller's snapshot; `vfs_file` is their
unlocked counterpart of `vfs_lock_file`. Raw inserts into `vfs_extents` (tests) must give both line columns.

Search (migration 008) scans those extents: `vfs_search` filters each extent with a pattern's file form, splits
only the extents that pass into lines, and returns the matching ones with their line numbers and byte offsets,
stopping at the limit; a file with content that isn't UTF-8 is skipped whole. The scope comes from `vfs_glob` in a
statement of its own, so `minifs.vfs.search` can refuse it over the file and byte caps before content is read.

Errors come in two groups (migration 009):

| Group | Errnos | How the function reports it |
| --- | --- | --- |
| **Outcomes**: the answer is "no" | ENOENT, ENOTDIR, EISDIR, EEXIST, ESTALE, ENOTEMPTY | **Returned** in two result columns, `err` (the errno name, or NULL) and `err_detail` |
| **Faults**: bad input or a broken invariant | EINVAL, ENAMETOOLONG, and anything not errno-coded (`data_corrupted`, ...) | **Raised** with `PERFORM vfs_raise('EINVAL', detail)` |

An outcome is something a correct caller meets in normal use: a path an agent guessed, a name already taken, a
lost Maildir claim or compare-and-swap, a non-empty directory. Raising it would cost a server log entry each time
(an ERROR with DETAIL, CONTEXT and the statement, which name real paths, and which a non-superuser can't silence
on Lakebase), an ERROR rate that can't tell guesses from faults, and the caller's transaction, which a raise
aborts (a REPEATABLE READ snapshot, say). So:

- A function that can produce an outcome has `err text, err_detail text` as its last result columns. On an outcome
  it changes nothing and returns one row with `err` set and every other column NULL. One that returns nothing else
  (`vfs_rename`, `vfs_unlink`) returns one row on success too, with `err` NULL. Assign the OUT variables and
  `RETURN NEXT; RETURN;`. A helper that finds an outcome (`vfs_lock_dir`, `vfs_lock_file`, `vfs_file`) returns it
  the same way, for its caller to pass on.
- A new outcome errno is returned, never raised: add it to `OUTCOMES` in `minifs.db.errors` too. A new errno of
  either group goes in `vfs_raise` and `SQLSTATE_BY_ERRNO`, which the constraints' backstop also uses.
- A node id (not a directory id, where NULL is the root) that is NULL raises EINVAL (`vfs_check_node`): it is what
  an unchecked failed lookup gives, and must fail loudly.
- Raw SQL and the rare races the functions don't pre-check still hit the constraints, which raise (a create
  committing between `vfs_rename`'s check and its update raises `unique_violation`); `errno_name(exc)` maps those
  and the faults back to errnos.
- Tests: `tests.test_tree.raises_errno` checks both the errno and its group (returned for an outcome, raised for a
  fault); `raises_db_errno` expects a raise, for the constraints.

Production runs on Databricks Lakebase (PostgreSQL 16 to 18, no superuser, no `pg_uuidv7`, default collation
`C.UTF-8`), so SQL must work as a plain role that owns its database; `test_migrations_need_no_superuser` checks
that.

## VFS library

`minifs.vfs.client.VFSClient` takes absolute paths and wraps one SQL function per operation (`stat`, `readdir`,
`pread`, `pread_lines`, `pread_many` (several preads, each path's outcome in its place, in one statement; with `groups`, only up to the first file of each group is read), `glob` (resolving and walking in one REPEATABLE READ snapshot on the search pool), `grep`
(the same, then `vfs_search`, under a 2 s `statement_timeout`; `SearchRefused` over the scope caps or on timeout), `create`, `mkdir`, `mkdir_p`, `append`, `write`, `rename`, `claim`, `unlink`, `compact`). Each runs as one
statement in its own transaction, resolving its paths on the server with `vfs_lookup` in a subquery, and calling
the operation only if every lookup succeeded (`_gated`). It raises `minifs.vfs.core.VFSError(errno, path, message,
detail)` for every errno the SQL layer reports, returned (through `_outcome`, with `db_error` None) or raised. `detail` (the
server's DETAIL, with node ids and real path components) and `db_error` are for logs only: a view that rewrites
paths (the chroot) translates `path` and never shows `detail`. Paths go through
`minifs.vfs.core.normalize` first. With a `DirCache` (the hook for #19's directory-entry cache), a fully cached
path is checked link by link in the same statement instead of walked, and falls back to `vfs_lookup` if any link
went stale, so a stale cache never reaches the wrong node. A server lookup (`vfs_lookup`) returns the ids it walked
(`chain`, migration 010) in the same row, and the client stores each component, as the kernel's dcache does on a
path walk. A query that `_run` runs with refs starts with their `_walks` columns, which it cuts from the rows. `minifs.vfs.cache.DirEntryCache` is that cache: positive
entries with a 2 s TTL, kept fresh by `follow(watcher)` (evict by id on `move` and `delete`, clear on `rescan`).

`minifs.vfs.view.VFSView(client, mounts, policy, actor)` is what the agent tools call: the same operations in
the paths of an actor's chroot (`workspace_mounts(workspace_id)` gives the agent's), each checked against the
ordered rules of `minifs.vfs.policy.Policy` first. Paths outside every mount are ENOENT, a denial is EACCES and
moving a mount point is EBUSY; errors come back with the visible path and no `detail`, other database errors
as EIO. `glob` returns a `GlobResult(entries, truncated)`, leaving out the files the actor may not read, and
`grep` a `GrepResult`, searching only those files (passed to `VFSClient.grep` as `keep`). `truncated` says at least one
more readable match exists, so exactly `limit` matches is not truncated, with two exceptions: glob reports False when
hidden files fill its refetch window, and a grep from a virtual directory reports True when a later mount's
search is refused after the limit is reached. A new client operation reaches the tools by a view method that
maps its paths with `_target` and wraps the call in `_mapped`.

Connections come from `minifs.db.pool`: a `Connector` (a static conninfo, plus an optional callable that returns
a fresh password per connection, for Lakebase OAuth tokens) is also the `ConnectionFactory` for LISTEN, and
`open_pool` / `Pools` build psycopg pools on it that check each connection at checkout, so connections closed by
scale-to-zero are replaced. Nothing may depend on session state across transactions (no session advisory locks
or settings). A REPEATABLE READ transaction goes through `retry_serializable`, which reruns it on 40001 and 40P01.

## Database fixtures

| Fixture | Gives |
| --- | --- |
| `db` | conninfo of a fresh, migrated database (UTF-8 locale) |
| `db_c` | the same with `LOCALE 'C'` |
| `db_any_locale` | parametrised over `C`, `C.UTF-8` (Lakebase's default) and the UTF-8 locale; use for tests that must hold in every locale |
| `empty_db` | a fresh database with no migrations |
| `conn` | an autocommit `AsyncConnection` to `db` |
| `pg_version` | `server_version_num` (e.g. `160004`), for version-dependent tests |
| `rg_path` | the ripgrep binary |

The server and the migrated templates are created once per session; each test's database is a template clone,
dropped afterwards.

## Wheels

`.github/workflows/wheels.yml` builds abi3 wheels (Linux x86_64 and aarch64 manylinux, macOS arm64) and an sdist
on `v*` tags, on PRs that touch the build, and on demand. Release wheels don't include the `oracle` feature. `uv build` does include it (it reads
`[tool.uv] config-settings-package` when run in this checkout), so build release artifacts with maturin-action or `maturin build`.
On a `v*` tag, the `publish` job uploads them to PyPI; [RELEASING.md](RELEASING.md) has the steps.
