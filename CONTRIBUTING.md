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

The tree lives in `vfs_nodes` (migration 003). Every operation is a SQL function (`vfs_resolve`, `vfs_readdir`,
`vfs_create`, `vfs_mkdir_p`, `vfs_rename`, `vfs_unlink`); the header comment of each says what it does and
raises. A NULL directory id stands for the virtual root `/`.

File bytes live in `vfs_extents` (migration 004): `vfs_append`, `vfs_write`, `vfs_pread` and `vfs_compact`, and
`vfs_create` takes optional initial content. The extents of a file's current generation tile `[0, size)` with no
gaps. Every function that changes an existing file's content keeps that invariant under the inode lock
(`vfs_lock_file`); `vfs_create` needs no lock, since nobody else can see the new file yet. Readers take no locks.

Functions report errors with `PERFORM vfs_raise('ENOENT', detail)`, which raises SQLSTATE `MFnnn` (`nnn` = the
Linux errno number) with the errno name as the message. `minifs.db.errors.errno_name(exc)` maps it back. Add a
new errno to both `vfs_raise` and `SQLSTATE_BY_ERRNO`.

Production runs on Databricks Lakebase (PostgreSQL 16 to 18, no superuser, no `pg_uuidv7`, default collation
`C.UTF-8`), so SQL must work as a plain role that owns its database; `test_migrations_need_no_superuser` checks
that.

## Database fixtures

| Fixture | Gives |
| --- | --- |
| `db` | conninfo of a fresh, migrated database (UTF-8 locale) |
| `db_c` | the same with `LOCALE 'C'` |
| `db_any_locale` | parametrised over both; use for tests that must hold in either locale |
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
