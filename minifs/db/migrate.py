"""Schema migrations: ordered ``NNN_name.sql`` files applied once each, recorded in ``vfs_schema_version``.

Each pending migration runs in its own transaction together with its ``vfs_schema_version`` row, under a
transaction-scoped advisory lock, so concurrent runners apply every migration exactly once. A migration file
that was edited after it was applied is reported as a checksum mismatch instead of being silently skipped.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from importlib.resources import files
from importlib.resources.abc import Traversable

from psycopg import AsyncConnection, sql
from psycopg.pq import TransactionStatus

# Arbitrary constant key for pg_advisory_xact_lock, shared by every minifs migration runner.
_LOCK_KEY = 0x6D696E6966730001

_FILENAME = re.compile(r"^(?P<version>\d{3})_(?P<name>[a-z0-9_]+)\.sql$")

_CREATE_VERSION_TABLE = """
CREATE TABLE {} (
    version    integer     PRIMARY KEY,
    name       text        NOT NULL,
    checksum   text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)
"""


class MigrationError(Exception):
    """A migration set is malformed or disagrees with what the database recorded."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def filename(self) -> str:
        return f"{self.version:03d}_{self.name}.sql"

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()


def default_migrations_dir() -> Traversable:
    return files("minifs.db") / "migrations"


def load_migrations(directory: Traversable | None = None) -> list[Migration]:
    """Read every ``NNN_name.sql`` file in ``directory``, ordered by version."""
    directory = directory or default_migrations_dir()
    migrations: dict[int, Migration] = {}
    for entry in directory.iterdir():
        if not entry.name.endswith(".sql"):
            continue
        match = _FILENAME.match(entry.name)
        if match is None:
            raise MigrationError(f"{entry.name}: migration files must be named NNN_lower_snake.sql")
        version = int(match["version"])
        if version in migrations:
            raise MigrationError(f"{entry.name}: version {version:03d} is also used by {migrations[version].filename}")
        migrations[version] = Migration(version, match["name"], entry.read_text(encoding="utf-8"))
    return [migrations[v] for v in sorted(migrations)]


async def applied_versions(conn: AsyncConnection, schema: str | None = None) -> dict[int, str]:
    """Version -> checksum for every recorded migration; empty if the table doesn't exist yet.

    The table lives in ``schema``, by default the connection's ``current_schema()``.
    """
    table = _version_table(schema or await _current_schema(conn))
    cur = await conn.execute("SELECT to_regclass(%s) IS NOT NULL", (table.as_string(conn),))
    row = await cur.fetchone()
    if row is None or not row[0]:
        return {}
    cur = await conn.execute(sql.SQL("SELECT version, checksum FROM {}").format(table))
    return {version: checksum for version, checksum in await cur.fetchall()}


async def migrate(conn: AsyncConnection, migrations: list[Migration] | None = None) -> list[Migration]:
    """Apply every pending migration in order and return the ones this call applied.

    Safe to run concurrently from several processes. ``conn`` must not be inside a transaction. The version table
    and the migrations' objects go in the connection's ``current_schema()``; each migration starts with the
    caller's ``search_path``, and the caller gets it back afterwards even if a migration ran a plain ``SET``.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise MigrationError("migrate() needs a connection that is not inside a transaction")
    migrations = load_migrations() if migrations is None else migrations
    schema = await _current_schema(conn)
    table = _version_table(schema)
    cur = await conn.execute("SELECT current_setting('search_path')")
    row = await cur.fetchone()
    assert row is not None
    search_path: str = row[0]

    applied: list[Migration] = []
    try:
        for migration in migrations:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(%s)", (_LOCK_KEY,))
                await conn.execute("SELECT set_config('search_path', %s, true)", (search_path,))
                recorded = await applied_versions(conn, schema)
                if not recorded:
                    cur = await conn.execute("SELECT to_regclass(%s) IS NULL", (table.as_string(conn),))
                    if (await cur.fetchone()) == (True,):
                        await conn.execute(sql.SQL(_CREATE_VERSION_TABLE).format(table))
                if migration.version in recorded:
                    if recorded[migration.version] != migration.checksum:
                        raise MigrationError(f"{migration.filename} changed after it was applied (checksum mismatch)")
                    continue
                newer = [v for v in recorded if v > migration.version]
                if newer:
                    raise MigrationError(
                        f"{migration.filename} is pending but {max(newer):03d} is already applied;"
                        " renumber it after the latest migration"
                    )
                await conn.execute(migration.sql.encode())
                await conn.execute(
                    sql.SQL("INSERT INTO {} (version, name, checksum) VALUES (%s, %s, %s)").format(table),
                    (migration.version, migration.name, migration.checksum),
                )
                applied.append(migration)
    finally:
        if conn.info.transaction_status == TransactionStatus.IDLE:
            await conn.execute("SELECT set_config('search_path', %s, false)", (search_path,))
    return applied


async def _current_schema(conn: AsyncConnection) -> str:
    cur = await conn.execute("SELECT current_schema()")
    row = await cur.fetchone()
    if row is None or row[0] is None:
        raise MigrationError("no schema to migrate: search_path names no existing schema")
    return row[0]


def _version_table(schema: str) -> sql.Identifier:
    return sql.Identifier(schema, "vfs_schema_version")
