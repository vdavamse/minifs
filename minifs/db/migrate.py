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

from psycopg import AsyncConnection

# Arbitrary constant key for pg_advisory_xact_lock, shared by every minifs migration runner.
_LOCK_KEY = 0x6D696E6966730001

_FILENAME = re.compile(r"^(?P<version>\d{3})_(?P<name>[a-z0-9_]+)\.sql$")

_CREATE_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS vfs_schema_version (
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


async def applied_versions(conn: AsyncConnection) -> dict[int, str]:
    """Version -> checksum for every recorded migration; empty if the table doesn't exist yet."""
    cur = await conn.execute("SELECT to_regclass('vfs_schema_version') IS NOT NULL")
    row = await cur.fetchone()
    if row is None or not row[0]:
        return {}
    cur = await conn.execute("SELECT version, checksum FROM vfs_schema_version")
    return {version: checksum for version, checksum in await cur.fetchall()}


async def migrate(conn: AsyncConnection, migrations: list[Migration] | None = None) -> list[Migration]:
    """Apply every pending migration in order and return the ones this call applied.

    Safe to run concurrently from several processes. ``conn`` must not be inside a transaction.
    """
    migrations = load_migrations() if migrations is None else migrations
    applied: list[Migration] = []
    for migration in migrations:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(%s)", (_LOCK_KEY,))
            await conn.execute(_CREATE_VERSION_TABLE)
            recorded = await applied_versions(conn)
            if migration.version in recorded:
                if recorded[migration.version] != migration.checksum:
                    raise MigrationError(f"{migration.filename} changed after it was applied (checksum mismatch)")
                continue
            if _has_statements(migration.sql):
                await conn.execute(migration.sql.encode())
            await conn.execute(
                "INSERT INTO vfs_schema_version (version, name, checksum) VALUES (%s, %s, %s)",
                (migration.version, migration.name, migration.checksum),
            )
            applied.append(migration)
    return applied


def _has_statements(sql: str) -> bool:
    """False for a file of only whitespace and ``--`` comments, which Postgres rejects as an empty query."""
    return any(line.strip() and not line.strip().startswith("--") for line in sql.splitlines())
