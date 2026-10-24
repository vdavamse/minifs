"""``minifs`` command line: ``minifs db migrate`` and ``minifs db status``."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import psycopg
from psycopg import AsyncConnection

from minifs.db.migrate import MigrationError, applied_versions, load_migrations, migrate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="minifs")
    commands = parser.add_subparsers(dest="command", required=True)
    db = commands.add_parser("db", help="database administration")
    db_commands = db.add_subparsers(dest="db_command", required=True)
    for name, help_text in (("migrate", "apply pending schema migrations"), ("status", "list schema migrations")):
        sub = db_commands.add_parser(name, help=help_text)
        sub.add_argument(
            "--dsn",
            default=os.environ.get("MINIFS_DSN", ""),
            help="libpq connection string (default: $MINIFS_DSN, then the PG* environment variables)",
        )
    args = parser.parse_args(argv)

    try:
        if args.db_command == "migrate":
            return asyncio.run(_migrate(args.dsn))
        return asyncio.run(_status(args.dsn))
    except (MigrationError, psycopg.Error) as e:
        print(f"minifs db {args.db_command}: {e}", file=sys.stderr)
        return 1


async def _migrate(dsn: str) -> int:
    async with await AsyncConnection.connect(dsn, autocommit=True) as conn:
        applied = await migrate(conn)
    for migration in applied:
        print(f"applied {migration.filename}")
    if not applied:
        print("schema is up to date")
    return 0


async def _status(dsn: str) -> int:
    async with await AsyncConnection.connect(dsn, autocommit=True) as conn:
        recorded = await applied_versions(conn)
    migrations = load_migrations()
    for migration in migrations:
        if migration.version not in recorded:
            state = "pending"
        elif recorded[migration.version] != migration.checksum:
            state = "modified"
        else:
            state = "applied"
        print(f"{state} {migration.filename}")
    known = {m.version for m in migrations}
    for version in sorted(set(recorded) - known):
        print(f"unknown {version:03d} (applied, but not in this minifs version)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
