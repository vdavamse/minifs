"""``minifs`` command line: ``db migrate`` / ``db status``, and ``ls``, ``cat``, ``put``, ``rm``, ``mv``, ``glob`` for
humans debugging the tree."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

import psycopg
from psycopg import AsyncConnection

from minifs.db.migrate import MigrationError, applied_versions, load_migrations, migrate
from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import VFSClient
from minifs.vfs.core import VFSError
from minifs.vfs.glob import GlobError

_CAT_CHUNK = 1 << 20


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="minifs")
    commands = parser.add_subparsers(dest="command", required=True)
    db = commands.add_parser("db", help="database administration")
    db_commands = db.add_subparsers(dest="db_command", required=True)
    for name, help_text in (("migrate", "apply pending schema migrations"), ("status", "list schema migrations")):
        _add_dsn(db_commands.add_parser(name, help=help_text))

    ls = _add_dsn(commands.add_parser("ls", help="list a directory"))
    ls.add_argument("path", nargs="?", default="/")
    ls.add_argument("-l", dest="long", action="store_true", help="kind, size and mtime too")
    cat = _add_dsn(commands.add_parser("cat", help="print a file"))
    cat.add_argument("path")
    put = _add_dsn(commands.add_parser("put", help="write a file from a local file or stdin"))
    put.add_argument("path")
    put.add_argument("source", nargs="?", help="local file to copy (default: stdin)")
    put.add_argument("-a", "--append", action="store_true", help="append instead of replacing the content")
    put.add_argument("-p", "--parents", action="store_true", help="create missing parent directories")
    rm = _add_dsn(commands.add_parser("rm", help="remove files or empty directories"))
    rm.add_argument("paths", nargs="+", metavar="path")
    glob = _add_dsn(commands.add_parser("glob", help="list the files matching a glob pattern, newest first"))
    glob.add_argument("pattern")
    glob.add_argument("path", nargs="?", default="/", help="directory to search (default: /)")
    glob.add_argument("--hidden", action="store_true", help="include hidden files and directories")
    mv = _add_dsn(commands.add_parser("mv", help="move or rename (fails if the destination exists)"))
    mv.add_argument("src")
    mv.add_argument("dst")
    args = parser.parse_args(argv)

    label = f"db {args.db_command}" if args.command == "db" else args.command
    try:
        if args.command == "db":
            return asyncio.run(_migrate(args.dsn) if args.db_command == "migrate" else _status(args.dsn))
        return asyncio.run(_with_client(args.dsn, _VFS_COMMANDS[args.command], args))
    except (MigrationError, VFSError, GlobError, psycopg.Error, OSError) as e:
        detail = f" ({e.detail})" if isinstance(e, VFSError) and e.detail else ""
        print(f"minifs {label}: {e}{detail}", file=sys.stderr)
        return 1


def _add_dsn(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "--dsn",
        default=os.environ.get("MINIFS_DSN", ""),
        help="libpq connection string (default: $MINIFS_DSN, then the PG* environment variables)",
    )
    return parser


async def _with_client(
    dsn: str, command: Callable[[VFSClient, argparse.Namespace], Awaitable[int]], args: argparse.Namespace
) -> int:
    connector = Connector(dsn)
    # Connect once directly, so a bad DSN or password reports libpq's error at once rather than a pool timeout.
    async with await connector():
        pass
    async with open_pool(connector, min_size=1, max_size=1, name="minifs-cli") as pool:
        return await command(VFSClient(pool), args)


async def _ls(vfs: VFSClient, args: argparse.Namespace) -> int:
    entry = await vfs.stat(args.path)
    entries = await vfs.readdir(args.path) if entry.is_dir else [entry]
    for e in entries:
        name = e.name + "/" if e.is_dir else (e.name if entry.is_dir else e.path)
        if args.long:
            mtime = e.mtime.isoformat(timespec="seconds") if e.mtime else "-"
            print(f"{e.kind:4} {e.size:>12} {mtime} {name}")
        else:
            print(name)
    return 0


async def _cat(vfs: VFSClient, args: argparse.Namespace) -> int:
    out = sys.stdout.buffer
    result = await vfs.pread(args.path, 0, _CAT_CHUNK)
    version = (result.id, result.generation)
    while True:
        out.write(result.content)
        if result.eof:
            break
        result = await vfs.pread(args.path, result.next_offset, _CAT_CHUNK)
        if (result.id, result.generation) != version:
            raise VFSError("ESTALE", args.path, "the file was replaced while it was read")
    out.flush()
    return 0


async def _put(vfs: VFSClient, args: argparse.Namespace) -> int:
    data = Path(args.source).read_bytes() if args.source else sys.stdin.buffer.read()
    try:
        if args.append:
            await vfs.append(args.path, data)
        else:
            await vfs.write(args.path, data)
    except VFSError as e:
        if e.errno != "ENOENT":
            raise
        await vfs.create(args.path, data, parents=args.parents)
    return 0


async def _rm(vfs: VFSClient, args: argparse.Namespace) -> int:
    for path in args.paths:
        await vfs.unlink(path)
    return 0


async def _glob(vfs: VFSClient, args: argparse.Namespace) -> int:
    for entry in await vfs.glob(args.pattern, args.path, hidden=args.hidden):
        print(entry.path)
    return 0


async def _mv(vfs: VFSClient, args: argparse.Namespace) -> int:
    await vfs.rename(args.src, args.dst)
    return 0


_VFS_COMMANDS: dict[str, Callable[[VFSClient, argparse.Namespace], Awaitable[int]]] = {
    "ls": _ls,
    "cat": _cat,
    "put": _put,
    "rm": _rm,
    "mv": _mv,
    "glob": _glob,
}


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
