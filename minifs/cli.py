"""``minifs`` command line: ``db migrate`` / ``db status``, and ``ls``, ``cat``, ``put``, ``quota``, ``rm``, ``mv``,
``glob``, ``grep`` for humans debugging the tree."""

from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import re
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import NoReturn
from urllib.parse import quote, unquote

import psycopg
from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from minifs._regex import RegexError
from minifs.db.migrate import MigrationError, applied_versions, load_migrations, migrate
from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import DEFAULT_LOCK_TIMEOUT, DEFAULT_WRITE_TIMEOUT, MAX_TIMEOUT, VFSClient
from minifs.vfs.core import MAX_FILE_SIZE, Quota, VFSError, normalize
from minifs.vfs.glob import GlobError
from minifs.vfs.search import DEFAULT_LIMIT, MAX_NODES, TIMEOUT, SearchRefused

_CAT_CHUNK = 1 << 20


class _Omitted:
    """The value of a quota limit flag (``--bytes``, ``--nodes``) that wasn't given: distinct from the None that
    its value ``none`` parses to, since an omitted limit is kept and ``none`` clears it."""


_OMITTED = _Omitted()


def _max_nodes(text: str) -> int | None:
    """The ``--max-nodes`` argument: an int >= 0, or the word ``none`` for no cap."""
    if text == "none":
        return None
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid max_nodes value: {text!r}") from None
    if value < 0:
        raise argparse.ArgumentTypeError(f"max_nodes must be >= 0 or 'none', not {value}")
    return value


def _limit(text: str) -> int | None:
    """A quota limit argument (``--bytes``, ``--nodes``): an int >= 0, or the word ``none`` in any case for no
    limit on that one."""
    if text.casefold() == "none":
        return None
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid limit: {text!r} (an integer >= 0, or 'none')") from None
    if value < 0:
        raise argparse.ArgumentTypeError(f"a limit must be >= 0 or 'none', not {value}")
    return value


def _timeout(text: str) -> float:
    """The ``--timeout`` argument: seconds over 0 and at most ``MAX_TIMEOUT``."""
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid timeout: {text!r}") from None
    if not 0 < value <= MAX_TIMEOUT:
        raise argparse.ArgumentTypeError(f"timeout must be over 0 and at most {MAX_TIMEOUT:g}, not {text}")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(prog="minifs")
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
    quota = _add_dsn(commands.add_parser("quota", help="show or set the quota of a directory"))
    quota.add_argument("path")
    quota.add_argument(
        "--bytes",
        type=_limit,
        default=_OMITTED,
        dest="max_bytes",
        metavar="N",
        help='cap on the bytes of the files below PATH ("none": no cap; omitted: keep the current cap)',
    )
    quota.add_argument(
        "--nodes",
        type=_limit,
        default=_OMITTED,
        dest="max_nodes",
        metavar="N",
        help='cap on the number of nodes below PATH ("none": no cap; omitted: keep the current cap)',
    )
    quota.add_argument("--remove", action="store_true", help="remove the quota (with --children: stop giving one)")
    quota.add_argument(
        "--children",
        action="store_true",
        help="set the quota of the directories created directly in PATH, not of PATH itself: there is no quota to "
        "keep, so an omitted limit is no limit; re-running this with other limits changes only the child "
        "directories without a quota of their own; setting both limits is recommended",
    )
    quota.add_argument(
        "--timeout",
        type=_timeout,
        help=f"seconds each write may run and wait for locks, over 0 and at most {MAX_TIMEOUT:g} (default "
        f"{DEFAULT_WRITE_TIMEOUT:g} to run, {DEFAULT_LOCK_TIMEOUT:g} to wait): setting a quota recounts its subtree, "
        "which can be slow on a big one, and --children waits for the creates in PATH",
    )
    rm = _add_dsn(commands.add_parser("rm", help="remove files or empty directories"))
    rm.add_argument("paths", nargs="+", metavar="path")
    glob = _add_dsn(commands.add_parser("glob", help="list the files matching a glob pattern, newest first"))
    glob.add_argument("pattern")
    glob.add_argument("path", nargs="?", default="/", help="directory to search (default: /)")
    glob.add_argument("--hidden", action="store_true", help="include hidden files and directories")
    glob.add_argument(
        "--max-nodes",
        type=_max_nodes,
        default=MAX_NODES,
        help=f'stop past this many files and directories walked (default {MAX_NODES}; "none": no cap)',
    )
    glob.add_argument(
        "--timeout",
        type=_timeout,
        default=TIMEOUT,
        help=f"seconds each statement may run, over 0 and at most {MAX_TIMEOUT:g} (default {TIMEOUT:g})",
    )
    grep = _add_dsn(commands.add_parser("grep", help="print the lines matching a regex (ripgrep syntax)"))
    grep.add_argument("pattern")
    grep.add_argument("path", nargs="?", default="/", help="directory or file to search (default: /)")
    grep.add_argument("-g", "--include", help="search only the files matching this glob")
    grep.add_argument("-i", "--ignore-case", action="store_true", help="case-insensitive search")
    grep.add_argument("-F", "--fixed-strings", action="store_true", help="the pattern is a literal string")
    grep.add_argument("--no-hidden", action="store_true", help="skip hidden files and directories")
    grep.add_argument(
        "-m", "--limit", type=int, default=DEFAULT_LIMIT, help=f"at most this many hits ({DEFAULT_LIMIT})"
    )
    mv = _add_dsn(commands.add_parser("mv", help="move or rename (fails if the destination exists)"))
    mv.add_argument("src")
    mv.add_argument("dst")
    args = parser.parse_args(argv)
    if args.command == "quota":
        # The flag rules are argparse errors, so they are reported with the quota parser's usage (exit code 2).
        # A given limit (even "none") is not _OMITTED; an omitted one is.
        if args.remove and (args.max_bytes is not _OMITTED or args.max_nodes is not _OMITTED):
            quota.error("--remove can't be combined with --bytes or --nodes")
        if args.children and not (args.remove or args.max_bytes is not _OMITTED or args.max_nodes is not _OMITTED):
            quota.error("--children needs --bytes, --nodes or --remove")

    label = f"db {args.db_command}" if args.command == "db" else args.command
    secrets: list[str] = []
    try:
        dsn, secrets = _connection(args, label)
        if args.command == "db":
            return asyncio.run(_migrate(dsn) if args.db_command == "migrate" else _status(dsn))
        # quota's --timeout is the client's write and lock timeout for this one command: setting a quota
        # recounts its subtree, which can outlast the default, and a child quota waits for the creates in its
        # parent. The other commands keep their own timeouts.
        timeout = args.timeout if args.command == "quota" else None
        return asyncio.run(_with_client(dsn, _VFS_COMMANDS[args.command], args, timeout=timeout))
    except (MigrationError, VFSError, GlobError, RegexError, SearchRefused, psycopg.Error, OSError) as e:
        if isinstance(e, psycopg.Error) and any(len(s) < _MIN_MASKED for s in secrets):
            # Masking a password this short would garble the message and show where it matched.
            print(f"minifs {label}: database error (not shown: the password is too short to mask)", file=sys.stderr)
            return 1
        detail = f" ({e.detail})" if isinstance(e, VFSError) and e.detail else ""
        print(_mask(f"minifs {label}: {e}{detail}", secrets), file=sys.stderr)
        return 1
    except _Refused as e:
        print(f"minifs {label}: {e}", file=sys.stderr)
        return 1


class _Parser(argparse.ArgumentParser):
    """argparse's parser, but its errors, which quote argv (``unrecognized arguments: --dns=postgresql://...``),
    have connection-string passwords masked. Subparsers are made with the same class."""

    def error(self, message: str) -> NoReturn:
        prefix = "unrecognized arguments: "
        if message.startswith(prefix):
            # Any word may be a password (``-Wsecret`` leaves ``-secret`` over, ``-W --secret`` leaves ``--secret``):
            # show only the CLI's own option names.
            options = self._options()
            message = prefix + " ".join(_shown(w, options) for w in message.removeprefix(prefix).split())
        # ``minifs -W secret ls``: the word in the subcommand slot is shown only if it looks like a command name.
        message = _INVALID_CHOICE.sub(lambda m: m[0] if re.fullmatch(r"'[a-z]+'", m[2]) else m[1] + "'****'", message)
        for pattern, repl in _ARGV_PASSWORDS:
            message = pattern.sub(repl, message)
        super().error(message)

    def _options(self) -> set[str]:
        """The option strings of this parser and its subparsers."""
        found: set[str] = set()
        for action in self._actions:
            found.update(action.option_strings)
            if isinstance(action, argparse._SubParsersAction):  # pyright: ignore[reportPrivateUsage]
                for sub in action.choices.values():  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
                    if isinstance(sub, _Parser):
                        found |= sub._options()
        return found


def _shown(word: str, options: set[str]) -> str:
    """An unrecognized argv word as an error may show it: one of the CLI's options as is, an abbreviation of a long
    one too (its ``=value`` masked); anything else, which may be a password, as ``****``."""
    name, eq, _ = word.partition("=")
    if name in options or (name.startswith("--") and any(o.startswith(name) for o in options)):
        return f"{name}=****" if eq else name
    return "****"


_INVALID_CHOICE = re.compile(r"(invalid choice: )(.*?)(?= \(choose from|$)")
# Where an argparse error can quote a password: a URI's userinfo (up to the last @ of the message, since the
# password may hold an unescaped @ or a space), a ``password=`` keyword (also ``--password=...``; to the end of the
# message, as argparse's repr of a quoted value escapes it unpredictably), and the value given to a flag that takes
# none (``--hidden=secret``). Argparse messages are one line.
_ARGV_PASSWORDS = (
    (re.compile(r"(://[^:/@]*:).*@"), r"\1****@"),
    (re.compile(r"(password\s*=).*"), r"\1****"),
    (re.compile(r"(ignored explicit argument )'.*'"), r"\1'****'"),
)


def _add_dsn(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--dsn",
        help="libpq connection string (default: $MINIFS_DSN, then the PG* environment variables and ~/.pgpass). "
        "Don't put a password here: other users see it in the process list, and it stays in your shell history",
    )
    source.add_argument("--dsn-file", type=Path, metavar="PATH", help="read the connection string from this file")
    # No long --password: it reads like an option that takes the password as its value.
    parser.add_argument("-W", dest="ask_password", action="store_true", help="prompt for the password")
    return parser


class _Refused(Exception):
    """A connection setting the CLI won't use. The message is the CLI's own fixed text: libpq's may quote the
    connection string, password included."""


_INVALID = "invalid connection string (not shown: it may hold a password)"
_URI_SCHEMES = ("postgresql://", "postgres://")
# Shorter forms of a password aren't masked (see main).
_MIN_MASKED = 4
_MAX_DSN_FILE = 64 * 1024


def _connection(args: argparse.Namespace, label: str) -> tuple[str, list[str]]:
    """The conninfo to connect with, from ``--dsn``, ``--dsn-file`` or ``$MINIFS_DSN`` (empty: libpq's ``PG*``
    variables and ``~/.pgpass``), with the prompted password if ``-W``; and the strings no message may show
    (see ``_mask``). Warns, without showing it, when ``--dsn`` carries a password."""
    if args.dsn is not None:
        dsn = args.dsn
    elif args.dsn_file is not None:
        try:
            with args.dsn_file.open("rb") as f:  # bounded: not /dev/zero or put's stdin to the end
                raw = f.read(_MAX_DSN_FILE + 1)
            if len(raw) > _MAX_DSN_FILE:
                raise _Refused(f"--dsn-file is over {_MAX_DSN_FILE} bytes")
            dsn = raw.decode("utf-8-sig").strip()
        except UnicodeDecodeError:
            raise _Refused(f"{_INVALID}: --dsn-file is not UTF-8 text") from None
        except OSError as e:  # not its text: the name, which may be a mistyped connection string
            raise _Refused(f"can't read --dsn-file: {e.strerror or type(e).__name__}") from None
    else:
        dsn = os.environ.get("MINIFS_DSN", "")
    try:
        params = conninfo_to_dict(dsn)
    except (psycopg.Error, UnicodeError):  # UnicodeError: undecodable argv bytes, kept as surrogates
        raise _Refused(_INVALID) from None
    if "\0" in dsn:  # libpq stops reading at a NUL, so it and these checks would see different strings
        raise _Refused(_INVALID)
    if _misread_uri(dsn):
        raise _Refused(
            f"{_INVALID}: in a postgresql:// URI, write @ as %40 and / as %2F"
            " (in the user, password, database name and query values)"
        )
    passwords = [str(params[k]) for k in ("password", "sslpassword") if params.get(k)]  # sslpassword: key passphrase
    secrets = [s for p in passwords for s in _forms(p, dsn)]
    if passwords and args.dsn is not None:
        print(
            f"minifs {label}: warning: --dsn holds a password, which other users can see in the process list and"
            " which stays in your shell history; use $MINIFS_DSN, PGPASSWORD or ~/.pgpass, --dsn-file, or -W",
            file=sys.stderr,
        )
    if args.ask_password:
        typed = _ask_password()
        try:
            dsn = make_conninfo(dsn, password=typed)
        except (psycopg.Error, UnicodeError):
            raise _Refused("the password typed can't be used in a connection string") from None
        secrets += _forms(typed, dsn)
    return dsn, secrets


def _misread_uri(dsn: str) -> bool:
    """Whether a URI holds an unescaped ``@`` that libpq doesn't read as the end of the userinfo, so that part of
    a password may land in a field an error message quotes (``invalid integer value "rest-of-password@host" for
    connection option "port"``) without being parsed as the password, hence unmasked. libpq ends the userinfo
    at the first ``@`` before the first ``/``; any ``@`` past it is refused: a second ``@`` lands in the host,
    port or host list; a ``/`` in the password leaves no userinfo and puts the ``@`` in the database name; a
    ``?`` in it puts the rest in a query parameter (``?port=rest@host``). That also refuses a real ``@`` in a
    database name or query value, which ``%40`` writes as well (libpq decodes both)."""
    scheme = next((s for s in _URI_SCHEMES if dsn.startswith(s)), None)
    if scheme is None:
        return False
    rest = dsn[len(scheme) :]
    if "@" in rest.partition("/")[0]:
        rest = rest.partition("@")[2]
    return "@" in rest


def _ask_password() -> str:
    """The password, typed at the terminal. Without one, getpass would read it from stdin, which is ``put``'s
    data, so this refuses instead."""
    if not _has_terminal():
        raise _Refused("-W needs a terminal to ask for the password; use PGPASSWORD, ~/.pgpass or --dsn-file")
    try:
        return getpass.getpass("Password: ")
    except (EOFError, KeyboardInterrupt):
        raise _Refused("no password entered") from None


def _has_terminal() -> bool:
    """Whether getpass can prompt at a terminal (on POSIX it opens ``/dev/tty``)."""
    if os.name != "posix":
        return True
    try:
        with open("/dev/tty"):
            return True
    except OSError:
        return False


def _forms(password: str, dsn: str) -> list[str]:
    """``password`` as a message might show it (as typed, percent-encoded, percent-decoded), and the whole
    conninfo that holds it."""
    return [s for s in {password, quote(password, safe=""), unquote(password), dsn} if s]


def _mask(text: str, secrets: list[str]) -> str:
    """``text`` with each of ``secrets`` of ``_MIN_MASKED`` characters or more replaced by ``****``, longest
    first (the conninfo before the password inside it)."""
    for secret in sorted({s for s in secrets if len(s) >= _MIN_MASKED}, key=len, reverse=True):
        text = text.replace(secret, "****")
    return text


async def _with_client(
    dsn: str,
    command: Callable[[VFSClient, argparse.Namespace], Awaitable[int]],
    args: argparse.Namespace,
    timeout: float | None = None,
) -> int:
    connector = Connector(dsn)
    # Connect once directly, so a bad DSN or password reports libpq's error at once rather than a pool timeout.
    async with await connector():
        pass
    async with open_pool(connector, min_size=1, max_size=1, name="minifs-cli") as pool:
        if timeout is None:
            client = VFSClient(pool)
        else:  # the lock timeout stays above the server's deadlock_timeout (see VFSClient)
            client = VFSClient(pool, write_timeout=timeout, lock_timeout=max(timeout, DEFAULT_LOCK_TIMEOUT))
        return await command(client, args)


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
    # The read is bounded (#83): more than a file may hold is refused before any database call, so an
    # oversized source is never read whole only for the write to fail.
    if args.source:
        with open(args.source, "rb") as source:
            data = source.read(MAX_FILE_SIZE + 1)
    else:
        data = sys.stdin.buffer.read(MAX_FILE_SIZE + 1)
    if len(data) > MAX_FILE_SIZE:
        raise VFSError("EFBIG", normalize(args.path), f"over the {MAX_FILE_SIZE}-byte limit")
    # Look the path up, then create or write (#35). Both cost two statements for a new file; neither raises on the
    # server, which returns ENOENT. The fallback below only covers a file removed since the stat.
    try:
        await vfs.stat(args.path)
    except VFSError as e:
        if e.errno != "ENOENT":
            raise
        await vfs.create(args.path, data, parents=args.parents)
        return 0
    try:
        if args.append:
            await vfs.append(args.path, data)
        else:
            await vfs.write(args.path, data)
    except VFSError as e:
        if e.errno != "ENOENT":  # removed since the stat
            raise
        await vfs.create(args.path, data, parents=args.parents)
    return 0


def _quota_line(quota: Quota) -> str:
    """The one line ``minifs quota`` prints: the limits (``none``: no limit on that one) and the usage."""

    def shown(value: int | None) -> str:
        return "none" if value is None else str(value)

    return (
        f"max_bytes={shown(quota.max_bytes)} max_nodes={shown(quota.max_nodes)}"
        f" used_bytes={quota.used_bytes} used_nodes={quota.used_nodes}"
    )


async def _quota(vfs: VFSClient, args: argparse.Namespace) -> int:
    if args.children:
        # A child quota has no row to read: an omitted limit is no limit here, unlike a quota's own.
        max_bytes = None if args.max_bytes is _OMITTED else args.max_bytes
        max_nodes = None if args.max_nodes is _OMITTED else args.max_nodes
        if args.remove or (max_bytes is None and max_nodes is None):  # both "none" is --remove
            await vfs.set_child_quota(args.path)
            print("no quota")
            return 0
        given = await vfs.set_child_quota(args.path, max_bytes=max_bytes, max_nodes=max_nodes)
        print(f"{given} existing directories got the quota")
        return 0
    if args.remove:
        await vfs.set_quota(args.path)
        print("no quota")
        return 0
    if args.max_bytes is _OMITTED and args.max_nodes is _OMITTED:
        quota = await vfs.quota(args.path)
    else:
        max_bytes = args.max_bytes
        max_nodes = args.max_nodes
        if max_bytes is _OMITTED or max_nodes is _OMITTED:
            # An omitted limit is kept: the current quota supplies it (without a quota, it is no limit). Read and
            # set are two transactions, so two operators setting one quota at once can each undo the other's.
            current = await vfs.quota(args.path)
            if max_bytes is _OMITTED:
                max_bytes = current.max_bytes if current is not None else None
            if max_nodes is _OMITTED:
                max_nodes = current.max_nodes if current is not None else None
        # Both None (two "none"s, or nothing to keep) removes the quota, as --remove does.
        quota = await vfs.set_quota(args.path, max_bytes=max_bytes, max_nodes=max_nodes)
    print("no quota" if quota is None else _quota_line(quota))
    return 0


async def _rm(vfs: VFSClient, args: argparse.Namespace) -> int:
    for path in args.paths:
        await vfs.unlink(path)
    return 0


async def _glob(vfs: VFSClient, args: argparse.Namespace) -> int:
    entries = await vfs.glob(
        args.pattern, args.path, hidden=args.hidden, max_nodes=args.max_nodes, timeout=args.timeout
    )
    for entry in entries:
        print(entry.path)
    return 0


async def _grep(vfs: VFSClient, args: argparse.Namespace) -> int:
    result = await vfs.grep(
        args.pattern,
        args.path,
        include=args.include,
        ignore_case=args.ignore_case,
        fixed=args.fixed_strings,
        hidden=not args.no_hidden,
        limit=args.limit,
        max_line_chars=None,
    )
    for hit in result.hits:
        print(f"{hit.path}:{hit.line}:{hit.text}")
    if result.truncated:
        print(f"minifs grep: showing the first {args.limit} matches; there are more", file=sys.stderr)
    return 0


async def _mv(vfs: VFSClient, args: argparse.Namespace) -> int:
    await vfs.rename(args.src, args.dst)
    return 0


_VFS_COMMANDS: dict[str, Callable[[VFSClient, argparse.Namespace], Awaitable[int]]] = {
    "ls": _ls,
    "cat": _cat,
    "put": _put,
    "quota": _quota,
    "rm": _rm,
    "mv": _mv,
    "glob": _glob,
    "grep": _grep,
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
