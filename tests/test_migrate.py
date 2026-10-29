import asyncio
import uuid
from pathlib import Path
from typing import cast

import psycopg
import pytest
from psycopg import AsyncConnection, sql
from psycopg.conninfo import make_conninfo
from psycopg.pq import TransactionStatus

from minifs.cli import main
from minifs.db.migrate import Migration, MigrationError, applied_versions, load_migrations, migrate
from tests.conftest import PgServer


def _write(directory: Path, files: dict[str, str]) -> Path:
    for name, body in files.items():
        (directory / name).write_text(body)
    return directory


def test_packaged_migrations_start_with_baseline() -> None:
    migrations = load_migrations()
    assert migrations[0].filename == "001_baseline.sql"
    assert [m.version for m in migrations] == sorted({m.version for m in migrations})


def test_load_orders_by_version_and_ignores_other_files(tmp_path: Path) -> None:
    _write(tmp_path, {"010_later.sql": "SELECT 1;", "002_first.sql": "SELECT 2;", "README.md": "notes"})
    assert [m.filename for m in load_migrations(tmp_path)] == ["002_first.sql", "010_later.sql"]


@pytest.mark.parametrize("name", ["1_short.sql", "001-dash.sql", "001_Upper.sql", "001_.sql"])
def test_load_rejects_bad_names(tmp_path: Path, name: str) -> None:
    _write(tmp_path, {name: ""})
    with pytest.raises(MigrationError, match="NNN_lower_snake"):
        load_migrations(tmp_path)


def test_load_rejects_duplicate_versions(tmp_path: Path) -> None:
    _write(tmp_path, {"001_a.sql": "", "001_b.sql": ""})
    with pytest.raises(MigrationError, match="version 001"):
        load_migrations(tmp_path)


async def test_empty_migration_applies_once(empty_db: str) -> None:
    empty = [Migration(1, "baseline", "-- nothing yet\n\n")]
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        assert await migrate(conn, empty) == empty
        assert await migrate(conn, empty) == []
        cur = await conn.execute("SELECT version, name FROM vfs_schema_version")
        assert await cur.fetchall() == [(1, "baseline")]


async def test_pending_migrations_apply_in_order(empty_db: str) -> None:
    first = Migration(1, "create", "CREATE TABLE t (x int);")
    second = Migration(2, "insert", "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);")
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        assert await migrate(conn, [first]) == [first]
        assert await migrate(conn, [first, second]) == [second]
        cur = await conn.execute("SELECT count(*) FROM t")
        assert await cur.fetchone() == (2,)


async def test_name_control_chars_migration_refuses_legacy_names(empty_db: str) -> None:
    """Migration 016 validates its constraint: a name with a control character fails it, with the fix in its
    header (rename the node), and it applies once the name is clean."""
    migrations = load_migrations()
    before = [m for m in migrations if m.version < 16]
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        await migrate(conn, before)
        await conn.execute("INSERT INTO vfs_nodes (name, kind) VALUES (%s, 'file')", ("a\nb",))
        with pytest.raises(psycopg.errors.CheckViolation) as info:
            await migrate(conn, migrations)
        assert info.value.diag.constraint_name == "vfs_nodes_name_check"
        assert 16 not in await applied_versions(conn)
        await conn.execute("UPDATE vfs_nodes SET name = 'a b' WHERE name = %s", ("a\nb",))
        applied = await migrate(conn, migrations)
        assert [m.version for m in applied] == [m.version for m in migrations if m.version >= 16]
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute("INSERT INTO vfs_nodes (name, kind) VALUES (%s, 'file')", ("a\nb",))


async def test_failed_migration_is_not_recorded(empty_db: str) -> None:
    good = Migration(1, "good", "CREATE TABLE ok ();")
    bad = Migration(2, "bad", "CREATE TABLE half (); SELECT * FROM missing;")
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        with pytest.raises(Exception, match="missing"):
            await migrate(conn, [good, bad])
        assert set(await applied_versions(conn)) == {1}
        cur = await conn.execute("SELECT to_regclass('half')")
        assert await cur.fetchone() == (None,)


async def test_edited_migration_is_rejected(empty_db: str) -> None:
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        await migrate(conn, [Migration(1, "a", "SELECT 1;")])
        with pytest.raises(MigrationError, match="checksum"):
            await migrate(conn, [Migration(1, "a", "SELECT 2;")])


async def test_concurrent_runners_apply_each_migration_once(empty_db: str) -> None:
    migrations = [Migration(1, "a", "CREATE TABLE a ();"), Migration(2, "b", "CREATE TABLE b ();")]

    async def run() -> list[Migration]:
        async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
            return await migrate(conn, migrations)

    results = await asyncio.gather(*(run() for _ in range(4)))
    assert sorted(m.version for applied in results for m in applied) == [1, 2]


def test_cli_migrate_and_status(empty_db: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["db", "status", "--dsn", empty_db]) == 0
    assert "pending 001_baseline.sql" in capsys.readouterr().out
    assert main(["db", "migrate", "--dsn", empty_db]) == 0
    assert "applied 001_baseline.sql" in capsys.readouterr().out
    assert main(["db", "migrate", "--dsn", empty_db]) == 0
    assert "up to date" in capsys.readouterr().out
    assert main(["db", "status", "--dsn", empty_db]) == 0
    assert "applied 001_baseline.sql" in capsys.readouterr().out


@pytest.mark.parametrize("body", ["-- nothing\n", "/* nothing */", "", "-- x\n;"])
async def test_statement_free_migrations_apply(empty_db: str, body: str) -> None:
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        assert len(await migrate(conn, [Migration(1, "empty", body)])) == 1


async def test_migration_changing_search_path_does_not_leak(empty_db: str) -> None:
    # pg_dump output starts with set_config('search_path', '', false); a plain SET outlives its transaction.
    migrations = [
        Migration(1, "dump", "SELECT pg_catalog.set_config('search_path', '', false); SET search_path = pg_catalog;"),
        Migration(2, "table", "CREATE TABLE lands_in_public ();"),
    ]
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        await conn.execute("SET search_path = public")
        assert await migrate(conn, migrations) == migrations
        cur = await conn.execute("SELECT current_setting('search_path'), to_regclass('public.lands_in_public')::text")
        assert await cur.fetchone() == ("public", "lands_in_public")
        assert set(await applied_versions(conn)) == {1, 2}


async def test_version_table_goes_in_current_schema(empty_db: str) -> None:
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        await conn.execute("CREATE SCHEMA app; SET search_path = app, public")
        await migrate(conn, [Migration(1, "a", "CREATE TABLE t ();")])
        cur = await conn.execute("SELECT to_regclass('app.vfs_schema_version')::text, to_regclass('app.t')::text")
        assert await cur.fetchone() == ("vfs_schema_version", "t")


async def test_refuses_connection_without_autocommit(empty_db: str) -> None:
    async with await AsyncConnection.connect(empty_db) as conn:
        with pytest.raises(MigrationError, match="autocommit"):
            await migrate(conn, [Migration(1, "a", "")])


async def test_refuses_connection_inside_transaction(empty_db: str) -> None:
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn, conn.transaction():
        with pytest.raises(MigrationError, match="inside a transaction"):
            await migrate(conn, [Migration(1, "a", "")])


@pytest.mark.parametrize("body", ["CREATE TABLE x (); COMMIT;", "COMMIT; BEGIN; CREATE TABLE x ();"])
async def test_refuses_transaction_control(empty_db: str, body: str) -> None:
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        with pytest.raises(MigrationError, match="transaction control"):
            await migrate(conn, [Migration(1, "a", body)])


async def test_refuses_pending_migration_older_than_applied(empty_db: str) -> None:
    async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
        await migrate(conn, [Migration(3, "c", "")])
        with pytest.raises(MigrationError, match="003 is already applied"):
            await migrate(conn, [Migration(1, "a", ""), Migration(3, "c", "")])


def test_cli_status_reports_modified_and_unknown(empty_db: str, capsys: pytest.CaptureFixture[str]) -> None:
    async def record() -> None:
        async with await AsyncConnection.connect(empty_db, autocommit=True) as conn:
            await migrate(conn, [Migration(1, "baseline", "-- edited"), Migration(99, "future", "")])

    asyncio.run(record())
    assert main(["db", "status", "--dsn", empty_db]) == 0
    out = capsys.readouterr().out
    assert "modified 001_baseline.sql" in out
    assert "unknown 099" in out
    assert main(["db", "migrate", "--dsn", empty_db]) == 1
    assert "checksum mismatch" in capsys.readouterr().err


def test_cli_reports_connection_errors(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["db", "status", "--dsn", "postgresql://nobody@127.0.0.1:1/none?connect_timeout=2"]) == 1
    assert capsys.readouterr().err.startswith("minifs db status: ")


def test_migrations_need_no_superuser(pg_server: PgServer) -> None:
    # Managed Postgres such as Databricks Lakebase has no real superuser: the schema must install as an ordinary
    # role that owns its database.
    role, database = f"minifs_owner_{uuid.uuid4().hex[:8]}", f"test_{uuid.uuid4().hex}"
    with psycopg.connect(pg_server.conninfo, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'pw' NOSUPERUSER").format(sql.Identifier(role)))
        admin.execute(
            sql.SQL("CREATE DATABASE {} OWNER {} TEMPLATE template0").format(
                sql.Identifier(database), sql.Identifier(role)
            )
        )
        try:
            conninfo = make_conninfo(pg_server.database(database), user=role, password="pw")
            # public belongs to pg_database_owner since PostgreSQL 15; give it to the role, as a managed
            # service's database owner has it.
            with psycopg.connect(pg_server.database(database), autocommit=True) as conn:
                conn.execute(sql.SQL("ALTER SCHEMA public OWNER TO {}").format(sql.Identifier(role)))

            async def run() -> None:
                async with await AsyncConnection.connect(conninfo, autocommit=True) as conn:
                    assert await migrate(conn)
                    cur = await conn.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
                    assert await cur.fetchone() == (False,)
                    await conn.execute("SELECT * FROM vfs_mkdir_p(ARRAY['tasks', 'new'])")
                    cur = await conn.execute("SELECT id FROM vfs_lookup(ARRAY['tasks', 'new'])")
                    row = await cur.fetchone()
                    assert row is not None
                    # A directory move takes the ancestor-walk locks; unlink takes FOR UPDATE.
                    for call in ("vfs_rename(%s, NULL, 'new')", "vfs_unlink(%s)"):
                        cur = await conn.execute(f"SELECT err FROM {call}".encode(), (row[0],))
                        assert await cur.fetchone() == (None,)
                    cur = await conn.execute("SELECT name FROM vfs_readdir(NULL)")
                    assert await cur.fetchall() == [("tasks",)]

            asyncio.run(run())
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


async def test_refuses_servers_older_than_15() -> None:
    class Info:
        server_version = 140011
        transaction_status = TransactionStatus.IDLE

    class OldServer:
        autocommit = True
        info = Info()

    with pytest.raises(MigrationError, match="PostgreSQL 15"):
        await migrate(cast(AsyncConnection, OldServer()), [])
