import asyncio
from pathlib import Path

import pytest
from psycopg import AsyncConnection

from minifs.cli import main
from minifs.db.migrate import Migration, MigrationError, applied_versions, load_migrations, migrate


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
