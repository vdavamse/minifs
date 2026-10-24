"""The database fixtures give each test an isolated, migrated database with the requested locale."""

from psycopg import AsyncConnection

from tests.conftest import PG_IMAGE, TEMPLATES, PgServer


async def _collate(conninfo: str) -> str:
    async with await AsyncConnection.connect(conninfo) as conn:
        cur = await conn.execute("SELECT datcollate FROM pg_database WHERE datname = current_database()")
        row = await cur.fetchone()
        assert row is not None
        return row[0]


async def test_db_is_utf8_and_migrated(conn: AsyncConnection) -> None:
    cur = await conn.execute("SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = current_database()")
    assert await cur.fetchone() == ("UTF8",)
    cur = await conn.execute("SELECT count(*) FROM vfs_schema_version")
    row = await cur.fetchone()
    assert row is not None and row[0] >= 1


async def _e_acute_is_alpha(conninfo: str) -> bool:
    async with await AsyncConnection.connect(conninfo) as conn:
        cur = await conn.execute("SELECT 'é' ~ '^[[:alpha:]]$'")
        row = await cur.fetchone()
        assert row is not None
        return row[0]


async def test_db_c_uses_c_locale(db_c: str) -> None:
    assert await _collate(db_c) == "C"
    assert not await _e_acute_is_alpha(db_c)


async def test_db_has_utf8_locale(db: str) -> None:
    # Behavioural, not just the name: a musl (alpine) image accepts any locale name and acts like C.
    assert await _collate(db) != "C"
    assert await _e_acute_is_alpha(db)


async def test_templates_cover_c_c_utf8_and_utf8(pg_server: PgServer) -> None:
    collations = {key: await _collate(pg_server.database(name)) for key, name in TEMPLATES.items()}
    assert collations["c"] == "C"
    assert collations["c_utf8"] == "C.UTF-8"  # Databricks Lakebase's default
    assert collations["utf8"] not in {"C", "C.UTF-8"}


async def test_each_test_gets_a_clean_database(conn: AsyncConnection) -> None:
    # If databases leaked between tests, whichever of these two tests runs second would see the table.
    cur = await conn.execute("SELECT to_regclass('leak_probe')")
    assert await cur.fetchone() == (None,)
    await conn.execute("CREATE TABLE leak_probe ()")


async def test_each_test_gets_a_clean_database_again(conn: AsyncConnection) -> None:
    await test_each_test_gets_a_clean_database(conn)


def test_server_version_matches_image(pg_version: int) -> None:
    tag = PG_IMAGE.rsplit(":", 1)[-1]
    if tag.split(".")[0].isdigit():
        assert pg_version // 10000 == int(tag.split(".")[0])
