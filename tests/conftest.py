"""Test fixtures: a real PostgreSQL in testcontainers, and a clean database per test.

The server starts once per session (image from ``MINIFS_TEST_PG_IMAGE``, default ``postgres:18``). Three template
databases are migrated once: one with the image's UTF-8 locale, one with ``LOCALE 'C'`` and one with
``LOCALE 'C.UTF-8'``, the default on Databricks Lakebase. Each test then gets
its own database cloned from a template, which is much cheaper than migrating, and dropped afterwards.

Clones isolate everything inside a database, not cluster-wide objects such as roles: migrations that create
those must be idempotent.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass

import psycopg
import pytest
from psycopg import AsyncConnection, sql
from psycopg.conninfo import make_conninfo
from testcontainers.community.postgres import PostgresContainer

from minifs.db.migrate import migrate

PG_IMAGE = os.environ.get("MINIFS_TEST_PG_IMAGE", "postgres:18")

TEMPLATES = {"utf8": "minifs_template_utf8", "c": "minifs_template_c", "c_utf8": "minifs_template_c_utf8"}


@dataclass(frozen=True)
class PgServer:
    conninfo: str  # connects to the server's maintenance database
    version_num: int  # server_version_num, e.g. 180000

    def database(self, name: str) -> str:
        return make_conninfo(self.conninfo, dbname=name)


@pytest.fixture(scope="session")
def pg_server() -> Iterator[PgServer]:
    with PostgresContainer(PG_IMAGE, driver=None) as container:
        conninfo = container.get_connection_url()
        with psycopg.connect(conninfo, autocommit=True) as conn:
            row = conn.execute("SELECT current_setting('server_version_num')::int").fetchone()
            assert row is not None
            server = PgServer(conninfo, row[0])
            # The image's own locale (en_US.utf8 in the official images) for the UTF-8 template.
            row = conn.execute("SELECT datcollate FROM pg_database WHERE datname = 'template0'").fetchone()
            assert row is not None
            utf8_locale: str = row[0]
            for locale, template in TEMPLATES.items():
                _create_database(conn, template, {"c": "C", "c_utf8": "C.UTF-8"}.get(locale, utf8_locale))
                asyncio.run(_migrate(server.database(template)))
                conn.execute(sql.SQL("ALTER DATABASE {} IS_TEMPLATE true").format(sql.Identifier(template)))
        yield server


@pytest.fixture
def pg_version(pg_server: PgServer) -> int:
    """server_version_num of the test server, for tests that depend on the Postgres major version."""
    return pg_server.version_num


@pytest.fixture
def db(pg_server: PgServer) -> Iterator[str]:
    """Conninfo of a fresh, migrated database with the UTF-8 locale."""
    yield from _clone(pg_server, TEMPLATES["utf8"])


@pytest.fixture
def db_c(pg_server: PgServer) -> Iterator[str]:
    """Conninfo of a fresh, migrated database created with ``LOCALE 'C'``."""
    yield from _clone(pg_server, TEMPLATES["c"])


@pytest.fixture(params=sorted(TEMPLATES))
def db_any_locale(request: pytest.FixtureRequest, pg_server: PgServer) -> Iterator[str]:
    """A fresh, migrated database; the test runs once per locale (``c``, ``c_utf8`` and ``utf8``)."""
    yield from _clone(pg_server, TEMPLATES[request.param])


@pytest.fixture
def empty_db(pg_server: PgServer) -> Iterator[str]:
    """Conninfo of a fresh database with no migrations applied."""
    yield from _clone(pg_server, "template0")


@pytest.fixture
async def conn(db: str) -> AsyncIterator[AsyncConnection]:
    """An autocommit connection to ``db``."""
    async with await AsyncConnection.connect(db, autocommit=True) as connection:
        yield connection


@pytest.fixture(scope="session")
def rg_path() -> str:
    """Path of the ripgrep binary. Missing rg skips locally and fails in CI, where it is installed."""
    path = shutil.which("rg")
    if path is None:
        if os.environ.get("CI"):
            pytest.fail("rg (ripgrep) is required in CI")
        pytest.skip("rg (ripgrep) is not installed")
    return path


def _create_database(conn: psycopg.Connection, name: str, locale: str, template: str = "template0") -> None:
    conn.execute(
        sql.SQL("CREATE DATABASE {} TEMPLATE {} ENCODING 'UTF8' LOCALE {}").format(
            sql.Identifier(name), sql.Identifier(template), sql.Literal(locale)
        )
    )


def _clone(server: PgServer, template: str) -> Iterator[str]:
    name = f"test_{uuid.uuid4().hex}"
    with psycopg.connect(server.conninfo, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(sql.Identifier(name), sql.Identifier(template)))
    try:
        yield server.database(name)
    finally:
        with psycopg.connect(server.conninfo, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


async def _migrate(conninfo: str) -> None:
    async with await AsyncConnection.connect(conninfo, autocommit=True) as conn:
        await migrate(conn)
