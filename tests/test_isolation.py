"""Tenant isolation (#90): one schema per tenant, one role each, the recipe the docs give operators.

The recipe, which ``Tenants`` installs and these tests prove:

- an owner role (LOGIN, NOSUPERUSER) owns a fresh database and, in it, one schema per tenant;
- each schema is migrated by the owner connecting with ``options=-c search_path=<schema>`` in the conninfo:
  the migrations install into the connection's ``current_schema()``;
- each tenant role (LOGIN, NOSUPERUSER, not the owner) gets, from the owner, ``WRITER`` (or ``READER``)
  below: USAGE on its schema, the table privileges the functions use and no more (none on
  ``vfs_schema_version`` or ``vfs_channel_name``), and EXECUTE on the functions except the operator's quota
  calls and the notification triggers, revoked from PUBLIC too (functions are executable by PUBLIC by
  default); it connects with its own ``options=-c search_path=<schema>``. docs/design.md copies this recipe;
- ``public`` is left alone: every SQL function is ``SET search_path FROM CURRENT``, so nothing escapes
  into it or reads out of it.

What that buys: both tenants build the same paths and each reads only its own content; neither can read
the other's schema, named outright or through its own search_path; each schema notifies on its own
channel, so a watcher hears only its tenant; and a role granted reads alone reads but can't write.
Roles and databases are cluster-wide or per-test: every test gets a fresh set, dropped afterwards
(the pattern of ``test_migrate.py::test_migrations_need_no_superuser``). The container's admin user is
the only superuser step, and only to create the roles and the database.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from collections.abc import Iterator
from typing import Any, LiteralString

import psycopg
import pytest
from psycopg import AsyncConnection, sql
from psycopg.conninfo import make_conninfo

from minifs.db.migrate import migrate
from minifs.db.pool import Connector, open_pool
from minifs.vfs.client import VFSClient
from minifs.vfs.core import Quota, VFSError
from minifs.vfs.watch import Watcher
from tests.conftest import PgServer

SCHEMA_A = "tenant_a"
SCHEMA_B = "tenant_b"

# The recipe's grants, as docs/design.md gives them ({s}: the schema, {r}: the tenant role).
_FUNCTIONS: LiteralString = "EXECUTE ON ALL FUNCTIONS IN SCHEMA {s}"
_OPERATOR: LiteralString = (
    "EXECUTE ON FUNCTION {s}.vfs_set_quota(uuid, bigint, bigint), {s}.vfs_set_child_quota(uuid, bigint, bigint),"
    " {s}.vfs_give_child_quota(uuid, uuid), {s}.vfs_notify(), {s}.vfs_notify_truncate()"
)
WRITER: list[LiteralString] = [
    "GRANT USAGE ON SCHEMA {s} TO {r}",
    "GRANT SELECT, INSERT, UPDATE, DELETE ON {s}.vfs_nodes, {s}.vfs_extents TO {r}",
    "GRANT SELECT, INSERT, DELETE, UPDATE (used_bytes, used_nodes) ON {s}.vfs_quotas TO {r}",
    "GRANT SELECT, DELETE ON {s}.vfs_quota_defaults TO {r}",
    f"REVOKE {_FUNCTIONS} FROM PUBLIC",
    f"GRANT {_FUNCTIONS} TO {{r}}",
    f"REVOKE {_OPERATOR} FROM {{r}}",
]
READER: list[LiteralString] = [
    "GRANT USAGE ON SCHEMA {s} TO {r}",
    "GRANT SELECT ON {s}.vfs_nodes, {s}.vfs_extents, {s}.vfs_quotas, {s}.vfs_quota_defaults TO {r}",
    f"REVOKE {_FUNCTIONS} FROM PUBLIC",
    f"GRANT {_FUNCTIONS} TO {{r}}",
    f"REVOKE {_OPERATOR}, {{s}}.vfs_channel() FROM {{r}}",  # a reader that watches keeps vfs_channel()
]


class Tenants:
    """The recipe installed: an owner role, a database with two migrated tenant schemas, and a login role
    per tenant granted into its schema. ``destroy`` drops the database and every role created, including
    the ones ``add_role`` added for a test."""

    def __init__(self, server: PgServer) -> None:
        suffix = uuid.uuid4().hex[:8]
        self.server = server
        self.database = f"iso_{uuid.uuid4().hex}"
        self.owner = f"minifs_own_{suffix}"
        self.role_a = f"minifs_a_{suffix}"
        self.role_b = f"minifs_b_{suffix}"
        self._roles: list[str] = []
        try:
            with psycopg.connect(server.conninfo, autocommit=True) as admin:
                for role in (self.owner, self.role_a, self.role_b):
                    admin.execute(
                        sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'pw' NOSUPERUSER").format(sql.Identifier(role))
                    )
                    self._roles.append(role)
                admin.execute(
                    sql.SQL("CREATE DATABASE {} OWNER {} TEMPLATE template0").format(
                        sql.Identifier(self.database), sql.Identifier(self.owner)
                    )
                )
            asyncio.run(self._install())
        except BaseException:
            self.destroy()
            raise

    def owner_conninfo(self, schema: str | None = None) -> str:
        """The owner's conninfo; with ``schema``, pinned to it, as the recipe migrates each schema."""
        conninfo = make_conninfo(self.server.database(self.database), user=self.owner, password="pw")
        return conninfo if schema is None else make_conninfo(conninfo, options=f"-c search_path={schema}")

    def conninfo(self, role: str, schema: str) -> str:
        """A tenant role's conninfo, pinned to its schema."""
        return make_conninfo(
            self.server.database(self.database), user=role, password="pw", options=f"-c search_path={schema}"
        )

    async def add_role(self, role: str) -> None:
        """Creates one more LOGIN role (for the read-only test); ``destroy`` drops it with the rest."""
        async with await AsyncConnection.connect(self.server.conninfo, autocommit=True) as admin:
            await admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'pw' NOSUPERUSER").format(sql.Identifier(role)))
        self._roles.append(role)

    async def grant(self, schema: str, role: str, *, write: bool = True) -> None:
        """The recipe's grants for one tenant role, run as the owner (no superuser involved)."""
        async with await AsyncConnection.connect(self.owner_conninfo(), autocommit=True) as conn:
            await self._grant(conn, schema, role, write=write)

    def destroy(self) -> None:
        """Drops the database and every role. Tolerates a half-built environment (IF EXISTS); every pool
        and connection of the test is closed by then, and WITH (FORCE) ends any straggler."""
        with psycopg.connect(self.server.conninfo, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(self.database)))
            for role in reversed(self._roles):
                admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))
        self._roles.clear()

    async def _install(self) -> None:
        # The schemas must exist before a connection can pin its search_path to one: current_schema() of a
        # path naming nothing that exists is NULL, and migrate() refuses that.
        async with await AsyncConnection.connect(self.owner_conninfo(), autocommit=True) as conn:
            for schema in (SCHEMA_A, SCHEMA_B):
                await conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        for schema in (SCHEMA_A, SCHEMA_B):
            async with await AsyncConnection.connect(self.owner_conninfo(schema), autocommit=True) as conn:
                cur = await conn.execute("SELECT current_schema()")
                assert await cur.fetchone() == (schema,)  # the options= took: the migrations install here
                assert await migrate(conn)
        async with await AsyncConnection.connect(self.owner_conninfo(), autocommit=True) as conn:
            await self._grant(conn, SCHEMA_A, self.role_a, write=True)
            await self._grant(conn, SCHEMA_B, self.role_b, write=True)

    @staticmethod
    async def _grant(conn: AsyncConnection[Any], schema: str, role: str, *, write: bool) -> None:
        for statement in WRITER if write else READER:
            await conn.execute(sql.SQL(statement).format(s=sql.Identifier(schema), r=sql.Identifier(role)))


@pytest.fixture
def tenants(pg_server: PgServer) -> Iterator[Tenants]:
    """One database of the recipe per test, with its roles; all dropped afterwards."""
    environment = Tenants(pg_server)
    try:
        yield environment
    finally:
        environment.destroy()


def test_a_schema_per_tenant_isolates_tenants(tenants: Tenants) -> None:
    """Both tenants build the same paths and each reads only its own content; neither can read the other's
    schema, named outright or through its own search_path; and each schema has its own channel."""
    asyncio.run(_tenants_write_the_same_paths(tenants))
    _foreign_schema_is_closed(tenants.conninfo(tenants.role_a, SCHEMA_A), SCHEMA_B)
    _foreign_schema_is_closed(tenants.conninfo(tenants.role_b, SCHEMA_B), SCHEMA_A)
    with psycopg.connect(tenants.owner_conninfo(), autocommit=True) as owner:
        row = owner.execute("SELECT tenant_a.vfs_channel(), tenant_b.vfs_channel()").fetchone()
    assert row is not None
    assert row[0] != row[1]
    assert re.fullmatch(r"vfs_[0-9a-f]{32}", row[0]) and re.fullmatch(r"vfs_[0-9a-f]{32}", row[1])


async def _tenants_write_the_same_paths(tenants: Tenants) -> None:
    async def write_and_read(conninfo: str, data: bytes) -> None:
        async with open_pool(Connector(conninfo), max_size=2) as pool:
            client = VFSClient(pool)
            assert await client.mkdir_p("/workspaces/w") is not None
            await client.create("/workspaces/w/f", data)  # no EEXIST: the other tenant's node isn't there
            assert (await client.pread("/workspaces/w/f")).content == data

    await write_and_read(tenants.conninfo(tenants.role_a, SCHEMA_A), b"a's data")
    await write_and_read(tenants.conninfo(tenants.role_b, SCHEMA_B), b"b's data")
    # B's write at the same path left A's tree alone: A still reads its own content.
    async with open_pool(Connector(tenants.conninfo(tenants.role_a, SCHEMA_A)), max_size=2) as pool:
        assert (await VFSClient(pool).pread("/workspaces/w/f")).content == b"a's data"


def _foreign_schema_is_closed(conninfo: str, foreign: str) -> None:
    """A tenant role can't read another tenant's schema: not by name (no USAGE on it), and not with it on
    its own search_path (SET search_path needs no privilege, but the function then isn't the caller's to
    run, or doesn't resolve at all). Either way, no row comes back."""
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(sql.SQL("SELECT * FROM {}.vfs_nodes").format(sql.Identifier(foreign)))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            conn.execute(sql.SQL("SELECT * FROM {}.vfs_lookup(ARRAY['workspaces'])").format(sql.Identifier(foreign)))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):  # nor learn its channel (migration 018)
            conn.execute(sql.SQL("SELECT {}.vfs_channel()").format(sql.Identifier(foreign)))
        conn.execute(sql.SQL("SET search_path = {}").format(sql.Identifier(foreign)))
        with pytest.raises((psycopg.errors.InsufficientPrivilege, psycopg.errors.UndefinedFunction)):
            conn.execute("SELECT * FROM vfs_lookup(ARRAY['workspaces'])")


async def test_a_tenant_watcher_hears_only_its_own_schema(tenants: Tenants) -> None:
    """A watcher for tenant A listens on tenant_a's channel alone (migration 018): it hears A's create, and
    B's create in tenant_b, committed first, never arrives. Notifications reach a listener in commit order,
    so a shared channel would deliver B's event first: waiting for a second before A's create and asserting
    the first event is A's makes the check deterministic."""
    a = tenants.conninfo(tenants.role_a, SCHEMA_A)
    b = tenants.conninfo(tenants.role_b, SCHEMA_B)

    async def workspace(conninfo: str) -> None:
        async with open_pool(Connector(conninfo), max_size=2) as pool:
            await VFSClient(pool).mkdir_p("/workspaces/w")

    await workspace(a)
    await workspace(b)

    async with Watcher(Connector(a)) as watcher, watcher.watch("/workspaces/w") as events:
        async with open_pool(Connector(b), max_size=2) as pool:
            hidden = await VFSClient(pool).create("/workspaces/w/from-b", b"b's data")
        with pytest.raises(TimeoutError):  # nothing within a second: tenant_b notifies on its own channel
            await asyncio.wait_for(events.get(), 1.0)
        async with open_pool(Connector(a), max_size=2) as pool:
            seen = await VFSClient(pool).create("/workspaces/w/from-a", b"a's data")
        event = await asyncio.wait_for(events.get(), 10)
        assert (event.op, event.id, event.name) == ("create", seen.id, "from-a")
        assert event.id != hidden.id


async def test_a_read_only_role_reads_but_cannot_write(tenants: Tenants) -> None:
    """A third role with USAGE, SELECT on all tables and EXECUTE on all functions, but no INSERT, UPDATE or
    DELETE: it reads tenant A's tree through VFSClient, and a create fails. The minifs functions report
    their outcomes as VFSError, but InsufficientPrivilege (42501) has no errno in minifs.db.errors, so
    VFSClient lets the psycopg error propagate unchanged."""
    writer = tenants.conninfo(tenants.role_a, SCHEMA_A)
    async with open_pool(Connector(writer), max_size=2) as pool:
        client = VFSClient(pool)
        await client.mkdir_p("/workspaces/w")
        await client.create("/workspaces/w/f", b"a's data")

    reader = f"minifs_r_{uuid.uuid4().hex[:8]}"
    await tenants.add_role(reader)
    await tenants.grant(SCHEMA_A, reader, write=False)
    async with open_pool(Connector(tenants.conninfo(reader, SCHEMA_A)), max_size=2) as pool:
        client = VFSClient(pool)
        entry = await client.stat("/workspaces/w/f")
        assert (entry.kind, entry.size) == ("file", len(b"a's data"))
        assert [e.name for e in await client.readdir("/workspaces/w")] == ["f"]
        assert (await client.pread("/workspaces/w/f")).content == b"a's data"
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            await client.create("/workspaces/w/g", b"x")

    # A's tree is unchanged: no /workspaces/w/g, and f still holds A's data.
    async with open_pool(Connector(writer), max_size=2) as pool:
        client = VFSClient(pool)
        assert [e.name for e in await client.readdir("/workspaces/w")] == ["f"]
        assert (await client.pread("/workspaces/w/f")).content == b"a's data"
        with pytest.raises(VFSError) as missing:
            await client.stat("/workspaces/w/g")
        assert missing.value.errno == "ENOENT"


async def test_a_tenant_role_writes_under_its_quota_but_cannot_administer_it(tenants: Tenants) -> None:
    """The writer's grants are what the functions use: under a quota the owner set, the tenant role creates,
    renames into the quota root, and removes a directory with its quota rows, and the charges apply (EDQUOT
    over the limit). It can't call the operator's quota functions, write the migration-version table or the
    channel row, or run the notification triggers. What it can still do, since the functions are SECURITY
    INVOKER and need the rest, is the documented gap: delete or reset its own quota row."""
    owner = tenants.owner_conninfo(SCHEMA_A)
    async with open_pool(Connector(owner), max_size=2) as pool:
        operator = VFSClient(pool)
        await operator.mkdir_p("/workspaces/w")
        await operator.mkdir_p("/elsewhere/d")
        assert await operator.set_child_quota("/workspaces/w", max_bytes=100, max_nodes=10) == 0
        await operator.set_quota("/workspaces/w", max_bytes=100, max_nodes=10)
    async with open_pool(Connector(tenants.conninfo(tenants.role_a, SCHEMA_A)), max_size=2) as pool:
        client = VFSClient(pool)
        await client.create("/workspaces/w/f", b"x" * 60)
        await client.mkdir("/workspaces/w/sub")  # inherits the child quota: an INSERT into vfs_quotas
        await client.rename("/elsewhere/d", "/workspaces/w/d")  # charges both sides: FOR UPDATE, UPDATE
        with pytest.raises(VFSError) as over:
            await client.create("/workspaces/w/g", b"y" * 50)
        assert over.value.errno == "EDQUOT"
        await client.unlink("/workspaces/w/sub")  # deletes its quota row
        assert await client.quota("/workspaces/w") == Quota(100, 10, 60, 2)
        w = (await client.stat("/workspaces/w")).id
        async with pool.connection() as conn:
            for statement, params in [
                ("SELECT vfs_set_quota(%s, 1000000000, NULL)", (w,)),
                ("SELECT vfs_set_child_quota(%s, 1000000000, NULL)", (w,)),
                ("SELECT vfs_give_child_quota(%s, %s)", (w, w)),
                ("INSERT INTO vfs_schema_version (version, name, checksum) VALUES (999, 'x', 'x')", ()),
                ("DELETE FROM vfs_schema_version", ()),
                ("DELETE FROM vfs_channel_name", ()),
                ("SELECT * FROM vfs_channel_name", ()),
                ("UPDATE vfs_quotas SET max_bytes = 1000000000", ()),
                ("UPDATE vfs_quota_defaults SET max_bytes = 1000000000", ()),
                ("SELECT vfs_notify()", ()),
            ]:
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    await conn.execute(statement.encode(), params)
                await conn.rollback()
    async with open_pool(Connector(owner), max_size=2) as pool:
        assert await VFSClient(pool).quota("/workspaces/w") == Quota(100, 10, 60, 2)
