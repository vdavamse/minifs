"""Errors reported by the minifs SQL functions, and by the schema's constraints.

The functions return the expected outcomes (``OUTCOMES``: a missing path, a taken name, a lost compare-and-swap,
...) in their ``err`` and ``err_detail`` columns (migration 009), and raise the faults (EINVAL, ENAMETOOLONG) as
SQLSTATE ``MFnnn``, where ``nnn`` is the Linux errno number, with the errno name as the message and the context in
DETAIL (see ``vfs_raise`` in migration 003). The codes don't depend on the client platform's errno numbering;
every errno has one, so ``vfs_raise`` can raise any of them.

The constraints on ``vfs_nodes`` and ``vfs_extents`` are the backstop for raw SQL and for the rare races the
functions don't pre-check; their violations map to errnos too, on a best-effort basis: a foreign-key error can't
tell a missing parent from a parent that is a file, so both map to ENOTDIR, and an extent on a missing node maps
to EISDIR like one on a directory. The functions report each case precisely.
"""

from __future__ import annotations

import psycopg

SQLSTATE_BY_ERRNO: dict[str, str] = {
    "ENOENT": "MF002",
    "EEXIST": "MF017",
    "ENOTDIR": "MF020",
    "EISDIR": "MF021",
    "EINVAL": "MF022",
    "ENAMETOOLONG": "MF036",
    "ENOTEMPTY": "MF039",
    "ESTALE": "MF116",
}

# The errnos the functions return rather than raise.
OUTCOMES: frozenset[str] = frozenset({"ENOENT", "ENOTDIR", "EISDIR", "EEXIST", "ESTALE", "ENOTEMPTY"})

ERRNO_BY_SQLSTATE: dict[str, str] = {code: name for name, code in SQLSTATE_BY_ERRNO.items()}

_UNIQUE_VIOLATION = "23505"
_FOREIGN_KEY_VIOLATION = "23503"
_RESTRICT_VIOLATION = "23001"  # what PostgreSQL 18 raises for ON DELETE RESTRICT; earlier versions raise 23503
_CHECK_VIOLATION = "23514"


def errno_name(error: psycopg.Error) -> str | None:
    """The errno name (``"ENOENT"``, ...) for an error from the minifs schema, or None for any other error."""
    sqlstate = error.sqlstate or ""
    if sqlstate in ERRNO_BY_SQLSTATE:
        return ERRNO_BY_SQLSTATE[sqlstate]
    constraint = error.diag.constraint_name
    if sqlstate == _UNIQUE_VIOLATION and constraint == "vfs_nodes_parent_name_key":
        return "EEXIST"
    if sqlstate == _CHECK_VIOLATION and constraint == "vfs_nodes_name_check":
        return "EINVAL"
    if sqlstate == _CHECK_VIOLATION and constraint == "vfs_extents_node_kind_check":
        return "EISDIR"
    if sqlstate == _CHECK_VIOLATION and constraint == "vfs_extents_data_check":
        return "EINVAL"
    if (
        sqlstate == _FOREIGN_KEY_VIOLATION
        and constraint == "vfs_extents_node_fkey"
        and "is not present in table" in (error.diag.message_detail or "")
    ):
        return "EISDIR"
    if constraint == "vfs_nodes_parent_fkey":
        if sqlstate == _RESTRICT_VIOLATION:
            return "ENOTEMPTY"
        if sqlstate == _FOREIGN_KEY_VIOLATION:
            # The referenced side (deleting a directory that has entries) reports the referencing table in DETAIL
            # as "... referenced from table"; the referencing side (an entry under a file or a missing
            # directory) reports "... is not present in table". Both name the same constraint. (This reads the
            # English server message; under another lc_messages a raw delete would map to ENOTDIR.)
            detail = error.diag.message_detail or ""
            return "ENOTEMPTY" if "referenced from table" in detail else "ENOTDIR"
    return None
