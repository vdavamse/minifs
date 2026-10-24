"""Errors raised by the minifs SQL functions.

Each function raises SQLSTATE ``MFnnn``, where ``nnn`` is the Linux errno number, with the errno name as the
message and the context in DETAIL (see ``vfs_raise`` in migration 003). The codes don't depend on the client
platform's errno numbering.
"""

from __future__ import annotations

import psycopg

SQLSTATE_BY_ERRNO: dict[str, str] = {
    "ENOENT": "MF002",
    "EEXIST": "MF017",
    "ENOTDIR": "MF020",
    "EISDIR": "MF021",
    "EINVAL": "MF022",
    "ENOTEMPTY": "MF039",
    "ESTALE": "MF116",
}

ERRNO_BY_SQLSTATE: dict[str, str] = {code: name for name, code in SQLSTATE_BY_ERRNO.items()}


def errno_name(error: psycopg.Error) -> str | None:
    """The errno name (``"ENOENT"``, ...) a minifs SQL function raised, or None for any other database error."""
    return ERRNO_BY_SQLSTATE.get(error.sqlstate or "")
