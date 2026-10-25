"""``Write``: create or overwrite a file, creating parent directories. Keeps an existing file's BOM. ladderframe's
``Write`` (``ladderframe.tools.write``, which follows opencode's write tool), over a ``VFSView``.

ladderframe serialises writers of a path with a thread lock; here an overwrite is a compare-and-swap on the
version read for the diff, so a concurrent writer anywhere is detected. A lost race (``ESTALE``, or a create that
finds the file created, or a file gone before its overwrite) is tried once more from the start, which is safe since
``Write`` replaces the whole file; a second loss is the ``ModelRetry`` of ``tool_error``.
"""

from __future__ import annotations

import posixpath

from pydantic_ai import ModelRetry, RunContext
from pydantic_ai.messages import ToolReturn

from minifs.tools._text import diff_metadata, encode_text, read_file, split_bom
from minifs.tools.base import vfs_tool
from minifs.tools.deps import HasVFS, resolve
from minifs.tools.errors import tool_error
from minifs.vfs.core import Entry, VFSError
from minifs.vfs.view import VFSView

DESCRIPTION = """Writes a file to the filesystem.

Usage:
- This tool will overwrite the existing file if there is one at the provided path.
- If this is an existing file, you MUST use the Read tool first to read the file's contents. This tool will fail if you did not read the file first.
- ALWAYS prefer editing existing files in the codebase. NEVER write new files unless explicitly required.
- NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
- Only use emojis if the user explicitly requests it. Avoid writing emojis to files unless asked."""


class _Lost(Exception):
    """Another writer got in between two steps of a ``_write``; starting over resolves it."""


async def write(ctx: RunContext[HasVFS], content: str, filePath: str) -> str | ToolReturn:
    """Write a file.

    Args:
        content: The content to write to the file
        filePath: The absolute path to the file to write (must be absolute, not relative)
    """
    path = resolve(ctx, filePath)
    try:
        try:
            exists, content_old, content_new = await _write(ctx.deps.vfs, path, content)
        except _Lost:
            try:
                exists, content_old, content_new = await _write(ctx.deps.vfs, path, content)
            except _Lost:
                raise VFSError("ESTALE", path) from None
    except VFSError as e:
        return tool_error(e, "write")
    return ToolReturn(
        "Wrote file successfully.",
        metadata={"filepath": path, "exists": exists, **diff_metadata(path, content_old, content_new)},
    )


async def _write(vfs: VFSView, path: str, content: str) -> tuple[bool, str, str]:
    """Writes ``content`` to ``path``: ``(the file existed, its old text, the new text)``, without BOMs. ``_Lost``
    where another writer got in between: a create beaten to the file, a file changed or removed after its stat."""
    next_bom, content_new = split_bom(content)
    try:
        entry = await vfs.stat(path)
    except VFSError as e:
        if e.errno != "ENOENT":
            raise
        parent = await _parent(vfs, path)
        try:
            await vfs.create(path, encode_text(content_new, next_bom), parents=parent is None)
        except VFSError as e:
            # created meanwhile, or its parent removed (a virtual directory never is: outside the mounts below it)
            if e.errno == "EEXIST" or (e.errno == "ENOENT" and parent is not None and parent.id is not None):
                raise _Lost from None
            raise
        return False, "", content_new
    # ladderframe checks the permission first: a denied write is refused before the directory check, and before
    # reading the old text (all of an append-only log, say).
    vfs.policy.check(vfs.actor, "write", entry.path)
    if entry.is_dir:
        raise ModelRetry(f"Path is a directory, not a file: {path}")
    try:
        data, version = await read_file(vfs, path)
        # The old text only feeds the diff, so an undecodable file is still overwritten.
        source_bom, content_old = split_bom(data.decode("utf-8", errors="replace"))
        await vfs.write(path, encode_text(content_new, source_bom or next_bom), expect=version)
    except VFSError as e:
        if e.errno in ("ESTALE", "ENOENT", "EISDIR"):  # changed, removed, or replaced by a directory since the stat
            raise _Lost from None
        raise
    return True, content_old, content_new


async def _parent(vfs: VFSView, path: str) -> Entry | None:
    """``path``'s parent directory, None if it is missing. ``create`` gets ``parents`` only then, since the view
    checks ``write`` on every directory a ``parents`` create could make, existing or not."""
    try:
        return await vfs.stat(posixpath.dirname(path))
    except VFSError as e:
        if e.errno != "ENOENT":
            raise
        return None


Write = vfs_tool(write, "Write", DESCRIPTION, sequential=True)
