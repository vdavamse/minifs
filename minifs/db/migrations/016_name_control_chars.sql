-- Names can't contain control characters (#92). A name used to need only to be non-empty, not '.' or '..', free
-- of '/' and at most 255 bytes, so a newline, an escape sequence, DEL, a C1 control or a Unicode bidi override
-- was a legal name. The Read tool lists directory entries one per line, Glob and Grep print one path per line,
-- and the CLI prints names to the terminal, so such a name could inject lines into a tool result or escape
-- sequences into a terminal.
--
-- Rejected from now on, as EINVAL, in every name:
--
--   U+0001-U+001F  C0 controls (a NUL can't be in text at all)
--   U+007F         DEL
--   U+0080-U+009F  C1 controls
--   U+2028-U+2029  LINE SEPARATOR and PARAGRAPH SEPARATOR, which str.splitlines() treats as line ends
--   U+202A-U+202E  bidi embeddings and overrides (LRE, RLE, PDF, LRO, RLO)
--   U+2066-U+2069  bidi isolates (LRI, RLI, FSI, PDI)
--
-- Spaces, '<', '>' and every other printable character stay legal, as on disk. The same pattern is in
-- minifs.vfs.core.split, which refuses such a path before any round trip.
--
-- vfs_check_name is replaced with the same signature, so every caller (vfs_create, vfs_mkdir_p, vfs_rename)
-- picks it up. The constraint is replaced under the same name, so the backstop mapping in minifs.db.errors
-- (vfs_nodes_name_check -> EINVAL) is unchanged.
--
-- Deploying: the new constraint is validated, so this migration scans vfs_nodes once, holding an ACCESS
-- EXCLUSIVE lock on it (reads and writes of the tree wait; vfs_extents isn't touched), and fails if an existing
-- name contains one of these characters (a pre-check names the count and this header). That is deliberate: once
-- the client refuses such paths, a row with such a name could no longer be reached to be renamed. Find them first
-- with
--
--   SELECT id, parent_id, name FROM vfs_nodes WHERE name ~ '[\u0001-\u001F\u007F-\u009F\u2028-\u202E\u2066-\u2069]';
--
-- and rename each (SELECT * FROM vfs_rename(id, parent_id, '<new name>')), then run the migration again.

CREATE OR REPLACE FUNCTION vfs_check_name(p_name text) RETURNS void
LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
BEGIN
    IF p_name IS NULL OR p_name = '' OR p_name IN ('.', '..') OR strpos(p_name, '/') > 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid name %L', p_name));
    ELSIF p_name ~ '[\u0001-\u001F\u007F-\u009F\u2028-\u202E\u2066-\u2069]' THEN
        -- The code point, not the name: %L wouldn't escape the character, and DETAIL goes to the server log.
        PERFORM vfs_raise('EINVAL', format('name contains the control character U+%s',
            lpad(upper(to_hex(ascii(substring(p_name FROM '[\u0001-\u001F\u007F-\u009F\u2028-\u202E\u2066-\u2069]')))), 4, '0')));
    ELSIF octet_length(p_name) > 255 THEN
        PERFORM vfs_raise('ENAMETOOLONG', format('name of %s bytes, the limit is 255', octet_length(p_name)));
    END IF;
END $$;

-- Before the constraint scans the table: the count and where the fix is, rather than a bare check violation.
DO $$
DECLARE v_n bigint;
BEGIN
    SELECT count(*) INTO v_n FROM vfs_nodes WHERE name ~ '[\u0001-\u001F\u007F-\u009F\u2028-\u202E\u2066-\u2069]';
    IF v_n > 0 THEN
        RAISE EXCEPTION '% name(s) in vfs_nodes contain control characters; rename them first (see the header of '
            '016_name_control_chars.sql), then run the migration again', v_n;
    END IF;
END $$;

ALTER TABLE vfs_nodes
    DROP CONSTRAINT vfs_nodes_name_check,
    ADD CONSTRAINT vfs_nodes_name_check
        CHECK (name <> '' AND name NOT IN ('.', '..') AND strpos(name, '/') = 0 AND octet_length(name) <= 255
               AND name !~ '[\u0001-\u001F\u007F-\u009F\u2028-\u202E\u2066-\u2069]');
