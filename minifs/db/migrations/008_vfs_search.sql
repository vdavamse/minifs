-- Search (#10, README: Search): the content scan behind the Grep tool. There is no line table and no index: a
-- search reads the current extents of the files in scope, the way ripgrep reads a disk.
--
-- The scope comes from vfs_glob (006), in a statement of its own that reads only vfs_nodes, so the client can
-- refuse an oversized scope before any content is read. vfs_search then scans it. The client runs both in one
-- REPEATABLE READ transaction under SET LOCAL statement_timeout (minifs.vfs.search).
--
-- Every extent starts at a line start and ends just after a \n, except perhaps a file's last (007), so an extent
-- holds whole lines and never splits a UTF-8 character: each one is decoded and tested on its own.


-- p_data decoded as UTF-8, or NULL if it isn't valid UTF-8 (or holds a NUL, which text can't), so a search skips
-- such content instead of failing.
CREATE FUNCTION vfs_utf8(p_data bytea) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
BEGIN
    RETURN convert_from(p_data, 'UTF8');
EXCEPTION WHEN character_not_in_repertoire OR untranslatable_character THEN
    RETURN NULL;
END $$;


-- The lines of files p_ids that match, in order of path then line, at most p_limit (NULL: all). p_paths are the
-- files' paths as the caller wants them back, in the same order as p_ids; an id that is no longer a file gives
-- nothing. Each hit has its 1-based line number, the file byte offset of the line's start (vfs_pread's
-- currency), the line without its \n, cut to p_max_chars characters (NULL: whole), and whether it was cut (a
-- final \r doesn't count, since the Grep tool drops it).
--
-- Files are searched one at a time in path order (bytewise, COLLATE "C"), and the search stops once it has
-- p_limit hits, so a common pattern costs the files up to its first p_limit hits, not the whole scope. In each
-- file, two passes per extent of the current generation:
-- 1. Extent filter: the decoded extent, without its final \n, is tested once against p_file_pattern, the
--    translated pattern in newline-sensitive mode ('(?n)', minifs.vfs.regex.Translation.file_form), where ^ and
--    $ match at line breaks and . and [^...] don't cross them. The translator keeps \n out of every class, so
--    an extent passes exactly when one of its lines matches.
-- 2. Line check: only passing extents are split on \n into their cardinality(line_offsets) lines (so a final
--    \n adds no empty phantom line after it, and an extent that is one empty line still yields it), and each
--    line is tested against p_line_pattern. Line k of an extent is file line line_start + k, starting at file
--    byte byte_offset + line_offsets[k].
-- With p_fixed, both patterns are literal strings tested with strpos instead of regexes. Case-insensitive
-- search is in the translated pattern, so the regexes are always tested with ~, never ~* (which depends on the
-- database's locale).
--
-- A file with any extent that isn't UTF-8 (vfs_utf8) is skipped whole, so results don't depend on how the file's
-- bytes happen to be cut into extents. Each file's extents are decoded once (the MATERIALIZED CTE).
CREATE FUNCTION vfs_search(
    p_ids uuid[],
    p_paths text[],
    p_file_pattern text,
    p_line_pattern text,
    p_fixed boolean DEFAULT false,
    p_limit int DEFAULT NULL,
    p_max_chars int DEFAULT NULL
) RETURNS TABLE (path text, line_no bigint, byte_offset bigint, line text, cut boolean)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_left bigint := p_limit;               -- hits still wanted; NULL: no limit
    v_rows bigint;
    f record;
BEGIN
    IF v_left IS NOT NULL AND v_left <= 0 THEN
        RETURN;
    END IF;
    FOR f IN SELECT g.path, n.id, n.generation
               FROM unnest(p_ids, p_paths) AS g(id, path)
               JOIN vfs_nodes n ON n.id = g.id AND n.kind = 'file'
              ORDER BY g.path COLLATE "C"
    LOOP
        RETURN QUERY
        WITH extents AS MATERIALIZED (
            SELECT e.byte_offset, e.line_start, e.line_offsets, vfs_utf8(e.data) AS body
              FROM vfs_extents e
             WHERE e.node_id = f.id AND e.generation = f.generation
        ), hits AS (
            SELECT x.* FROM extents x
             WHERE NOT EXISTS (SELECT FROM extents b WHERE b.body IS NULL)
               AND CASE WHEN p_fixed THEN strpos(x.body, p_file_pattern) > 0
                        ELSE (CASE WHEN right(x.body, 1) = E'\n' THEN left(x.body, -1) ELSE x.body END)
                             ~ p_file_pattern
                   END
        )
        SELECT f.path, h.line_start + l.k, h.byte_offset + h.line_offsets[l.k::int],
               CASE WHEN p_max_chars IS NULL THEN l.text ELSE left(l.text, p_max_chars) END,
               p_max_chars IS NOT NULL
                   AND char_length(l.text) - (right(l.text, 1) = E'\r')::int > p_max_chars
          FROM hits h, string_to_table(h.body, E'\n') WITH ORDINALITY AS l(text, k)
         WHERE l.k <= cardinality(h.line_offsets)
           AND CASE WHEN p_fixed THEN strpos(l.text, p_line_pattern) > 0 ELSE l.text ~ p_line_pattern END
         ORDER BY h.line_start + l.k
         LIMIT v_left;
        IF v_left IS NOT NULL THEN
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            v_left := v_left - v_rows;
            EXIT WHEN v_left <= 0;
        END IF;
    END LOOP;
END $$;
