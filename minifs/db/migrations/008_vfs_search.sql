-- Search (#10, README: Search): the content scan behind the Grep tool. There is no line table and no index: a
-- search reads the current extents of the files in scope, the way ripgrep reads a disk.
--
-- The scope comes from vfs_glob (006), in a statement of its own that reads only vfs_nodes, so the client can
-- refuse an oversized scope before any content is read. vfs_search then scans it. The client runs both in one
-- REPEATABLE READ transaction under SET LOCAL statement_timeout (minifs.vfs.search).
--
-- Every extent starts at a line start and ends just after a \n, except perhaps a file's last (007), so an extent
-- holds whole lines and never splits a UTF-8 character: each one is decoded and tested on its own.


-- The lines of files p_ids that match, in order of path then line, at most p_limit (NULL: all). p_paths are the
-- files' paths as the caller wants them back, in the same order as p_ids; an id that is no longer a file gives
-- nothing. Each hit has its 1-based line number, the file byte offset of the line's start (vfs_pread's
-- currency), the line without its \n, cut to p_max_chars characters (NULL: whole), and whether it was cut (a
-- final \r doesn't count, since the Grep tool drops it).
--
-- Files are searched one at a time in path order (bytewise, COLLATE "C"), each file's extents in line order, and
-- the search stops once it has p_limit hits: a common pattern costs the extents up to its first p_limit hits, not
-- the whole scope or the whole file. For each extent of a file's current generation:
-- 1. Extent filter: the decoded extent, without its final \n, is tested once against p_file_pattern, the
--    translated pattern in newline-sensitive mode ('(?n)', minifs.vfs.regex.Translation.file_form), where ^ and
--    $ match at line breaks and . and [^...] don't cross them. The translator keeps \n out of every class, so
--    an extent passes exactly when one of its lines matches.
-- 2. Line check: only passing extents are split on \n into their cardinality(line_offsets) lines (so a final
--    \n adds no empty phantom line after it, and an extent that is one empty line still yields it), and each
--    line is tested against p_line_pattern. Line k of an extent is file line line_start + k, starting at file
--    byte byte_offset + line_offsets[k] (the pieces are zipped with unnest(line_offsets), see below).
-- With p_fixed, both patterns are literal strings tested with strpos instead of regexes. Case-insensitive
-- search is in the translated pattern, so the regexes are always tested with ~, never ~* (which depends on the
-- database's locale).
--
-- A file with any content that isn't UTF-8 is skipped whole, so results don't depend on how its bytes happen to
-- be cut into extents. That is checked first, in one subtransaction per file (an EXCEPTION block per extent
-- would cost one per extent), and costs a decode of the whole file even when its first extent has the hits. As
-- in rg, a UTF-8 byte order mark at the start of a file isn't part of line 1, whose offset is then 3.
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
    c_bom constant text := chr(65279);
    v_left bigint := p_limit;               -- hits still wanted; NULL: no limit
    v_rows bigint;
    f record;
    x record;
    c refcursor;
BEGIN
    IF v_left IS NOT NULL AND v_left <= 0 THEN
        RETURN;
    END IF;
    <<files>>
    FOR f IN SELECT g.path, n.id, n.generation
               FROM unnest(p_ids, p_paths) AS g(id, path)
               JOIN vfs_nodes n ON n.id = g.id AND n.kind = 'file'
              ORDER BY g.path COLLATE "C"
    LOOP
        BEGIN
            PERFORM convert_from(e.data, 'UTF8') FROM vfs_extents e
              WHERE e.node_id = f.id AND e.generation = f.generation;
        EXCEPTION WHEN character_not_in_repertoire OR untranslatable_character THEN
            CONTINUE files;
        END;
        -- Fetched one row at a time (an explicit FETCH; a FOR loop would fetch them 10, then 50 at a time), so
        -- extents after the last one needed aren't tested or split. The order is taken below the decode and the
        -- filter (OFFSET 0 keeps it there): if the planner sorts instead of walking vfs_extents_line_key, as it
        -- may for a file holding most of the table, it sorts raw rows and still filters one extent at a time.
        OPEN c FOR
            SELECT e.byte_offset, e.line_start, e.line_offsets, m.bom, b.body
              FROM (SELECT e.byte_offset, e.line_start, e.line_offsets, e.data
                      FROM vfs_extents e
                     WHERE e.node_id = f.id AND e.generation = f.generation
                     ORDER BY e.line_start
                    OFFSET 0) e
                   CROSS JOIN LATERAL (SELECT convert_from(e.data, 'UTF8') AS raw OFFSET 0) d
                   CROSS JOIN LATERAL (SELECT e.line_start = 0 AND left(d.raw, 1) = c_bom AS bom) m
                   CROSS JOIN LATERAL (SELECT CASE WHEN m.bom THEN substr(d.raw, 2) ELSE d.raw END AS body
                                       OFFSET 0) b
             WHERE CASE WHEN p_fixed THEN strpos(b.body, p_file_pattern) > 0
                        ELSE (CASE WHEN right(b.body, 1) = E'\n' THEN left(b.body, -1) ELSE b.body END)
                             ~ p_file_pattern
                   END
             ORDER BY e.line_start;                 -- already the subquery's order: no second sort
        LOOP
            FETCH c INTO x;
            EXIT WHEN NOT FOUND;
            -- The lines zipped with their offsets: unnest reads line_offsets once, where a subscript per line would
            -- detoast it again for every line (an extent of 2000 lines has 8 KB of offsets, stored out of line).
            -- string_to_table's extra empty piece after a final \n pairs with no offset and is dropped.
            RETURN QUERY
            SELECT f.path, x.line_start + l.k,
                   x.byte_offset + l.off + CASE WHEN x.bom AND l.k = 1 THEN 3 ELSE 0 END,
                   CASE WHEN p_max_chars IS NULL THEN l.text ELSE left(l.text, p_max_chars) END,
                   p_max_chars IS NOT NULL
                       AND char_length(l.text) - (right(l.text, 1) = E'\r')::int > p_max_chars
              FROM ROWS FROM (string_to_table(x.body, E'\n'), unnest(x.line_offsets)) WITH ORDINALITY AS l(text, off, k)
             WHERE l.off IS NOT NULL
               AND CASE WHEN p_fixed THEN strpos(l.text, p_line_pattern) > 0 ELSE l.text ~ p_line_pattern END
             ORDER BY l.k
             LIMIT v_left;
            IF v_left IS NOT NULL THEN
                GET DIAGNOSTICS v_rows = ROW_COUNT;
                v_left := v_left - v_rows;
                IF v_left <= 0 THEN
                    CLOSE c;
                    EXIT files;
                END IF;
            END IF;
        END LOOP;
        CLOSE c;
    END LOOP;
END $$;
