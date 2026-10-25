-- UTF-8 validity per extent (#66): vfs_search skips a file with content that isn't UTF-8, and used to find out by
-- decoding the whole file before searching it, so every file in scope was decoded twice, and a search that stops
-- at its limit in the first lines still decoded the whole first file. Validity is now decided once, when an
-- extent is written, and stored with it.
--
--   vfs_extents.utf8   whether the extent's bytes decode as UTF-8 on their own (vfs_utf8): the very check the
--                      search ran, extent by extent. A STORED generated column, so every writer sets it with no
--                      change (vfs_create, vfs_append, vfs_write, vfs_compact, raw inserts), and adding it computes
--                      it for the extents already stored.
--
-- Per extent is exact for the whole file: every extent but a file's last ends with \n (007), and a \n byte never
-- occurs inside a multi-byte sequence, so the file is UTF-8 exactly when each of its extents is. Only the last
-- extent can end mid-character, and then it is false, as the file was; the next append re-cuts that extent, which
-- computes the flag again. A merged extent (vfs_compact) is decoded again rather than combined from its parts.
--
-- Write-path cost: one decode per extent written, which the search used to pay on every search instead. An append
-- that re-cuts a mid-line last extent decodes that extent again, next to the copy and compression it already pays.
--
-- Deploying: adding a stored column rewrites vfs_extents (every extent read, decoded and written again, and the
-- indexes rebuilt) under an ACCESS EXCLUSIVE lock, so reads and writes of file content wait until the migration
-- commits, and it needs room for a second copy of the table. Plan a pause that grows with the bytes stored
-- (about 0.3 s for 20 MB).


-- Whether p_data is valid UTF-8, as convert_from(p_data, 'UTF8') decides: NUL bytes and a sequence cut at the end
-- are invalid, a BOM is valid. NULL for NULL. IMMUTABLE for the generated column: in a UTF8 database (what minifs
-- needs; Lakebase's) convert_from only verifies the bytes, without a conversion lookup, so the answer depends on
-- the bytes alone. One subtransaction per call, which a read-only block keeps cheap. The column stores its value:
-- a changed vfs_utf8 doesn't recompute existing rows, so change the column's expression too, which rewrites the
-- table (ALTER COLUMN ... SET EXPRESSION, PG 17+, or drop and add it again).
CREATE FUNCTION vfs_utf8(p_data bytea) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM convert_from(p_data, 'UTF8');
    RETURN true;
EXCEPTION WHEN character_not_in_repertoire OR untranslatable_character THEN
    RETURN false;
END $$;

ALTER TABLE vfs_extents ADD COLUMN utf8 boolean NOT NULL GENERATED ALWAYS AS (vfs_utf8(data)) STORED;

-- The search's check is one probe of this index, empty unless binary or other non-UTF-8 files are stored.
CREATE INDEX vfs_extents_not_utf8_idx ON vfs_extents (node_id, generation) WHERE NOT utf8;


-- vfs_search as in 008, except for the UTF-8 check: a file with an extent of its current generation that isn't
-- UTF-8 is left out of the files loop, which reads the flags and decodes nothing. The skip and the BOM handling
-- are unchanged.
CREATE OR REPLACE FUNCTION vfs_search(
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
              WHERE NOT EXISTS (SELECT FROM vfs_extents e
                                 WHERE e.node_id = n.id AND e.generation = n.generation AND NOT e.utf8)
              ORDER BY g.path COLLATE "C"
    LOOP
        -- Fetched one row at a time (an explicit FETCH; a FOR loop would fetch them 10, then 50 at a time), so
        -- extents after the last one needed aren't decoded, tested or split. The order is taken below the decode
        -- and the filter (OFFSET 0 keeps it there): if the planner sorts instead of walking vfs_extents_line_key,
        -- as it may for a file holding most of the table, it sorts raw rows and still filters one extent at a time.
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
