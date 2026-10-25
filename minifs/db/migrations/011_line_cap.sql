-- A per-line cap for vfs_pread_lines (#50): each line longer than p_max_line_bytes comes back as its first
-- p_max_line_bytes bytes, flagged as cut, so a page of long lines is one call. Read cuts each line to 2,000
-- characters anyway; with the raw byte cap alone, one long line took a whole call.
--
-- The cap is in bytes: the server can't count characters in bytes that may not be valid UTF-8. A caller that
-- cuts to N characters asks for 4 * (N + 1) bytes, which always decode to more than N characters (a decoded
-- character never takes more than 4 bytes), the first N of them the line's own.
--
-- The result gains next_offset, where the next unread byte of the file is (with cut lines, no longer
-- byte_offset + length(data)), and cut, the numbers of the lines that came back cut. The new columns go before
-- err and err_detail, which stay last; a changed return type needs DROP FUNCTION first.

DROP FUNCTION vfs_pread_lines(uuid, bigint, int, int);


-- Reads lines of file p_node, like Read (migration 007): the bytes from the start of line p_first_line (1-based)
-- through at most p_max_lines lines, each with its \n, capped at p_max_bytes bytes (so the last line may come
-- back cut by the byte cap). With p_max_line_bytes, a line longer than that (not counting its \n) is cut first:
-- it comes back as its first p_max_line_bytes bytes and a \n (one is added to a last line that had none), and
-- its number is in cut. A line the byte cap cuts as well keeps its number in cut.
--
-- Returns one row: the file's size, generation and line count, p_first_line, the byte offset where that line
-- starts, the bytes, next_offset (where the next unread byte is: just past the last line read whole, or past
-- the bytes of a line the byte cap cut) and cut (empty with no line cap). A p_first_line past the last line
-- returns no bytes, byte_offset = next_offset = size and the line count. One snapshot, no locks.
-- err ENOENT, EISDIR; EINVAL for a NULL argument (but p_max_line_bytes), a p_first_line below 1 or a negative
-- limit.
--
-- Index steps as in migration 007. With a line cap, step 3 reads only the extents it may need: a line holds at
-- least the smaller of its length and p_max_line_bytes + 1 bytes of the result, so an extent holds at least
-- the smaller of its own length and that, and the scan stops at the extent where that bound reaches
-- p_max_bytes. Of each extent, only the slice the lines read lie in is taken, and of a line longer than an
-- extent's target (an extent of its own, vfs_cut), only its prefix. substring of a stored value decompresses
-- only up to the slice's end; with pglz (the default) it also fetches only the TOAST chunks it needs, while lz4
-- fetches the whole compressed value first.
CREATE FUNCTION vfs_pread_lines(p_node uuid, p_first_line bigint, p_max_lines int, p_max_bytes int,
                                p_max_line_bytes int DEFAULT NULL)
RETURNS TABLE (size bigint, generation int, lines bigint, first_line bigint, byte_offset bigint, data bytea,
               next_offset bigint, cut bigint[], err text, err_detail text)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_size bigint;
    v_gen int;
    v_lines bigint;
    v_first bigint;
    v_last bigint;                                  -- the last line wanted
    v_ext bigint;                                   -- the extent where line v_first starts
    v_start bigint;
    v_stop bigint;
    v_bound bigint;
    v_data bytea;
    v_next bigint;
    v_cut bigint[];
BEGIN
    IF p_first_line IS NULL OR p_first_line < 1 OR p_max_lines IS NULL OR p_max_lines < 0
       OR p_max_bytes IS NULL OR p_max_bytes < 0 OR p_max_line_bytes < 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid first line %s, max lines %s, max bytes %s or max line bytes %s',
                                           p_first_line, p_max_lines, p_max_bytes, p_max_line_bytes));
    END IF;
    SELECT f.size, f.generation, f.lines, f.err, f.err_detail INTO v_size, v_gen, v_lines, err, err_detail
      FROM vfs_file(p_node) f;
    IF err IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;
    IF p_first_line > v_lines THEN
        RETURN QUERY SELECT v_size, v_gen, v_lines, p_first_line, v_size, ''::bytea, v_size, '{}'::bigint[],
                            NULL::text, NULL::text;
        RETURN;
    END IF;
    v_first := p_first_line;
    v_last := least(v_first + p_max_lines - 1, v_lines);
    -- Step 1: the extent with the largest line_start below v_first holds the start of line v_first. (One
    -- subscript of line_offsets per call, not per row.)
    SELECT e.byte_offset, e.byte_offset + e.line_offsets[v_first - e.line_start] INTO v_ext, v_start
      FROM vfs_extents e
     WHERE e.node_id = p_node AND e.generation = v_gen AND e.line_start < v_first
     ORDER BY e.line_start DESC LIMIT 1;
    IF v_start IS NULL THEN                         -- only raw SQL can make lines disagree with the extents
        RAISE EXCEPTION 'line % of % (% lines) is in no extent', v_first, p_node, v_lines
            USING ERRCODE = 'data_corrupted';
    END IF;
    -- Step 2: the end, where line v_last + 1 starts, or the end of the file if there's no such line.
    IF v_last = v_lines THEN
        v_stop := v_size;
    ELSE
        SELECT e.byte_offset + e.line_offsets[v_last + 1 - e.line_start] INTO v_stop
          FROM vfs_extents e
         WHERE e.node_id = p_node AND e.generation = v_gen AND e.line_start < v_last + 1
         ORDER BY e.line_start DESC LIMIT 1;
    END IF;
    IF p_max_line_bytes IS NULL THEN
        v_stop := least(v_stop, v_start + p_max_bytes);
        -- Step 3: a range scan forward from the first extent.
        SELECT string_agg(substring(e.data          -- substring(bytea) takes int, not bigint
                                    FROM (greatest(v_start - e.byte_offset, 0) + 1)::int
                                    FOR (least(e.byte_offset + length(e.data), v_stop)
                                         - greatest(e.byte_offset, v_start))::int),
                          ''::bytea ORDER BY e.byte_offset) INTO v_data
          FROM vfs_extents e
         WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset >= v_ext AND e.byte_offset < v_stop;
        v_data := coalesce(v_data, ''::bytea);
        RETURN QUERY SELECT v_size, v_gen, v_lines, p_first_line, v_start, v_data, v_start + length(v_data),
                            '{}'::bigint[], NULL::text, NULL::text;
        RETURN;
    END IF;
    -- Step 3a: the end of the first extent where the bytes the extents hold for sure reach p_max_bytes. length()
    -- of a stored value reads its header, not its bytes.
    SELECT min(b.byte_offset + b.len) INTO v_bound
      FROM (SELECT e.byte_offset, length(e.data) AS len,
                   sum(least(least(e.byte_offset + length(e.data), v_stop) - greatest(e.byte_offset, v_start),
                             p_max_line_bytes::bigint + 1)) OVER (ORDER BY e.byte_offset) AS held
              FROM vfs_extents e
             WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset >= v_ext
               AND e.byte_offset < v_stop) b
     WHERE b.held >= p_max_bytes;
    v_stop := least(v_stop, v_bound);
    -- Step 3b: each line of those extents, cut, then the lines up to the byte cap. x.d is the part of the extent
    -- the lines read lie in, from x.s, or just the prefix of a single long line: one slice per extent, which
    -- substring decompresses only up to its end, never the rest of a big extent (after vfs_compact with a large
    -- target, say). The OFFSET 0s keep x and c subqueries: x so its slice is taken once, not once per line, and c
    -- so the window's sort gets the pieces, not every line's copy of its extent's slice. A line's last byte is \n
    -- unless it's the file's last line and that has none.
    SELECT string_agg(substring(l.piece FROM 1 FOR (p_max_bytes - l.before)::int), ''::bytea ORDER BY l.line),
           (array_agg(CASE WHEN l.before + length(l.piece) <= p_max_bytes THEN l.ends_at
                           ELSE l.starts_at + (p_max_bytes - l.before) END ORDER BY l.line DESC))[1],
           coalesce(array_agg(l.line ORDER BY l.line) FILTER (WHERE l.is_cut), '{}')
      INTO v_data, v_next, v_cut
      FROM (SELECT c.*, sum(length(c.piece)) OVER (ORDER BY c.line) - length(c.piece) AS before
              FROM (SELECT x.line_start + u.k AS line,
                           x.byte_offset + u.o AS starts_at,
                           x.byte_offset + u.o_end AS ends_at,
                           CASE WHEN t.is_cut THEN substring(x.d FROM u.o - x.s + 1 FOR p_max_line_bytes) || '\x0a'::bytea
                                ELSE substring(x.d FROM u.o - x.s + 1 FOR u.o_end - u.o) END AS piece,
                           t.is_cut
                      FROM (SELECT e.byte_offset, e.line_start, e.line_offsets, length(e.data) AS len, r.s, r.prefix,
                                   substring(e.data FROM r.s + 1
                                             FOR CASE WHEN r.prefix THEN p_max_line_bytes
                                                      ELSE (least(length(e.data), v_stop - e.byte_offset) - r.s)::int END) AS d
                              FROM vfs_extents e
                             CROSS JOIN LATERAL (SELECT greatest(v_start - e.byte_offset, 0)::int AS s,
                                                        cardinality(e.line_offsets) = 1
                                                        AND length(e.data) > p_max_line_bytes::bigint + 1 AS prefix) r
                             WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset >= v_ext
                               AND e.byte_offset < v_stop
                            OFFSET 0) x
                     CROSS JOIN LATERAL (SELECT w.o, w.k, w.o_end
                                           FROM (SELECT z.o, z.k, lead(z.o, 1, x.len) OVER (ORDER BY z.k) AS o_end
                                                   FROM unnest(x.line_offsets) WITH ORDINALITY AS z(o, k)) w
                                          WHERE x.line_start + w.k BETWEEN v_first AND v_last) u
                     CROSS JOIN LATERAL (SELECT CASE WHEN x.prefix THEN true
                                                     ELSE u.o_end - u.o - (get_byte(x.d, u.o_end - x.s - 1) = 10)::int
                                                          > p_max_line_bytes END AS is_cut) t
                    OFFSET 0) c) l
     WHERE l.before < p_max_bytes;
    RETURN QUERY SELECT v_size, v_gen, v_lines, p_first_line, v_start, coalesce(v_data, ''::bytea),
                        coalesce(v_next, v_start), v_cut, NULL::text, NULL::text;
END $$;
