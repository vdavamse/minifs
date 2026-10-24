-- Line-aware extents (#29): every extent starts at a line start, and carries the line number it starts at and
-- where each of its lines starts, so line reads (vfs_pread_lines) and search address lines in Postgres.
--
-- Lines split on \n only. Every extent of a generation except the last ends with \n; the last may end mid-line
-- when the file has no trailing newline. A \n byte never occurs inside a multi-byte UTF-8 sequence, so no extent
-- splits a UTF-8 character either, and each extent decodes on its own.
--
--   vfs_extents.line_start    the number of \n before byte_offset: the extent's first line is line_start + 1
--   vfs_extents.line_offsets  the offset of each line start inside the extent, so line_offsets[1] = 0: line
--                             line_start + k starts at file byte byte_offset + line_offsets[k]
--   vfs_nodes.lines           the line count, as the Read tool counts it: a trailing \n adds no empty last line,
--                             so 'a\nb\n' and 'a\nb' both have 2 lines. Updated with size, under the inode lock.
--                             Not indexed, so appends stay HOT.
--
-- vfs_cut cuts content into such extents of about 64 KB, always just after a \n; a line longer than that is one
-- extent of its own. vfs_append, vfs_write and vfs_create cut what they write, and vfs_compact merges whole
-- extents, which keeps the alignment. vfs_pread returns what it did in 004, but is rewritten (see below).
--
-- Cost of the alignment: an append to a file that ends mid-line rewrites the file's last extent, which is the
-- whole last line when that line is longer than the target. So a file with no newlines at all (minified JSON, a
-- token stream) is rewritten whole on every append, and one line can't grow past bytea's 1 GB limit.
--
-- The writers' errors and locking are as in migration 004. vfs_append to a file that ends mid-line now deletes
-- and rewrites its last extent, so under REPEATABLE READ it can also fail with 40001 after a concurrent
-- compaction: retry.

ALTER TABLE vfs_nodes ADD COLUMN lines bigint NOT NULL DEFAULT 0;   -- 0 for directories

ALTER TABLE vfs_extents ADD COLUMN line_start bigint, ADD COLUMN line_offsets int4[];
-- Compressed TOAST: extents are now small and read whole, so slice reads of uncompressed values no longer pay
-- for themselves, and text compresses 3 to 4 times. Rows already stored keep their storage.
ALTER TABLE vfs_extents ALTER COLUMN data SET STORAGE EXTENDED;


-- Cuts p_data into extents that each start at a line start: consecutive whole lines, cut just after a \n once
-- the next line would take the extent past p_target bytes. A line longer than p_target is an extent of its own;
-- only the last extent may end without a \n. Returns, in order, each extent's offset in p_data, the number of
-- \n before it, its bytes and its line_offsets. No rows for empty or NULL data; EINVAL for a NULL or
-- non-positive p_target.
--
-- The lines are found in C, not byte by byte: encode(..., 'escape') turns bytea into text that keeps every \n
-- byte as a newline (it escapes only NUL, high-bit bytes and backslashes, never in a way that produces one), so
-- splitting that text on newlines and decoding each piece gives the length of each line. It encodes 1 MB at a
-- time: escaping can make the text 4 times longer than the bytes, and a text value can't pass 1 GB. A line may
-- span windows, so the last piece of a window carries on into the next.
CREATE FUNCTION vfs_cut(p_data bytea, p_target int DEFAULT 65536)
RETURNS TABLE (rel_offset bigint, rel_line bigint, data bytea, line_offsets int4[])
LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
DECLARE
    c_window constant int := 1048576;
    v_lens bigint[] := '{}';                        -- the length of each line, \n included
    v_open bigint := 0;                             -- the length so far of a line that spans windows
    v_len bigint;
    v_last boolean;
    v_pos bigint := 0;                              -- where the current line starts
BEGIN
    IF p_target IS NULL OR p_target < 1 THEN
        PERFORM vfs_raise('EINVAL', format('invalid target size %s', p_target));
    END IF;
    IF p_data IS NULL THEN
        RETURN;
    END IF;
    FOR w IN 0 .. (length(p_data) - 1) / c_window LOOP     -- no iterations for empty data
        FOR v_len, v_last IN
            SELECT length(decode(s.piece, 'escape')), s.i = count(*) OVER ()
              FROM string_to_table(encode(substring(p_data FROM w * c_window + 1 FOR c_window), 'escape'), E'\n')
                   WITH ORDINALITY AS s(piece, i)
             ORDER BY s.i
        LOOP
            IF v_last THEN
                v_open := v_open + v_len;           -- no \n after it in this window
            ELSE
                v_lens := v_lens || (v_open + v_len + 1);
                v_open := 0;
            END IF;
        END LOOP;
    END LOOP;
    IF v_open > 0 THEN
        v_lens := v_lens || v_open;                 -- the last line, with no \n
    END IF;
    rel_offset := 0;
    rel_line := 0;
    line_offsets := '{}';
    FOREACH v_len IN ARRAY v_lens LOOP
        IF v_pos > rel_offset AND v_pos - rel_offset + v_len > p_target THEN
            data := substring(p_data FROM (rel_offset + 1)::int FOR (v_pos - rel_offset)::int);
            RETURN NEXT;
            rel_line := rel_line + cardinality(line_offsets);
            rel_offset := v_pos;
            line_offsets := '{}';
        END IF;
        line_offsets := line_offsets || (v_pos - rel_offset)::int;
        v_pos := v_pos + v_len;
    END LOOP;
    IF v_pos > rel_offset THEN
        data := substring(p_data FROM (rel_offset + 1)::int FOR (v_pos - rel_offset)::int);
        RETURN NEXT;
    END IF;
END $$;


-- Existing content (nothing is deployed, but a database migrated to 006 may hold some): cut each file's current
-- generation again. Older generations have no extents; vfs_write deletes them.
DO $$
DECLARE
    v_node uuid;
    v_gen int;
    v_data bytea;
    v_lines bigint;
BEGIN
    FOR v_node, v_gen IN SELECT n.id, n.generation FROM vfs_nodes n
                          WHERE n.kind = 'file' AND EXISTS (SELECT FROM vfs_extents e WHERE e.node_id = n.id)
    LOOP
        SELECT string_agg(e.data, ''::bytea ORDER BY e.byte_offset) INTO v_data
          FROM vfs_extents e WHERE e.node_id = v_node AND e.generation = v_gen;
        DELETE FROM vfs_extents WHERE node_id = v_node;
        WITH ins AS (
            INSERT INTO vfs_extents (node_id, generation, byte_offset, line_start, data, line_offsets)
            SELECT v_node, v_gen, c.rel_offset, c.rel_line, c.data, c.line_offsets FROM vfs_cut(v_data) c
            RETURNING vfs_extents.line_start + cardinality(vfs_extents.line_offsets) AS lines_after
        )
        SELECT coalesce(max(ins.lines_after), 0) INTO v_lines FROM ins;
        UPDATE vfs_nodes SET lines = v_lines WHERE id = v_node;
    END LOOP;
END $$;

ALTER TABLE vfs_extents
    ALTER COLUMN line_start SET NOT NULL,
    ALTER COLUMN line_offsets SET NOT NULL,
    ADD CONSTRAINT vfs_extents_line_offsets_check CHECK (line_offsets[1] = 0),   -- an extent starts a line
    -- Line lookups (vfs_pread_lines). Every extent starts a line, so line_start is distinct in a generation.
    ADD CONSTRAINT vfs_extents_line_key UNIQUE (node_id, generation, line_start);


-- Appends p_data to file p_node, like write(2) with O_APPEND, and returns the offset it landed at. The inode
-- lock is held until commit, so concurrent appenders take turns: offsets are gap-free and offset order is
-- commit order. Keeps the generation. An empty p_data changes nothing. ENOENT, EISDIR; EINVAL for NULL data.
-- mtime is the later of its old value and the clock, so it never goes back when a transaction that started
-- earlier commits later (now() is the transaction's start time).
--
-- When the file ends mid-line, its last extent is deleted and cut again together with p_data, so extents keep
-- starting at line starts. That rewrites one extent: about 64 KB at most, unless its last line is longer, in
-- which case all of that line (a file with no newlines: all of it). The bytes before the end are unchanged.
CREATE OR REPLACE FUNCTION vfs_append(p_node uuid, p_data bytea) RETURNS bigint
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_size bigint;
    v_gen int;
    v_off bigint;                                   -- where the cut content goes
    v_line bigint;                                  -- the number of \n before v_off
    v_last bytea;                                   -- the file's last extent
    v_lines bigint;
BEGIN
    IF p_data IS NULL THEN
        PERFORM vfs_raise('EINVAL', 'data is NULL');
    END IF;
    SELECT f.size, f.generation INTO v_size, v_gen FROM vfs_lock_file(p_node) f;
    -- A zero-length extent would take the primary key of the next append's extent.
    IF length(p_data) = 0 THEN
        RETURN v_size;
    END IF;
    SELECT e.byte_offset, e.line_start, e.data, cardinality(e.line_offsets) INTO v_off, v_line, v_last, v_lines
      FROM vfs_extents e
     WHERE e.node_id = p_node AND e.generation = v_gen ORDER BY e.byte_offset DESC LIMIT 1;
    IF NOT FOUND THEN                               -- an empty file
        v_off := 0;
        v_line := 0;
        v_last := ''::bytea;
    ELSIF get_byte(v_last, length(v_last) - 1) = 10 THEN
        v_off := v_size;                            -- ends with \n: the new data starts a line
        v_line := v_line + v_lines;
        v_last := ''::bytea;
    ELSE
        DELETE FROM vfs_extents e WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset = v_off;
    END IF;
    WITH ins AS (
        INSERT INTO vfs_extents (node_id, generation, byte_offset, line_start, data, line_offsets)
        SELECT p_node, v_gen, v_off + c.rel_offset, v_line + c.rel_line, c.data, c.line_offsets
          FROM vfs_cut(v_last || p_data) c
        RETURNING vfs_extents.line_start + cardinality(vfs_extents.line_offsets) AS lines_after
    )
    SELECT max(ins.lines_after) INTO v_lines FROM ins;
    UPDATE vfs_nodes SET size = v_size + length(p_data), lines = v_lines, mtime = greatest(mtime, clock_timestamp())
     WHERE id = p_node;
    RETURN v_size;
END $$;


-- Replaces the content of file p_node with p_data, like open(O_TRUNC) then write(2): a new generation, the old
-- extents deleted, the new content cut by vfs_cut. Returns the new generation and size. mtime as in vfs_append.
--
-- With p_expect_gen, p_expect_size and p_expect_name it is a compare-and-swap for the Write tool: it writes only
-- if the file still has that generation, size, name and parent (p_expect_parent NULL meaning the root), and
-- raises ESTALE otherwise. The three are all or none, and p_expect_parent needs them: EINVAL otherwise.
-- ENOENT, EISDIR; EINVAL for NULL data.
CREATE OR REPLACE FUNCTION vfs_write(p_node uuid, p_data bytea,
                                     p_expect_gen int DEFAULT NULL, p_expect_size bigint DEFAULT NULL,
                                     p_expect_parent uuid DEFAULT NULL, p_expect_name text DEFAULT NULL)
RETURNS TABLE (generation int, size bigint) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_size bigint;
    v_gen int;
    v_parent uuid;
    v_name text;
    v_lines bigint;
BEGIN
    IF p_data IS NULL THEN
        PERFORM vfs_raise('EINVAL', 'data is NULL');
    END IF;
    IF num_nulls(p_expect_gen, p_expect_size, p_expect_name) NOT IN (0, 3)
       OR (p_expect_gen IS NULL AND p_expect_parent IS NOT NULL) THEN
        PERFORM vfs_raise('EINVAL', 'expect_gen, expect_size and expect_name go together, and expect_parent needs them');
    END IF;
    SELECT f.size, f.generation INTO v_size, v_gen FROM vfs_lock_file(p_node) f;
    IF p_expect_gen IS NOT NULL THEN
        SELECT n.parent_id, n.name INTO v_parent, v_name FROM vfs_nodes n WHERE n.id = p_node;
        IF (v_gen, v_size, v_name) IS DISTINCT FROM (p_expect_gen, p_expect_size, p_expect_name)
           OR v_parent IS DISTINCT FROM p_expect_parent THEN
            PERFORM vfs_raise('ESTALE', format('%s is now generation %s, %s bytes, %L in %s', p_node, v_gen, v_size,
                                               v_name, coalesce(v_parent::text, '/')));
        END IF;
    END IF;
    DELETE FROM vfs_extents WHERE node_id = p_node;
    WITH ins AS (
        INSERT INTO vfs_extents (node_id, generation, byte_offset, line_start, data, line_offsets)
        SELECT p_node, v_gen + 1, c.rel_offset, c.rel_line, c.data, c.line_offsets FROM vfs_cut(p_data) c
        RETURNING vfs_extents.line_start + cardinality(vfs_extents.line_offsets) AS lines_after
    )
    SELECT coalesce(max(ins.lines_after), 0) INTO v_lines FROM ins;
    UPDATE vfs_nodes SET generation = v_gen + 1, size = length(p_data), lines = v_lines,
                         mtime = greatest(mtime, clock_timestamp())
     WHERE id = p_node;
    RETURN QUERY SELECT v_gen + 1, length(p_data)::bigint;
END $$;


-- Merges the extents of file p_node into pieces of about p_target bytes (64 KB by default) under the inode lock,
-- and returns how many extents it removed. A piece starts at an extent shorter than half the target and takes
-- the extents after it that end within p_target bytes of its start. Extents of at least half the target never
-- start a piece, extents longer than p_target are never copied, and a compacted file isn't rewritten again.
-- Pieces are whole extents, so they start at line starts too: a piece keeps its first extent's line_start, and
-- its line_offsets are its extents' offsets, shifted.
-- Bytes, size, lines, generation and mtime are unchanged and vfs_nodes isn't updated (no NOTIFY), so readers,
-- cursors and caches don't notice. Appends wait for it. ENOENT, EISDIR; EINVAL for a p_target outside
-- [1, 64 MB]. Extents that don't tile the file (only raw SQL can make them) raise data_corrupted.
CREATE OR REPLACE FUNCTION vfs_compact(p_node uuid, p_target int DEFAULT 65536) RETURNS int
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_gen int;
    v_pos bigint := 0;                              -- every extent before this offset is done
    v_len int;
    v_n int;
    v_end bigint;
    v_sum bigint;
    v_removed int := 0;
BEGIN
    IF p_target IS NULL OR p_target < 1 OR p_target > 64 * 1024 * 1024 THEN
        PERFORM vfs_raise('EINVAL', format('invalid target size %s', p_target));
    END IF;
    SELECT f.generation INTO v_gen FROM vfs_lock_file(p_node) f;
    LOOP
        SELECT e.byte_offset, length(e.data) INTO v_pos, v_len FROM vfs_extents e
         WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset >= v_pos
           AND length(e.data) < p_target / 2
         ORDER BY e.byte_offset LIMIT 1;
        EXIT WHEN NOT FOUND;
        -- The extents that follow and end within p_target of v_pos. Extents tile the file, so they are a prefix.
        SELECT count(*), max(e.byte_offset + length(e.data)), sum(length(e.data)) INTO v_n, v_end, v_sum
          FROM vfs_extents e
         WHERE e.node_id = p_node AND e.generation = v_gen
           AND e.byte_offset > v_pos AND e.byte_offset < v_pos + p_target
           AND e.byte_offset + length(e.data) <= v_pos + p_target;
        IF v_n > 0 AND v_len + v_sum <> v_end - v_pos THEN
            RAISE EXCEPTION 'extents of % in [%, %) do not tile the file', p_node, v_pos, v_end
                USING ERRCODE = 'data_corrupted';
        END IF;
        IF v_n = 0 THEN
            v_pos := v_pos + v_len;
            CONTINUE;
        END IF;
        -- One statement: the aggregates consume every deleted row before the INSERT emits its one, and the
        -- unique checks don't see rows this transaction deleted, so the new extent can take the first one's keys.
        WITH gone AS (
            DELETE FROM vfs_extents e
             WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset >= v_pos AND e.byte_offset < v_end
            RETURNING e.byte_offset, e.line_start, e.data, e.line_offsets
        )
        INSERT INTO vfs_extents (node_id, generation, byte_offset, line_start, data, line_offsets)
        SELECT p_node, v_gen, v_pos, min(gone.line_start), string_agg(gone.data, ''::bytea ORDER BY gone.byte_offset),
               (SELECT array_agg((g.byte_offset - v_pos + u.o)::int ORDER BY g.byte_offset, u.k)
                  FROM gone g, unnest(g.line_offsets) WITH ORDINALITY AS u(o, k))
          FROM gone;
        v_removed := v_removed + v_n;
        v_pos := v_end;
    END LOOP;
    RETURN v_removed;
END $$;


-- Creates p_name ('file' or 'dir') in directory p_parent (NULL: the root), like open(O_CREAT|O_EXCL) / mkdir,
-- with p_data as the file's content (generation 0), cut by vfs_cut. EEXIST if the name is taken, ENOENT /
-- ENOTDIR for a missing parent / a parent that is a file, EINVAL for content on a directory.
CREATE OR REPLACE FUNCTION vfs_create(p_parent uuid, p_name text, p_kind text, p_data bytea DEFAULT NULL)
RETURNS TABLE (id uuid, generation int, size bigint)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_size bigint := coalesce(length(p_data), 0);
BEGIN
    IF p_kind IS NULL OR p_kind NOT IN ('file', 'dir') THEN
        PERFORM vfs_raise('EINVAL', format('invalid kind %L', p_kind));
    END IF;
    IF p_kind = 'dir' AND p_data IS NOT NULL THEN
        PERFORM vfs_raise('EINVAL', 'a directory has no content');
    END IF;
    PERFORM vfs_check_name(p_name);
    PERFORM vfs_lock_dir(p_parent);
    -- One statement, so the inode is inserted with its line count (a later UPDATE would NOTIFY a modify). The
    -- extents' foreign key is checked at the end of the statement, after the inode is in.
    WITH c AS MATERIALIZED (
        SELECT * FROM vfs_cut(p_data)
    ), n AS (
        INSERT INTO vfs_nodes (parent_id, name, kind, size, lines)
        SELECT p_parent, p_name, p_kind, v_size, coalesce((SELECT max(c.rel_line + cardinality(c.line_offsets)) FROM c), 0)
            ON CONFLICT ON CONSTRAINT vfs_nodes_parent_name_key DO NOTHING
            RETURNING vfs_nodes.id
    ), x AS (
        INSERT INTO vfs_extents (node_id, generation, byte_offset, line_start, data, line_offsets)
        SELECT n.id, 0, c.rel_offset, c.rel_line, c.data, c.line_offsets FROM n, c
    )
    SELECT n.id INTO v_id FROM n;
    IF v_id IS NULL THEN
        PERFORM vfs_raise('EEXIST', format('%L already exists in %s', p_name, coalesce(p_parent::text, '/')));
    END IF;
    RETURN QUERY SELECT v_id, 0, v_size;
END $$;


-- The size, generation and line count of file p_node, read without a lock. ENOENT if it doesn't exist, EISDIR
-- if it is a directory. The readers' counterpart of vfs_lock_file.
CREATE FUNCTION vfs_file(p_node uuid, OUT size bigint, OUT generation int, OUT lines bigint)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE v_kind text;
BEGIN
    SELECT n.size, n.generation, n.lines INTO size, generation, lines FROM vfs_nodes n
     WHERE n.id = p_node AND n.kind = 'file';
    IF FOUND THEN
        RETURN;
    END IF;
    SELECT n.kind INTO v_kind FROM vfs_nodes n WHERE n.id = p_node;
    IF NOT FOUND THEN
        PERFORM vfs_raise('ENOENT', format('%s does not exist', p_node));
    END IF;
    PERFORM vfs_raise('EISDIR', format('%s is a directory', p_node));
END $$;


-- vfs_pread, results unchanged: the extents' new columns made the planner turn its one-statement form's range
-- scan into a sequential scan of the file's extents, because it can't see how narrow bounds computed in the same
-- query are. Each step is now its own statement, so its bounds reach the planner as values. A STABLE function's
-- statements all run on the caller's snapshot, so size, generation and bytes still come from one snapshot.
--
-- Reads up to p_limit bytes of file p_node from byte p_offset, like pread(2); a negative p_offset counts from
-- the end (-N: the last N bytes, clamped to the start). Returns one row per extent read, in offset order, each
-- with the file's size and generation, the offset of its bytes and the bytes, trimmed to the range. When no
-- bytes are in range (past EOF, an empty file, p_limit 0) it returns one row with NULL data, and byte_offset
-- the resolved start offset (clamped to [0, size]). Takes no locks.
--
-- Two index steps, like walking ext4's extent tree: a backward descent finds the extent containing the start,
-- then a forward range scan from it, bounded by offset + limit, reads the rest. (The overlap test alone can't
-- bound a scan of the primary key, so a single scan would read every extent from the start of the file.)
-- ENOENT, EISDIR; EINVAL for a NULL offset or a NULL or negative limit.
CREATE OR REPLACE FUNCTION vfs_pread(p_node uuid, p_offset bigint, p_limit int)
RETURNS TABLE (size bigint, generation int, byte_offset bigint, data bytea)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_size bigint;
    v_gen int;
    v_off bigint;
    v_start bigint;
BEGIN
    IF p_offset IS NULL OR p_limit IS NULL OR p_limit < 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid offset %s or limit %s', p_offset, p_limit));
    END IF;
    SELECT f.size, f.generation INTO v_size, v_gen FROM vfs_file(p_node) f;
    -- Clamped, so v_off + p_limit can't overflow.
    v_off := CASE WHEN p_offset < 0 THEN greatest(v_size + p_offset, 0) ELSE least(p_offset, v_size) END;
    -- Step 1: the extent that contains v_off.
    SELECT e.byte_offset INTO v_start FROM vfs_extents e
     WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset <= v_off
     ORDER BY e.byte_offset DESC LIMIT 1;
    -- Step 2: a range scan forward from there.
    RETURN QUERY
    SELECT v_size, v_gen, greatest(e.byte_offset, v_off),
           substring(e.data                         -- substring(bytea) takes int, not bigint
                     FROM (greatest(v_off - e.byte_offset, 0) + 1)::int
                     FOR (least(e.byte_offset + length(e.data), v_off + p_limit) - greatest(e.byte_offset, v_off))::int)
      FROM vfs_extents e
     WHERE e.node_id = p_node AND e.generation = v_gen
       AND e.byte_offset >= v_start
       AND e.byte_offset < v_off + p_limit
       AND e.byte_offset + length(e.data) > v_off
       AND p_limit > 0
     ORDER BY e.byte_offset;
    IF NOT FOUND THEN
        RETURN QUERY SELECT v_size, v_gen, v_off, NULL::bytea;
    END IF;
END $$;


-- Reads lines of file p_node, like Read: the bytes from the start of line p_first_line (1-based) through at most
-- p_max_lines whole lines, each with its \n, capped at p_max_bytes bytes (so the last line may come back cut).
-- Returns one row: the file's size, generation and line count, p_first_line, the byte offset where that line
-- starts (so callers can mix line and byte reads) and the bytes. A p_first_line past the last line returns no
-- bytes (an empty bytea), byte_offset = size and the line count, so the tool can say the offset is out of range.
--
-- One snapshot, as in vfs_pread: every statement of this STABLE function runs on the caller's snapshot, even
-- while another session appends. Takes no locks.
--
-- Index steps as in vfs_pread: a backward descent of (node_id, generation, line_start) finds the extent where
-- the first line starts, another the one where the line after the last one wanted starts (unless that is past
-- the end), then a forward range scan of the primary key between the two reads the bytes.
-- ENOENT, EISDIR; EINVAL for a NULL argument, a p_first_line below 1 or a negative p_max_lines or p_max_bytes.
CREATE FUNCTION vfs_pread_lines(p_node uuid, p_first_line bigint, p_max_lines int, p_max_bytes int)
RETURNS TABLE (size bigint, generation int, lines bigint, first_line bigint, byte_offset bigint, data bytea)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_size bigint;
    v_gen int;
    v_lines bigint;
    v_first bigint;
    v_ext bigint;                                   -- the extent where line v_first starts
    v_start bigint;
    v_stop bigint;
    v_data bytea;
BEGIN
    IF p_first_line IS NULL OR p_first_line < 1 OR p_max_lines IS NULL OR p_max_lines < 0
       OR p_max_bytes IS NULL OR p_max_bytes < 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid first line %s, max lines %s or max bytes %s',
                                           p_first_line, p_max_lines, p_max_bytes));
    END IF;
    SELECT f.size, f.generation, f.lines INTO v_size, v_gen, v_lines FROM vfs_file(p_node) f;
    IF p_first_line > v_lines THEN
        RETURN QUERY SELECT v_size, v_gen, v_lines, p_first_line, v_size, ''::bytea;
        RETURN;
    END IF;
    v_first := p_first_line;
    -- Step 1: the extent with the largest line_start below v_first holds the start of line v_first.
    SELECT e.byte_offset, e.byte_offset + e.line_offsets[v_first - e.line_start] INTO v_ext, v_start
      FROM vfs_extents e
     WHERE e.node_id = p_node AND e.generation = v_gen AND e.line_start < v_first
     ORDER BY e.line_start DESC LIMIT 1;
    IF v_start IS NULL THEN                         -- only raw SQL can make lines disagree with the extents
        RAISE EXCEPTION 'line % of % (% lines) is in no extent', v_first, p_node, v_lines
            USING ERRCODE = 'data_corrupted';
    END IF;
    -- Step 2: the end, where line v_first + p_max_lines starts, or the end of the file if there's no such line.
    IF v_first + p_max_lines > v_lines THEN
        v_stop := v_size;
    ELSE
        SELECT e.byte_offset + e.line_offsets[v_first + p_max_lines - e.line_start] INTO v_stop
          FROM vfs_extents e
         WHERE e.node_id = p_node AND e.generation = v_gen AND e.line_start < v_first + p_max_lines
         ORDER BY e.line_start DESC LIMIT 1;
    END IF;
    v_stop := least(v_stop, v_start + p_max_bytes);
    -- Step 3: a range scan forward from the first extent.
    SELECT string_agg(substring(e.data              -- substring(bytea) takes int, not bigint
                                FROM (greatest(v_start - e.byte_offset, 0) + 1)::int
                                FOR (least(e.byte_offset + length(e.data), v_stop)
                                     - greatest(e.byte_offset, v_start))::int),
                      ''::bytea ORDER BY e.byte_offset) INTO v_data
      FROM vfs_extents e
     WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset >= v_ext AND e.byte_offset < v_stop;
    RETURN QUERY SELECT v_size, v_gen, v_lines, p_first_line, v_start, coalesce(v_data, ''::bytea);
END $$;
