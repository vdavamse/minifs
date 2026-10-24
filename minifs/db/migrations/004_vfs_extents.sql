-- File content: vfs_extents and the content operations (vfs_append, vfs_write, vfs_pread, vfs_compact), plus
-- vfs_create with initial content.
--
-- A file's bytes are variable-length extents keyed by (node_id, generation, byte_offset). The extents of the
-- current generation tile [0, size) with no gaps or overlaps: an append adds one extent at offset size, an
-- overwrite replaces them all with a new generation, and compaction merges neighbours without moving a byte.
-- Bytes are never decoded here, so an append may end in the middle of a UTF-8 character.
--
-- Errors as in migration 003: SQLSTATE 'MFnnn' with the errno name as the message (ENOENT, EISDIR, EINVAL,
-- ESTALE). Every function pins search_path to the one this migration ran with.
--
-- The writers lock the inode FOR UPDATE until commit. Under REPEATABLE READ, locking a file that another
-- transaction changed after the snapshot raises a serialization failure (40001), as any row lock does, and so
-- does compacting extents a concurrent compaction replaced: retry the transaction.

CREATE TABLE vfs_extents (                       -- data blocks
    node_id     uuid   NOT NULL,
    generation  int    NOT NULL,
    byte_offset bigint NOT NULL,
    data        bytea  NOT NULL
        CONSTRAINT vfs_extents_data_check CHECK (length(data) > 0),   -- an empty one would break the tiling
    node_kind   text   NOT NULL DEFAULT 'file'
        CONSTRAINT vfs_extents_node_kind_check CHECK (node_kind = 'file'),
    CONSTRAINT vfs_extents_pkey PRIMARY KEY (node_id, generation, byte_offset),
    CONSTRAINT vfs_extents_node_fkey FOREIGN KEY (node_id, node_kind)   -- only files have extents: EISDIR
        REFERENCES vfs_nodes (id, kind) ON DELETE CASCADE
);
-- Uncompressed TOAST: a substring() of a large extent fetches only the chunks it covers.
ALTER TABLE vfs_extents ALTER COLUMN data SET STORAGE EXTERNAL;


-- Locks file p_node FOR UPDATE until commit and returns its size and generation. ENOENT if it doesn't exist,
-- EISDIR if it is a directory. Directories are never locked here.
CREATE FUNCTION vfs_lock_file(p_node uuid, OUT size bigint, OUT generation int)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE v_kind text;
BEGIN
    SELECT n.size, n.generation INTO size, generation FROM vfs_nodes n WHERE n.id = p_node AND n.kind = 'file' FOR UPDATE;
    IF FOUND THEN
        RETURN;
    END IF;
    SELECT n.kind INTO v_kind FROM vfs_nodes n WHERE n.id = p_node;
    IF NOT FOUND THEN
        PERFORM vfs_raise('ENOENT', format('%s does not exist', p_node));
    END IF;
    PERFORM vfs_raise('EISDIR', format('%s is a directory', p_node));
END $$;


-- Appends p_data to file p_node, like write(2) with O_APPEND, and returns the offset it landed at. The inode
-- lock is held until commit, so concurrent appenders take turns: offsets are gap-free and offset order is
-- commit order. Keeps the generation. An empty p_data changes nothing. ENOENT, EISDIR; EINVAL for NULL data.
-- mtime is the later of its old value and the clock, so it never goes back when a transaction that started
-- earlier commits later (now() is the transaction's start time).
CREATE FUNCTION vfs_append(p_node uuid, p_data bytea) RETURNS bigint LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_size bigint;
    v_gen int;
BEGIN
    IF p_data IS NULL THEN
        PERFORM vfs_raise('EINVAL', 'data is NULL');
    END IF;
    SELECT f.size, f.generation INTO v_size, v_gen FROM vfs_lock_file(p_node) f;
    -- A zero-length extent would take the primary key of the next append's extent.
    IF length(p_data) = 0 THEN
        RETURN v_size;
    END IF;
    INSERT INTO vfs_extents (node_id, generation, byte_offset, data) VALUES (p_node, v_gen, v_size, p_data);
    UPDATE vfs_nodes SET size = v_size + length(p_data), mtime = greatest(mtime, clock_timestamp()) WHERE id = p_node;
    RETURN v_size;
END $$;


-- Replaces the content of file p_node with p_data, like open(O_TRUNC) then write(2): a new generation, the old
-- extents deleted. Returns the new generation and size. mtime as in vfs_append.
--
-- With p_expect_gen, p_expect_size and p_expect_name it is a compare-and-swap for the Write tool: it writes only
-- if the file still has that generation, size, name and parent (p_expect_parent NULL meaning the root), and
-- raises ESTALE otherwise. The three are all or none, and p_expect_parent needs them: EINVAL otherwise.
-- ENOENT, EISDIR; EINVAL for NULL data.
CREATE FUNCTION vfs_write(p_node uuid, p_data bytea,
                          p_expect_gen int DEFAULT NULL, p_expect_size bigint DEFAULT NULL,
                          p_expect_parent uuid DEFAULT NULL, p_expect_name text DEFAULT NULL)
RETURNS TABLE (generation int, size bigint) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_size bigint;
    v_gen int;
    v_parent uuid;
    v_name text;
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
    IF length(p_data) > 0 THEN
        INSERT INTO vfs_extents (node_id, generation, byte_offset, data) VALUES (p_node, v_gen + 1, 0, p_data);
    END IF;
    UPDATE vfs_nodes SET generation = v_gen + 1, size = length(p_data), mtime = greatest(mtime, clock_timestamp())
     WHERE id = p_node;
    RETURN QUERY SELECT v_gen + 1, length(p_data)::bigint;
END $$;


-- Reads up to p_limit bytes of file p_node from byte p_offset, like pread(2); a negative p_offset counts from
-- the end (-N: the last N bytes, clamped to the start). Returns one row per extent read, in offset order, each
-- with the file's size and generation, the offset of its bytes and the bytes, trimmed to the range. When no
-- bytes are in range (past EOF, an empty file, p_limit 0) it returns one row with NULL data, and byte_offset
-- the resolved start offset (clamped to [0, size]).
--
-- One snapshot: the function is STABLE, so size, generation and bytes all come from the caller's statement
-- snapshot, even while another session appends. Takes no locks.
--
-- Two index steps, like walking ext4's extent tree: a backward descent finds the extent containing the start,
-- then a forward range scan from it, bounded by offset + limit, reads the rest. (The overlap test alone can't
-- bound a scan of the primary key, so a single scan would read every extent from the start of the file.)
-- ENOENT, EISDIR; EINVAL for a NULL offset or a NULL or negative limit.
CREATE FUNCTION vfs_pread(p_node uuid, p_offset bigint, p_limit int)
RETURNS TABLE (size bigint, generation int, byte_offset bigint, data bytea)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE v_kind text;
BEGIN
    IF p_offset IS NULL OR p_limit IS NULL OR p_limit < 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid offset %s or limit %s', p_offset, p_limit));
    END IF;
    RETURN QUERY
    WITH n AS (
        SELECT v.id, v.size, v.generation,
               CASE WHEN p_offset < 0 THEN greatest(v.size + p_offset, 0) ELSE least(p_offset, v.size) END AS off
          FROM vfs_nodes v WHERE v.id = p_node AND v.kind = 'file'
    ), s AS (                                       -- step 1: the extent that contains off
        SELECT n.*, coalesce((SELECT e.byte_offset FROM vfs_extents e
                               WHERE e.node_id = n.id AND e.generation = n.generation AND e.byte_offset <= n.off
                               ORDER BY e.byte_offset DESC LIMIT 1), 0) AS start
          FROM n
    )
    SELECT s.size, s.generation,
           greatest(e.byte_offset, s.off),
           substring(e.data                         -- substring(bytea) takes int, not bigint
                     FROM (greatest(s.off - e.byte_offset, 0) + 1)::int
                     FOR (least(e.byte_offset + length(e.data), s.off + p_limit) - greatest(e.byte_offset, s.off))::int)
      FROM s
      LEFT JOIN vfs_extents e                       -- step 2: range scan forward from there
             ON e.node_id = s.id AND e.generation = s.generation
            AND e.byte_offset >= s.start
            AND e.byte_offset < s.off + p_limit
            AND e.byte_offset + length(e.data) > s.off
            AND p_limit > 0
     ORDER BY e.byte_offset;
    IF FOUND THEN
        RETURN;
    END IF;
    SELECT v.kind INTO v_kind FROM vfs_nodes v WHERE v.id = p_node;
    IF NOT FOUND THEN
        PERFORM vfs_raise('ENOENT', format('%s does not exist', p_node));
    END IF;
    PERFORM vfs_raise('EISDIR', format('%s is a directory', p_node));
END $$;


-- Merges the extents of file p_node into pieces of about p_target bytes (64 KB by default) under the inode lock,
-- and returns how many extents it removed. A piece starts at an extent shorter than half the target and takes
-- the extents after it that end within p_target bytes of its start. Longer extents count as full: they start
-- no piece and are never copied, so a compacted file is walked by one index scan and isn't rewritten again.
-- Bytes, size, generation and mtime are unchanged and vfs_nodes isn't updated (no NOTIFY), so readers,
-- cursors and caches don't notice. Appends wait for it. ENOENT, EISDIR; EINVAL for a p_target outside
-- [1, 64 MB]. Extents that don't tile the file (only raw SQL can make them) raise data_corrupted.
CREATE FUNCTION vfs_compact(p_node uuid, p_target int DEFAULT 65536) RETURNS int
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
        -- One statement: string_agg consumes every deleted row before the INSERT emits its one, and the unique
        -- check doesn't see rows this transaction deleted, so the new extent can take the first one's key.
        WITH gone AS (
            DELETE FROM vfs_extents e
             WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset >= v_pos AND e.byte_offset < v_end
            RETURNING e.byte_offset, e.data
        )
        INSERT INTO vfs_extents (node_id, generation, byte_offset, data)
        SELECT p_node, v_gen, v_pos, string_agg(gone.data, ''::bytea ORDER BY gone.byte_offset) FROM gone;
        v_removed := v_removed + v_n;
        v_pos := v_end;
    END LOOP;
    RETURN v_removed;
END $$;


-- vfs_create gains initial content (migration 003 anticipated this): drop the old signature so calls with three
-- arguments aren't ambiguous between the two.
DROP FUNCTION vfs_create(uuid, text, text);

-- Creates p_name ('file' or 'dir') in directory p_parent (NULL: the root), like open(O_CREAT|O_EXCL) / mkdir,
-- with p_data as the file's content (generation 0). EEXIST if the name is taken, ENOENT / ENOTDIR for a missing
-- parent / a parent that is a file, EINVAL for content on a directory.
CREATE FUNCTION vfs_create(p_parent uuid, p_name text, p_kind text, p_data bytea DEFAULT NULL)
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
    INSERT INTO vfs_nodes (parent_id, name, kind, size) VALUES (p_parent, p_name, p_kind, v_size)
        ON CONFLICT ON CONSTRAINT vfs_nodes_parent_name_key DO NOTHING
        RETURNING vfs_nodes.id INTO v_id;
    IF v_id IS NULL THEN
        PERFORM vfs_raise('EEXIST', format('%L already exists in %s', p_name, coalesce(p_parent::text, '/')));
    END IF;
    IF v_size > 0 THEN
        INSERT INTO vfs_extents (node_id, generation, byte_offset, data) VALUES (v_id, 0, 0, p_data);
    END IF;
    RETURN QUERY SELECT v_id, 0, v_size;
END $$;
