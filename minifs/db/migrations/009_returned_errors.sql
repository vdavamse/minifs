-- Returned errors (#35): expected outcomes come back as rows instead of being raised.
--
-- Postgres logs every raised error with its DETAIL (node ids, path components), the PL/pgSQL CONTEXT and the
-- statement, a non-superuser can't turn that off on Lakebase, and a raise aborts the caller's transaction (a
-- REPEATABLE READ snapshot, say). An agent guessing a path that doesn't exist, a lost Maildir claim or a
-- compare-and-swap conflict are normal answers, not faults, so the errnos split in two groups:
--
--   Outcomes: ENOENT, ENOTDIR, EISDIR, EEXIST, ESTALE, ENOTEMPTY
--     Returned. Every function that can produce one has two result columns, err (the errno name, or NULL) and
--     err_detail (what DETAIL used to carry). On an outcome the function changes nothing and returns one row
--     with err set and every other column NULL. Functions that return nothing else (vfs_rename, vfs_unlink)
--     return one row on success too, with err NULL.
--   Faults: EINVAL, ENAMETOOLONG, and anything not errno-coded (data_corrupted, ...)
--     Raised through vfs_raise as before: bad arguments or a broken invariant.
--
-- A node id (not a directory id, where NULL is the root) that is NULL raises EINVAL: it is what an unchecked
-- failed lookup gives, and must fail loudly rather than act on nothing. A caller that composes a lookup with an
-- operation calls the operation only when the lookup's err is NULL (minifs.vfs.client does, in one statement),
-- which also keeps a failed lookup of a directory from acting on the root.
--
-- An outcome commits like a success, so the row locks a function took before finding it (vfs_write's inode lock
-- before ESTALE, vfs_unlink's before ENOTEMPTY, vfs_create's directory lock before EEXIST) are held until the
-- caller's transaction ends, where a raise rolled them back with it. One statement per transaction, as
-- minifs.vfs.client runs them, makes that moot; a longer transaction should end soon after an outcome.
--
-- vfs_resolve becomes vfs_lookup, which returns its error. vfs_search needs no change: it takes ids and skips
-- those that are no longer files. The constraints stay the backstop for raw SQL and for the rare races the
-- functions don't pre-check (a create committing between vfs_rename's EEXIST check and its update raises
-- unique_violation, which minifs.db.errors maps to EEXIST).
--
-- Every changed return type needs DROP FUNCTION first. The helpers that reported errors (vfs_lock_dir,
-- vfs_lock_file, vfs_file) return them too, so every function that calls them is replaced here.

DROP FUNCTION vfs_resolve(text[]);
DROP FUNCTION vfs_lock_dir(uuid);
DROP FUNCTION vfs_lock_file(uuid);
DROP FUNCTION vfs_file(uuid);
DROP FUNCTION vfs_readdir(uuid);
DROP FUNCTION vfs_create(uuid, text, text, bytea);
DROP FUNCTION vfs_mkdir_p(text[]);
DROP FUNCTION vfs_rename(uuid, uuid, text, uuid);
DROP FUNCTION vfs_unlink(uuid);
DROP FUNCTION vfs_append(uuid, bytea);
DROP FUNCTION vfs_write(uuid, bytea, int, bigint, uuid, text);
DROP FUNCTION vfs_compact(uuid, int);
DROP FUNCTION vfs_pread(uuid, bigint, int);
DROP FUNCTION vfs_pread_lines(uuid, bigint, int, int);
DROP FUNCTION vfs_glob(uuid, text, text, int, text[], boolean, int, text[], text[]);


-- p_id, or EINVAL if p_err is set: wraps a looked-up id passed on to an operation, so even a directory argument
-- (where NULL is the root) fails loudly if the caller didn't check the lookup's err.
CREATE FUNCTION vfs_gate(p_id uuid, p_err text) RETURNS uuid LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
BEGIN
    IF p_err IS NOT NULL THEN
        PERFORM vfs_raise('EINVAL', format('the lookup failed with %s', p_err));
    END IF;
    RETURN p_id;
END $$;


-- EINVAL for a NULL node id (see above).
CREATE FUNCTION vfs_check_node(p_node uuid) RETURNS void LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
BEGIN
    IF p_node IS NULL THEN
        PERFORM vfs_raise('EINVAL', 'node id is NULL');
    END IF;
END $$;


-- Walks p_segments from the root in one round trip. Returns one row: (NULL, 'dir') for the root (empty array),
-- else the node's id and kind; or err ENOENT if a component is missing, ENOTDIR if a non-final component is a
-- file, with id and kind NULL.
CREATE FUNCTION vfs_lookup(p_segments text[]) RETURNS TABLE (id uuid, kind text, err text, err_detail text)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_kind text := 'dir';
    v_seg text;
    v_i int := 0;
BEGIN
    FOREACH v_seg IN ARRAY coalesce(p_segments, '{}') LOOP
        v_i := v_i + 1;
        IF v_kind <> 'dir' THEN
            err := 'ENOTDIR';
            err_detail := format('component %s of %L is a file', v_i - 1, p_segments);
            RETURN NEXT;
            RETURN;
        END IF;
        IF v_id IS NULL THEN
            SELECT n.id, n.kind INTO v_id, v_kind FROM vfs_nodes n WHERE n.parent_id IS NULL AND n.name = v_seg;
        ELSE
            SELECT n.id, n.kind INTO v_id, v_kind FROM vfs_nodes n WHERE n.parent_id = v_id AND n.name = v_seg;
        END IF;
        IF NOT FOUND THEN
            err := 'ENOENT';
            err_detail := format('component %s of %L does not exist', v_i, p_segments);
            RETURN NEXT;
            RETURN;
        END IF;
    END LOOP;
    id := v_id;
    kind := v_kind;
    RETURN NEXT;
END $$;


-- Locks directory p_dir against deletion until commit (FOR KEY SHARE). NULL is the root. err ENOENT if it
-- doesn't exist, ENOTDIR if it is a file. Files are never locked here, so the error doesn't wait behind an
-- appender holding the file's inode lock.
CREATE FUNCTION vfs_lock_dir(p_dir uuid, OUT err text, OUT err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF p_dir IS NULL THEN
        RETURN;
    END IF;
    PERFORM FROM vfs_nodes n WHERE n.id = p_dir AND n.kind = 'dir' FOR KEY SHARE;
    IF FOUND THEN
        RETURN;
    END IF;
    IF EXISTS (SELECT FROM vfs_nodes n WHERE n.id = p_dir) THEN
        err := 'ENOTDIR';
        err_detail := format('%s is a file', p_dir);
    ELSE
        err := 'ENOENT';
        err_detail := format('directory %s does not exist', p_dir);
    END IF;
END $$;


-- Locks file p_node FOR UPDATE until commit and returns its size and generation; err ENOENT if it doesn't exist,
-- EISDIR if it is a directory. Directories are never locked here. EINVAL for a NULL p_node.
CREATE FUNCTION vfs_lock_file(p_node uuid, OUT size bigint, OUT generation int, OUT err text, OUT err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM vfs_check_node(p_node);
    SELECT n.size, n.generation INTO size, generation FROM vfs_nodes n WHERE n.id = p_node AND n.kind = 'file' FOR UPDATE;
    IF FOUND THEN
        RETURN;
    END IF;
    IF EXISTS (SELECT FROM vfs_nodes n WHERE n.id = p_node) THEN
        err := 'EISDIR';
        err_detail := format('%s is a directory', p_node);
    ELSE
        err := 'ENOENT';
        err_detail := format('%s does not exist', p_node);
    END IF;
END $$;


-- The size, generation and line count of file p_node, read without a lock; err ENOENT if it doesn't exist,
-- EISDIR if it is a directory. The readers' counterpart of vfs_lock_file. EINVAL for a NULL p_node.
CREATE FUNCTION vfs_file(p_node uuid, OUT size bigint, OUT generation int, OUT lines bigint, OUT err text,
                         OUT err_detail text)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM vfs_check_node(p_node);
    SELECT n.size, n.generation, n.lines INTO size, generation, lines FROM vfs_nodes n
     WHERE n.id = p_node AND n.kind = 'file';
    IF FOUND THEN
        RETURN;
    END IF;
    IF EXISTS (SELECT FROM vfs_nodes n WHERE n.id = p_node) THEN
        err := 'EISDIR';
        err_detail := format('%s is a directory', p_node);
    ELSE
        err := 'ENOENT';
        err_detail := format('%s does not exist', p_node);
    END IF;
END $$;


-- The entries of directory p_dir (NULL: the root entries), in byte order of name; no rows for an empty
-- directory. err ENOENT if p_dir doesn't exist, ENOTDIR if it is a file.
CREATE FUNCTION vfs_readdir(p_dir uuid)
RETURNS TABLE (id uuid, name text, kind text, size bigint, mtime timestamptz, err text, err_detail text)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE v_kind text;
BEGIN
    IF p_dir IS NULL THEN
        RETURN QUERY SELECT n.id, n.name::text, n.kind, n.size, n.mtime, NULL::text, NULL::text
                       FROM vfs_nodes n WHERE n.parent_id IS NULL ORDER BY n.name;
        RETURN;
    END IF;
    SELECT n.kind INTO v_kind FROM vfs_nodes n WHERE n.id = p_dir;
    IF NOT FOUND THEN
        err := 'ENOENT';
        err_detail := format('directory %s does not exist', p_dir);
        RETURN NEXT;
        RETURN;
    ELSIF v_kind <> 'dir' THEN
        err := 'ENOTDIR';
        err_detail := format('%s is a file', p_dir);
        RETURN NEXT;
        RETURN;
    END IF;
    RETURN QUERY SELECT n.id, n.name::text, n.kind, n.size, n.mtime, NULL::text, NULL::text
                   FROM vfs_nodes n WHERE n.parent_id = p_dir ORDER BY n.name;
END $$;


-- Creates p_name ('file' or 'dir') in directory p_parent (NULL: the root), like open(O_CREAT|O_EXCL) / mkdir,
-- with p_data as the file's content (generation 0), cut by vfs_cut. err EEXIST if the name is taken, ENOENT /
-- ENOTDIR for a missing parent / a parent that is a file. EINVAL for a bad kind, content on a directory or a bad
-- name, ENAMETOOLONG.
CREATE FUNCTION vfs_create(p_parent uuid, p_name text, p_kind text, p_data bytea DEFAULT NULL)
RETURNS TABLE (id uuid, generation int, size bigint, err text, err_detail text)
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
    SELECT l.err, l.err_detail INTO err, err_detail FROM vfs_lock_dir(p_parent) l;
    IF err IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;
    -- One statement, so the inode is inserted with its line count (a later UPDATE would NOTIFY a modify). The
    -- extents' foreign key is checked at the end of the statement, after the inode is in. On a conflict n is
    -- empty, so no extents are inserted either.
    WITH c AS MATERIALIZED (
        SELECT * FROM vfs_cut(p_data, 65536, true)
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
        err := 'EEXIST';
        err_detail := format('%L already exists in %s', p_name, coalesce(p_parent::text, '/'));
        RETURN NEXT;
        RETURN;
    END IF;
    id := v_id;
    generation := 0;
    size := v_size;
    RETURN NEXT;
END $$;


-- Like mkdir -p: creates every missing directory along p_segments and returns the last one's id (NULL for an
-- empty array, the root). Safe against concurrent creators of the same directories. err ENOTDIR if a non-final
-- component is a file, EEXIST if the final one is; nothing has been created then, since every directory after
-- one this call created is new too. EINVAL / ENAMETOOLONG for a bad component.
CREATE FUNCTION vfs_mkdir_p(p_segments text[]) RETURNS TABLE (id uuid, err text, err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_next uuid;
    v_seg text;
    v_i int := 0;
    v_n int := coalesce(cardinality(p_segments), 0);
BEGIN
    FOREACH v_seg IN ARRAY coalesce(p_segments, '{}') LOOP
        v_i := v_i + 1;
        PERFORM vfs_check_name(v_seg);
        LOOP
            -- Lock the directory we find, so it can't be removed before our child insert or our caller's use.
            -- Only directories: a file is reported without waiting for its inode lock.
            IF v_id IS NULL THEN
                SELECT n.id INTO v_next FROM vfs_nodes n
                 WHERE n.parent_id IS NULL AND n.name = v_seg AND n.kind = 'dir' FOR KEY SHARE;
            ELSE
                SELECT n.id INTO v_next FROM vfs_nodes n
                 WHERE n.parent_id = v_id AND n.name = v_seg AND n.kind = 'dir' FOR KEY SHARE;
            END IF;
            EXIT WHEN FOUND;
            -- Only a file is an error. A directory that appears here (committed since the SELECT above, for
            -- example replacing one that was removed while we waited) conflicts below and is found next time.
            IF (v_id IS NULL AND EXISTS (SELECT FROM vfs_nodes n
                                          WHERE n.parent_id IS NULL AND n.name = v_seg AND n.kind = 'file'))
               OR EXISTS (SELECT FROM vfs_nodes n WHERE n.parent_id = v_id AND n.name = v_seg AND n.kind = 'file') THEN
                err := CASE WHEN v_i = v_n THEN 'EEXIST' ELSE 'ENOTDIR' END;
                err_detail := format('component %s of %L is a file', v_i, p_segments);
                RETURN NEXT;
                RETURN;
            END IF;
            INSERT INTO vfs_nodes (parent_id, name, kind) VALUES (v_id, v_seg, 'dir')
                ON CONFLICT ON CONSTRAINT vfs_nodes_parent_name_key DO NOTHING
                RETURNING vfs_nodes.id INTO v_next;
            EXIT WHEN v_next IS NOT NULL;
            -- A concurrent transaction created it and has committed: look it up again.
        END LOOP;
        v_id := v_next;
    END LOOP;
    id := v_id;
    RETURN NEXT;
END $$;


-- Moves p_node to p_new_name in directory p_new_parent (NULL: the root), like rename(2) with RENAME_NOREPLACE,
-- and returns one row, with err NULL when it moved.
-- With p_expect_parent, it is a compare-and-swap: it moves only if the node is still in that directory, and
-- returns err ESTALE otherwise (someone else moved it first: a lost Maildir claim). p_expect_parent NULL means no
-- compare-and-swap, so a claim can't expect the root; claim between directories.
-- err ENOENT for a missing node or destination, ENOTDIR when the destination is a file, EEXIST when the new name
-- is taken. EINVAL when a directory would move into its own subtree, for a NULL p_node or a bad name;
-- ENAMETOOLONG.
--
-- Cycles: moving a directory locks every ancestor of the destination FOR KEY SHARE while it checks them. A
-- concurrent move of one of those ancestors needs FOR UPDATE on it, so the two moves are ordered (or, if they
-- lock in opposite orders, one gets a deadlock error), and the later one sees the earlier one's result. Under
-- REPEATABLE READ, locking an ancestor that moved after the snapshot raises a serialization failure instead.
-- The walk locks only directories, so it never waits on a file's inode lock (appends hold it FOR UPDATE).
CREATE FUNCTION vfs_rename(p_node uuid, p_new_parent uuid, p_new_name text, p_expect_parent uuid DEFAULT NULL)
RETURNS TABLE (err text, err_detail text) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_parent uuid;
    v_name text;
    v_kind text;
    v_up uuid;
    v_depth int := 0;
BEGIN
    PERFORM vfs_check_node(p_node);
    PERFORM vfs_check_name(p_new_name);
    -- After waiting for a concurrent mover, this returns the row as that mover left it.
    SELECT n.parent_id, n.name, n.kind INTO v_parent, v_name, v_kind FROM vfs_nodes n WHERE n.id = p_node FOR UPDATE;
    IF NOT FOUND THEN
        err := 'ENOENT';
        err_detail := format('%s does not exist', p_node);
        RETURN NEXT;
        RETURN;
    END IF;
    IF p_expect_parent IS NOT NULL AND v_parent IS DISTINCT FROM p_expect_parent THEN
        err := 'ESTALE';
        err_detail := format('%s is in %s, not %s', p_node, coalesce(v_parent::text, '/'), p_expect_parent);
        RETURN NEXT;
        RETURN;
    END IF;
    SELECT l.err, l.err_detail INTO err, err_detail FROM vfs_lock_dir(p_new_parent) l;
    IF err IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;
    IF v_kind = 'dir' THEN
        v_up := p_new_parent;
        WHILE v_up IS NOT NULL LOOP
            IF v_up = p_node THEN
                PERFORM vfs_raise('EINVAL', format('cannot move directory %s into its own subtree', p_node));
            END IF;
            v_depth := v_depth + 1;
            IF v_depth > 10000 THEN
                PERFORM vfs_raise('EINVAL', format('the ancestors of %s form a cycle', p_new_parent));
            END IF;
            SELECT n.parent_id INTO v_up FROM vfs_nodes n WHERE n.id = v_up FOR KEY SHARE;
        END LOOP;
    END IF;
    IF v_parent IS NOT DISTINCT FROM p_new_parent AND v_name = p_new_name THEN
        RETURN NEXT;
        RETURN;
    END IF;
    IF (p_new_parent IS NULL AND EXISTS (SELECT FROM vfs_nodes n WHERE n.parent_id IS NULL AND n.name = p_new_name))
       OR EXISTS (SELECT FROM vfs_nodes n WHERE n.parent_id = p_new_parent AND n.name = p_new_name) THEN
        err := 'EEXIST';
        err_detail := format('%L already exists in %s', p_new_name, coalesce(p_new_parent::text, '/'));
        RETURN NEXT;
        RETURN;
    END IF;
    -- A create that commits between the check and here surfaces as unique_violation on
    -- vfs_nodes_parent_name_key, which minifs.db.errors maps to EEXIST. (Catching it here would cost a
    -- subtransaction on every rename.)
    UPDATE vfs_nodes n SET parent_id = p_new_parent, name = p_new_name, ctime = now() WHERE n.id = p_node;
    RETURN NEXT;
END $$;


-- Removes p_node, like unlink(2) for a file and rmdir(2) for a directory, and returns one row, with err NULL
-- when it was removed. err ENOENT if it doesn't exist, ENOTEMPTY for a directory with entries. EINVAL for a NULL
-- p_node.
CREATE FUNCTION vfs_unlink(p_node uuid) RETURNS TABLE (err text, err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE v_kind text;
BEGIN
    PERFORM vfs_check_node(p_node);
    SELECT n.kind INTO v_kind FROM vfs_nodes n WHERE n.id = p_node FOR UPDATE;
    IF NOT FOUND THEN
        err := 'ENOENT';
        err_detail := format('%s does not exist', p_node);
        RETURN NEXT;
        RETURN;
    END IF;
    -- A concurrent create in this directory holds FOR KEY SHARE on it, so the FOR UPDATE above waited for it
    -- and this check sees its entry.
    IF v_kind = 'dir' AND EXISTS (SELECT FROM vfs_nodes n WHERE n.parent_id = p_node) THEN
        err := 'ENOTEMPTY';
        err_detail := format('directory %s is not empty', p_node);
        RETURN NEXT;
        RETURN;
    END IF;
    DELETE FROM vfs_nodes n WHERE n.id = p_node;
    RETURN NEXT;
END $$;


-- Appends p_data to file p_node, like write(2) with O_APPEND, and returns the offset it landed at. The inode
-- lock is held until commit, so concurrent appenders take turns: offsets are gap-free and offset order is
-- commit order. Keeps the generation. An empty p_data changes nothing. err ENOENT, EISDIR; EINVAL for a NULL
-- p_node or NULL data. mtime is the later of its old value and the clock, so it never goes back when a
-- transaction that started earlier commits later (now() is the transaction's start time).
--
-- When the file ends mid-line, its last extent is deleted and cut again together with p_data, so extents keep
-- starting at line starts (migration 007).
CREATE FUNCTION vfs_append(p_node uuid, p_data bytea) RETURNS TABLE (byte_offset bigint, err text, err_detail text)
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
    SELECT f.size, f.generation, f.err, f.err_detail INTO v_size, v_gen, err, err_detail FROM vfs_lock_file(p_node) f;
    IF err IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;
    byte_offset := v_size;
    -- A zero-length extent would take the primary key of the next append's extent.
    IF length(p_data) = 0 THEN
        RETURN NEXT;
        RETURN;
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
          FROM vfs_cut(v_last || p_data, 65536, true) c
        RETURNING vfs_extents.line_start + cardinality(vfs_extents.line_offsets) AS lines_after
    )
    SELECT max(ins.lines_after) INTO v_lines FROM ins;
    UPDATE vfs_nodes n SET size = v_size + length(p_data), lines = v_lines, mtime = greatest(n.mtime, clock_timestamp())
     WHERE n.id = p_node;
    RETURN NEXT;
END $$;


-- Replaces the content of file p_node with p_data, like open(O_TRUNC) then write(2): a new generation, the old
-- extents deleted, the new content cut by vfs_cut. Returns the new generation and size. mtime as in vfs_append.
--
-- With p_expect_gen, p_expect_size and p_expect_name it is a compare-and-swap for the Write tool: it writes only
-- if the file still has that generation, size, name and parent (p_expect_parent NULL meaning the root), and
-- returns err ESTALE otherwise. The three are all or none, and p_expect_parent needs them: EINVAL otherwise.
-- err ENOENT, EISDIR; EINVAL for a NULL p_node or NULL data.
CREATE FUNCTION vfs_write(p_node uuid, p_data bytea,
                          p_expect_gen int DEFAULT NULL, p_expect_size bigint DEFAULT NULL,
                          p_expect_parent uuid DEFAULT NULL, p_expect_name text DEFAULT NULL)
RETURNS TABLE (generation int, size bigint, err text, err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
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
    SELECT f.size, f.generation, f.err, f.err_detail INTO v_size, v_gen, err, err_detail FROM vfs_lock_file(p_node) f;
    IF err IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;
    IF p_expect_gen IS NOT NULL THEN
        SELECT n.parent_id, n.name INTO v_parent, v_name FROM vfs_nodes n WHERE n.id = p_node;
        IF (v_gen, v_size, v_name) IS DISTINCT FROM (p_expect_gen, p_expect_size, p_expect_name)
           OR v_parent IS DISTINCT FROM p_expect_parent THEN
            err := 'ESTALE';
            err_detail := format('%s is now generation %s, %s bytes, %L in %s', p_node, v_gen, v_size, v_name,
                                 coalesce(v_parent::text, '/'));
            RETURN NEXT;
            RETURN;
        END IF;
    END IF;
    DELETE FROM vfs_extents e WHERE e.node_id = p_node;
    WITH ins AS (
        INSERT INTO vfs_extents (node_id, generation, byte_offset, line_start, data, line_offsets)
        SELECT p_node, v_gen + 1, c.rel_offset, c.rel_line, c.data, c.line_offsets FROM vfs_cut(p_data, 65536, true) c
        RETURNING vfs_extents.line_start + cardinality(vfs_extents.line_offsets) AS lines_after
    )
    SELECT coalesce(max(ins.lines_after), 0) INTO v_lines FROM ins;
    UPDATE vfs_nodes n SET generation = v_gen + 1, size = length(p_data), lines = v_lines,
                           mtime = greatest(n.mtime, clock_timestamp())
     WHERE n.id = p_node;
    generation := v_gen + 1;
    size := length(p_data);
    RETURN NEXT;
END $$;


-- Merges the extents of file p_node into pieces of about p_target bytes (64 KB by default) under the inode lock,
-- and returns how many extents it removed (migration 007). err ENOENT, EISDIR; EINVAL for a NULL p_node or a
-- p_target outside [1, 64 MB]. Extents that don't tile the file (only raw SQL can make them) raise data_corrupted.
CREATE FUNCTION vfs_compact(p_node uuid, p_target int DEFAULT 65536)
RETURNS TABLE (removed int, err text, err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_gen int;
    v_pos bigint := 0;                              -- every extent before this offset is done
    v_len int;
    v_n int;
    v_end bigint;
    v_sum bigint;
BEGIN
    IF p_target IS NULL OR p_target < 1 OR p_target > 64 * 1024 * 1024 THEN
        PERFORM vfs_raise('EINVAL', format('invalid target size %s', p_target));
    END IF;
    SELECT f.generation, f.err, f.err_detail INTO v_gen, err, err_detail FROM vfs_lock_file(p_node) f;
    IF err IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;
    removed := 0;
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
        removed := removed + v_n;
        v_pos := v_end;
    END LOOP;
    RETURN NEXT;
END $$;


-- Reads up to p_limit bytes of file p_node from byte p_offset, like pread(2) (migration 007); a negative
-- p_offset counts from the end (-N: the last N bytes, clamped to the start). Returns one row per extent read, in
-- offset order, each with the file's size and generation, the offset of its bytes and the bytes, trimmed to the
-- range. When no bytes are in range (past EOF, an empty file, p_limit 0) it returns one row with NULL data, and
-- byte_offset the resolved start offset (clamped to [0, size]). One snapshot, no locks.
-- err ENOENT, EISDIR; EINVAL for a NULL p_node, a NULL offset or a NULL or negative limit.
CREATE FUNCTION vfs_pread(p_node uuid, p_offset bigint, p_limit int)
RETURNS TABLE (size bigint, generation int, byte_offset bigint, data bytea, err text, err_detail text)
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
    SELECT f.size, f.generation, f.err, f.err_detail INTO v_size, v_gen, err, err_detail FROM vfs_file(p_node) f;
    IF err IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;
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
                     FOR (least(e.byte_offset + length(e.data), v_off + p_limit) - greatest(e.byte_offset, v_off))::int),
           NULL::text, NULL::text
      FROM vfs_extents e
     WHERE e.node_id = p_node AND e.generation = v_gen
       AND e.byte_offset >= v_start
       AND e.byte_offset < v_off + p_limit
       AND e.byte_offset + length(e.data) > v_off
       AND p_limit > 0
     ORDER BY e.byte_offset;
    IF NOT FOUND THEN
        RETURN QUERY SELECT v_size, v_gen, v_off, NULL::bytea, NULL::text, NULL::text;
    END IF;
END $$;


-- Reads lines of file p_node, like Read (migration 007): the bytes from the start of line p_first_line (1-based)
-- through at most p_max_lines whole lines, each with its \n, capped at p_max_bytes bytes (so the last line may
-- come back cut). Returns one row: the file's size, generation and line count, p_first_line, the byte offset
-- where that line starts and the bytes. A p_first_line past the last line returns no bytes (an empty bytea),
-- byte_offset = size and the line count. One snapshot, no locks.
-- err ENOENT, EISDIR; EINVAL for a NULL argument, a p_first_line below 1 or a negative p_max_lines or p_max_bytes.
CREATE FUNCTION vfs_pread_lines(p_node uuid, p_first_line bigint, p_max_lines int, p_max_bytes int)
RETURNS TABLE (size bigint, generation int, lines bigint, first_line bigint, byte_offset bigint, data bytea,
               err text, err_detail text)
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
    SELECT f.size, f.generation, f.lines, f.err, f.err_detail INTO v_size, v_gen, v_lines, err, err_detail
      FROM vfs_file(p_node) f;
    IF err IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;
    IF p_first_line > v_lines THEN
        RETURN QUERY SELECT v_size, v_gen, v_lines, p_first_line, v_size, ''::bytea, NULL::text, NULL::text;
        RETURN;
    END IF;
    v_first := p_first_line;
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
    RETURN QUERY SELECT v_size, v_gen, v_lines, p_first_line, v_start, coalesce(v_data, ''::bytea), NULL::text, NULL::text;
END $$;


-- The files below directory p_root (NULL: the root) whose relative path matches p_regex, newest first (ties by
-- path), at most p_limit of them (NULL: all). err ENOENT if p_root doesn't exist, ENOTDIR if it is a file. The
-- walk and the hints are as in migration 006; a prefix that is missing, is a file, or goes through a directory
-- the walk would skip gives no rows, not an error.
CREATE FUNCTION vfs_glob(
    p_root uuid,
    p_regex text,
    p_name_like text DEFAULT NULL,
    p_max_depth int DEFAULT NULL,
    p_excludes text[] DEFAULT '{}',
    p_hidden boolean DEFAULT false,
    p_limit int DEFAULT NULL,
    p_prefix text[] DEFAULT '{}',
    p_prune_dirs text[] DEFAULT '{}'
) RETURNS TABLE (id uuid, path text, size bigint, mtime timestamptz, err text, err_detail text)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_kind text;
    v_start uuid := p_root;
    v_path text COLLATE "C";              -- the start's path relative to p_root; NULL for p_root itself
    v_seg text;
    v_prune text[] := array_append(coalesce(p_prune_dirs, '{}'), '.git');
    v_excludes text[] := coalesce(p_excludes, '{}');
    v_hidden boolean := coalesce(p_hidden, false);
BEGIN
    IF p_root IS NOT NULL THEN
        SELECT n.kind INTO v_kind FROM vfs_nodes n WHERE n.id = p_root;
        IF NOT FOUND THEN
            err := 'ENOENT';
            err_detail := format('directory %s does not exist', p_root);
            RETURN NEXT;
            RETURN;
        ELSIF v_kind <> 'dir' THEN
            err := 'ENOTDIR';
            err_detail := format('%s is a file', p_root);
            RETURN NEXT;
            RETURN;
        END IF;
    END IF;

    -- Walk down the prefix, applying the same rules as the walk below would to each directory.
    FOREACH v_seg IN ARRAY coalesce(p_prefix, '{}') LOOP
        v_path := concat_ws('/', v_path, v_seg);
        IF v_seg = ANY (v_prune) OR (NOT v_hidden AND v_seg LIKE '.%') OR (v_path || '/') ~ ANY (v_excludes) THEN
            RETURN;
        END IF;
        IF v_start IS NULL THEN
            SELECT n.id INTO v_start FROM vfs_nodes n WHERE n.parent_id IS NULL AND n.name = v_seg AND n.kind = 'dir';
        ELSE
            SELECT n.id INTO v_start FROM vfs_nodes n WHERE n.parent_id = v_start AND n.name = v_seg AND n.kind = 'dir';
        END IF;
        IF NOT FOUND THEN
            RETURN;
        END IF;
    END LOOP;

    -- Each term keeps a row only if the walk visits it: a directory it may descend into, or a file whose name
    -- passes p_name_like. The start's entries come from one of the two seed terms: the root's
    -- (parent_id IS NULL) or a directory's (parent_id = v_start). Two terms, not IS NOT DISTINCT FROM, so each
    -- uses the (parent_id, name) index; the v_start IS NULL guard is a one-time filter.
    RETURN QUERY
    WITH RECURSIVE t (id, path, kind, size, mtime, depth) AS (
        SELECT n.id, s.path, n.kind, n.size, n.mtime, 1
          FROM vfs_nodes n CROSS JOIN LATERAL (SELECT concat_ws('/', v_path, n.name) COLLATE "C" AS path) s
         WHERE v_start IS NULL AND n.parent_id IS NULL
           AND CASE n.kind
               WHEN 'file' THEN p_name_like IS NULL OR n.name LIKE p_name_like
               ELSE NOT (n.name = ANY (v_prune) OR (NOT v_hidden AND n.name LIKE '.%') OR (s.path || '/') ~ ANY (v_excludes))
               END
        UNION ALL
        SELECT n.id, s.path, n.kind, n.size, n.mtime, 1
          FROM vfs_nodes n CROSS JOIN LATERAL (SELECT concat_ws('/', v_path, n.name) COLLATE "C" AS path) s
         WHERE n.parent_id = v_start
           AND CASE n.kind
               WHEN 'file' THEN p_name_like IS NULL OR n.name LIKE p_name_like
               ELSE NOT (n.name = ANY (v_prune) OR (NOT v_hidden AND n.name LIKE '.%') OR (s.path || '/') ~ ANY (v_excludes))
               END
        UNION ALL
        SELECT c.id, s.path, c.kind, c.size, c.mtime, t.depth + 1
          FROM t JOIN vfs_nodes c ON c.parent_id = t.id
               CROSS JOIN LATERAL (SELECT t.path || '/' || c.name AS path) s
         WHERE t.kind = 'dir'
           AND (p_max_depth IS NULL OR t.depth < p_max_depth)
           AND CASE c.kind
               WHEN 'file' THEN p_name_like IS NULL OR c.name LIKE p_name_like
               ELSE NOT (c.name = ANY (v_prune) OR (NOT v_hidden AND c.name LIKE '.%') OR (s.path || '/') ~ ANY (v_excludes))
               END
    )
    SELECT t.id, t.path, t.size, t.mtime, NULL::text, NULL::text
      FROM t
     WHERE t.kind = 'file'
       AND t.path ~ p_regex
       AND NOT (t.path ~ ANY (v_excludes))
     ORDER BY t.mtime DESC, t.path
     LIMIT p_limit;
END $$;
