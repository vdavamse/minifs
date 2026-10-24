-- The tree: vfs_nodes (inode + directory entry) and the tree operations.
--
-- Errors: every function raises SQLSTATE 'MFnnn', where nnn is the Linux errno number, with the errno name as
-- the message (ENOENT, EEXIST, ENOTDIR, EISDIR, EINVAL, ENAMETOOLONG, ENOTEMPTY) and the context in DETAIL.
-- The constraints below are the backstop for raw SQL; minifs.db.errors maps their violations to errnos too.
--
-- Every function pins search_path to the one this migration ran with, so it finds vfs_nodes whatever the
-- caller's search_path is.
--
-- Changing a function's parameters (for example #3 adding data to vfs_create) needs DROP FUNCTION of the old
-- signature first: a second overload with a defaulted parameter makes every call to the old one ambiguous.
--
-- The tree root "/" is virtual: root entries have parent_id NULL, and NULL stands for "/" wherever a function
-- takes or returns a directory id.

CREATE TABLE vfs_nodes (                         -- inode + directory entry (no hard links)
    id          uuid PRIMARY KEY DEFAULT uuidv7(),
    parent_id   uuid,                            -- NULL for root entries
    name        text COLLATE "C" NOT NULL        -- byte order: listings don't depend on the database locale
        CONSTRAINT vfs_nodes_name_check
        CHECK (name <> '' AND name NOT IN ('.', '..') AND strpos(name, '/') = 0 AND octet_length(name) <= 255),
    kind        text NOT NULL CHECK (kind IN ('file', 'dir')),   -- never changes after create (trigger below)
    size        bigint NOT NULL DEFAULT 0,       -- exact byte count; 0 for directories
    generation  int    NOT NULL DEFAULT 0,       -- +1 on overwrite; appends keep it; unused for directories
    ctime       timestamptz NOT NULL DEFAULT now(),   -- status change: creation, rename
    mtime       timestamptz NOT NULL DEFAULT now(),   -- content change; directories: creation only
    parent_kind text NOT NULL DEFAULT 'dir' CHECK (parent_kind = 'dir'),
    CONSTRAINT vfs_nodes_parent_name_key UNIQUE NULLS NOT DISTINCT (parent_id, name),
    CONSTRAINT vfs_nodes_id_kind_key UNIQUE (id, kind),          -- target of the kind-checking foreign keys
    CONSTRAINT vfs_nodes_parent_fkey FOREIGN KEY (parent_id, parent_kind)   -- parent must be a dir: ENOTDIR
        REFERENCES vfs_nodes (id, kind) ON DELETE RESTRICT                   -- non-empty rmdir: ENOTEMPTY
) WITH (fillfactor = 80);                        -- room for HOT updates of size/mtime on append


-- Raises errno p_errno ('ENOENT', ...) as SQLSTATE MFnnn. The one place that knows the codes.
CREATE FUNCTION vfs_raise(p_errno text, p_detail text) RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_code text := CASE p_errno
        WHEN 'ENOENT' THEN 'MF002'
        WHEN 'EEXIST' THEN 'MF017'
        WHEN 'ENOTDIR' THEN 'MF020'
        WHEN 'EISDIR' THEN 'MF021'
        WHEN 'EINVAL' THEN 'MF022'
        WHEN 'ENAMETOOLONG' THEN 'MF036'
        WHEN 'ENOTEMPTY' THEN 'MF039'
        WHEN 'ESTALE' THEN 'MF116'
    END;
BEGIN
    IF v_code IS NULL THEN
        RAISE EXCEPTION 'vfs_raise: unknown errno %', p_errno;
    END IF;
    RAISE EXCEPTION USING MESSAGE = p_errno, DETAIL = p_detail, ERRCODE = v_code;
END $$;


CREATE FUNCTION vfs_kind_immutable() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM vfs_raise('EINVAL', format('kind of %s cannot change (%s to %s)', OLD.id, OLD.kind, NEW.kind));
    RETURN NULL;
END $$;

CREATE TRIGGER vfs_nodes_kind_immutable BEFORE UPDATE OF kind ON vfs_nodes
    FOR EACH ROW WHEN (OLD.kind IS DISTINCT FROM NEW.kind) EXECUTE FUNCTION vfs_kind_immutable();


-- EINVAL unless p_name is a valid single path component; ENAMETOOLONG over 255 bytes, as in Linux.
CREATE FUNCTION vfs_check_name(p_name text) RETURNS void LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
BEGIN
    IF p_name IS NULL OR p_name = '' OR p_name IN ('.', '..') OR strpos(p_name, '/') > 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid name %L', p_name));
    ELSIF octet_length(p_name) > 255 THEN
        PERFORM vfs_raise('ENAMETOOLONG', format('name of %s bytes, the limit is 255', octet_length(p_name)));
    END IF;
END $$;


-- Locks directory p_dir against deletion until commit (FOR KEY SHARE) and returns. NULL is the root.
-- ENOENT if it doesn't exist, ENOTDIR if it is a file.
CREATE FUNCTION vfs_lock_dir(p_dir uuid) RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE v_kind text;
BEGIN
    IF p_dir IS NULL THEN
        RETURN;
    END IF;
    SELECT kind INTO v_kind FROM vfs_nodes WHERE id = p_dir FOR KEY SHARE;
    IF NOT FOUND THEN
        PERFORM vfs_raise('ENOENT', format('directory %s does not exist', p_dir));
    ELSIF v_kind <> 'dir' THEN
        PERFORM vfs_raise('ENOTDIR', format('%s is a file', p_dir));
    END IF;
END $$;


-- Walks p_segments from the root in one round trip. Returns one row: (NULL, 'dir') for the root (empty array),
-- else the node's id and kind. ENOENT if a component is missing, ENOTDIR if a non-final component is a file.
CREATE FUNCTION vfs_resolve(p_segments text[]) RETURNS TABLE (id uuid, kind text)
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
            PERFORM vfs_raise('ENOTDIR', format('component %s of %L is a file', v_i - 1, p_segments));
        END IF;
        IF v_id IS NULL THEN
            SELECT n.id, n.kind INTO v_id, v_kind FROM vfs_nodes n WHERE n.parent_id IS NULL AND n.name = v_seg;
        ELSE
            SELECT n.id, n.kind INTO v_id, v_kind FROM vfs_nodes n WHERE n.parent_id = v_id AND n.name = v_seg;
        END IF;
        IF NOT FOUND THEN
            PERFORM vfs_raise('ENOENT', format('component %s of %L does not exist', v_i, p_segments));
        END IF;
    END LOOP;
    RETURN QUERY SELECT v_id, v_kind;
END $$;


-- The entries of directory p_dir (NULL: the root entries), in byte order of name.
CREATE FUNCTION vfs_readdir(p_dir uuid)
RETURNS TABLE (id uuid, name text, kind text, size bigint, mtime timestamptz)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE v_kind text;
BEGIN
    IF p_dir IS NULL THEN
        RETURN QUERY SELECT n.id, n.name::text, n.kind, n.size, n.mtime
                       FROM vfs_nodes n WHERE n.parent_id IS NULL ORDER BY n.name;
        RETURN;
    END IF;
    SELECT n.kind INTO v_kind FROM vfs_nodes n WHERE n.id = p_dir;
    IF NOT FOUND THEN
        PERFORM vfs_raise('ENOENT', format('directory %s does not exist', p_dir));
    ELSIF v_kind <> 'dir' THEN
        PERFORM vfs_raise('ENOTDIR', format('%s is a file', p_dir));
    END IF;
    RETURN QUERY SELECT n.id, n.name::text, n.kind, n.size, n.mtime
                   FROM vfs_nodes n WHERE n.parent_id = p_dir ORDER BY n.name;
END $$;


-- Creates p_name ('file' or 'dir') in directory p_parent (NULL: the root), like open(O_CREAT|O_EXCL) / mkdir.
-- EEXIST if the name is taken, ENOENT / ENOTDIR for a missing parent / a parent that is a file.
CREATE FUNCTION vfs_create(p_parent uuid, p_name text, p_kind text)
RETURNS TABLE (id uuid, generation int, size bigint)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE v_id uuid;
BEGIN
    IF p_kind IS NULL OR p_kind NOT IN ('file', 'dir') THEN
        PERFORM vfs_raise('EINVAL', format('invalid kind %L', p_kind));
    END IF;
    PERFORM vfs_check_name(p_name);
    PERFORM vfs_lock_dir(p_parent);
    INSERT INTO vfs_nodes (parent_id, name, kind) VALUES (p_parent, p_name, p_kind)
        ON CONFLICT ON CONSTRAINT vfs_nodes_parent_name_key DO NOTHING
        RETURNING vfs_nodes.id INTO v_id;
    IF v_id IS NULL THEN
        PERFORM vfs_raise('EEXIST', format('%L already exists in %s', p_name, coalesce(p_parent::text, '/')));
    END IF;
    RETURN QUERY SELECT v_id, 0, 0::bigint;
END $$;


-- Like mkdir -p: creates every missing directory along p_segments and returns the last one's id (NULL for an
-- empty array, the root). Safe against concurrent creators of the same directories. ENOTDIR if a non-final
-- component is a file, EEXIST if the final one is.
CREATE FUNCTION vfs_mkdir_p(p_segments text[]) RETURNS uuid LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_next uuid;
    v_kind text;
    v_seg text;
    v_i int := 0;
    v_n int := coalesce(cardinality(p_segments), 0);
BEGIN
    FOREACH v_seg IN ARRAY coalesce(p_segments, '{}') LOOP
        v_i := v_i + 1;
        PERFORM vfs_check_name(v_seg);
        LOOP
            -- Lock what we find, so it can't be removed before our child insert or our caller's use.
            IF v_id IS NULL THEN
                SELECT n.id, n.kind INTO v_next, v_kind FROM vfs_nodes n
                 WHERE n.parent_id IS NULL AND n.name = v_seg FOR KEY SHARE;
            ELSE
                SELECT n.id, n.kind INTO v_next, v_kind FROM vfs_nodes n
                 WHERE n.parent_id = v_id AND n.name = v_seg FOR KEY SHARE;
            END IF;
            EXIT WHEN FOUND;
            INSERT INTO vfs_nodes (parent_id, name, kind) VALUES (v_id, v_seg, 'dir')
                ON CONFLICT ON CONSTRAINT vfs_nodes_parent_name_key DO NOTHING
                RETURNING vfs_nodes.id INTO v_next;
            v_kind := 'dir';
            EXIT WHEN v_next IS NOT NULL;
            -- A concurrent transaction created it and has committed: look it up again.
        END LOOP;
        IF v_kind <> 'dir' THEN
            PERFORM vfs_raise(CASE WHEN v_i = v_n THEN 'EEXIST' ELSE 'ENOTDIR' END,
                              format('component %s of %L is a file', v_i, p_segments));
        END IF;
        v_id := v_next;
    END LOOP;
    RETURN v_id;
END $$;


-- Moves p_node to p_new_name in directory p_new_parent (NULL: the root), like rename(2) with RENAME_NOREPLACE.
-- With p_expect_parent, it is a compare-and-swap: it moves only if the node is still in that directory, and
-- returns false otherwise (someone else moved it first: a lost Maildir claim). Returns true when moved.
-- p_expect_parent NULL means no compare-and-swap, so a claim can't expect the root; claim between directories.
-- ENOENT for a missing node or destination, ENOTDIR when the destination is a file, EEXIST when the new name
-- is taken, EINVAL when a directory would move into its own subtree.
--
-- Cycles: moving a directory locks every ancestor of the destination FOR KEY SHARE while it checks them. A
-- concurrent move of one of those ancestors needs FOR UPDATE on it, so the two moves are ordered (or, if they
-- lock in opposite orders, one gets a deadlock error), and the later one sees the earlier one's result. Under
-- REPEATABLE READ, locking an ancestor that moved after the snapshot raises a serialization failure instead.
-- Appends take FOR NO KEY UPDATE, which these locks don't block.
CREATE FUNCTION vfs_rename(p_node uuid, p_new_parent uuid, p_new_name text, p_expect_parent uuid DEFAULT NULL)
RETURNS boolean LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_parent uuid;
    v_name text;
    v_kind text;
    v_up uuid;
    v_depth int := 0;
BEGIN
    PERFORM vfs_check_name(p_new_name);
    -- After waiting for a concurrent mover, this returns the row as that mover left it.
    SELECT n.parent_id, n.name, n.kind INTO v_parent, v_name, v_kind FROM vfs_nodes n WHERE n.id = p_node FOR UPDATE;
    IF NOT FOUND THEN
        PERFORM vfs_raise('ENOENT', format('%s does not exist', p_node));
    END IF;
    IF p_expect_parent IS NOT NULL AND v_parent IS DISTINCT FROM p_expect_parent THEN
        RETURN false;
    END IF;
    PERFORM vfs_lock_dir(p_new_parent);
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
        RETURN true;
    END IF;
    IF (p_new_parent IS NULL AND EXISTS (SELECT FROM vfs_nodes n WHERE n.parent_id IS NULL AND n.name = p_new_name))
       OR EXISTS (SELECT FROM vfs_nodes n WHERE n.parent_id = p_new_parent AND n.name = p_new_name) THEN
        PERFORM vfs_raise('EEXIST', format('%L already exists in %s', p_new_name, coalesce(p_new_parent::text, '/')));
    END IF;
    -- A create that commits between the check and here surfaces as unique_violation on
    -- vfs_nodes_parent_name_key, which minifs.db.errors maps to EEXIST. (Catching it here would cost a
    -- subtransaction on every rename.)
    UPDATE vfs_nodes SET parent_id = p_new_parent, name = p_new_name, ctime = now() WHERE id = p_node;
    RETURN true;
END $$;


-- Removes p_node, like unlink(2) for a file and rmdir(2) for a directory. ENOENT if it doesn't exist,
-- ENOTEMPTY for a directory with entries.
CREATE FUNCTION vfs_unlink(p_node uuid) RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE v_kind text;
BEGIN
    SELECT n.kind INTO v_kind FROM vfs_nodes n WHERE n.id = p_node FOR UPDATE;
    IF NOT FOUND THEN
        PERFORM vfs_raise('ENOENT', format('%s does not exist', p_node));
    END IF;
    -- A concurrent create in this directory holds FOR KEY SHARE on it, so the FOR UPDATE above waited for it
    -- and this check sees its entry.
    IF v_kind = 'dir' AND EXISTS (SELECT FROM vfs_nodes n WHERE n.parent_id = p_node) THEN
        PERFORM vfs_raise('ENOTEMPTY', format('directory %s is not empty', p_node));
    END IF;
    DELETE FROM vfs_nodes WHERE id = p_node;
END $$;
