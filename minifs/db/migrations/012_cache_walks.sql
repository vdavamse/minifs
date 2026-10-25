-- More walks for the directory-entry cache (#55):
--
-- - vfs_lookup takes the ids of a cached prefix of the path, checks every link of it in one statement and walks
--   only the rest, so a partly cached path no longer re-walks from the root one statement per component. A
--   prefix that no longer holds is ignored: the walk starts from the root, as before.
-- - vfs_mkdir_p returns the chain of directories it found or made, as vfs_lookup does, so mkdir_p and a create
--   with parents teach the cache every directory of the path.
--
-- Both return types or argument lists change, so DROP FUNCTION first. Every caller names the columns it reads,
-- and p_cached defaults to NULL, so the existing calls keep working.

DROP FUNCTION vfs_lookup(text[]);
DROP FUNCTION vfs_mkdir_p(text[]);


-- Walks p_segments from the root in one round trip. Returns one row: (NULL, 'dir') for the root (empty array),
-- else the node's id and kind; or err ENOENT if a component is missing, ENOTDIR if a non-final component is a
-- file, with id and kind NULL. ROWS 1, so a join on its id plans as an index lookup (the default estimate of 1000
-- rows makes the planner hash all of vfs_nodes instead).
--
-- chain holds the ids of the components found, in order: all of them on success (the last is id), the prefix
-- before the missing one on ENOENT, and up to the file on ENOTDIR (its last element is that file). Every other
-- element is a directory. It is the one column an outcome sets besides err and err_detail: what was found is
-- true whether or not the whole path resolved.
--
-- p_cached is the ids of the first components of the path, as a client's cache has them, shorter than the path.
-- If every one is still the directory of that name in the one before it (the first in the root), checked in one
-- statement, the walk starts below the last one, and chain starts with them; otherwise it starts from the root.
-- Either way the result is the one a walk from the root gives.
CREATE FUNCTION vfs_lookup(p_segments text[], p_cached uuid[] DEFAULT NULL)
RETURNS TABLE (id uuid, kind text, chain uuid[], err text, err_detail text)
LANGUAGE plpgsql STABLE ROWS 1 SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_kind text := 'dir';
    v_n int := coalesce(cardinality(p_segments), 0);
    v_k int := coalesce(cardinality(p_cached), 0);
    v_i int := 0;
BEGIN
    chain := '{}';
    -- p_cached[0] is NULL: the first component's parent is the root. A NULL id matches no row.
    IF v_k > 0 AND v_k < v_n AND (SELECT count(*)
                                    FROM unnest(p_cached) WITH ORDINALITY AS c(id, i)
                                    JOIN vfs_nodes n ON n.id = c.id
                                   WHERE n.name = p_segments[c.i] AND n.kind = 'dir'
                                     AND n.parent_id IS NOT DISTINCT FROM p_cached[c.i - 1]) = v_k THEN
        v_i := v_k;
        v_id := p_cached[v_k];
        chain := p_cached;
    END IF;
    WHILE v_i < v_n LOOP
        v_i := v_i + 1;
        IF v_kind <> 'dir' THEN
            err := 'ENOTDIR';
            err_detail := format('component %s of %L is a file', v_i - 1, p_segments);
            RETURN NEXT;
            RETURN;
        END IF;
        IF v_id IS NULL THEN
            SELECT n.id, n.kind INTO v_id, v_kind FROM vfs_nodes n
             WHERE n.parent_id IS NULL AND n.name = p_segments[v_i];
        ELSE
            SELECT n.id, n.kind INTO v_id, v_kind FROM vfs_nodes n
             WHERE n.parent_id = v_id AND n.name = p_segments[v_i];
        END IF;
        IF NOT FOUND THEN
            err := 'ENOENT';
            err_detail := format('component %s of %L does not exist', v_i, p_segments);
            RETURN NEXT;
            RETURN;
        END IF;
        chain := chain || v_id;
    END LOOP;
    id := v_id;
    kind := v_kind;
    RETURN NEXT;
END $$;


-- Like mkdir -p: creates every missing directory along p_segments and returns the last one's id (NULL for an
-- empty array, the root). Safe against concurrent creators of the same directories. err ENOTDIR if a non-final
-- component is a file, EEXIST if the final one is; nothing has been created then, since every directory after
-- one this call created is new too. EINVAL / ENAMETOOLONG for a bad component.
--
-- chain holds the ids of the directories found or made, in order: all of them on success (the last is id), the
-- ones before the file on ENOTDIR or EEXIST. Every element is a directory.
CREATE FUNCTION vfs_mkdir_p(p_segments text[]) RETURNS TABLE (id uuid, chain uuid[], err text, err_detail text)
LANGUAGE plpgsql ROWS 1 SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_next uuid;
    v_seg text;
    v_i int := 0;
    v_n int := coalesce(cardinality(p_segments), 0);
BEGIN
    chain := '{}';
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
        chain := chain || v_id;
    END LOOP;
    id := v_id;
    RETURN NEXT;
END $$;
