-- The lookup chain (#46): vfs_lookup also returns the id of every component it walked, so a client's
-- directory-entry cache learns the whole path from one lookup, as the kernel adds a dentry for each component of
-- a path walk. On ENOENT or ENOTDIR the chain is the prefix that resolved.
--
-- The return type changes, so DROP FUNCTION first. Every caller names the columns it reads, so the new column
-- breaks none of them.

DROP FUNCTION vfs_lookup(text[]);


-- Walks p_segments from the root in one round trip. Returns one row: (NULL, 'dir') for the root (empty array),
-- else the node's id and kind; or err ENOENT if a component is missing, ENOTDIR if a non-final component is a
-- file, with id and kind NULL. ROWS 1, so a join on its id plans as an index lookup (the default estimate of 1000
-- rows makes the planner hash all of vfs_nodes instead).
--
-- chain holds the ids of the components found, in order: all of them on success (the last is id), the prefix
-- before the missing one on ENOENT, and up to the file on ENOTDIR (its last element is that file). Every other
-- element is a directory. It is the one column an outcome sets besides err and err_detail: what was found is
-- true whether or not the whole path resolved. It is appended to as the walk goes, never subscripted.
CREATE FUNCTION vfs_lookup(p_segments text[])
RETURNS TABLE (id uuid, kind text, chain uuid[], err text, err_detail text)
LANGUAGE plpgsql STABLE ROWS 1 SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_kind text := 'dir';
    v_seg text;
    v_i int := 0;
BEGIN
    chain := '{}';
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
        chain := chain || v_id;
    END LOOP;
    id := v_id;
    kind := v_kind;
    RETURN NEXT;
END $$;
