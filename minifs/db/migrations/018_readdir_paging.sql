-- Paged directory listings (#86): vfs_readdir takes the name to continue after and a limit, so a client can list a
-- huge directory a page at a time without fetching it whole. Paging is by key, not by OFFSET: a page costs its own
-- rows whatever came before it, and pages taken in separate statements neither skip nor repeat an entry when the
-- directory changes in between. The argument list changes, so DROP FUNCTION first; a call with the directory alone
-- still lists everything, as before.

DROP FUNCTION vfs_readdir(uuid);


-- The entries of directory p_dir (NULL: the root entries), in byte order of name: those whose name sorts after
-- p_after (NULL: from the first), at most p_limit of them (NULL: all). No rows for an empty page. Each page is one
-- range scan of the (parent_id, name) index. err ENOENT if p_dir doesn't exist, ENOTDIR if it is a file; EINVAL
-- for a negative p_limit.
CREATE FUNCTION vfs_readdir(p_dir uuid, p_after text DEFAULT NULL, p_limit int DEFAULT NULL)
RETURNS TABLE (id uuid, name text, kind text, size bigint, mtime timestamptz, err text, err_detail text)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_kind text;
    -- A name is never empty, so '' sorts before every entry: one comparison serves both cases, and stays an index
    -- bound in a generic plan (an "p_after IS NULL OR ..." wouldn't).
    v_after text := coalesce(p_after, '');
BEGIN
    IF p_limit < 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid limit %s', p_limit));
    END IF;
    -- Two branches, not IS NOT DISTINCT FROM, so each uses the (parent_id, name) index (as vfs_glob's seeds).
    IF p_dir IS NULL THEN
        RETURN QUERY SELECT n.id, n.name::text, n.kind, n.size, n.mtime, NULL::text, NULL::text
                       FROM vfs_nodes n WHERE n.parent_id IS NULL AND n.name > v_after COLLATE "C"
                      ORDER BY n.name LIMIT p_limit;
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
                   FROM vfs_nodes n WHERE n.parent_id = p_dir AND n.name > v_after COLLATE "C"
                  ORDER BY n.name LIMIT p_limit;
END $$;
