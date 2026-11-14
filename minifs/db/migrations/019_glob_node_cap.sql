-- A cap on the nodes vfs_glob walks (#85). The walk had no bound: every file under the root is collected before
-- ORDER BY mtime ... LIMIT applies, so a glob of a tree of a million nodes ran as long as Postgres needed, and
-- returned every match to a caller that gave no limit. The client now runs it under a statement timeout as well
-- (VFSClient.glob); the cap refuses a walk that is too wide before it has cost that long, with a result that
-- doesn't depend on the server's load.
--
-- The argument list changes, so DROP FUNCTION first. p_max_nodes defaults to NULL (no cap), so the existing
-- calls keep working.

DROP FUNCTION vfs_glob(uuid, text, text, int, text[], boolean, int, text[], text[]);


-- The files below directory p_root (NULL: the root) whose relative path matches p_regex, newest first (ties by
-- path), at most p_limit of them (NULL: all). err ENOENT if p_root doesn't exist, ENOTDIR if it is a file. The
-- walk and the hints are as in migration 006; a prefix that is missing, is a file, or goes through a directory
-- the walk would skip gives no rows, not an error.
--
-- p_max_nodes (NULL: no cap) bounds the walk: the rows it keeps, which are the directories it descends into and
-- the files whose name passes p_name_like (every file, without that hint), below the prefix. Past the cap the
-- result is one row with err EOVERFLOW and no files: a partial walk can't say which files are the newest. The
-- walk stops one row past the cap. Files that p_name_like filters out are read from the index and not counted,
-- so the cap doesn't bound a walk of a huge tree for a name nothing has; the caller's statement timeout does.
-- EOVERFLOW is an outcome of this function only (the client reports it as a refused search, not a VFSError).
-- EINVAL for a negative p_max_nodes.
CREATE FUNCTION vfs_glob(
    p_root uuid,
    p_regex text,
    p_name_like text DEFAULT NULL,
    p_max_depth int DEFAULT NULL,
    p_excludes text[] DEFAULT '{}',
    p_hidden boolean DEFAULT false,
    p_limit int DEFAULT NULL,
    p_prefix text[] DEFAULT '{}',
    p_prune_dirs text[] DEFAULT '{}',
    p_max_nodes int DEFAULT NULL
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
    IF p_max_nodes < 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid node cap %s', p_max_nodes));
    END IF;
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
    ),
    -- The cap: the walk is read one row past it and no further. A recursive query is evaluated only as far as
    -- its rows are fetched, level by level, so the LIMIT ends the walk there (LIMIT NULL: no cap). MATERIALIZED,
    -- so the count and the result read the same rows and the walk runs once. The materialisation costs every
    -- glob a second pass over the walked rows (about 10% on a 250,000-node tree, measured), and grep's scope
    -- query, which passes no cap, pays it too.
    walked AS MATERIALIZED (SELECT * FROM t LIMIT p_max_nodes::bigint + 1),
    capped AS (SELECT coalesce(count(*) > p_max_nodes, false) AS over FROM walked)
    (SELECT w.id, w.path, w.size, w.mtime, NULL::text, NULL::text
       FROM walked w
      WHERE NOT (SELECT c.over FROM capped c)
        AND w.kind = 'file'
        AND w.path ~ p_regex
        AND NOT (w.path ~ ANY (v_excludes))
      ORDER BY w.mtime DESC, w.path
      LIMIT p_limit)
    UNION ALL
    SELECT NULL::uuid, NULL::text, NULL::bigint, NULL::timestamptz, 'EOVERFLOW',
           format('the walk is over %s files and directories', p_max_nodes)
      FROM capped c
     WHERE c.over;
END $$;
