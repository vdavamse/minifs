-- The glob walker: vfs_glob, shared by the Glob and Grep tools (README: Glob). It reads only vfs_nodes.
--
-- minifs.vfs.glob translates the glob pattern and the excludes to anchored AREs and derives the pruning hints
-- (p_prefix, p_max_depth, p_name_like). The hints only make the walk smaller: the result is the same without
-- them, which the tests check.
--
-- Paths are relative to p_root, with no leading '/', and are COLLATE "C" like vfs_nodes.name, so the regex
-- match and the ORDER BY don't depend on the database locale. The translated patterns use no character
-- classes of ARE's own, only explicit characters and ranges, which mean the same in every locale.


-- The files below directory p_root (NULL: the root) whose relative path matches p_regex, newest first (ties by
-- path), at most p_limit of them (NULL: all). ENOENT if p_root doesn't exist, ENOTDIR if it is a file.
--
-- The walk, as in rg: a directory is not walked if its name is in p_prune_dirs or is '.git' (at any depth), if
-- its relative path followed by '/' matches one of p_excludes (so an exclude can match only directories), or,
-- unless p_hidden, if its name starts with '.'. A file is returned if its path matches p_regex and none of
-- p_excludes; hidden files are returned either way.
--
-- Hints:
-- - p_prefix: the pattern's literal leading directories, relative to p_root. The walk starts in that directory,
--   still producing paths relative to p_root. A prefix that is missing, is a file, or goes through a directory
--   the walk would skip gives no rows, not an error.
-- - p_max_depth: the deepest level to return below the walk's start (1: the start's own entries).
-- - p_name_like: a LIKE pattern every returned file's name matches, checked before the regex.
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
) RETURNS TABLE (id uuid, path text, size bigint, mtime timestamptz)
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
            PERFORM vfs_raise('ENOENT', format('directory %s does not exist', p_root));
        ELSIF v_kind <> 'dir' THEN
            PERFORM vfs_raise('ENOTDIR', format('%s is a file', p_root));
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
    SELECT t.id, t.path, t.size, t.mtime
      FROM t
     WHERE t.kind = 'file'
       AND t.path ~ p_regex
       AND NOT (t.path ~ ANY (v_excludes))
     ORDER BY t.mtime DESC, t.path
     LIMIT p_limit;
END $$;
