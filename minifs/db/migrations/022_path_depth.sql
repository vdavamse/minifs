-- Paths and nodes are capped at 1024 levels deep (#89).
--
-- The walks were unbounded: vfs_lookup probed once per segment for any number of segments, and vfs_mkdir_p
-- created any depth in one transaction, so one call cost (and wrote) as many levels as a caller's path asked
-- for. And vfs_rename called any destination deeper than 10,000 levels a cycle: a deep but well-formed
-- destination was reported as EINVAL 'the ancestors of ... form a cycle', and that guess was the only bound
-- on the ancestor walk.
--
-- vfs_max_components() below is the one SQL source of the cap: 1024, the same number as the client's
-- minifs.vfs.core.MAX_COMPONENTS, which refuses an over-long path before any round trip (Linux's PATH_MAX of
-- 4096 bytes allows at most 2048 components). Over the cap is ENAMETOOLONG: vfs_lookup and vfs_mkdir_p refuse
-- p_segments before any probe or insert (the detail counts the segments, it never lists them), and vfs_rename
-- refuses a destination deeper than the cap, a node that would land deeper than the cap, and, for a directory
-- that moves deeper, a subtree that would go over it. Its ancestor walk now tells apart what the guess
-- conflated: a real cycle (vfs_ancestors_cycle, Floyd's tortoise and hare over parent_id, without locks) is
-- still EINVAL, and a chain merely deeper than the cap is ENAMETOOLONG.
--
-- vfs_create, and a mkdir by parent id, don't check the depth: they add one level per call, and a check there
-- would walk the ancestors on every create; the lookups that find a parent by path refuse an over-long path
-- anyway, so only raw SQL can get over the cap.
--
-- Deploying: unlike 016, this migration applies on a tree that already has deeper nodes: a deep node can
-- still be moved up by id (a move that doesn't go deeper never scans the subtree), read by id and removed;
-- only the paths that name it are refused, as the lookups refuse them anyway. Find the nodes deeper than the
-- cap with
--
--   WITH RECURSIVE t(id, depth) AS (
--       SELECT n.id, 1 FROM vfs_nodes n WHERE n.parent_id IS NULL
--       UNION ALL
--       SELECT n.id, t.depth + 1 FROM vfs_nodes n JOIN t ON n.parent_id = t.id)
--   SELECT id, depth FROM t WHERE depth > 1024 ORDER BY depth;
--
-- and move the shallowest ones up by id (SELECT * FROM vfs_rename(id, NULL, name)), which shortens everything
-- below them, until the query finds nothing.


-- The cap: the most components a path may have, and the deepest a node may be. The one SQL source of the
-- number; the client's minifs.vfs.core.MAX_COMPONENTS is the same number.
CREATE FUNCTION vfs_max_components() RETURNS int LANGUAGE sql IMMUTABLE PARALLEL SAFE RETURN 1024;


-- The number of nodes from p_node up to the root, p_node included; 0 for NULL (the root). Walks parent_id
-- without locks and stops at p_limit + 1, so a cycle or an over-deep chain returns p_limit + 1: callers pass
-- the cap and read that as "over the cap, exact depth unknown".
CREATE FUNCTION vfs_depth(p_node uuid, p_limit int) RETURNS int
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid := p_node;
    v_up uuid;
    v_depth int := 0;
BEGIN
    WHILE v_id IS NOT NULL LOOP
        v_depth := v_depth + 1;
        IF v_depth > p_limit THEN
            RETURN p_limit + 1;
        END IF;
        SELECT n.parent_id INTO v_up FROM vfs_nodes n WHERE n.id = v_id;
        v_id := v_up;
    END LOOP;
    RETURN v_depth;
END $$;


-- Whether following parent_id from p_node loops, by Floyd's tortoise and hare: slow moves one step and fast
-- two, and they meet only in a cycle; reaching NULL, or a missing row, ends the walk with false. No arrays,
-- no recursion, no locks: O(depth) probes. Only the rare vfs_rename whose (locking) ancestor walk went over
-- the cap pays for it.
CREATE FUNCTION vfs_ancestors_cycle(p_node uuid) RETURNS boolean
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_slow uuid := p_node;
    v_fast uuid := p_node;
BEGIN
    LOOP
        SELECT n.parent_id INTO v_slow FROM vfs_nodes n WHERE n.id = v_slow;
        IF v_slow IS NULL THEN
            RETURN false;
        END IF;
        SELECT n.parent_id INTO v_fast FROM vfs_nodes n WHERE n.id = v_fast;
        IF v_fast IS NULL THEN
            RETURN false;
        END IF;
        SELECT n.parent_id INTO v_fast FROM vfs_nodes n WHERE n.id = v_fast;
        IF v_fast IS NULL THEN
            RETURN false;
        END IF;
        IF v_slow = v_fast THEN
            RETURN true;
        END IF;
    END LOOP;
END $$;


-- Walks p_segments from the root in one round trip. Returns one row: (NULL, 'dir') for the root (empty array),
-- else the node's id and kind; or err ENOENT if a component is missing, ENOTDIR if a non-final component is a
-- file, with id and kind NULL. Raises ENAMETOOLONG (022) for more than vfs_max_components() segments, before
-- any probe; the detail counts them, it never lists them. ROWS 1, so a join on its id plans as an index lookup
-- (the default estimate of 1000 rows makes the planner hash all of vfs_nodes instead).
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
CREATE OR REPLACE FUNCTION vfs_lookup(p_segments text[], p_cached uuid[] DEFAULT NULL)
RETURNS TABLE (id uuid, kind text, chain uuid[], err text, err_detail text)
LANGUAGE plpgsql STABLE ROWS 1 SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_kind text := 'dir';
    v_n int := coalesce(cardinality(p_segments), 0);
    v_k int := coalesce(cardinality(p_cached), 0);
    v_i int := 0;
BEGIN
    IF v_n > vfs_max_components() THEN
        PERFORM vfs_raise('ENAMETOOLONG', format('%s components, over the %s limit', v_n, vfs_max_components()));
    END IF;
    chain := '{}';
    -- Nested, so a call without p_cached tests a simple expression and runs no query. p_cached[0] is NULL: the
    -- first component's parent is the root. A NULL id matches no row.
    IF v_k > 0 AND v_k < v_n THEN
        IF (SELECT count(*)
              FROM unnest(p_cached) WITH ORDINALITY AS c(id, i)
              JOIN vfs_nodes n ON n.id = c.id
             WHERE n.name = p_segments[c.i] AND n.kind = 'dir'
               AND n.parent_id IS NOT DISTINCT FROM p_cached[c.i - 1]) = v_k THEN
            v_i := v_k;
            v_id := p_cached[v_k];
            chain := p_cached;
        END IF;
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
-- one this call created is new too. EINVAL / ENAMETOOLONG for a bad component; ENAMETOOLONG (022) too for more
-- than vfs_max_components() segments, raised before anything is created.
--
-- chain holds the ids of the directories found or made, in order: all of them on success (the last is id), the
-- ones before the file on ENOTDIR or EEXIST. Every element is a directory.
CREATE OR REPLACE FUNCTION vfs_mkdir_p(p_segments text[]) RETURNS TABLE (id uuid, chain uuid[], err text, err_detail text)
LANGUAGE plpgsql ROWS 1 SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_next uuid;
    v_seg text;
    v_i int := 0;
    v_n int := coalesce(cardinality(p_segments), 0);
BEGIN
    IF v_n > vfs_max_components() THEN
        PERFORM vfs_raise('ENAMETOOLONG', format('%s components, over the %s limit', v_n, vfs_max_components()));
    END IF;
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


-- Moves p_node to p_new_name in directory p_new_parent (NULL: the root), like rename(2) with RENAME_NOREPLACE,
-- and returns one row, with err NULL when it moved.
-- With p_expect_parent, it is a compare-and-swap: it moves only if the node is still in that directory, and
-- returns err ESTALE otherwise (someone else moved it first: a lost Maildir claim). p_expect_parent NULL means no
-- compare-and-swap, so a claim can't expect the root; claim between directories.
-- err ENOENT for a missing node or destination, ENOTDIR when the destination is a file, EEXIST when the new name
-- is taken. EINVAL when a directory would move into its own subtree, for a NULL p_node or a bad name.
-- ENAMETOOLONG for a bad name and, since 022, for the depth cap: a destination deeper than vfs_max_components(),
-- a move that would land p_node over it, or, for a directory that moves deeper, a subtree that would go over it.
--
-- Cycles: a directory's move locks every ancestor of the destination FOR KEY SHARE while it checks them (a file's
-- move walks them without locks, to count the destination's depth (022)). A concurrent move of one of those ancestors needs FOR
-- UPDATE on it, so the two moves are ordered (or, if they lock in opposite orders, one gets a deadlock error),
-- and the later one sees the earlier one's result. Under REPEATABLE READ, locking an ancestor that moved after
-- the snapshot raises a serialization failure instead. The walk locks only directories, so it never waits on a
-- file's inode lock (appends hold it FOR UPDATE). A walk that goes over the cap stops there: a real cycle
-- (vfs_ancestors_cycle) is still EINVAL, and a chain merely deeper than the cap is ENAMETOOLONG, not the false
-- cycle the old 10,000-step guess reported.
--
-- The subtree check of a directory that moves deeper reads the descendants without locking them, so it is best
-- effort: a concurrent mkdir inside the moved subtree can slip one over the cap; so is a file's depth check. The
-- scan reads every directory of the subtree down to the room left, so moving a wide tree deeper costs more. Moving sideways or up never
-- scans the subtree, so an over-deep legacy subtree can always be moved up.
CREATE OR REPLACE FUNCTION vfs_rename(p_node uuid, p_new_parent uuid, p_new_name text, p_expect_parent uuid DEFAULT NULL)
RETURNS TABLE (err text, err_detail text) LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_parent uuid;
    v_name text;
    v_kind text;
    v_up uuid;
    v_depth int := 0;
    v_old int;
    v_room int;
    v_over boolean;
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
    -- Walk the destination's ancestors, for every kind (022), counting the destination's depth (the root is 0).
    -- A directory's move locks each one FOR KEY SHARE, as before, for the cycle check. A file can't close a
    -- cycle, so its walk takes no locks, as before: renames of files (Maildir claims) don't lock every ancestor
    -- up to the root, and their depth check is best effort, like the subtree check below. A walk over the cap
    -- stops: a real cycle is EINVAL as before, a chain merely deeper than the cap is ENAMETOOLONG.
    v_up := p_new_parent;
    WHILE v_up IS NOT NULL LOOP
        IF v_up = p_node THEN
            PERFORM vfs_raise('EINVAL', format('cannot move directory %s into its own subtree', p_node));
        END IF;
        v_depth := v_depth + 1;
        IF v_depth > vfs_max_components() THEN
            IF vfs_ancestors_cycle(p_new_parent) THEN
                PERFORM vfs_raise('EINVAL', format('the ancestors of %s form a cycle', p_new_parent));
            END IF;
            PERFORM vfs_raise('ENAMETOOLONG',
                              format('%s is deeper than %s levels', p_new_parent, vfs_max_components()));
        END IF;
        IF v_kind = 'dir' THEN
            SELECT n.parent_id INTO v_up FROM vfs_nodes n WHERE n.id = v_up FOR KEY SHARE;
        ELSE
            SELECT n.parent_id INTO v_up FROM vfs_nodes n WHERE n.id = v_up;
        END IF;
    END LOOP;
    IF v_depth + 1 > vfs_max_components() THEN
        PERFORM vfs_raise('ENAMETOOLONG', format('%s would be %s levels deep, over the %s limit',
                                                 p_node, v_depth + 1, vfs_max_components()));
    END IF;
    -- A directory that moves deeper takes its subtree with it. The old parent's depth comes from vfs_depth,
    -- without locks; past the cap it is cap + 1, more than v_depth (at most cap - 1 here), so a move out of an
    -- over-deep region is a move up and is never scanned, even when its subtree stays over the cap. v_room is
    -- the deepest relative level allowed below p_node, p_node itself at level 0: the scan (directories only, no
    -- locks on the descendants; it reads every directory of the subtree down to v_room) looks for any child,
    -- file or directory, of a directory at level v_room.
    IF v_kind = 'dir' THEN
        v_old := vfs_depth(v_parent, vfs_max_components());
        IF v_depth > v_old THEN
            v_room := vfs_max_components() - v_depth - 1;
            WITH RECURSIVE down(id, level) AS (
                SELECT p_node, 0
                UNION ALL
                SELECT n.id, d.level + 1 FROM vfs_nodes n JOIN down d ON n.parent_id = d.id
                 WHERE n.kind = 'dir' AND d.level < v_room)
            SELECT EXISTS (SELECT FROM down d JOIN vfs_nodes n ON n.parent_id = d.id WHERE d.level = v_room)
              INTO v_over;
            IF v_over THEN
                PERFORM vfs_raise('ENAMETOOLONG', format('the subtree of %s would go over the %s-level limit',
                                                         p_node, vfs_max_components()));
            END IF;
        END IF;
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
