-- Write quotas (#83). Nothing bounded what an actor could store: a write took any bytea up to Postgres's 1 GB,
-- appends grew a file without limit, and nothing counted a workspace's bytes or nodes. An agent in a write loop
-- could fill the storage every workspace shares. And vfs_cut kept the length of every line of the whole write in
-- one PL/pgSQL array, about 8 bytes of backend memory per newline of the write, under the inode lock.
--
-- 1. A file is at most vfs_max_file_size() bytes: 64 MiB, the same number as the client's
--    minifs.vfs.core.MAX_FILE_SIZE, which refuses larger content before any round trip. vfs_create and vfs_write
--    refuse larger content, and vfs_append an append that would make the file larger, as EFBIG. That one number
--    bounds every vfs_cut input too (an append re-cuts at most the file's newline-less tail with the new data).
--
-- 2. vfs_cut places each line as soon as it finds its \n, so it holds one extent's line offsets, not the length of
--    every line of the write. Its output is the same, row for row.
--
-- 3. Quotas. A row in vfs_quotas makes a directory a quota root: max_bytes caps the sum of the sizes of the files
--    below it, max_nodes the number of nodes below it (files and directories, not the root itself); NULL is no
--    limit on that one. Every writer charges its change once, at its end, through vfs_charge: it walks from the
--    changed node up to the root (no locks; at most vfs_max_components() steps, the depth cap of 022), locks the
--    quota rows it finds FOR UPDATE in root id order (one order for every charge, so two charges never
--    deadlock; and a charge is the last lock a writer takes, vfs_rename's after its UPDATE), adds the change to
--    each and raises EDQUOT if an increase takes one over its max, which rolls the whole write back. A decrease (a shrinking write, an unlink) is never refused, even over a lowered max.
--    Quotas nest: every quota root above the change is charged. vfs_rename moves the moved subtree's bytes and
--    nodes from the quota roots above the source to those above the destination (a scan of the whole subtree,
--    capped in depth only, while the moved node is locked; only when the two sets of roots differ). vfs_compact changes no size.
--
--    Cost: every write under a quota root holds that root's row until it commits, so writers in one workspace
--    take turns there (writers of one file already take turns on its inode). Put quotas on the workspaces, not
--    on /workspaces itself, which would make every write in the deployment take turns on one row. Once any
--    quota exists, every write walks its ancestors (one probe per level; workspaces are a few levels deep).
--
--    vfs_set_quota(dir, max_bytes, max_nodes) sets a root's limits and recounts its usage under the row lock
--    (both NULL removes the row); writes below that root wait for the recount, which scans its subtree.
--    vfs_set_child_quota(dir, max_bytes, max_nodes) gives every directory created directly in dir that quota at
--    its creation (vfs_create, vfs_mkdir_p): set once on /workspaces, every workspace has a quota from its first
--    mkdir. It locks dir FOR UPDATE only to set that, in a short transaction of its own; the caller
--    (VFSClient.set_child_quota) then gives every existing child directory without a quota one, each in a
--    transaction of its own (vfs_quota_pending_children, vfs_give_child_quota), so no write waits on the recount
--    of another child. Calling it again with other limits changes only the children without a quota (set_quota
--    changes one that has one); a directory moved into dir, rather than created there, gets none until the next
--    call. If it stops halfway, calling it again finishes. Set both limits: without max_nodes an actor can create
--    nodes the byte limit doesn't see. These are operator calls: a deployment that gives agents' code its own
--    role can REVOKE EXECUTE on vfs_set_quota, vfs_set_child_quota and vfs_give_child_quota, and the write
--    privileges on vfs_quotas and vfs_quota_defaults, from it.
--
--    What's counted is the files' logical sizes. Storage per byte is a small factor more: each line costs 4 bytes
--    of line_offsets, and each node and extent a row of about 200 bytes. max_nodes bounds the node rows, and
--    vfs_append keeps the extent rows to about one per 2 KiB of file (it re-cuts a last extent under 4 KiB with
--    the appended data rather than adding a row per append).
--
--    Best effort, by design: the ancestor walk takes no locks (it would put a lock on every directory above every
--    write), so a write that races a move of one of its ancestors to another quota root may be charged to the
--    old roots, while the move's scan, which doesn't see the uncommitted write, leaves it out of what it moves.
--    If the source has no quota root the write is charged to none. vfs_set_quota recounts a root. A row that vfs_set_quota
--    creates may also miss a write that commits while the row isn't committed yet (call it again to recount).
--    vfs_create by parent id is charged like any create; raw SQL is not charged at all.
--
-- New errnos: EFBIG (MF027) and EDQUOT (MF122), in vfs_raise and minifs.db.errors.
--
-- Deploying: two new tables, no rewrite of existing data. No quota exists until an operator sets one, so until
-- then only the file size cap applies. An existing file over 64 MiB stays readable and removable, and can be
-- rewritten smaller; appending to it is EFBIG.


-- Raises errno p_errno ('ENOENT', ...) as SQLSTATE MFnnn. The one place that knows the codes. 003's, with EFBIG
-- and EDQUOT (023).
CREATE OR REPLACE FUNCTION vfs_raise(p_errno text, p_detail text) RETURNS void LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_code text := CASE p_errno
        WHEN 'ENOENT' THEN 'MF002'
        WHEN 'EEXIST' THEN 'MF017'
        WHEN 'ENOTDIR' THEN 'MF020'
        WHEN 'EISDIR' THEN 'MF021'
        WHEN 'EINVAL' THEN 'MF022'
        WHEN 'EFBIG' THEN 'MF027'
        WHEN 'ENAMETOOLONG' THEN 'MF036'
        WHEN 'ENOTEMPTY' THEN 'MF039'
        WHEN 'ESTALE' THEN 'MF116'
        WHEN 'EDQUOT' THEN 'MF122'
    END;
BEGIN
    IF v_code IS NULL THEN
        RAISE EXCEPTION 'vfs_raise: unknown errno %', p_errno;
    END IF;
    RAISE EXCEPTION USING MESSAGE = p_errno, DETAIL = p_detail, ERRCODE = v_code;
END $$;


-- The largest a file may be, in bytes: 64 MiB. The one SQL source of the number; the client's
-- minifs.vfs.core.MAX_FILE_SIZE is the same number.
CREATE FUNCTION vfs_max_file_size() RETURNS bigint LANGUAGE sql IMMUTABLE PARALLEL SAFE RETURN 67108864;


-- EFBIG if a file of p_size bytes would be over vfs_max_file_size(). p_node names the file in the detail (NULL
-- for one being created).
CREATE FUNCTION vfs_check_file_size(p_size bigint, p_node uuid) RETURNS void
LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
BEGIN
    IF p_size > vfs_max_file_size() THEN
        PERFORM vfs_raise('EFBIG', format('%s would be %s bytes, over the %s-byte limit',
                                          coalesce(p_node::text, 'the file'), p_size, vfs_max_file_size()));
    END IF;
END $$;


CREATE TABLE vfs_quotas (
    root        uuid PRIMARY KEY,                       -- a directory (no foreign key: see below)
    max_bytes   bigint CHECK (max_bytes >= 0),          -- NULL: no limit
    max_nodes   bigint CHECK (max_nodes >= 0),          -- NULL: no limit
    used_bytes  bigint NOT NULL DEFAULT 0,              -- the sizes of the files below root
    used_nodes  bigint NOT NULL DEFAULT 0,              -- the nodes below root, root itself not included
    CHECK (max_bytes IS NOT NULL OR max_nodes IS NOT NULL)
);

CREATE TABLE vfs_quota_defaults (
    parent      uuid PRIMARY KEY,                       -- a directory
    max_bytes   bigint CHECK (max_bytes >= 0),
    max_nodes   bigint CHECK (max_nodes >= 0),
    CHECK (max_bytes IS NOT NULL OR max_nodes IS NOT NULL)
);
-- No foreign keys to vfs_nodes: one would make TRUNCATE vfs_nodes (which the watchers handle as a rescan) fail.
-- vfs_unlink removes the rows of the directory it removes; a row raw SQL orphans names an id no node will have
-- again, so it is never matched (delete it with DELETE ... WHERE root NOT IN (SELECT id FROM vfs_nodes)).


-- The quota roots at p_start and above it (p_start included; NULL, the root, has none), bottom up. Walks
-- parent_id without locks. Past vfs_max_components() steps (only a legacy tree, or a cycle, gets there): a cycle
-- is EINVAL; otherwise ENAMETOOLONG, or with p_partial the roots found so far. p_partial is for a charge that
-- only gives usage back (unlink, a rename's source): a root it misses keeps that usage, which overstates it and
-- never lets a write past a limit, so an over-deep legacy subtree can still be removed and moved up (022). A
-- charge that adds usage needs every root. Without any quota at all it reads nothing but vfs_quotas.
CREATE FUNCTION vfs_quota_roots(p_start uuid, p_partial boolean DEFAULT false) RETURNS uuid[]
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid := p_start;
    v_up uuid;
    v_has boolean;
    v_roots uuid[] := '{}';
    v_steps int := 0;
BEGIN
    IF v_id IS NULL OR NOT EXISTS (SELECT FROM vfs_quotas) THEN
        RETURN v_roots;
    END IF;
    WHILE v_id IS NOT NULL LOOP
        v_steps := v_steps + 1;
        IF v_steps > vfs_max_components() THEN
            IF vfs_ancestors_cycle(p_start) THEN
                PERFORM vfs_raise('EINVAL', format('the ancestors of %s form a cycle', p_start));
            END IF;
            EXIT WHEN p_partial;
            PERFORM vfs_raise('ENAMETOOLONG', format('%s is deeper than %s levels', p_start, vfs_max_components()));
        END IF;
        SELECT n.parent_id, EXISTS (SELECT FROM vfs_quotas q WHERE q.root = n.id) INTO v_up, v_has
          FROM vfs_nodes n WHERE n.id = v_id;
        IF v_has THEN
            v_roots := v_roots || v_id;
        END IF;
        v_id := v_up;
    END LOOP;
    RETURN v_roots;
END $$;


-- Moves p_bytes and p_nodes of usage from the quota roots in p_from to those in p_to: a root in both is
-- unchanged, one only in p_to gains them, one only in p_from loses them. The rows are locked FOR UPDATE in root
-- id order. EDQUOT, with nothing changed, if a gain takes a root over its max_bytes or max_nodes; a loss is never
-- refused. Negative amounts reverse the direction.
CREATE FUNCTION vfs_charge_move(p_from uuid[], p_to uuid[], p_bytes bigint, p_nodes bigint) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_q record;
    v_sign int;
    v_bytes bigint;
    v_nodes bigint;
BEGIN
    IF (p_bytes = 0 AND p_nodes = 0) OR cardinality(p_from) + cardinality(p_to) = 0 THEN
        RETURN;
    END IF;
    FOR v_q IN
        SELECT q.root, q.max_bytes, q.max_nodes, q.used_bytes, q.used_nodes FROM vfs_quotas q
         WHERE q.root = ANY (p_from || p_to) AND (q.root = ANY (p_from)) <> (q.root = ANY (p_to))  -- changed ones only
         ORDER BY q.root FOR UPDATE
    LOOP
        v_sign := (v_q.root = ANY (p_to))::int - (v_q.root = ANY (p_from))::int;
        v_bytes := v_sign * p_bytes;
        v_nodes := v_sign * p_nodes;
        IF v_bytes > 0 AND v_q.used_bytes + v_bytes > v_q.max_bytes THEN
            PERFORM vfs_raise('EDQUOT', format('quota of %s: %s bytes used, %s more would pass the %s-byte limit',
                                               v_q.root, v_q.used_bytes, v_bytes, v_q.max_bytes));
        END IF;
        IF v_nodes > 0 AND v_q.used_nodes + v_nodes > v_q.max_nodes THEN
            PERFORM vfs_raise('EDQUOT', format('quota of %s: %s nodes used, %s more would pass the %s-node limit',
                                               v_q.root, v_q.used_nodes, v_nodes, v_q.max_nodes));
        END IF;
        UPDATE vfs_quotas q SET used_bytes = q.used_bytes + v_bytes, used_nodes = q.used_nodes + v_nodes
         WHERE q.root = v_q.root;
    END LOOP;
END $$;


-- Charges p_bytes and p_nodes (either may be negative) to every quota root at p_start and above it. A charge
-- that only gives usage back walks partially (vfs_quota_roots), so a decrease is never refused, even in a legacy
-- subtree deeper than the cap.
CREATE FUNCTION vfs_charge(p_start uuid, p_bytes bigint, p_nodes bigint) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF p_bytes <> 0 OR p_nodes <> 0 THEN
        PERFORM vfs_charge_move('{}', vfs_quota_roots(p_start, p_bytes <= 0 AND p_nodes <= 0), p_bytes, p_nodes);
    END IF;
END $$;


-- The bytes and nodes of the subtree at p_node, p_node included, down to vfs_max_components() levels below it.
CREATE FUNCTION vfs_subtree_usage(p_node uuid, OUT bytes bigint, OUT nodes bigint)
LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    WITH RECURSIVE down(id, level) AS (
        SELECT p_node, 0
        UNION ALL
        SELECT n.id, d.level + 1 FROM vfs_nodes n JOIN down d ON n.parent_id = d.id
         WHERE d.level < vfs_max_components())
    SELECT coalesce(sum(n.size), 0)::bigint, count(*)::bigint FROM down d JOIN vfs_nodes n ON n.id = d.id
$$;


-- Gives the new directory p_dir, created in p_parent, p_parent's child quota if it has one (usage 0).
CREATE FUNCTION vfs_quota_inherit(p_dir uuid, p_parent uuid) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    IF p_parent IS NOT NULL AND EXISTS (SELECT FROM vfs_quota_defaults) THEN
        INSERT INTO vfs_quotas (root, max_bytes, max_nodes)
        SELECT p_dir, d.max_bytes, d.max_nodes FROM vfs_quota_defaults d WHERE d.parent = p_parent;
    END IF;
END $$;


-- Sets the quota of directory p_root: at most p_max_bytes bytes and p_max_nodes nodes below it (NULL: no limit
-- on that one); both NULL removes it. Recounts the usage under the row's lock and returns it (NULL, NULL when
-- removed). err ENOENT, ENOTDIR; EINVAL for a NULL p_root or a negative max. Usage already over a new max is
-- kept: only increases are refused from then on.
CREATE FUNCTION vfs_set_quota(p_root uuid, p_max_bytes bigint, p_max_nodes bigint)
RETURNS TABLE (used_bytes bigint, used_nodes bigint, err text, err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_bytes bigint;
    v_nodes bigint;
BEGIN
    PERFORM vfs_check_node(p_root);
    IF p_max_bytes < 0 OR p_max_nodes < 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid quota (%s bytes, %s nodes)', p_max_bytes, p_max_nodes));
    END IF;
    -- FOR KEY SHARE: the directory can't be removed (the row would go with it) before this commits.
    SELECT l.err, l.err_detail INTO err, err_detail FROM vfs_lock_dir(p_root) l;
    IF err IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;
    IF p_max_bytes IS NULL AND p_max_nodes IS NULL THEN
        DELETE FROM vfs_quotas q WHERE q.root = p_root;
        RETURN NEXT;
        RETURN;
    END IF;
    INSERT INTO vfs_quotas AS q (root, max_bytes, max_nodes) VALUES (p_root, p_max_bytes, p_max_nodes)
        ON CONFLICT (root) DO UPDATE SET max_bytes = EXCLUDED.max_bytes, max_nodes = EXCLUDED.max_nodes;
    -- The row is locked now, so every charge to it waits; this statement's snapshot sees every charge that came
    -- before (READ COMMITTED takes a snapshot per statement, after the lock).
    SELECT u.bytes, u.nodes - 1 INTO v_bytes, v_nodes FROM vfs_subtree_usage(p_root) u;
    UPDATE vfs_quotas q SET used_bytes = v_bytes, used_nodes = v_nodes WHERE q.root = p_root;
    used_bytes := v_bytes;
    used_nodes := v_nodes;
    RETURN NEXT;
END $$;


-- The quota of directory p_root: one row, all NULL if it has none. err ENOENT, ENOTDIR; EINVAL for NULL.
CREATE FUNCTION vfs_quota(p_root uuid)
RETURNS TABLE (max_bytes bigint, max_nodes bigint, used_bytes bigint, used_nodes bigint, err text, err_detail text)
LANGUAGE plpgsql STABLE SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM vfs_check_node(p_root);
    IF NOT EXISTS (SELECT FROM vfs_nodes n WHERE n.id = p_root) THEN
        err := 'ENOENT';
        err_detail := format('directory %s does not exist', p_root);
    ELSIF NOT EXISTS (SELECT FROM vfs_nodes n WHERE n.id = p_root AND n.kind = 'dir') THEN
        err := 'ENOTDIR';
        err_detail := format('%s is a file', p_root);
    ELSE
        SELECT q.max_bytes, q.max_nodes, q.used_bytes, q.used_nodes INTO max_bytes, max_nodes, used_bytes, used_nodes
          FROM vfs_quotas q WHERE q.root = p_root;
    END IF;
    RETURN NEXT;
END $$;


-- Sets the quota every directory created directly in p_parent gets at its creation (vfs_quota_inherit); both NULL
-- stops it (the quotas already given stay). err ENOENT, ENOTDIR; EINVAL as vfs_set_quota. Only that: the caller
-- commits at once, then gives each existing child its quota in a transaction of its own
-- (vfs_quota_pending_children, vfs_give_child_quota), so no write waits on a recount of other children.
--
-- FOR UPDATE, taken first (a FOR KEY SHARE before it, as vfs_lock_dir takes, would let two callers each hold one
-- and deadlock upgrading): it conflicts with the FOR KEY SHARE that every create in p_parent, and every mkdir_p
-- through it, takes on p_parent. Every create that holds one when this gets the lock has committed, so its
-- directory exists when the caller lists the children; one that starts after waits for this short transaction,
-- then inherits. While this waits, new creates can still take FOR KEY SHARE ahead of it (Postgres queues only
-- the lockers that sleep), so under steady create traffic it can run out its lock_timeout (EAGAIN): call again.
-- Writes that create nothing (append, write) don't wait.
CREATE FUNCTION vfs_set_child_quota(p_parent uuid, p_max_bytes bigint, p_max_nodes bigint)
RETURNS TABLE (err text, err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_dir boolean;
BEGIN
    PERFORM vfs_check_node(p_parent);
    IF p_max_bytes < 0 OR p_max_nodes < 0 THEN
        PERFORM vfs_raise('EINVAL', format('invalid quota (%s bytes, %s nodes)', p_max_bytes, p_max_nodes));
    END IF;
    SELECT n.kind = 'dir' INTO v_dir FROM vfs_nodes n WHERE n.id = p_parent FOR UPDATE;
    IF NOT FOUND THEN
        err := 'ENOENT';
        err_detail := format('directory %s does not exist', p_parent);
    ELSIF NOT v_dir THEN
        err := 'ENOTDIR';
        err_detail := format('%s is a file', p_parent);
    ELSIF p_max_bytes IS NULL AND p_max_nodes IS NULL THEN
        DELETE FROM vfs_quota_defaults d WHERE d.parent = p_parent;
    ELSE
        INSERT INTO vfs_quota_defaults AS d (parent, max_bytes, max_nodes)
        VALUES (p_parent, p_max_bytes, p_max_nodes)
            ON CONFLICT (parent) DO UPDATE SET max_bytes = EXCLUDED.max_bytes, max_nodes = EXCLUDED.max_nodes;
    END IF;
    RETURN NEXT;
END $$;


-- The child directories of p_parent without a quota of their own, for the caller of vfs_set_child_quota to give
-- one each with vfs_give_child_quota.
CREATE FUNCTION vfs_quota_pending_children(p_parent uuid) RETURNS SETOF uuid
LANGUAGE sql STABLE SET search_path FROM CURRENT AS $$
    SELECT n.id FROM vfs_nodes n
     WHERE n.parent_id = p_parent AND n.kind = 'dir' AND NOT EXISTS (SELECT FROM vfs_quotas q WHERE q.root = n.id)
     ORDER BY n.id
$$;


-- Gives directory p_child the child quota of p_parent and counts its usage, as vfs_set_quota does; returns
-- whether it did. False, changing nothing, if p_child is gone or no longer in p_parent (a rename moved it), if it
-- has a quota of its own by now (vfs_set_quota, or it was created after the defaults and inherited), or if
-- p_parent has no child quota any more. One transaction per child: a write races only its own child's first
-- recount (the window vfs_set_quota documents for a new row).
CREATE FUNCTION vfs_give_child_quota(p_child uuid, p_parent uuid) RETURNS boolean
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_parent uuid;
    v_max_bytes bigint;
    v_max_nodes bigint;
    v_bytes bigint;
    v_nodes bigint;
    v_given int;
BEGIN
    PERFORM vfs_check_node(p_child);
    PERFORM vfs_check_node(p_parent);
    -- FOR SHARE conflicts with any UPDATE of the row (a rename's, whether or not it takes FOR UPDATE first:
    -- changing parent_id alone is a no-key update, which FOR KEY SHARE would let through) and with an unlink's
    -- delete: one in flight commits first, and this reads the row it left (READ COMMITTED re-reads a locked row);
    -- one that starts later waits for this to commit.
    SELECT n.parent_id INTO v_parent FROM vfs_nodes n WHERE n.id = p_child AND n.kind = 'dir' FOR SHARE;
    IF NOT FOUND OR v_parent IS DISTINCT FROM p_parent THEN
        RETURN false;
    END IF;
    SELECT d.max_bytes, d.max_nodes INTO v_max_bytes, v_max_nodes FROM vfs_quota_defaults d WHERE d.parent = p_parent;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    INSERT INTO vfs_quotas (root, max_bytes, max_nodes) VALUES (p_child, v_max_bytes, v_max_nodes)
        ON CONFLICT (root) DO NOTHING;
    GET DIAGNOSTICS v_given = ROW_COUNT;
    IF v_given = 0 THEN
        RETURN false;
    END IF;
    -- A later statement, so a later snapshot: it sees every charge committed before the insert (see
    -- vfs_set_quota).
    SELECT u.bytes, u.nodes - 1 INTO v_bytes, v_nodes FROM vfs_subtree_usage(p_child) u;
    UPDATE vfs_quotas q SET used_bytes = v_bytes, used_nodes = v_nodes WHERE q.root = p_child;
    RETURN true;
END $$;


-- Cuts p_data into extents that each start at a line start: consecutive whole lines, cut just after a \n once
-- the next line would take the extent past p_target bytes. A line longer than p_target is an extent of its own;
-- only the last extent may end without a \n. Returns, in order, each extent's offset in p_data, the number of
-- \n before it, its bytes and its line_offsets. No rows for empty or NULL data; EINVAL for a NULL or
-- non-positive p_target. With p_split_tail, a last line without \n goes in an extent of its own, so the next
-- append, which re-cuts the last extent (vfs_append), rewrites only that partial line.
--
-- The lines are found in C, not byte by byte: encode(..., 'escape') turns bytea into text that keeps every \n
-- byte as a newline (it escapes only NUL, high-bit bytes and backslashes, never in a way that produces one), so
-- splitting that text on newlines and decoding each piece gives the length of each line. It encodes 1 MB at a
-- time: escaping can make the text 4 times longer than the bytes, and a text value can't pass 1 GB. A line may
-- span windows, so the last piece of a window carries on into the next.
--
-- 007's rule, placed as it goes (023): a line is placed as soon as its \n is found, since where it goes depends
-- only on the extent being filled, and only the last line, if it has no \n, is placed after the windows (where
-- p_split_tail applies). So the function holds one extent's line_offsets, never one entry per line of p_data.
CREATE OR REPLACE FUNCTION vfs_cut(p_data bytea, p_target int DEFAULT 65536, p_split_tail boolean DEFAULT false)
RETURNS TABLE (rel_offset bigint, rel_line bigint, data bytea, line_offsets int4[])
LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
DECLARE
    c_window constant int := 1048576;
    v_open bigint := 0;                             -- the length so far of a line that spans windows
    v_len bigint;
    v_last boolean;
    v_pos bigint := 0;                              -- where the next line starts
BEGIN
    IF p_target IS NULL OR p_target < 1 THEN
        PERFORM vfs_raise('EINVAL', format('invalid target size %s', p_target));
    END IF;
    IF p_data IS NULL THEN
        RETURN;
    END IF;
    rel_offset := 0;
    rel_line := 0;
    line_offsets := '{}';
    FOR w IN 0 .. (length(p_data) - 1) / c_window LOOP     -- no iterations for empty data
        FOR v_len, v_last IN
            SELECT length(decode(s.piece, 'escape')), s.i = count(*) OVER ()
              FROM string_to_table(encode(substring(p_data FROM w * c_window + 1 FOR c_window), 'escape'), E'\n')
                   WITH ORDINALITY AS s(piece, i)
             ORDER BY s.i
        LOOP
            IF v_last THEN
                v_open := v_open + v_len;           -- no \n after it in this window
                CONTINUE;
            END IF;
            v_len := v_open + v_len + 1;            -- a whole line, \n included
            v_open := 0;
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
    END LOOP;
    IF v_open > 0 THEN                              -- the last line, with no \n
        IF v_pos > rel_offset AND (v_pos - rel_offset + v_open > p_target OR p_split_tail) THEN
            data := substring(p_data FROM (rel_offset + 1)::int FOR (v_pos - rel_offset)::int);
            RETURN NEXT;
            rel_line := rel_line + cardinality(line_offsets);
            rel_offset := v_pos;
            line_offsets := '{}';
        END IF;
        line_offsets := line_offsets || (v_pos - rel_offset)::int;
        v_pos := v_pos + v_open;
    END IF;
    IF v_pos > rel_offset THEN
        data := substring(p_data FROM (rel_offset + 1)::int FOR (v_pos - rel_offset)::int);
        RETURN NEXT;
    END IF;
END $$;


-- Creates p_name ('file' or 'dir') in directory p_parent (NULL: the root), like open(O_CREAT|O_EXCL) / mkdir,
-- with p_data as the file's content (generation 0), cut by vfs_cut. err EEXIST if the name is taken, ENOENT /
-- ENOTDIR for a missing parent / a parent that is a file. EINVAL for a bad kind, content on a directory or a bad
-- name, ENAMETOOLONG. Since 023: EFBIG for content over vfs_max_file_size(), EDQUOT over a quota (the node and
-- its bytes are charged), and a directory gets p_parent's child quota.
CREATE OR REPLACE FUNCTION vfs_create(p_parent uuid, p_name text, p_kind text, p_data bytea DEFAULT NULL)
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
    PERFORM vfs_check_file_size(v_size, NULL);
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
    PERFORM vfs_charge(p_parent, v_size, 1);
    IF p_kind = 'dir' THEN
        PERFORM vfs_quota_inherit(v_id, p_parent);
    END IF;
    id := v_id;
    generation := 0;
    size := v_size;
    RETURN NEXT;
END $$;


-- Like mkdir -p: creates every missing directory along p_segments and returns the last one's id (NULL for an
-- empty array, the root). Safe against concurrent creators of the same directories. err ENOTDIR if a non-final
-- component is a file, EEXIST if the final one is; nothing has been created then, since every directory after
-- one this call created is new too. EINVAL / ENAMETOOLONG for a bad component; ENAMETOOLONG (022) too for more
-- than vfs_max_components() segments, raised before anything is created. Since 023: EDQUOT over a quota (every
-- directory it creates is charged), and each directory it creates gets its parent's child quota.
--
-- chain holds the ids of the directories found or made, in order: all of them on success (the last is id), the
-- ones before the file on ENOTDIR or EEXIST. Every element is a directory.
--
-- The directories it creates are consecutive, from the first one down: the quota roots above the first one's
-- parent gain them all, and a new directory that got a child quota gains the ones created below it.
CREATE OR REPLACE FUNCTION vfs_mkdir_p(p_segments text[]) RETURNS TABLE (id uuid, chain uuid[], err text, err_detail text)
LANGUAGE plpgsql ROWS 1 SET search_path FROM CURRENT AS $$
DECLARE
    v_id uuid;
    v_next uuid;
    v_seg text;
    v_i int := 0;
    v_n int := coalesce(cardinality(p_segments), 0);
    v_first int;                                    -- the index in chain of the first directory created
    v_top uuid;                                     -- its parent (NULL: the root)
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
            IF v_next IS NOT NULL THEN
                IF v_first IS NULL THEN
                    v_first := v_i;
                    v_top := v_id;
                END IF;
                PERFORM vfs_quota_inherit(v_next, v_id);
                EXIT;
            END IF;
            -- A concurrent transaction created it and has committed: look it up again.
        END LOOP;
        v_id := v_next;
        chain := chain || v_id;
    END LOOP;
    IF v_first IS NOT NULL THEN
        -- A directory found after the first one created can't exist: it would be inside a new one.
        PERFORM vfs_charge(v_top, 0, v_n - v_first + 1);
        -- The new ones with a child quota (rows this transaction inserted, so locked by no one else).
        UPDATE vfs_quotas q SET used_nodes = v_n - c.j
          FROM unnest(chain) WITH ORDINALITY AS c(id, j)
         WHERE c.j >= v_first AND c.j < v_n AND q.root = c.id;
        IF EXISTS (SELECT FROM vfs_quotas q, unnest(chain) WITH ORDINALITY AS c(id, j)
                    WHERE c.j >= v_first AND q.root = c.id AND q.used_nodes > q.max_nodes) THEN
            PERFORM vfs_raise('EDQUOT', format('a directory created along %L gets a quota of fewer nodes than '
                                               'the ones created below it', p_segments));
        END IF;
    END IF;
    id := v_id;
    RETURN NEXT;
END $$;


-- Removes p_node, like unlink(2) for a file and rmdir(2) for a directory, and returns one row, with err NULL
-- when it was removed. err ENOENT if it doesn't exist, ENOTEMPTY for a directory with entries. EINVAL for a NULL
-- p_node. Since 023 the quota roots above it lose its bytes and the node, and a quota or child quota on it goes.
CREATE OR REPLACE FUNCTION vfs_unlink(p_node uuid) RETURNS TABLE (err text, err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_kind text;
    v_parent uuid;
    v_size bigint;
BEGIN
    PERFORM vfs_check_node(p_node);
    SELECT n.kind, n.parent_id, n.size INTO v_kind, v_parent, v_size FROM vfs_nodes n WHERE n.id = p_node FOR UPDATE;
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
    IF v_kind = 'dir' THEN
        DELETE FROM vfs_quotas q WHERE q.root = p_node;
        DELETE FROM vfs_quota_defaults d WHERE d.parent = p_node;
    END IF;
    PERFORM vfs_charge(v_parent, -v_size, -1);  -- only gives usage back: a partial walk (vfs_charge)
    RETURN NEXT;
END $$;


-- Appends p_data to file p_node, like write(2) with O_APPEND, and returns the offset it landed at. The inode
-- lock is held until commit, so concurrent appenders take turns: offsets are gap-free and offset order is
-- commit order. Keeps the generation. An empty p_data changes nothing. err ENOENT, EISDIR; EINVAL for a NULL
-- p_node or NULL data. mtime is the later of its old value and the clock, so it never goes back when a
-- transaction that started earlier commits later (now() is the transaction's start time). Since 023: EFBIG if
-- the file would pass vfs_max_file_size(), EDQUOT over a quota.
--
-- When the file ends mid-line, its last extent is deleted and cut again together with p_data, so extents keep
-- starting at line starts (migration 007). Since 023, so does a last extent that would hold at most 4 KiB with
-- p_data, ending in \n or not, cut as one extent (no split tail): appends of short lines fill an extent row
-- instead of adding one each (an extent row costs about 200 bytes of storage, which no quota counts, so one row
-- per 1-byte append would store some 200 times what the quota charges). A row is left behind only once it
-- holds over 4 KiB together with the append that follows it, so a file has about one extent row per 2 KiB at
-- most, and an append rewrites at most 4 KiB.
CREATE OR REPLACE FUNCTION vfs_append(p_node uuid, p_data bytea) RETURNS TABLE (byte_offset bigint, err text, err_detail text)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
    v_size bigint;
    v_gen int;
    v_off bigint;                                   -- where the cut content goes
    v_line bigint;                                  -- the number of \n before v_off
    v_last bytea;                                   -- the file's last extent
    v_lines bigint;
    v_split boolean := true;                        -- vfs_cut's p_split_tail
BEGIN
    IF p_data IS NULL THEN
        PERFORM vfs_raise('EINVAL', 'data is NULL');
    END IF;
    -- Before the lock: data alone over the cap needs no file.
    PERFORM vfs_check_file_size(length(p_data), p_node);
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
    PERFORM vfs_check_file_size(v_size + length(p_data), p_node);
    SELECT e.byte_offset, e.line_start, e.data, cardinality(e.line_offsets) INTO v_off, v_line, v_last, v_lines
      FROM vfs_extents e
     WHERE e.node_id = p_node AND e.generation = v_gen ORDER BY e.byte_offset DESC LIMIT 1;
    IF NOT FOUND THEN                               -- an empty file
        v_off := 0;
        v_line := 0;
        v_last := ''::bytea;
        v_split := length(p_data) > 4096;
    ELSIF length(v_last) + length(p_data) <= 4096 THEN
        -- Small: the last extent and p_data become one extent (4 KiB is under the 65536 target, so one piece).
        DELETE FROM vfs_extents e WHERE e.node_id = p_node AND e.generation = v_gen AND e.byte_offset = v_off;
        v_split := false;
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
          FROM vfs_cut(v_last || p_data, 65536, v_split) c
        RETURNING vfs_extents.line_start + cardinality(vfs_extents.line_offsets) AS lines_after
    )
    SELECT max(ins.lines_after) INTO v_lines FROM ins;
    UPDATE vfs_nodes n SET size = v_size + length(p_data), lines = v_lines, mtime = greatest(n.mtime, clock_timestamp())
     WHERE n.id = p_node;
    PERFORM vfs_charge(p_node, length(p_data), 0);
    RETURN NEXT;
END $$;


-- Replaces the content of file p_node with p_data, like open(O_TRUNC) then write(2): a new generation, the old
-- extents deleted, the new content cut by vfs_cut. Returns the new generation and size. mtime as in vfs_append.
--
-- With p_expect_gen, p_expect_size and p_expect_name it is a compare-and-swap for the Write tool: it writes only
-- if the file still has that generation, size, name and parent (p_expect_parent NULL meaning the root), and
-- returns err ESTALE otherwise. The three are all or none, and p_expect_parent needs them: EINVAL otherwise.
-- err ENOENT, EISDIR; EINVAL for a NULL p_node or NULL data. Since 023: EFBIG for data over vfs_max_file_size(),
-- EDQUOT if the file grows over a quota (a write that shrinks it is never refused).
CREATE OR REPLACE FUNCTION vfs_write(p_node uuid, p_data bytea,
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
    PERFORM vfs_check_file_size(length(p_data), p_node);
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
    PERFORM vfs_charge(p_node, length(p_data) - v_size, 0);
    generation := v_gen + 1;
    size := length(p_data);
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
-- Since 023: EDQUOT when the move takes a quota over its limit (see Quotas below).
--
-- Cycles: a directory's move locks every ancestor of the destination FOR KEY SHARE while it checks them. A
-- concurrent move of one of those ancestors needs FOR UPDATE on it, so the two moves are ordered (or, if they
-- lock in opposite orders, one gets a deadlock error), and the later one sees the earlier one's result. Under
-- REPEATABLE READ, locking an ancestor that moved after the snapshot raises a serialization failure instead.
-- The walk locks only directories, so it never waits on a file's inode lock (appends hold it FOR UPDATE). A
-- file's move walks the ancestors too since 022, without locks, only to count the destination's depth (one
-- probe per level). A walk that goes over the cap stops there: a real cycle (vfs_ancestors_cycle) is still
-- EINVAL, and a chain merely deeper than the cap is ENAMETOOLONG, not the false cycle the old 10,000-step
-- guess reported; a rename that keeps the parent doesn't change any depth, so it isn't refused for one. (A
-- directory moved into its own subtree more than the cap below it, possible only in a legacy over-deep tree,
-- is refused as ENAMETOOLONG rather than EINVAL: the walk stops before it reaches the directory.)
--
-- The subtree check of a directory that moves deeper reads the descendants without locking them, so it is
-- best effort: a concurrent mkdir inside the moved subtree can slip one over the cap; so is a file's depth
-- check. The scan reads every directory of the subtree down to the room left, so moving a wide tree deeper
-- costs more. Moving sideways or up never scans the subtree, so an over-deep legacy subtree can always be
-- moved up.
--
-- Quotas (023): when the quota roots above the source and above the destination differ, the moved subtree's
-- bytes and nodes (p_node included, scanned without locks down to vfs_max_components() levels) leave the roots
-- only above the source and join those only above the destination. A quota on p_node itself moves with it.
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
    v_from uuid[];
    v_to uuid[];
    v_bytes bigint;
    v_nodes bigint;
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
    -- cycle, so its walk (new in 022, only to count the depth) takes no locks: renames of files (Maildir claims)
    -- still don't lock the ancestors or wait on them. It costs one probe per level, and its depth check is best
    -- effort, like the subtree check below. A walk over the cap stops: a real cycle is EINVAL as before; a
    -- rename that keeps the parent changes no depth and goes on (the old parent passed the same walk); anything
    -- else is a destination merely deeper than the cap, ENAMETOOLONG.
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
            EXIT WHEN v_parent IS NOT DISTINCT FROM p_new_parent;
            PERFORM vfs_raise('ENAMETOOLONG',
                              format('%s is deeper than %s levels', p_new_parent, vfs_max_components()));
        END IF;
        IF v_kind = 'dir' THEN
            SELECT n.parent_id INTO v_up FROM vfs_nodes n WHERE n.id = v_up FOR KEY SHARE;
        ELSE
            SELECT n.parent_id INTO v_up FROM vfs_nodes n WHERE n.id = v_up;
        END IF;
    END LOOP;
    IF v_parent IS DISTINCT FROM p_new_parent AND v_depth + 1 > vfs_max_components() THEN
        PERFORM vfs_raise('ENAMETOOLONG', format('%s would be %s levels deep, over the %s limit',
                                                 p_node, v_depth + 1, vfs_max_components()));
    END IF;
    -- A directory that moves deeper takes its subtree with it. The old parent's depth comes from vfs_depth,
    -- without locks; past the cap it is cap + 1, more than v_depth (at most cap - 1 here), so a move out of an
    -- over-deep region is a move up and is never scanned, even when its subtree stays over the cap. v_room is
    -- the deepest relative level allowed below p_node, p_node itself at level 0: the scan (directories only, no
    -- locks on the descendants; it reads every directory of the subtree down to v_room) looks for any child,
    -- file or directory, of a directory at level v_room. A rename that keeps the parent can't go deeper.
    IF v_kind = 'dir' AND v_parent IS DISTINCT FROM p_new_parent THEN
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
    -- After the UPDATE, so the quota rows are the last locks taken: an UPDATE waiting on a concurrent create of
    -- the same name while holding them would deadlock with that create's charge. The subtree is p_node's, so
    -- the move changes neither its usage nor the roots above the old parent. A rename that keeps the parent
    -- keeps every root (and walks nothing: in a legacy subtree over the cap it isn't refused, as 022 promises).
    IF v_parent IS DISTINCT FROM p_new_parent THEN
        v_from := vfs_quota_roots(v_parent, true);  -- the source only gives usage back
        v_to := vfs_quota_roots(p_new_parent);
        IF NOT (v_from @> v_to AND v_to @> v_from) THEN
            SELECT u.bytes, u.nodes INTO v_bytes, v_nodes FROM vfs_subtree_usage(p_node) u;
            PERFORM vfs_charge_move(v_from, v_to, v_bytes, v_nodes);
        END IF;
    END IF;
    RETURN NEXT;
END $$;
