-- The modify notification carries the node's name (#96). A watch through a view (minifs.vfs.view_watch) applies the
-- policy to each child's path, and without a name in the payload it had to look the name up when the notification
-- arrived, which gives the name the child has then, not the one it had when it changed: a file appended to under a
-- name the actor may not read and renamed afterwards was reported with its size under the readable name. With the
-- name in the payload, every event is judged by the name it happened under.
--
-- The body is migration 018's with 'name' added to the modify payload. CREATE OR REPLACE keeps the function's
-- privileges (EXECUTE stays revoked from PUBLIC) but not its SECURITY DEFINER or its search_path, so the DO block
-- below sets them again as 018 did.

CREATE OR REPLACE FUNCTION vfs_notify() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE v_payload json;
BEGIN
    IF TG_RELID OPERATOR(pg_catalog.<>) 'vfs_nodes'::regclass THEN  -- before NEW or OLD is touched: see 018
        RAISE EXCEPTION 'vfs_notify() is the trigger function of vfs_nodes alone';
    END IF;
    IF TG_OP = 'INSERT' THEN
        v_payload := pg_catalog.json_build_object('op', 'create', 'id', NEW.id, 'parent', NEW.parent_id,
                                       'name', NEW.name, 'kind', NEW.kind);
    ELSIF TG_OP = 'DELETE' THEN
        v_payload := pg_catalog.json_build_object('op', 'delete', 'id', OLD.id, 'parent', OLD.parent_id,
                                       'name', OLD.name, 'kind', OLD.kind);
    ELSIF OLD.parent_id IS DISTINCT FROM NEW.parent_id OR OLD.name <> NEW.name THEN
        v_payload := pg_catalog.json_build_object('op', 'move', 'id', NEW.id, 'parent', NEW.parent_id, 'name', NEW.name,
                                       'old_parent', OLD.parent_id, 'old_name', OLD.name, 'kind', NEW.kind);
    ELSE
        v_payload := pg_catalog.json_build_object('op', 'modify', 'id', NEW.id, 'parent', NEW.parent_id,
                                       'name', NEW.name, 'size', NEW.size, 'gen', NEW.generation);
    END IF;
    PERFORM pg_catalog.pg_notify((SELECT channel FROM vfs_channel_name), v_payload::text);
    RETURN NULL;
END $$;

DO $$
BEGIN
    EXECUTE format('ALTER FUNCTION vfs_notify() SECURITY DEFINER SET search_path = %I, pg_temp', current_schema());
END $$;
