-- A NOTIFY channel per schema (#87). Channels are database-wide, not per schema, and any role with CONNECT on the
-- database may NOTIFY on any channel. With one minifs schema per search_path in a shared database (which
-- minifs.db.migrate supports), every deployment's LISTEN connection received every other deployment's events on
-- the fixed channel 'vfs', file and directory names included.
--
-- vfs_channel() returns this schema's channel: 'vfs_<schema>' when that fits a channel name (63 bytes, as any
-- identifier), else 'vfs_' and the md5 of the schema name. It is computed once, here, from current_schema(), and
-- stored as the function's body, so the trigger (vfs_notify and vfs_notify_truncate, replaced below with the same
-- bodies but for the channel) and minifs.vfs.watch, which runs SELECT vfs_channel() on its LISTEN connection, agree
-- without configuration. A client older than this migration listens on 'vfs' and receives nothing: upgrade the
-- schema and the clients together.
--
-- What this doesn't change: CONNECT on the database is still the trust boundary for notifications. A role that can
-- connect can NOTIFY on this channel too, so events stay hints (a watcher reads the database before acting on
-- one), and the watcher rate-limits rescans per subscription and truncates the payloads it logs.

DO $$
DECLARE
    v_channel text := 'vfs_' || current_schema();
BEGIN
    IF octet_length(v_channel) > 63 THEN
        v_channel := 'vfs_' || md5(current_schema());
    END IF;
    EXECUTE format('CREATE FUNCTION vfs_channel() RETURNS text LANGUAGE sql IMMUTABLE AS %L',
                   format('SELECT %L', v_channel));
    EXECUTE format('COMMENT ON FUNCTION vfs_channel() IS %L',
                   'The NOTIFY channel of this schema (migration 016): what vfs_notify sends on and a watcher listens to.');
END $$;


CREATE OR REPLACE FUNCTION vfs_notify() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE v_payload json;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_payload := json_build_object('op', 'create', 'id', NEW.id, 'parent', NEW.parent_id,
                                       'name', NEW.name, 'kind', NEW.kind);
    ELSIF TG_OP = 'DELETE' THEN
        v_payload := json_build_object('op', 'delete', 'id', OLD.id, 'parent', OLD.parent_id,
                                       'name', OLD.name, 'kind', OLD.kind);
    ELSIF OLD.parent_id IS DISTINCT FROM NEW.parent_id OR OLD.name <> NEW.name THEN
        v_payload := json_build_object('op', 'move', 'id', NEW.id, 'parent', NEW.parent_id, 'name', NEW.name,
                                       'old_parent', OLD.parent_id, 'old_name', OLD.name, 'kind', NEW.kind);
    ELSE
        v_payload := json_build_object('op', 'modify', 'id', NEW.id, 'parent', NEW.parent_id,
                                       'size', NEW.size, 'gen', NEW.generation);
    END IF;
    PERFORM pg_notify(vfs_channel(), v_payload::text);
    RETURN NULL;
END $$;


CREATE OR REPLACE FUNCTION vfs_notify_truncate() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM pg_notify(vfs_channel(), '{"op": "rescan"}');
    RETURN NULL;
END $$;
