-- A NOTIFY channel per schema, with a name outsiders can't guess (#87). Channels are database-wide, not per schema:
-- any role with CONNECT on the database may LISTEN and NOTIFY on any channel it can name. With one minifs schema
-- per search_path in a shared database (which minifs.db.migrate supports), every deployment's LISTEN connection
-- received every other deployment's events on the fixed channel 'vfs', file and directory names included, and any
-- connecting role could read them or forge its own.
--
-- This schema's channel is now 'vfs_' and 32 random hex digits, generated once, here, and kept in the one row of
-- vfs_channel_name. vfs_channel() returns it: the trigger functions (vfs_notify and vfs_notify_truncate, replaced
-- below with the same bodies but for the channel) send on it, and minifs.vfs.watch runs SELECT vfs_channel() on its
-- LISTEN connection, so they agree without configuration.
--
-- Who can learn the name: a role that can call vfs_channel(), which takes USAGE on this schema (EXECUTE is PUBLIC's
-- by default, as for every function). The table itself is its owner's alone, and vfs_channel() is SECURITY DEFINER
-- so that a role with USAGE and its table grants keeps working without a grant on the new table. The name is in a
-- table and not a literal in the function's body because pg_proc.prosrc is readable by every role. So USAGE on the
-- schema is now the trust boundary for notifications, not CONNECT on the database: a role without it can neither
-- read this schema's events nor forge them. A role with it still can, so events stay hints (a watcher reads the
-- database before acting on one), and the watcher rate-limits rescans and truncates the payloads it logs.
--
-- A client older than this migration listens on 'vfs' and receives nothing: upgrade the schema and the clients
-- together. A copy of this schema (dump and restore) in the same database shares the channel with the original
-- until the row is given a new name; a restore into another database doesn't matter.

CREATE TABLE vfs_channel_name (
    one     boolean PRIMARY KEY DEFAULT true CHECK (one),  -- a single row
    channel text    NOT NULL
);
COMMENT ON TABLE vfs_channel_name IS
    'The NOTIFY channel of this schema, one row (migration 016). Read it through vfs_channel().';
REVOKE ALL ON vfs_channel_name FROM PUBLIC;
INSERT INTO vfs_channel_name (channel) VALUES ('vfs_' || replace(gen_random_uuid()::text, '-', ''));


-- The schema is written into the body and the search_path (with pg_temp last), as a SECURITY DEFINER function
-- should have it: no temporary table of the caller's can stand in for vfs_channel_name.
DO $$
BEGIN
    EXECUTE format(
        'CREATE FUNCTION vfs_channel() RETURNS text LANGUAGE sql STABLE SECURITY DEFINER '
        'SET search_path = %I, pg_temp AS %L',
        current_schema(), format('SELECT channel FROM %I.vfs_channel_name', current_schema()));
END $$;
COMMENT ON FUNCTION vfs_channel() IS
    'The NOTIFY channel of this schema (migration 016): what vfs_notify sends on and a watcher listens to.';


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
