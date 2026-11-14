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
-- Who can learn the name: a role that can call vfs_channel(). EXECUTE on it is revoked from PUBLIC below, so that
-- is the schema's owner and whoever the owner grants it to. (USAGE on the schema would not do as the boundary:
-- every role has it on "public", where minifs is installed by default.) The table itself is its owner's alone, and
-- the name is in a table and not a literal in the function's body because pg_proc.prosrc is readable by every
-- role. vfs_channel() and the two trigger functions are SECURITY DEFINER, so a role that writes through its table
-- grants notifies without being able to read the name. So EXECUTE on vfs_channel() is now the trust boundary for
-- notifications, not CONNECT on the database: a role without it can neither read this schema's events nor forge
-- them. A watcher that doesn't connect as the owner needs
--     GRANT EXECUTE ON FUNCTION vfs_channel() TO <role>;
-- A role with it can still forge events, so they stay hints (a watcher reads the database before acting on one),
-- and the watcher rate-limits rescans and truncates the payloads it logs. The LISTEN statement carries the name,
-- so members of pg_read_all_stats (pg_stat_activity) and readers of the server's statement log can see it too.
--
-- Migrate with writers and watchers stopped, or restart the watchers afterwards: a transaction that fired the old
-- trigger before this migration commits notifies on 'vfs', which no new watcher hears, and a client older than
-- this migration keeps listening on 'vfs' and receives nothing, without an error.
--
-- To change the name later: UPDATE vfs_channel_name SET channel = 'vfs_' || replace(gen_random_uuid()::text, '-', '')
-- (one UPDATE, never a DELETE: with no row, every write fails in the trigger). Running watchers follow within
-- their keepalive interval. A copy of this schema (dump and restore) in the same database shares the channel with
-- the original until that is done; a restore into another database doesn't matter. A restore without privileges
-- (pg_dump --no-acl) loses the REVOKEs: every role can call vfs_channel() again until they are repeated.

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
REVOKE EXECUTE ON FUNCTION vfs_channel() FROM PUBLIC;
COMMENT ON FUNCTION vfs_channel() IS
    'The NOTIFY channel of this schema (migration 016): what vfs_notify sends on and a watcher listens to.';


CREATE OR REPLACE FUNCTION vfs_notify() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE v_payload json;
BEGIN
    IF TG_RELID <> 'vfs_nodes'::regclass THEN  -- before NEW or OLD is touched: see the note below
        RAISE EXCEPTION 'vfs_notify() is the trigger function of vfs_nodes alone';
    END IF;
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
    IF TG_RELID <> 'vfs_nodes'::regclass THEN
        RAISE EXCEPTION 'vfs_notify_truncate() is the trigger function of vfs_nodes alone';
    END IF;
    PERFORM pg_notify(vfs_channel(), '{"op": "rescan"}');
    RETURN NULL;
END $$;


-- The triggers run as the owner, who may call vfs_channel(), whoever writes. Like vfs_channel(), they get the
-- schema itself as their search_path (with pg_temp last) in place of the caller's, as SECURITY DEFINER wants.
--
-- A SECURITY DEFINER trigger function must not be PUBLIC's to execute: CREATE TRIGGER takes only EXECUTE on the
-- function and a table of one's own (a temporary one will do), so any role could put these on its table and
-- forge events without the name, or give its table a column type whose cast to json is its own function, which
-- json_build_object would then run as this schema's owner. Hence the REVOKE (firing a trigger doesn't check
-- EXECUTE, so roles that write through table grants are unaffected) and the TG_RELID checks in the bodies, which
-- hold even where the ACL is lost (see pg_dump --no-acl above) or EXECUTE is granted by mistake.
REVOKE EXECUTE ON FUNCTION vfs_notify(), vfs_notify_truncate() FROM PUBLIC;
DO $$
BEGIN
    EXECUTE format('ALTER FUNCTION vfs_notify() SECURITY DEFINER SET search_path = %I, pg_temp', current_schema());
    EXECUTE format('ALTER FUNCTION vfs_notify_truncate() SECURITY DEFINER SET search_path = %I, pg_temp',
                   current_schema());
END $$;
