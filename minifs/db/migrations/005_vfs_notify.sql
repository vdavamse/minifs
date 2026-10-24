-- Watch: a NOTIFY on channel 'vfs' for every change to vfs_nodes, delivered at commit (README: Watch).
--
-- The payload is a JSON object. Every event has op, id and parent (the directory the node is in after the
-- change; null for a root entry), so a watcher filters by id for a file and by parent for a directory:
--
--   create  id, parent, name, kind
--   modify  id, parent, size, gen        an append (size, mtime) or an overwrite (gen); never a rename
--   move    id, parent, name, old_parent, old_name, kind
--   delete  id, parent, name, kind
--
-- move carries old_parent so a watcher of the source directory sees the entry leave (a Maildir claim from
-- tasks/new to tasks/cur is one move, seen by watchers of both). Payloads stay well under NOTIFY's 8000-byte
-- limit: names are at most 255 bytes, so a move is at most about 3 KB even with JSON escapes.
--
-- An UPDATE that changes no column fires nothing (the WHEN clause), and compaction doesn't update vfs_nodes at
-- all, so neither emits. A rolled-back transaction emits nothing, since NOTIFY is sent at commit. TRUNCATE emits
-- one {"op": "rescan"}: watchers re-check everything.
--
-- Events are hints: PostgreSQL drops a payload identical to one already sent in the same transaction (so
-- renaming f to g, back to f and to g again reports the first two moves only), and any role that can connect
-- can NOTIFY on the channel. Watchers read the database before acting on an event.
--
-- Cost: a transaction that notifies takes a database-wide lock at commit (README: NOTIFY volume). If appends to a
-- hot file make it a bottleneck, the modify branch is the one to switch off for that file.
--
-- No event triggers or superuser: this is an ordinary trigger on a table the migration role owns.

CREATE FUNCTION vfs_notify() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
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
    PERFORM pg_notify('vfs', v_payload::text);
    RETURN NULL;
END $$;

CREATE TRIGGER vfs_nodes_notify_insert_delete AFTER INSERT OR DELETE ON vfs_nodes
    FOR EACH ROW EXECUTE FUNCTION vfs_notify();

-- Any changed column notifies: a column added later that changes often (an atime) would notify on every change.
CREATE TRIGGER vfs_nodes_notify_update AFTER UPDATE ON vfs_nodes
    FOR EACH ROW WHEN (OLD.* IS DISTINCT FROM NEW.*) EXECUTE FUNCTION vfs_notify();


CREATE FUNCTION vfs_notify_truncate() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
    PERFORM pg_notify('vfs', '{"op": "rescan"}');
    RETURN NULL;
END $$;

CREATE TRIGGER vfs_nodes_notify_truncate AFTER TRUNCATE ON vfs_nodes
    FOR EACH STATEMENT EXECUTE FUNCTION vfs_notify_truncate();
