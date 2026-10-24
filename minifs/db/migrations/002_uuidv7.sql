-- PostgreSQL 15+ is required (UNIQUE NULLS NOT DISTINCT). uuidv7() is built in from PostgreSQL 18; on 15-17
-- this defines it in the current schema: over the pg_uuidv7 extension when it is installed, otherwise in SQL
-- (Databricks Lakebase has no pg_uuidv7, so its 16 and 17 use the SQL version).
DO $$
BEGIN
    IF current_setting('server_version_num')::int < 150000 THEN
        RAISE EXCEPTION 'minifs needs PostgreSQL 15 or later, this is %', current_setting('server_version');
    END IF;
    IF to_regprocedure('pg_catalog.uuidv7()') IS NOT NULL THEN
        RETURN;
    END IF;
    IF EXISTS (SELECT FROM pg_extension WHERE extname = 'pg_uuidv7') THEN
        CREATE FUNCTION uuidv7() RETURNS uuid LANGUAGE sql VOLATILE PARALLEL SAFE
            RETURN uuid_generate_v7();
    ELSE
        -- RFC 9562 version 7: a 48-bit Unix timestamp in milliseconds, then random bits. Starts from a v4 UUID
        -- (random, variant 10), overwrites its first 6 bytes with the timestamp, and turns version 4 into 7.
        CREATE FUNCTION uuidv7() RETURNS uuid LANGUAGE sql VOLATILE PARALLEL SAFE
            RETURN encode(
                set_bit(set_bit(
                    overlay(uuid_send(gen_random_uuid())
                            PLACING substring(int8send(floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint) FROM 3)
                            FROM 1 FOR 6),
                    52, 1), 53, 1),
                'hex')::uuid;
    END IF;
END
$$;
