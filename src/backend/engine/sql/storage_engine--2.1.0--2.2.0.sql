-- storage_engine--2.1.0--2.2.0.sql
--
-- Upgrade script: 2.1.0 → 2.2.0
--
-- New in 2.2.0:
--   • engine.row_options gains orderby text column for rowcompress sort ordering
--   • engine.alter_rowcompress_table_set gets an orderby parameter
--   • engine.alter_rowcompress_table_reset gets an orderby parameter
--   • engine.rowcompress_options view updated to include orderby
--   • engine.rowcompress_repack now respects orderby (inserts rows ORDER BY when set)
--

-- 1. New column on row_options --------------------------------------------------
ALTER TABLE engine.row_options
    ADD COLUMN IF NOT EXISTS orderby text DEFAULT NULL;

COMMENT ON COLUMN engine.row_options.orderby
    IS 'ORDER BY clause used by rowcompress_repack to physically sort rows; NULL disables sort ordering';

-- 2. Replace alter_rowcompress_table_set with 7-argument version ----------------
DROP FUNCTION IF EXISTS engine.alter_rowcompress_table_set(
    regclass, int, name, int, text, bool);

CREATE OR REPLACE FUNCTION engine.alter_rowcompress_table_set(
    table_name        regclass,
    batch_size        int  DEFAULT NULL,
    compression       name DEFAULT NULL,
    compression_level int  DEFAULT NULL,
    pruning_column    text DEFAULT NULL,
    index_scan        bool DEFAULT NULL,
    orderby           text DEFAULT NULL)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_rowcompress_table_set';

COMMENT ON FUNCTION engine.alter_rowcompress_table_set(
    regclass, int, name, int, text, bool, text)
IS 'set one or more options on a rowcompress table; NULL means no change; orderby = ''col1 ASC, col2 DESC'' sets sort key for rowcompress_repack; empty string clears it';

-- 3. Replace alter_rowcompress_table_reset with 6-argument version --------------
DROP FUNCTION IF EXISTS engine.alter_rowcompress_table_reset(
    regclass, bool, bool, bool, bool);

CREATE OR REPLACE FUNCTION engine.alter_rowcompress_table_reset(
    table_name        regclass,
    batch_size        bool DEFAULT false,
    compression       bool DEFAULT false,
    compression_level bool DEFAULT false,
    index_scan        bool DEFAULT false,
    orderby           bool DEFAULT false)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_rowcompress_table_reset';

COMMENT ON FUNCTION engine.alter_rowcompress_table_reset(
    regclass, bool, bool, bool, bool, bool)
IS 'reset one or more options on a rowcompress table to system defaults';

-- 4. Refresh rowcompress_options view to expose orderby -------------------------
CREATE OR REPLACE VIEW engine.rowcompress_options AS
    SELECT
        regclass::text          AS table_name,
        batch_size,
        compression,
        compression_level,
        pruning_attnum,
        COALESCE(index_scan, false) AS index_scan,
        orderby
    FROM engine.row_options
    ORDER BY regclass::text;

COMMENT ON VIEW engine.rowcompress_options
    IS 'per-table options for rowcompress tables (mirrors engine.row_options)';

-- 5. Session-local scan statistics --------------------------------------------

CREATE OR REPLACE FUNCTION engine.rowcompress_scan_stats(
    OUT table_name      text,
    OUT total_scans     bigint,
    OUT batches_total   bigint,
    OUT batches_scanned bigint,
    OUT batches_pruned  bigint,
    OUT pruning_ratio   float4)
RETURNS SETOF record
LANGUAGE C
AS 'MODULE_PATHNAME', 'rowcompress_scan_stats';

COMMENT ON FUNCTION engine.rowcompress_scan_stats()
IS 'Session-local scan statistics for rowcompress tables: total scans, batches examined, batches pruned by min/max, and pruning effectiveness ratio. Accumulated since session start (or last reset). Reset on session end.';

GRANT EXECUTE ON FUNCTION engine.rowcompress_scan_stats() TO PUBLIC;

CREATE OR REPLACE FUNCTION engine.rowcompress_reset_scan_stats(
    table_name regclass DEFAULT NULL)
RETURNS void
LANGUAGE C
AS 'MODULE_PATHNAME', 'rowcompress_reset_scan_stats';

COMMENT ON FUNCTION engine.rowcompress_reset_scan_stats(regclass)
IS 'Reset session-local scan statistics for all rowcompress tables (NULL) or a specific table.';

GRANT EXECUTE ON FUNCTION engine.rowcompress_reset_scan_stats(regclass) TO PUBLIC;

-- 6. Storage Maintenance Auto-Scheduler ----------------------------------------

-- engine.storage_maintenance_auto() — dispatch merge/repack for all tables
-- whose storage_health.recommended_action != 'ok'.
-- Designed to be called from pg_cron, a background worker, or manually.
CREATE OR REPLACE PROCEDURE engine.storage_maintenance_auto(
    dry_run    boolean DEFAULT false,
    max_tables int     DEFAULT NULL,
    am_filter  text    DEFAULT NULL,
    p_verbose  boolean DEFAULT false)
LANGUAGE plpgsql AS $$
DECLARE
    r   record;
    cnt int := 0;
BEGIN
    FOR r IN
        SELECT table_name, am_name, recommended_action
          FROM engine.storage_health
         WHERE recommended_action IN ('run_incremental_merge', 'run_full_repack')
           AND (am_filter IS NULL OR am_name = am_filter)
         ORDER BY table_name
    LOOP
        EXIT WHEN max_tables IS NOT NULL AND cnt >= max_tables;

        IF p_verbose THEN
            RAISE NOTICE 'storage_maintenance_auto: % % -> %',
                r.am_name, r.table_name, r.recommended_action;
        END IF;

        IF NOT dry_run THEN
            IF r.am_name = 'colcompress' THEN
                CALL engine.colcompress_merge_incremental(r.table_name::regclass);
            ELSIF r.am_name = 'rowcompress' THEN
                IF r.recommended_action = 'run_full_repack' THEN
                    PERFORM engine.rowcompress_repack(r.table_name::regclass);
                ELSE
                    CALL engine.rowcompress_merge_incremental(r.table_name::regclass);
                END IF;
            END IF;
        END IF;

        cnt := cnt + 1;
    END LOOP;

    IF p_verbose THEN
        RAISE NOTICE 'storage_maintenance_auto: processed % table(s)', cnt;
    END IF;
END;
$$;

COMMENT ON PROCEDURE engine.storage_maintenance_auto(boolean, int, text, boolean)
IS 'Dispatch colcompress_merge_incremental or rowcompress_merge_incremental for '
   'every table whose storage_health.recommended_action != ''ok''. '
   'dry_run=true lists tables without acting. max_tables limits how many are '
   'processed per call. am_filter restricts to ''colcompress'' or ''rowcompress''. '
   'verbose emits a NOTICE per table. Can be scheduled with pg_cron or run '
   'automatically via the storage_engine background worker.';

GRANT EXECUTE ON PROCEDURE engine.storage_maintenance_auto(boolean, int, text, boolean)
    TO PUBLIC;

-- 7. View improvements ---------------------------------------------------------

-- engine.colcompress_stripes: adiciona pruning_valid e dirty_rows para diagnóstico
CREATE OR REPLACE VIEW engine.colcompress_stripes AS
    SELECT
        c.relname               AS table_name,
        s.storage_id,
        s.stripe_num,
        s.file_offset,
        s.data_length,
        s.column_count,
        s.chunk_row_count,
        s.row_count,
        s.chunk_group_row_count,
        s.first_row_number,
        s.pruning_valid,
        s.dirty_rows
    FROM engine.col_options co
    JOIN pg_class c ON c.oid = co.regclass::oid
    JOIN engine.stripe s ON s.storage_id = engine.colcompress_relation_storageid(co.regclass)
    ORDER BY s.storage_id, s.stripe_num;

COMMENT ON VIEW engine.colcompress_stripes
    IS 'stripe metadata for all colcompress tables (includes pruning_valid and dirty_rows for diagnostics)';

-- engine.rowcompress_batches: adiciona table_name, deleted_count e pruning_valid
CREATE OR REPLACE VIEW engine.rowcompress_batches AS
    SELECT
        c.relname               AS table_name,
        rb.storage_id,
        rb.batch_num,
        rb.file_offset,
        rb.data_length,
        rb.first_row_number,
        rb.row_count,
        rb.deleted_count,
        rb.pruning_valid
    FROM engine.row_options ro
    JOIN pg_class c ON c.oid = ro.regclass::oid
    JOIN engine.row_batch rb ON rb.storage_id = engine.rowcompress_relation_storageid(ro.regclass)
    ORDER BY rb.storage_id, rb.batch_num;

COMMENT ON VIEW engine.rowcompress_batches
    IS 'per-batch metadata for all rowcompress tables (includes table_name, deleted_count and pruning_valid for diagnostics)';
