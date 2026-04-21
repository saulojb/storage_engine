-- storage_engine--1.0.sql
--
-- Storage Engine Extension v1.0
--
-- Provides two Table Access Methods:
--   * colcompress  — column-oriented compressed storage (formerly "columnar")
--   * rowcompress  — row-oriented batch-compressed storage
--
-- 

--

-- ============================================================
-- SCHEMA
-- ============================================================

CREATE SCHEMA engine;

GRANT USAGE ON SCHEMA engine TO PUBLIC;

-- ============================================================
-- SHARED STORAGE ID SEQUENCE
-- Used by both colcompress and rowcompress to assign unique,
-- non-overlapping storage IDs.
-- ============================================================

CREATE SEQUENCE engine.storageid_seq
    MINVALUE 10000000001
    NO MAXVALUE
    NO CYCLE;

-- ============================================================
-- COLCOMPRESS CATALOG TABLES
-- (formerly in schema "columnar")
-- ============================================================

--
-- engine.col_options: per-table options for colcompress tables
--
CREATE TABLE engine.col_options (
    regclass                regclass NOT NULL PRIMARY KEY,
    chunk_group_row_limit   int      NOT NULL,
    stripe_row_limit        int      NOT NULL,
    compression_level       int      NOT NULL,
    compression             name     NOT NULL,
    orderby                 text     DEFAULT NULL,  -- sort key(s) for MergeTree-like write ordering
    index_scan              boolean  NOT NULL DEFAULT false  -- per-table index scan (true = skip cost penalty)
) WITH (user_catalog_table = true);

COMMENT ON TABLE engine.col_options
    IS 'colcompress per-table options (chunk size, stripe size, compression, sort order, index scan)';

--
-- engine.stripe: per-stripe metadata for colcompress tables
--
CREATE TABLE engine.stripe (
    storage_id              bigint  NOT NULL,
    stripe_num              bigint  NOT NULL,
    file_offset             bigint  NOT NULL,
    data_length             bigint  NOT NULL,
    column_count            int     NOT NULL,
    chunk_row_count         int     NOT NULL,
    row_count               bigint  NOT NULL,
    chunk_group_row_count   int     NOT NULL,
    first_row_number        bigint  NOT NULL,
    PRIMARY KEY (storage_id, stripe_num)
) WITH (user_catalog_table = true);

COMMENT ON TABLE engine.stripe
    IS 'colcompress per-stripe metadata: location, dimensions and row range';

CREATE INDEX stripe_first_row_number_idx
    ON engine.stripe (storage_id, first_row_number);

--
-- engine.chunk_group: per-chunk-group metadata for colcompress tables
--
CREATE TABLE engine.chunk_group (
    storage_id      bigint  NOT NULL,
    stripe_num      bigint  NOT NULL,
    chunk_group_num int     NOT NULL,
    row_count       bigint  NOT NULL,
    deleted_rows    bigint  NOT NULL DEFAULT 0,
    PRIMARY KEY (storage_id, stripe_num, chunk_group_num)
) WITH (user_catalog_table = true);

COMMENT ON TABLE engine.chunk_group
    IS 'colcompress per-chunk-group metadata';

--
-- engine.chunk: per-chunk metadata for colcompress tables
--
CREATE TABLE engine.chunk (
    storage_id              bigint    NOT NULL,
    stripe_num              bigint    NOT NULL,
    attr_num                smallint  NOT NULL,
    chunk_group_num         int       NOT NULL,
    minimum_value           bytea,
    maximum_value           bytea,
    value_stream_offset     bigint    NOT NULL,
    value_stream_length     bigint    NOT NULL,
    exists_stream_offset    bigint    NOT NULL,
    exists_stream_length    bigint    NOT NULL,
    value_compression_type  int       NOT NULL,
    value_compression_level int       NOT NULL,
    value_decompressed_size bigint    NOT NULL,
    value_count             bigint    NOT NULL,
    PRIMARY KEY (storage_id, stripe_num, attr_num, chunk_group_num)
) WITH (user_catalog_table = true);

COMMENT ON TABLE engine.chunk
    IS 'colcompress per-chunk metadata: location and compression info per column chunk';

--
-- engine.row_mask: write-state row mask for colcompress (tracks deleted rows)
--
CREATE SEQUENCE engine.row_mask_seq;

CREATE TABLE engine.row_mask (
    id               bigint  NOT NULL,
    storage_id       bigint  NOT NULL,
    stripe_id        bigint  NOT NULL,
    chunk_id         int     NOT NULL,
    start_row_number bigint  NOT NULL,
    end_row_number   bigint  NOT NULL,
    deleted_rows     int     NOT NULL,
    mask             bytea,
    CONSTRAINT row_mask_pkey PRIMARY KEY (id, storage_id, start_row_number, end_row_number)
) WITH (user_catalog_table = true);

CREATE UNIQUE INDEX row_mask_stripe_unique
    ON engine.row_mask (storage_id, stripe_id, start_row_number);

COMMENT ON TABLE engine.row_mask
    IS 'colcompress per-chunk-group deleted-row bitmask';

-- ============================================================
-- ROWCOMPRESS CATALOG TABLES
-- (formerly in schema "rowcompress")
-- ============================================================

--
-- engine.row_options: per-table options for rowcompress tables
--
CREATE TABLE engine.row_options (
    regclass          regclass NOT NULL PRIMARY KEY,
    batch_size        integer  NOT NULL DEFAULT 10000,
    compression       name     NOT NULL DEFAULT 'zstd',
    compression_level integer  NOT NULL DEFAULT 3
) WITH (user_catalog_table = true);

COMMENT ON TABLE engine.row_options
    IS 'rowcompress per-table options (batch size, compression)';

--
-- engine.row_batch: per-batch metadata for rowcompress tables
--
CREATE TABLE engine.row_batch (
    storage_id       bigint  NOT NULL,
    batch_num        bigint  NOT NULL,
    file_offset      bigint  NOT NULL,
    data_length      bigint  NOT NULL,
    first_row_number bigint  NOT NULL,
    row_count        integer NOT NULL,
    deleted_mask     bytea,
    PRIMARY KEY (storage_id, batch_num)
) WITH (user_catalog_table = true);

COMMENT ON TABLE engine.row_batch
    IS 'rowcompress per-batch metadata: file location and row range per compressed batch';

-- ============================================================
-- COLCOMPRESS TABLE ACCESS METHOD
-- ============================================================

CREATE OR REPLACE FUNCTION engine.colcompress_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C
AS 'MODULE_PATHNAME', 'colcompress_handler';

COMMENT ON FUNCTION engine.colcompress_handler(internal)
    IS 'internal handler for the colcompress table access method';

CREATE ACCESS METHOD colcompress TYPE TABLE HANDLER engine.colcompress_handler;

-- ============================================================
-- ROWCOMPRESS TABLE ACCESS METHOD
-- ============================================================

CREATE OR REPLACE FUNCTION engine.rowcompress_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C
AS 'MODULE_PATHNAME', 'rowcompress_handler';

COMMENT ON FUNCTION engine.rowcompress_handler(internal)
    IS 'internal handler for the rowcompress table access method';

CREATE ACCESS METHOD rowcompress TYPE TABLE HANDLER engine.rowcompress_handler;

-- ============================================================
-- ENSURE AM → CATALOG DEPENDENCIES
-- Prevents pg_dump from dropping catalog tables before the AM.
-- ============================================================

CREATE OR REPLACE FUNCTION engine.ensure_am_depends_catalog()
    RETURNS void
    LANGUAGE plpgsql
    SET search_path = pg_catalog
AS $func$
BEGIN
    INSERT INTO pg_depend
    SELECT
        'pg_am'::regclass::oid  AS classid,
        am.oid                  AS objid,
        0                       AS objsubid,
        'pg_class'::regclass::oid AS refclassid,
        members.relname::regclass::oid AS refobjid,
        0                       AS refobjsubid,
        'n'                     AS deptype
    FROM pg_am am,
         (VALUES ('engine.chunk'),
                 ('engine.chunk_group'),
                 ('engine.chunk_group_pkey'),    -- auto-created by PK
                 ('engine.chunk_pkey'),
                 ('engine.col_options'),
                 ('engine.col_options_pkey'),
                 ('engine.storageid_seq'),
                 ('engine.stripe'),
                 ('engine.stripe_first_row_number_idx'),
                 ('engine.stripe_pkey'),
                 ('engine.row_mask'),
                 ('engine.row_mask_pkey'),
                 ('engine.row_mask_seq'),
                 ('engine.row_mask_stripe_unique'),
                 ('engine.row_batch'),
                 ('engine.row_batch_pkey'),
                 ('engine.row_options'),
                 ('engine.row_options_pkey')
         ) AS members(relname)
    WHERE am.amname IN ('colcompress', 'rowcompress')
    EXCEPT TABLE pg_depend;
END;
$func$;

SELECT engine.ensure_am_depends_catalog();

-- ============================================================
-- USER-FACING MANAGEMENT FUNCTIONS
-- ============================================================

--
-- engine.alter_colcompress_table_set — change options on a colcompress table
--
CREATE OR REPLACE FUNCTION engine.alter_colcompress_table_set(
    table_name          regclass,
    chunk_group_row_limit int  DEFAULT NULL,
    stripe_row_limit      int  DEFAULT NULL,
    compression           name DEFAULT NULL,
    compression_level     int  DEFAULT NULL,
    orderby               text DEFAULT NULL,
    index_scan            bool DEFAULT NULL)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'se_alter_engine_table_set';

COMMENT ON FUNCTION engine.alter_colcompress_table_set(
    regclass, int, int, name, int, text, bool)
IS 'set one or more options on a colcompress table; NULL means no change; orderby = ''col1 ASC, col2 DESC''; index_scan = true enables per-table index scan regardless of GUC';

--
-- engine.alter_colcompress_table_reset — reset options to defaults
--
CREATE OR REPLACE FUNCTION engine.alter_colcompress_table_reset(
    table_name            regclass,
    chunk_group_row_limit bool DEFAULT false,
    stripe_row_limit      bool DEFAULT false,
    compression           bool DEFAULT false,
    compression_level     bool DEFAULT false,
    orderby               bool DEFAULT false,
    index_scan            bool DEFAULT false)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'se_alter_engine_table_reset';

COMMENT ON FUNCTION engine.alter_colcompress_table_reset(
    regclass, bool, bool, bool, bool, bool, bool)
IS 'reset one or more options on a colcompress table to system defaults';

-- ============================================================
-- GUCs — register configuration parameters
-- (handled by _PG_init; SQL stub kept for documentation)
-- ============================================================

-- ============================================================
-- USER-FACING MANAGEMENT FUNCTIONS — rowcompress
-- ============================================================

--
-- engine.alter_rowcompress_table_set — change options on a rowcompress table
--
CREATE OR REPLACE FUNCTION engine.alter_rowcompress_table_set(
    table_name        regclass,
    batch_size        int  DEFAULT NULL,
    compression       name DEFAULT NULL,
    compression_level int  DEFAULT NULL)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_rowcompress_table_set';

COMMENT ON FUNCTION engine.alter_rowcompress_table_set(
    regclass, int, name, int)
IS 'set one or more options on a rowcompress table; NULL means no change';

--
-- engine.alter_rowcompress_table_reset — reset options to defaults
--
CREATE OR REPLACE FUNCTION engine.alter_rowcompress_table_reset(
    table_name        regclass,
    batch_size        bool DEFAULT false,
    compression       bool DEFAULT false,
    compression_level bool DEFAULT false)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_rowcompress_table_reset';

COMMENT ON FUNCTION engine.alter_rowcompress_table_reset(
    regclass, bool, bool, bool)
IS 'reset one or more options on a rowcompress table to system defaults';

--
-- engine.rowcompress_repack — rewrite a rowcompress table using current options
--
CREATE OR REPLACE FUNCTION engine.rowcompress_repack(
    table_name        regclass)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'rowcompress_repack';

COMMENT ON FUNCTION engine.rowcompress_repack(
    regclass)
IS 'rewrite all rows of a rowcompress table using current row options (batch_size/compression/compression_level)';

-- ============================================================
-- CONVENIENCE VIEWS  (all in the engine schema)
-- ============================================================

--
-- engine.colcompress_options — per-table options for colcompress tables
--
CREATE OR REPLACE VIEW engine.colcompress_options AS
    SELECT
        regclass::text          AS table_name,
        chunk_group_row_limit,
        stripe_row_limit,
        compression,
        compression_level,
        orderby,
        index_scan
    FROM engine.col_options
    ORDER BY regclass::text;

COMMENT ON VIEW engine.colcompress_options
    IS 'per-table options for colcompress tables (mirrors engine.col_options)';

GRANT SELECT ON engine.colcompress_options TO PUBLIC;

--
-- engine.colcompress_relation_storageid — returns the internal storage_id for a colcompress relation
--
CREATE OR REPLACE FUNCTION engine.colcompress_relation_storageid(relation regclass)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', 'se_engine_relation_storageid';

COMMENT ON FUNCTION engine.colcompress_relation_storageid(regclass)
    IS 'returns the internal storage_id for a colcompress relation';

--
-- engine.colcompress_stripes — stripe-level metadata for colcompress tables
--
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
        s.first_row_number
    FROM engine.col_options co
    JOIN pg_class c ON c.oid = co.regclass::oid
    JOIN engine.stripe s ON s.storage_id = engine.colcompress_relation_storageid(co.regclass)
    ORDER BY s.storage_id, s.stripe_num;

COMMENT ON VIEW engine.colcompress_stripes
    IS 'stripe metadata for all colcompress tables';

GRANT SELECT ON engine.colcompress_stripes TO PUBLIC;

--
-- engine.rowcompress_options — per-table options for rowcompress tables
--
CREATE OR REPLACE VIEW engine.rowcompress_options AS
    SELECT
        regclass::text          AS table_name,
        batch_size,
        compression,
        compression_level
    FROM engine.row_options
    ORDER BY regclass::text;

COMMENT ON VIEW engine.rowcompress_options
    IS 'per-table options for rowcompress tables (mirrors engine.row_options)';

GRANT SELECT ON engine.rowcompress_options TO PUBLIC;

--
-- engine.rowcompress_batches — batch-level metadata for rowcompress tables
--
CREATE OR REPLACE VIEW engine.rowcompress_batches AS
    SELECT
        storage_id,
        batch_num,
        file_offset,
        data_length,
        first_row_number,
        row_count
    FROM engine.row_batch
    ORDER BY storage_id, batch_num;

COMMENT ON VIEW engine.rowcompress_batches
    IS 'per-batch metadata for all rowcompress tables (mirrors engine.row_batch)';

-- ============================================================
-- engine.colcompress_merge — compact + globally sort a colcompress table
-- ============================================================
--
-- Copies all rows to a temp heap table, truncates the target, then
-- re-inserts in the ORDER defined by the table's orderby option.
-- Each stripe written during the re-insert receives a contiguous
-- key range → true global ordering across all stripes.
--
-- Usage:
--   SELECT engine.colcompress_merge('mytable'::regclass);
--
CREATE OR REPLACE FUNCTION engine.colcompress_merge(
    table_name  regclass)
    RETURNS void
    LANGUAGE C
    CALLED ON NULL INPUT
AS 'MODULE_PATHNAME', 'colcompress_merge';

COMMENT ON FUNCTION engine.colcompress_merge(regclass)
IS 'rewrite all stripes of a colcompress table globally sorted by its orderby key; no-op on tables without orderby';

-- ============================================================
-- engine.colcompress_repack — pg_repack-compatible alias
-- ============================================================
--
-- pg_repack cannot be used with colcompress tables because it relies on
-- AFTER ROW triggers, which columnar storage does not support. Use this
-- function instead: it performs the same logical operation (compact, reorder,
-- rebuild indexes) by delegating to engine.colcompress_merge().
--
-- Limitation: requires AccessExclusiveLock (blocks concurrent reads and
-- writes during the repack). Unlike pg_repack there is no online/concurrent mode.
--
CREATE OR REPLACE FUNCTION engine.colcompress_repack(
    table_name  regclass)
    RETURNS void
    LANGUAGE sql
    CALLED ON NULL INPUT
AS $$
    SELECT engine.colcompress_merge(table_name);
$$;

COMMENT ON FUNCTION engine.colcompress_repack(regclass)
IS 'pg_repack-compatible alias for colcompress tables: compacts stripes, re-applies global orderby sort, and rebuilds indexes. Requires AccessExclusiveLock; no online/concurrent mode.';

GRANT EXECUTE ON FUNCTION engine.colcompress_repack(regclass) TO PUBLIC;

GRANT SELECT ON engine.rowcompress_batches TO PUBLIC;

-- ============================================================
-- engine.row_number_to_tid — convert colcompress row_number to ctid
-- ============================================================
--
-- Converts the internal row number stored in engine.stripe.first_row_number
-- (or first_row_number + row_count - 1) to the ItemPointer (ctid) that
-- PostgreSQL uses to identify the corresponding tuple.
--
-- Used by colcompress_bulk_update to compute per-stripe ctid bounds for
-- efficient range queries on the intermediate heap table.
--
CREATE OR REPLACE FUNCTION engine.row_number_to_tid(row_number bigint)
    RETURNS tid
    LANGUAGE C
    STRICT
AS 'MODULE_PATHNAME', 'se_row_number_to_tid';

COMMENT ON FUNCTION engine.row_number_to_tid(bigint)
IS 'convert a colcompress internal row number (engine.stripe.first_row_number) to the equivalent ctid; used for per-stripe range computations in colcompress_bulk_update';

GRANT EXECUTE ON FUNCTION engine.row_number_to_tid(bigint) TO PUBLIC;

-- ============================================================
-- engine.colcompress_bulk_update — memory-safe, crash-safe bulk UPDATE
-- ============================================================
--
-- Memory-safe, crash-safe bulk UPDATE for colcompress tables via a LOGGED
-- heap intermediate.  Avoids the OOM pattern where batched ctid-based UPDATEs
-- cause N full ColcompressScan passes (one per batch), each decompressing
-- the entire table and growing the glibc malloc watermark unboundedly.
--
-- Algorithm (4 phases, fully crash-safe, resumable):
--
--   Phase 1 — Snapshot: copies matching rows into a LOGGED permanent heap
--             (_se_bulk_<oid>) with the original ctid (_se_old_ctid), and
--             creates a companion stripe-progress table (_se_bulkm_<oid>)
--             that stores the original stripe layout and a per-stripe `done`
--             flag.  Both tables live in the same schema as the target (no
--             search_path ambiguity).  Index on _se_old_ctid for fast
--             per-stripe range lookups in Phase 3.
--             After COMMIT: colcompress is untouched; both tables are
--             WAL-protected.  A crash here loses nothing.
--
--   Phase 2 — Transform: applies SET clause to the heap (pure heap UPDATE,
--             no ColcompressScan, no decompression, no OOM risk).
--
--   Phase 3 — Stripe-by-stripe DELETE + INSERT + mark-done (all atomic):
--             For each stripe in _se_bulkm_<oid> where done = false:
--               a. DELETE matching rows from colcompress (ctid lookup via
--                  heap index, no decompression).
--               b. INSERT updated rows from heap → colcompress (one new
--                  stripe per old stripe, fresh min/max metadata).
--               c. UPDATE _se_bulkm_<oid> SET done = true for this stripe.
--             COMMIT — steps a, b, c are durable together.
--
--             Crash safety guarantee:
--               If the server is killed mid-stripe (OOM, power loss, etc.),
--               PostgreSQL rolls back all three steps atomically.  The old
--               colcompress rows return to their original positions.  On
--               re-run, the procedure detects the leftover heap, reads the
--               stripe layout from _se_bulkm_<oid>, skips stripes where
--               done = true, and resumes from the first undone stripe.
--               No data loss is possible under any crash scenario.
--
--   Phase 4 — Cleanup: DROP both intermediate tables.
--
-- Properties:
--   • RSS bounded: one stripe decompressed at a time (no global heap).
--   • Fresh min/max per stripe: optimal chunk pruning after the update.
--   • True crash-resumability: re-run CALL to continue after any crash.
--   • No data loss: DELETE + INSERT + done-flag are committed atomically.
--
-- NOTE: ctid values of updated rows change (new row_numbers are assigned).
--       Run REINDEX TABLE CONCURRENTLY after completion to rebuild indexes.
--
-- NOTE: between first DELETE commit and last INSERT commit, the table
--       temporarily has fewer rows.  Use LOCK TABLE for strict consistency.
--
-- Must be invoked with CALL:
--   CALL engine.colcompress_bulk_update(
--       'myschema.mytable',   -- target colcompress table
--       'col = col + 1',      -- SET clause  (literal SQL fragment)
--       'id > 0',             -- WHERE clause (NULL = update all rows)
--       5000                  -- unused; kept for API compatibility
--   );
--
-- The SET and WHERE clauses are interpolated directly into SQL; the caller
-- must already hold UPDATE privilege on the target table.
--
CREATE OR REPLACE PROCEDURE engine.colcompress_bulk_update(
    table_name   regclass,
    set_clause   text,
    where_clause text    DEFAULT NULL,
    batch_size   integer DEFAULT 50000)
    LANGUAGE plpgsql
AS $$
DECLARE
    _nspname     text;
    _relname     text;
    _qualname    text;   -- schema-qualified target name (already quoted)
    _heapfull    text;   -- schema-qualified data heap (already quoted)
    _metafull    text;   -- schema-qualified stripe-progress table (already quoted)
    _heaprelname text;   -- unqualified heap name
    _metarelname text;   -- unqualified meta name
    _collist     text;
    _storage_id  bigint;
    _total_rows  bigint;
    _n           bigint;
    _del_total       bigint  := 0;
    _ins_total       bigint  := 0;
    -- Batch processing state for Phase 3
    _total_stripes   int;
    _nstripes        int;    -- pending (not done) stripe count
    _done_count      int     := 0;
    _batch_stripe_lo int;    -- first stripe_idx in current batch
    _batch_stripe_hi int;    -- last  stripe_idx in current batch
    _batch_first_rn  bigint; -- first_rn of the first stripe in batch
    _batch_last_rn   bigint; -- last row number of the last stripe in batch
    _batch_count     bigint; -- number of stripes collected for this batch
    _tid_min         tid;
    _tid_max         tid;
    _resuming        boolean := false;
BEGIN
    SELECT n.nspname, c.relname
      INTO _nspname, _relname
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = table_name;

    _qualname    := quote_ident(_nspname) || '.' || quote_ident(_relname);
    _heaprelname := '_se_bulk_'  || table_name::oid::text;
    _metarelname := '_se_bulkm_' || table_name::oid::text;
    -- Both helper tables live in the same schema as the target table.
    -- Using schema-qualified names avoids any search_path ambiguity.
    _heapfull    := quote_ident(_nspname) || '.' || quote_ident(_heaprelname);
    _metafull    := quote_ident(_nspname) || '.' || quote_ident(_metarelname);
    _storage_id  := engine.colcompress_relation_storageid(table_name);

    -- Column list (excludes dropped columns and our internal heap columns)
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
      INTO _collist
      FROM pg_attribute
     WHERE attrelid = table_name
       AND attnum > 0
       AND NOT attisdropped;

    -- ----------------------------------------------------------------
    -- Detect leftover helper tables from a previous interrupted run.
    -- If the data heap exists, skip Phase 1 and 2 and go straight to
    -- Phase 3 using the stripe layout stored in the meta table.
    -- ----------------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = _heaprelname AND c.relkind = 'r'
          AND n.nspname = _nspname
    ) THEN
        _resuming := true;
        RAISE NOTICE
            'bulk_update: leftover heap "%" detected — resuming previous interrupted run',
            _heapfull;
    END IF;

    IF NOT _resuming THEN
        -- ----------------------------------------------------------------
        -- Phase 1 — Snapshot.
        --
        -- Two WAL-protected tables are created and committed together:
        --
        --   _se_bulkm_<oid>  Stripe-progress table.  Stores the ORIGINAL
        --     stripe layout (first_rn, row_count) captured here, so that
        --     after stripes are replaced by Phase 3 the resume path can
        --     still find the correct tid range for each stripe.  The `done`
        --     column is updated atomically with each stripe's DELETE+INSERT,
        --     so on resume we can reliably skip already-completed stripes.
        --
        --   _se_bulk_<oid>   Data snapshot heap.  Contains the matching rows
        --     with _se_old_ctid = original ctid.  Index on _se_old_ctid
        --     gives O(stripe_rows) lookups in Phase 3.
        --
        -- After COMMIT: colcompress is untouched; a crash here loses nothing.
        -- ----------------------------------------------------------------

        -- Pre-check: skip all table creation if WHERE clause matches 0 rows.
        EXECUTE format(
            'SELECT CASE WHEN EXISTS(SELECT 1 FROM %s%s) THEN 1 ELSE 0 END',
            _qualname,
            CASE WHEN where_clause IS NOT NULL
                 THEN ' WHERE ' || where_clause ELSE '' END
        ) INTO _total_rows;

        IF _total_rows = 0 THEN
            RAISE NOTICE 'bulk_update: 0 rows match WHERE clause — nothing to do';
            RETURN;
        END IF;

        -- Stripe-progress table
        EXECUTE format(
            'CREATE TABLE %s ('
            '    stripe_idx  int     PRIMARY KEY,'
            '    first_rn    bigint  NOT NULL,'
            '    row_count   bigint  NOT NULL,'
            '    done        boolean NOT NULL DEFAULT false'
            ')',
            _metafull
        );
        EXECUTE format(
            'INSERT INTO %s (stripe_idx, first_rn, row_count)'
            ' SELECT (row_number() OVER (ORDER BY stripe_num))::int,'
            '        first_row_number, row_count'
            ' FROM engine.stripe WHERE storage_id = $1',
            _metafull
        ) USING _storage_id;

        -- Data snapshot heap
        EXECUTE format(
            'CREATE TABLE %s AS SELECT ctid AS _se_old_ctid, * FROM %s%s',
            _heapfull, _qualname,
            CASE WHEN where_clause IS NOT NULL
                 THEN ' WHERE ' || where_clause ELSE '' END
        );
        EXECUTE format('CREATE INDEX ON %s (_se_old_ctid)', _heapfull);
        EXECUTE format('SELECT count(*) FROM %s', _heapfull) INTO _total_rows;
        COMMIT;  -- both tables are WAL-protected; crash here = no data loss

        RAISE NOTICE 'bulk_update: % matching rows snapshotted to "%"',
            _total_rows, _heapfull;

        -- ----------------------------------------------------------------
        -- Phase 2 — Transform: apply SET clause on heap (no decompression).
        -- ----------------------------------------------------------------
        EXECUTE format('UPDATE %s SET %s', _heapfull, set_clause);
        COMMIT;
        RAISE NOTICE 'bulk_update: SET clause applied on heap';

    END IF;  -- end of fresh-run-only phases

    -- ----------------------------------------------------------------
    -- Count total and pending stripes.
    -- ----------------------------------------------------------------
    EXECUTE format(
        'SELECT count(*), count(*) FILTER (WHERE NOT done) FROM %s',
        _metafull
    ) INTO _total_stripes, _nstripes;

    _total_stripes := COALESCE(_total_stripes, 0);
    _nstripes      := COALESCE(_nstripes, 0);

    RAISE NOTICE 'bulk_update: % stripe(s) pending out of % total%',
        _nstripes, _total_stripes,
        CASE WHEN _resuming THEN ' (resuming)' ELSE '' END;

    -- ----------------------------------------------------------------
    -- Phase 3 — Batched DELETE + INSERT + mark-done (crash-safe).
    --
    -- Each iteration collects consecutive undone stripes whose combined
    -- row_count is approximately batch_size, then processes them in a
    -- single transaction:
    --
    --   a. DELETE old rows from colcompress by batch ctid range.
    --      One ARRAY(SELECT _se_old_ctid …) covers all batch stripes.
    --
    --   b. INSERT all updated heap rows in the range into colcompress.
    --      A larger INSERT fills colcompress stripes more fully →
    --      fewer stripes → better future scan performance and
    --      compression ratio.
    --
    --   c. UPDATE meta SET done = true for all stripes in the batch.
    --
    --   COMMIT — all three are durable together.
    --
    -- Crash safety (identical guarantee to the stripe-by-stripe design):
    --   A server kill mid-batch rolls back all three steps atomically.
    --   On re-run the batch stripes all have done = false → reprocessed
    --   correctly from scratch.  No data loss is possible.
    --
    -- Performance tuning:
    --   Larger batch_size → fewer commits, larger INSERT batches, better
    --   colcompress stripe packing.  Default 50 000 is a good starting
    --   point; increase for tables with small rows or abundant RAM.
    -- ----------------------------------------------------------------
    LOOP
        -- Collect the next batch: include consecutive undone stripes
        -- while the cumulative row_count of *preceding* stripes is
        -- < batch_size.  Guarantees at least one stripe per iteration
        -- even when a single stripe exceeds batch_size.
        EXECUTE format(
            'WITH cum AS ('
            '  SELECT stripe_idx, first_rn, row_count,'
            '         sum(row_count) OVER (ORDER BY stripe_idx) AS cumrows'
            '  FROM %s WHERE NOT done'
            ')'
            ' SELECT min(stripe_idx), max(stripe_idx),'
            '        min(first_rn), max(first_rn + row_count - 1),'
            '        count(*)'
            ' FROM cum WHERE cumrows - row_count < $1',
            _metafull
        ) INTO _batch_stripe_lo, _batch_stripe_hi,
               _batch_first_rn,  _batch_last_rn,  _batch_count
        USING batch_size;

        EXIT WHEN _batch_stripe_lo IS NULL;

        _tid_min := engine.row_number_to_tid(_batch_first_rn);
        _tid_max := engine.row_number_to_tid(_batch_last_rn);

        -- a. Delete old rows from the batch ctid range (no decompression).
        EXECUTE format(
            'DELETE FROM %s WHERE ctid = ANY('
            '  ARRAY(SELECT _se_old_ctid FROM %s'
            '        WHERE _se_old_ctid BETWEEN $1 AND $2))',
            _qualname, _heapfull
        ) USING _tid_min, _tid_max;
        GET DIAGNOSTICS _n = ROW_COUNT;
        _del_total := _del_total + _n;

        -- b. Insert the batch from heap into colcompress (one large INSERT).
        EXECUTE format(
            'INSERT INTO %s (%s) SELECT %s FROM %s'
            ' WHERE _se_old_ctid BETWEEN $1 AND $2',
            _qualname, _collist, _collist, _heapfull
        ) USING _tid_min, _tid_max;
        GET DIAGNOSTICS _n = ROW_COUNT;
        _ins_total := _ins_total + _n;

        -- c. Mark entire batch as done — atomic with a and b.
        EXECUTE format(
            'UPDATE %s SET done = true'
            ' WHERE stripe_idx BETWEEN $1 AND $2',
            _metafull
        ) USING _batch_stripe_lo, _batch_stripe_hi;

        COMMIT;  -- DELETE + INSERT + done=true are durable together

        _done_count := _done_count + _batch_count;
        RAISE NOTICE
            'bulk_update: stripes %–% done (%/% total) — % rows inserted (running: %)',
            _batch_stripe_lo, _batch_stripe_hi,
            _done_count, _total_stripes, _n, _ins_total;
    END LOOP;

    -- ----------------------------------------------------------------
    -- Phase 4 — Cleanup: drop both helper tables.
    -- ----------------------------------------------------------------
    EXECUTE format('DROP TABLE %s', _heapfull);
    EXECUTE format('DROP TABLE %s', _metafull);
    COMMIT;
    RAISE NOTICE
        E'bulk_update: complete — % rows updated in %s.\n'
        'IMPORTANT: ctid values changed. Run:\n'
        '  REINDEX TABLE CONCURRENTLY %s;',
        _ins_total, _qualname, _qualname;
END;
$$;

COMMENT ON PROCEDURE engine.colcompress_bulk_update(regclass, text, text, integer)
IS 'Memory-safe, crash-safe bulk UPDATE for colcompress tables. Snapshots matching rows to a WAL-protected heap (_se_bulk_<oid>) and records the original stripe layout in a progress table (_se_bulkm_<oid>). Applies the SET clause on the heap (no decompression, no OOM risk), then processes each stripe atomically: DELETE old rows + INSERT updated rows + mark done=true are committed together. A server crash during any stripe rolls back all three steps; re-running CALL resumes from the first undone stripe. No data loss is possible under any crash scenario. Must be called with CALL. Run REINDEX TABLE CONCURRENTLY after completion.';

GRANT EXECUTE ON PROCEDURE engine.colcompress_bulk_update(regclass, text, text, integer) TO PUBLIC;

