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
    compression_level integer  NOT NULL DEFAULT 3,
    pruning_attnum    int2     DEFAULT NULL  -- 1-based attnum for batch-level min/max pruning
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
    batch_min_value  bytea,  -- serialized min of pruning column (NULL = no stats)
    batch_max_value  bytea,  -- serialized max of pruning column (NULL = no stats)
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
    compression_level int  DEFAULT NULL,
    pruning_column    text DEFAULT NULL)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_rowcompress_table_set';

COMMENT ON FUNCTION engine.alter_rowcompress_table_set(
    regclass, int, name, int, text)
IS 'set one or more options on a rowcompress table; NULL means no change; pruning_column enables batch-level min/max pruning';

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
    table_name    regclass,
    set_clause    text,
    where_clause  text      DEFAULT NULL,
    batch_size    integer   DEFAULT 50000,
    skip_columns  text[]    DEFAULT NULL)
    LANGUAGE plpgsql
AS $$
-- -----------------------------------------------------------------------
-- skip_columns: columns excluded from Phase 1 snapshot.
-- They are buffered per-batch in a TEMP TABLE ON COMMIT DROP during
-- Phase 3, allowing DELETE-before-INSERT without losing blob data.
-- Each batch decompresses only batch_size rows of blobs at a time.
-- Keep batch_size small for blob-heavy tables (e.g. 5000).
--
-- Safety: columns in skip_columns MUST NOT appear in set_clause.
-- -----------------------------------------------------------------------
DECLARE
    _nspname           text;
    _relname           text;
    _qualname          text;
    _heapfull          text;
    _metafull          text;
    _heaprelname       text;
    _metarelname       text;
    _collist           text;
    _small_collist     text;
    _blob_collist      text;
    _mixed_select_list text;
    _has_skip          boolean;
    _storage_id        bigint;
    _total_rows        bigint;
    _n                 bigint;
    _del_total         bigint  := 0;
    _ins_total         bigint  := 0;
    _total_stripes     int;
    _nstripes          int;
    _done_count        int     := 0;
    _batch_stripe_lo   int;
    _batch_stripe_hi   int;
    _batch_first_rn    bigint;
    _batch_last_rn     bigint;
    _batch_count       bigint;
    _tid_min           tid;
    _tid_max           tid;
    _resuming          boolean := false;
    _detected_skip     text[];
    _saved_timeout     text;
    _saved_lock_timeout text;
BEGIN
    -- Disable statement_timeout and lock_timeout for the duration of this
    -- maintenance procedure.  Long-running batches (large blobs, many rows)
    -- would otherwise be cancelled by the server's configured timeouts.
    -- Both are restored at the end (and on any error, the session ends or
    -- the caller can reconnect — no permanent side-effect).
    _saved_timeout      := current_setting('statement_timeout');
    _saved_lock_timeout := current_setting('lock_timeout');
    PERFORM set_config('statement_timeout', '0', false);
    PERFORM set_config('lock_timeout',      '0', false);

    SELECT n.nspname, c.relname
      INTO _nspname, _relname
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = table_name;

    _qualname    := quote_ident(_nspname) || '.' || quote_ident(_relname);
    _heaprelname := '_se_bulk_'  || table_name::oid::text;
    _metarelname := '_se_bulkm_' || table_name::oid::text;
    _heapfull    := quote_ident(_nspname) || '.' || quote_ident(_heaprelname);
    _metafull    := quote_ident(_nspname) || '.' || quote_ident(_metarelname);
    _storage_id  := engine.colcompress_relation_storageid(table_name);

    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
      INTO _collist
      FROM pg_attribute
     WHERE attrelid = table_name AND attnum > 0 AND NOT attisdropped;

    -- ----------------------------------------------------------------
    -- Detect resume from a previous interrupted run.
    -- Auto-detect skip_columns from heap structure (columns present in
    -- the original table but absent from the snapshot heap were skipped).
    -- ----------------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = _heaprelname AND c.relkind = 'r'
          AND n.nspname = _nspname
    ) THEN
        _resuming := true;

        SELECT COALESCE(array_agg(a.attname ORDER BY a.attnum), ARRAY[]::text[])
          INTO _detected_skip
          FROM pg_attribute a
         WHERE a.attrelid = table_name
           AND a.attnum > 0 AND NOT a.attisdropped
           AND NOT EXISTS (
               SELECT 1 FROM pg_attribute h
                WHERE h.attrelid = (
                    SELECT c2.oid FROM pg_class c2
                    JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
                    WHERE c2.relname = _heaprelname AND n2.nspname = _nspname
                )
                  AND h.attname = a.attname
           );

        IF array_length(_detected_skip, 1) > 0 THEN
            skip_columns := _detected_skip;
            RAISE NOTICE 'bulk_update: auto-detected skip_columns = %', skip_columns;
        END IF;

        RAISE NOTICE
            'bulk_update: leftover heap "%" detected — resuming previous interrupted run',
            _heapfull;
    END IF;

    _has_skip := skip_columns IS NOT NULL AND array_length(skip_columns, 1) > 0;

    IF _has_skip AND EXISTS (
        SELECT 1 FROM unnest(skip_columns) AS sc
         WHERE position(lower(sc) IN lower(set_clause)) > 0
    ) THEN
        RAISE EXCEPTION
            'bulk_update: a column listed in skip_columns appears in set_clause';
    END IF;

    -- Heap snapshot columns (excludes skip_columns)
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
      INTO _small_collist
      FROM pg_attribute
     WHERE attrelid = table_name AND attnum > 0 AND NOT attisdropped
       AND (NOT _has_skip OR NOT attname = ANY(skip_columns));

    -- Blob column list for per-batch temp table
    IF _has_skip THEN
        SELECT string_agg(quote_ident(col), ', ')
          INTO _blob_collist
          FROM unnest(skip_columns) AS col;
    END IF;

    -- Phase 3 SELECT list: 't.<col>' for skip_columns (temp), 'h.<col>' for others (heap)
    SELECT string_agg(
               CASE WHEN _has_skip AND attname = ANY(skip_columns)
                    THEN 't.' || quote_ident(attname)
                    ELSE 'h.' || quote_ident(attname)
               END,
               ', ' ORDER BY attnum
           )
      INTO _mixed_select_list
      FROM pg_attribute
     WHERE attrelid = table_name AND attnum > 0 AND NOT attisdropped;

    IF NOT _resuming THEN
        -- ----------------------------------------------------------------
        -- Phase 1 — Snapshot.
        -- _se_bulkm_<oid>: stripe-progress table with ORIGINAL stripe layout.
        -- _se_bulk_<oid>:  snapshot heap (all cols, or minus skip_columns).
        -- After COMMIT: colcompress untouched; crash here loses nothing.
        -- ----------------------------------------------------------------
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

        EXECUTE format(
            'CREATE TABLE %s AS SELECT ctid AS _se_old_ctid, %s FROM %s%s',
            _heapfull, _small_collist, _qualname,
            CASE WHEN where_clause IS NOT NULL
                 THEN ' WHERE ' || where_clause ELSE '' END
        );
        EXECUTE format('CREATE INDEX ON %s (_se_old_ctid)', _heapfull);
        EXECUTE format('SELECT count(*) FROM %s', _heapfull) INTO _total_rows;
        COMMIT;

        RAISE NOTICE 'bulk_update: % matching rows snapshotted to "%" (skip_columns: %)',
            _total_rows, _heapfull,
            CASE WHEN _has_skip THEN skip_columns::text ELSE 'none' END;

        -- ----------------------------------------------------------------
        -- Phase 2 — Transform: apply SET clause on heap (no decompression).
        -- ----------------------------------------------------------------
        EXECUTE format('UPDATE %s SET %s', _heapfull, set_clause);
        COMMIT;
        RAISE NOTICE 'bulk_update: SET clause applied on heap';

    END IF;

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
    -- Phase 3 — Batched DELETE-then-INSERT + mark-done (crash-safe).
    --
    -- WITHOUT skip_columns:
    --   a. DELETE old rows by ctid (no blob decompression).
    --   b. INSERT from heap (all columns present).
    --   c. UPDATE meta SET done=true.
    --   COMMIT.
    --
    -- WITH skip_columns:
    --   0. CREATE TEMP TABLE _se_blobs_tmp ON COMMIT DROP AS
    --        SELECT ctid AS _se_ctid, <skip_cols> FROM colcompress
    --        WHERE ctid BETWEEN $1 AND $2.
    --      Only batch_size rows buffered — bounded memory cost.
    --      ON COMMIT DROP: destroyed automatically at COMMIT (and on crash).
    --   a. DELETE old rows by ctid (blobs safe in temp table).
    --   b. INSERT joining heap (small/modified cols) with temp (blob cols).
    --   c. UPDATE meta SET done=true.
    --   COMMIT — ON COMMIT DROP destroys _se_blobs_tmp.
    --
    -- Why DELETE-before-INSERT (not INSERT-before-DELETE):
    --   INSERT-first causes duplicate key violations when the table has
    --   unique constraints: the old rows are still visible in the same
    --   transaction.  DELETE-first avoids this entirely.
    --
    -- Crash safety: a server kill rolls back DELETE+INSERT+done atomically.
    --   Temp table is never durable — vanishes on crash.  On re-run,
    --   done=false -> batch retried from scratch.  No data loss possible.
    -- ----------------------------------------------------------------
    LOOP
        -- Re-apply after each COMMIT: PostgreSQL resets session GUCs that were
        -- overridden by set_config when a new transaction starts inside a procedure.
        PERFORM set_config('statement_timeout', '0', false);
        PERFORM set_config('lock_timeout',      '0', false);

        EXECUTE format(
            'WITH cum AS ('
            '  SELECT stripe_idx, first_rn, row_count,'
            '         sum(row_count) OVER (ORDER BY stripe_idx) AS cumrows'
            '  FROM %s WHERE NOT done'
            ')'
            ' SELECT min(stripe_idx), max(stripe_idx),'
            '        min(first_rn), max(first_rn + row_count - 1), count(*)'
            ' FROM cum WHERE cumrows - row_count < $1',
            _metafull
        ) INTO _batch_stripe_lo, _batch_stripe_hi,
               _batch_first_rn, _batch_last_rn, _batch_count
        USING batch_size;

        EXIT WHEN _batch_stripe_lo IS NULL;

        _tid_min := engine.row_number_to_tid(_batch_first_rn);
        _tid_max := engine.row_number_to_tid(_batch_last_rn);

        -- 0. With skip_columns: buffer blob cols for this batch into a temp table.
        --    ON COMMIT DROP ensures automatic cleanup at COMMIT (and on crash).
        IF _has_skip THEN
            EXECUTE format(
                'CREATE TEMP TABLE _se_blobs_tmp ON COMMIT DROP AS'
                ' SELECT ctid AS _se_ctid, %s FROM %s'
                ' WHERE ctid BETWEEN $1 AND $2',
                _blob_collist, _qualname
            ) USING _tid_min, _tid_max;
        END IF;

        -- a. DELETE old rows by ctid (no blob decompression needed).
        EXECUTE format(
            'DELETE FROM %s WHERE ctid = ANY('
            '  ARRAY(SELECT _se_old_ctid FROM %s'
            '        WHERE _se_old_ctid BETWEEN $1 AND $2))',
            _qualname, _heapfull
        ) USING _tid_min, _tid_max;
        GET DIAGNOSTICS _n = ROW_COUNT;
        _del_total := _del_total + _n;

        -- b. INSERT updated rows into colcompress.
        IF _has_skip THEN
            -- Join heap (small/modified cols) with temp table (blob cols).
            EXECUTE format(
                'INSERT INTO %s (%s)'
                ' SELECT %s'
                ' FROM %s h JOIN _se_blobs_tmp t ON t._se_ctid = h._se_old_ctid'
                ' WHERE h._se_old_ctid BETWEEN $1 AND $2',
                _qualname, _collist, _mixed_select_list, _heapfull
            ) USING _tid_min, _tid_max;
        ELSE
            EXECUTE format(
                'INSERT INTO %s (%s) SELECT %s FROM %s'
                ' WHERE _se_old_ctid BETWEEN $1 AND $2',
                _qualname, _collist, _collist, _heapfull
            ) USING _tid_min, _tid_max;
        END IF;
        GET DIAGNOSTICS _n = ROW_COUNT;
        _ins_total := _ins_total + _n;

        -- c. Mark entire batch as done — atomic with a and b.
        EXECUTE format(
            'UPDATE %s SET done = true WHERE stripe_idx BETWEEN $1 AND $2',
            _metafull
        ) USING _batch_stripe_lo, _batch_stripe_hi;

        COMMIT;  -- DELETE + INSERT + done=true; ON COMMIT DROP fires here

        _done_count := _done_count + _batch_count;
        RAISE NOTICE
            'bulk_update: stripes %–% done (%/% total) — % rows inserted (running: %)',
            _batch_stripe_lo, _batch_stripe_hi,
            _done_count, _total_stripes, _n, _ins_total;
    END LOOP;

    -- ----------------------------------------------------------------
    -- Phase 4 — Cleanup.
    -- ----------------------------------------------------------------
    EXECUTE format('DROP TABLE %s', _heapfull);
    EXECUTE format('DROP TABLE %s', _metafull);
    -- Restore timeouts before final COMMIT so the session is clean.
    PERFORM set_config('statement_timeout', _saved_timeout,      false);
    PERFORM set_config('lock_timeout',      _saved_lock_timeout, false);
    COMMIT;
    RAISE NOTICE
        E'bulk_update: complete — % rows updated in %s.\n'
        'IMPORTANT: ctid values changed. Run:\n'
        '  REINDEX TABLE CONCURRENTLY %s;',
        _ins_total, _qualname, _qualname;
END;
$$;

COMMENT ON PROCEDURE engine.colcompress_bulk_update(regclass, text, text, integer, text[])
IS 'Memory-safe, crash-safe bulk UPDATE for colcompress tables. Snapshots matching rows (minus skip_columns) to a WAL-protected heap. With skip_columns, blob columns are excluded from Phase 1 (tiny snapshot) and buffered per-batch in a TEMP TABLE ON COMMIT DROP during Phase 3, keeping peak memory proportional to batch_size. Phase 3 order: buffer blobs -> DELETE old rows -> INSERT (heap + temp) -> mark done=true -> COMMIT (temp auto-destroyed). A server crash rolls back the entire batch; re-running CALL resumes from the first undone stripe. No data loss possible. Use smaller batch_size with skip_columns for large-blob tables. Run REINDEX TABLE CONCURRENTLY after completion.';

GRANT EXECUTE ON PROCEDURE engine.colcompress_bulk_update(regclass, text, text, integer, text[]) TO PUBLIC;

-- ============================================================
-- engine.smart_update — stripe-grouped full rewrite for colcompress tables
-- ============================================================
--
-- Performs UPDATE via per-stripe full rewrite — the only correct approach
-- for colcompress tables at any scale.
--
-- Why a direct UPDATE is NEVER appropriate for colcompress:
--   • engine_tuple_update() marks the old row deleted (hole in the original
--     stripe) and appends the new row as a new 1-row stripe.
--   • Even a handful of rows causes fragmentation: holes + tiny stripes.
--   • On a 100M-row table with 30% of rows changing, a direct UPDATE would
--     create 30M orphan holes and 30M single-row stripes — catastrophic for
--     compression ratio and scan performance.
--   • There is no safe absolute threshold: 150k rows may be 0.0001% on a
--     huge table (fine) or 100% on a small one (terrible).
--
-- Per-stripe rewrite algorithm (for each stripe with ≥ 1 matching row):
--   a. READ all rows from the stripe into a TEMP TABLE (heap, bounded memory).
--   b. UPDATE the temp table in-place: SET clause applied to matching rows
--      (pure heap UPDATE, zero colcompress overhead).
--   c. DELETE ALL rows from the stripe in colcompress
--      (marks deleted-bits in row_mask — no decompression).
--   d. INSERT all rows back (modified + unchanged) into colcompress.
--      One new, fully-packed stripe written; fresh min/max metadata.
--   e. COMMIT — ON COMMIT DROP destroys temp table atomically.
--   Stripes with zero matching rows are skipped entirely (fast ctid check).
--
-- Properties:
--   • No holes — every rewritten stripe is fully packed.
--   • Fresh min/max metadata → optimal chunk pruning on subsequent scans.
--   • Memory bounded to one stripe at a time regardless of table size.
--   • Crash-safe: each stripe commit is atomic; re-run CALL to retry.
--   • Correct at any scale: 1 row or 1 billion rows, 0.001% or 100%.
--
-- Arguments:
--   table_name    regclass  — target colcompress table
--   set_clause    text      — verbatim SET clause  (e.g. 'col = expr')
--   where_clause  text      — optional WHERE filter (NULL = all rows)
--   row_threshold bigint    — DEPRECATED, ignored; kept for API compatibility
--
-- Must be called with CALL:
--   CALL engine.smart_update('myschema.mytable', 'col = expr', 'id > 0');
--
-- NOTE: After completion, ctid values of rewritten rows change.
--   Run:  REINDEX TABLE CONCURRENTLY <table>;
--
CREATE OR REPLACE PROCEDURE engine.smart_update(
    table_name    regclass,
    set_clause    text,
    where_clause  text    DEFAULT NULL,
    row_threshold bigint  DEFAULT 150000)   -- deprecated, ignored
LANGUAGE plpgsql
AS $$
DECLARE
    _nspname             text;
    _relname             text;
    _qualname            text;
    _collist             text;
    _storage_id          bigint;
    _stripe_num          bigint;
    _first_rn            bigint;
    _last_rn             bigint;
    _tid_min             tid;
    _tid_max             tid;
    _rows_in_stripe      bigint;
    _n_ins               bigint;
    _total_ins           bigint := 0;
    _n_stripes_rewritten int    := 0;
    _saved_timeout       text;
    _saved_lock_timeout  text;
BEGIN
    -- ----------------------------------------------------------------
    -- Resolve qualified name and validate table type
    -- ----------------------------------------------------------------
    SELECT n.nspname, c.relname
      INTO _nspname, _relname
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = table_name;

    _qualname := quote_ident(_nspname) || '.' || quote_ident(_relname);

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_am a ON a.oid = c.relam
        WHERE c.oid = table_name AND a.amname = 'colcompress'
    ) THEN
        RAISE EXCEPTION 'smart_update: % is not a colcompress table', _qualname;
    END IF;

    -- ----------------------------------------------------------------
    -- Stripe-grouped full rewrite (always — see header comment)
    -- ----------------------------------------------------------------
    RAISE NOTICE 'smart_update: stripe-grouped rewrite starting on %', _qualname;

    _saved_timeout      := current_setting('statement_timeout');
    _saved_lock_timeout := current_setting('lock_timeout');
    PERFORM set_config('statement_timeout', '0', false);
    PERFORM set_config('lock_timeout',      '0', false);

    -- Use at most half the available parallel workers so smart_update
    -- does not crowd out production queries on servers with many CPUs.
    -- Integer division floors naturally: 1/2=0 (serial), 2/2=1, 4/2=2, …
    -- 0 disables parallel gather entirely, which is correct when
    -- max_parallel_workers is 0 or 1.
    PERFORM set_config(
        'max_parallel_workers_per_gather',
        (current_setting('max_parallel_workers')::int / 2)::text,
        false
    );

    _storage_id := engine.colcompress_relation_storageid(table_name);

    -- Build full column list for INSERT/SELECT
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
      INTO _collist
      FROM pg_attribute
     WHERE attrelid = table_name AND attnum > 0 AND NOT attisdropped;

    -- Iterate over stripes in physical order
    FOR _stripe_num, _first_rn, _last_rn IN
        SELECT stripe_num,
               first_row_number,
               first_row_number + row_count - 1
          FROM engine.stripe
         WHERE storage_id = _storage_id
         ORDER BY stripe_num
    LOOP
        -- Re-apply after each COMMIT: PostgreSQL resets session GUCs that were
        -- overridden by set_config when a new transaction starts inside a procedure.
        PERFORM set_config('statement_timeout', '0', false);
        PERFORM set_config('lock_timeout',      '0', false);

        _tid_min := engine.row_number_to_tid(_first_rn);
        _tid_max := engine.row_number_to_tid(_last_rn);

        -- Fast check: does this stripe contain any matching row?
        EXECUTE format(
            'SELECT count(*) FROM %s WHERE ctid BETWEEN $1 AND $2%s',
            _qualname,
            CASE WHEN where_clause IS NOT NULL
                 THEN ' AND (' || where_clause || ')' ELSE '' END
        ) USING _tid_min, _tid_max
          INTO _rows_in_stripe;

        CONTINUE WHEN _rows_in_stripe = 0;

        -- a. Buffer the ENTIRE stripe into a temp heap table (bounded memory)
        EXECUTE format(
            'CREATE TEMP TABLE _se_stripe_tmp ON COMMIT DROP AS'
            ' SELECT %s FROM %s WHERE ctid BETWEEN $1 AND $2',
            _collist, _qualname
        ) USING _tid_min, _tid_max;

        -- b. Apply SET clause to matching rows in the temp table (cheap heap UPDATE)
        EXECUTE format(
            'UPDATE _se_stripe_tmp SET %s%s',
            set_clause,
            CASE WHEN where_clause IS NOT NULL
                 THEN ' WHERE ' || where_clause ELSE '' END
        );

        -- c. DELETE ALL rows from this stripe in colcompress
        --    (just marks deleted bits in row_mask — no decompression)
        EXECUTE format(
            'DELETE FROM %s WHERE ctid BETWEEN $1 AND $2',
            _qualname
        ) USING _tid_min, _tid_max;

        -- d. INSERT all rows back (modified + unchanged) into colcompress.
        --    This writes one new, fully-packed stripe with fresh min/max metadata.
        EXECUTE format(
            'INSERT INTO %s (%s) SELECT %s FROM _se_stripe_tmp',
            _qualname, _collist, _collist
        );
        GET DIAGNOSTICS _n_ins = ROW_COUNT;

        -- e. COMMIT — ON COMMIT DROP destroys _se_stripe_tmp atomically.
        --    If the server is killed here, the entire stripe (DELETE+INSERT)
        --    is rolled back and the original rows are restored.
        COMMIT;

        _total_ins          := _total_ins + _n_ins;
        _n_stripes_rewritten := _n_stripes_rewritten + 1;

        RAISE NOTICE 'smart_update: stripe % — % rows rewritten (% matching), running total: %',
            _stripe_num, _n_ins, _rows_in_stripe, _total_ins;
    END LOOP;

    PERFORM set_config('statement_timeout', _saved_timeout,      false);
    PERFORM set_config('lock_timeout',      _saved_lock_timeout, false);
    COMMIT;

    RAISE NOTICE
        E'smart_update: complete — % rows rewritten across % stripe(s).\n'
        'IMPORTANT: ctid values of rewritten rows changed. Run:\n'
        '  REINDEX TABLE CONCURRENTLY %s;',
        _total_ins, _n_stripes_rewritten, _qualname;
END;
$$;

COMMENT ON PROCEDURE engine.smart_update(regclass, text, text, bigint)
IS 'Stripe-grouped full rewrite for colcompress tables. Always rewrites at the stripe level regardless of how many rows are affected: reads the entire affected stripe into a bounded temp heap, applies SET in-place, deletes all stripe rows (deleted-bit only, no decompression), and reinserts all rows — producing a fully packed new stripe with fresh min/max metadata. One COMMIT per stripe; crash-safe (re-run CALL to retry any rolled-back stripe). The row_threshold parameter is deprecated and ignored. Run REINDEX TABLE CONCURRENTLY after completion.';

GRANT EXECUTE ON PROCEDURE engine.smart_update(regclass, text, text, bigint) TO PUBLIC;


