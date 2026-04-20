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
-- engine.colcompress_bulk_update — memory-safe bulk UPDATE
-- ============================================================
--
-- Memory-safe bulk UPDATE for colcompress tables via a logged heap
-- intermediate.  Avoids the OOM pattern where batched ctid-based UPDATEs
-- cause N full ColcompressScan passes (one per batch), each decompress-
-- ing the entire table and growing the glibc malloc watermark unboundedly.
--
-- Algorithm (4 phases, all crash-safe):
--
--   Phase 1 — Snapshot: copies matching rows into a LOGGED permanent heap
--             table (_se_bulk_<oid>) including the original ctid.  After
--             COMMIT the original colcompress rows are untouched and the
--             heap is WAL-protected.  A crash here loses nothing.
--
--   Phase 2 — Transform: applies SET clause to the heap (pure heap UPDATE,
--             no colcompress access, no OOM risk).
--
--   Phase 3 — Delete: removes old rows from colcompress in a SINGLE pass.
--             ctid = ANY(ARRAY(...)) with empty projected columns means the
--             ColcompressScan reads stripe metadata only (no decompression).
--             A crash here leaves the heap intact — re-run to re-INSERT.
--
--   Phase 4 — Insert: re-inserts transformed rows from heap → colcompress
--             in a SINGLE INSERT (no loop).  Reads from heap (no
--             decompression, no OOM risk) → 1 stripe, much faster.
--             A crash here: heap survives; re-run procedure to resume.
--
-- Must be invoked with CALL:
--   CALL engine.colcompress_bulk_update(
--       'myschema.mytable',   -- target colcompress table
--       'col = col + 1',      -- SET clause (literal SQL fragment)
--       'id > 0',             -- WHERE clause (NULL = all rows)
--       5000                  -- rows per INSERT batch (default 5000)
--   );
--
-- The SET and WHERE clauses are interpolated directly into SQL; the caller
-- must already hold UPDATE privilege on the target table.
--
-- NOTE: between Phase 3 (DELETE committed) and the end of Phase 4 (INSERT)
-- the table temporarily has fewer rows.  For maintenance windows this is
-- acceptable; wrap in an explicit lock if concurrent reads must be blocked.
--
CREATE OR REPLACE PROCEDURE engine.colcompress_bulk_update(
    table_name   regclass,
    set_clause   text,
    where_clause text    DEFAULT NULL,
    batch_size   integer DEFAULT 5000)
    LANGUAGE plpgsql
AS $$
DECLARE
    _nspname    text;
    _relname    text;
    _qualname   text;
    _heapname   text;
    _collist    text;
    _total_rows bigint;
    _rn_start   bigint  := 1;
    _n          integer;
    _del_total  bigint  := 0;
    _ins_total  bigint  := 0;
BEGIN
    IF batch_size < 10 THEN
        RAISE EXCEPTION 'bulk_update: batch_size must be >= 10';
    END IF;

    SELECT n.nspname, c.relname
      INTO _nspname, _relname
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = table_name;

    _qualname := quote_ident(_nspname) || '.' || quote_ident(_relname);
    _heapname := '_se_bulk_' || table_name::oid::text;

    -- Column list from original table (excludes our internal heap columns)
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
      INTO _collist
      FROM pg_attribute
     WHERE attrelid = table_name
       AND attnum > 0
       AND NOT attisdropped;

    -- Refuse to proceed if a leftover heap from a previous run exists.
    -- The user must inspect and either resume or drop it manually.
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = _heapname
          AND c.relkind = 'r'
          AND n.nspname NOT LIKE 'pg_temp%'
    ) THEN
        RAISE EXCEPTION
            'bulk_update: table "%" exists (leftover from a previous crashed run). '
            'To resume: INSERT INTO % (%s) SELECT %s FROM "%" WHERE _se_rn > <last_n>; '
            'then DROP TABLE "%" '
            'To restart clean: DROP TABLE "%" and re-run.',
            _heapname, _qualname, _collist, _collist, _heapname,
            _heapname, _heapname;
    END IF;

    -- ----------------------------------------------------------------
    -- Phase 1: Snapshot matching rows into a LOGGED (WAL-protected) heap.
    --   _se_old_ctid — original ctid, used for deletion in Phase 3
    --   _se_rn       — sequential row number, used for INSERT batching
    --
    --   After COMMIT: colcompress rows untouched + heap is crash-safe.
    -- ----------------------------------------------------------------
    EXECUTE format(
        'CREATE TABLE %I AS SELECT ctid AS _se_old_ctid, * FROM %s%s',
        _heapname, _qualname,
        CASE WHEN where_clause IS NOT NULL
             THEN ' WHERE ' || where_clause ELSE '' END
    );
    EXECUTE format('ALTER TABLE %I ADD COLUMN _se_rn bigserial', _heapname);
    EXECUTE format('SELECT count(*) FROM %I', _heapname) INTO _total_rows;
    COMMIT;  -- heap is now durable; crash here = no data loss at all

    RAISE NOTICE 'bulk_update: % rows snapshotted to "%" (WAL-protected, crash-safe)',
        _total_rows, _heapname;

    IF _total_rows = 0 THEN
        EXECUTE format('DROP TABLE %I', _heapname);
        RAISE NOTICE 'bulk_update: nothing to do';
        RETURN;
    END IF;

    -- ----------------------------------------------------------------
    -- Phase 2: Apply SET clause on the heap (pure heap UPDATE).
    --   No ColcompressScan, no stripe decompression, no OOM risk.
    -- ----------------------------------------------------------------
    EXECUTE format('UPDATE %I SET %s', _heapname, set_clause);
    COMMIT;
    RAISE NOTICE 'bulk_update: SET clause applied on heap';

    -- ----------------------------------------------------------------
    -- Phase 3: DELETE old rows from colcompress in a single statement.
    --   The ctid = ANY(ARRAY(...)) predicate with empty projected columns
    --   causes ColcompressScan to read stripe metadata only (no column
    --   decompression) → trivial memory footprint.
    --   Crash here: heap still holds all transformed rows → re-INSERT.
    -- ----------------------------------------------------------------
    EXECUTE format(
        'DELETE FROM %s WHERE ctid = ANY(ARRAY(SELECT _se_old_ctid FROM %I))',
        _qualname, _heapname
    );
    GET DIAGNOSTICS _del_total = ROW_COUNT;
    COMMIT;
    RAISE NOTICE 'bulk_update: % old rows deleted from colcompress', _del_total;

    -- ----------------------------------------------------------------
    -- Phase 4: INSERT transformed rows from heap → colcompress (single pass).
    --   Reads from heap (no ColcompressScan, no decompression → no OOM risk).
    --   Single INSERT → single stripe → much faster than batched approach.
    --   Crash here: heap survives; re-run procedure to resume (will detect
    --   leftover heap and emit resume instructions).
    -- ----------------------------------------------------------------
    EXECUTE format(
        'INSERT INTO %s (%s) SELECT %s FROM %I',
        _qualname, _collist, _collist, _heapname
    );
    GET DIAGNOSTICS _n = ROW_COUNT;
    _ins_total := _n;
    COMMIT;
    RAISE NOTICE 'bulk_update: inserted % rows into colcompress', _ins_total;

    -- ----------------------------------------------------------------
    -- Phase 5: Cleanup
    -- ----------------------------------------------------------------
    EXECUTE format('DROP TABLE %I', _heapname);
    RAISE NOTICE 'bulk_update: done — % rows updated in %s', _ins_total, _qualname;
END;
$$;

COMMENT ON PROCEDURE engine.colcompress_bulk_update(regclass, text, text, integer)
IS 'Memory-safe bulk UPDATE for colcompress tables: snapshots rows to a WAL-protected heap, applies the SET clause there, then DELETEs old colcompress rows (1 scan, no decompression) and re-INSERTs from heap in a single INSERT (1 stripe, fast). Avoids OOM from N full decompression passes. Must be called with CALL.';

GRANT EXECUTE ON PROCEDURE engine.colcompress_bulk_update(regclass, text, text, integer) TO PUBLIC;
