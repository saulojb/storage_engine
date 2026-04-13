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
    FROM engine.stripe s
    JOIN pg_class c ON c.relfilenode = (
        SELECT relfilenode FROM pg_class WHERE oid = (
            SELECT regclass FROM engine.col_options co
            WHERE (SELECT relfilenode FROM pg_class WHERE oid = co.regclass) = s.storage_id
            LIMIT 1
        ) LIMIT 1
    )
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

GRANT SELECT ON engine.rowcompress_batches TO PUBLIC;
