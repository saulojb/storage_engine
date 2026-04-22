-- storage_engine--1.1--1.2.0.sql
--
-- Upgrade script: 1.1 → 1.2.0
--
-- New in 1.2.0:
--   • engine.row_options gains index_scan bool column
--   • engine.alter_rowcompress_table_set gets an index_scan parameter
--   • engine.alter_rowcompress_table_reset gets an index_scan parameter
--   • engine.rowcompress_options view updated to include index_scan
--

\echo Use "ALTER EXTENSION storage_engine UPDATE TO '1.2.0'" to load this file. \quit

-- 1. New column on row_options --------------------------------------------------
ALTER TABLE engine.row_options
    ADD COLUMN IF NOT EXISTS index_scan bool DEFAULT NULL;

COMMENT ON COLUMN engine.row_options.index_scan
    IS 'true = allow index scans (OLTP mode); NULL/false = remove range index paths (OLAP default)';

-- 2. Replace alter_rowcompress_table_set with the new 6-argument version --------
DROP FUNCTION IF EXISTS engine.alter_rowcompress_table_set(
    regclass, int, name, int, text);

CREATE OR REPLACE FUNCTION engine.alter_rowcompress_table_set(
    table_name        regclass,
    batch_size        int  DEFAULT NULL,
    compression       name DEFAULT NULL,
    compression_level int  DEFAULT NULL,
    pruning_column    text DEFAULT NULL,
    index_scan        bool DEFAULT NULL)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_rowcompress_table_set';

COMMENT ON FUNCTION engine.alter_rowcompress_table_set(
    regclass, int, name, int, text, bool)
IS 'set one or more options on a rowcompress table; NULL means no change; index_scan=true allows index scans (OLTP mode)';

-- 3. Replace alter_rowcompress_table_reset with the new 5-argument version ------
DROP FUNCTION IF EXISTS engine.alter_rowcompress_table_reset(
    regclass, bool, bool, bool);

CREATE OR REPLACE FUNCTION engine.alter_rowcompress_table_reset(
    table_name        regclass,
    batch_size        bool DEFAULT false,
    compression       bool DEFAULT false,
    compression_level bool DEFAULT false,
    index_scan        bool DEFAULT false)
    RETURNS void
    LANGUAGE C
AS 'MODULE_PATHNAME', 'alter_rowcompress_table_reset';

COMMENT ON FUNCTION engine.alter_rowcompress_table_reset(
    regclass, bool, bool, bool, bool)
IS 'reset one or more options on a rowcompress table to system defaults';

-- 4. Refresh rowcompress_options view to expose index_scan ----------------------
CREATE OR REPLACE VIEW engine.rowcompress_options AS
    SELECT
        regclass::text          AS table_name,
        batch_size,
        compression,
        compression_level,
        pruning_attnum,
        COALESCE(index_scan, false) AS index_scan
    FROM engine.row_options
    ORDER BY regclass::text;

COMMENT ON VIEW engine.rowcompress_options
    IS 'per-table options for rowcompress tables (mirrors engine.row_options)';
