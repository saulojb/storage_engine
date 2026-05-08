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
