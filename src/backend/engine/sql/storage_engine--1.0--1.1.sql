-- storage_engine--1.0--1.1.sql
--
-- Upgrade script: 1.0 → 1.1
--
-- New in 1.1:
--   • engine.row_options gains pruning_attnum for batch-level min/max pruning
--   • engine.row_batch gains batch_min_value / batch_max_value (serialised stats)
--   • engine.alter_rowcompress_table_set gets a pruning_column parameter
--   • RowcompressScan custom scan node with batch pruning and async prefetch
--

\echo Use "ALTER EXTENSION storage_engine UPDATE TO '1.1'" to load this file. \quit

-- 1. New column on row_options --------------------------------------------------
ALTER TABLE engine.row_options
    ADD COLUMN IF NOT EXISTS pruning_attnum int2 DEFAULT NULL;

COMMENT ON COLUMN engine.row_options.pruning_attnum
    IS '1-based attribute number used for batch-level min/max pruning; NULL disables pruning';

-- 2. New columns on row_batch ---------------------------------------------------
ALTER TABLE engine.row_batch
    ADD COLUMN IF NOT EXISTS batch_min_value bytea;

ALTER TABLE engine.row_batch
    ADD COLUMN IF NOT EXISTS batch_max_value bytea;

COMMENT ON COLUMN engine.row_batch.batch_min_value
    IS 'Serialised minimum value of the pruning column for this batch (NULL = no stats)';

COMMENT ON COLUMN engine.row_batch.batch_max_value
    IS 'Serialised maximum value of the pruning column for this batch (NULL = no stats)';

-- 3. Replace alter_rowcompress_table_set with the new 5-argument version --------
DROP FUNCTION IF EXISTS engine.alter_rowcompress_table_set(
    regclass, int, name, int);

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
