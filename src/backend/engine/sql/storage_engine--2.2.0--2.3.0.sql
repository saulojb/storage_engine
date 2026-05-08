-- storage_engine--2.2.0--2.3.0.sql
--
-- Upgrade script: 2.2.0 → 2.3.0
--
-- New in 2.3.0:
--   • Arco 1: CH-style transform codecs: delta, doubledelta, gorilla
--             (pure-C, no chDB dependency — valid options for compression= in
--              colcompress and rowcompress tables)
--   • Arco 2: chcompress — new Table Access Method backed by embedded ClickHouse
--             (requires libchdb at runtime; gracefully errors without it)
--   • engine.ch_options — options catalog for chcompress tables
--   • chcompress_handler() — TAM handler function
--   • CREATE ACCESS METHOD chcompress
--
-- Copyright (c) Saulo J. Benvenutti, 2026
-- License: PostgreSQL License
--

-- -----------------------------------------------------------------------
-- 1. engine.ch_options — options catalog for chcompress tables
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS engine.ch_options (
    relation_id  OID     NOT NULL,
    ch_engine    TEXT    NOT NULL DEFAULT 'MergeTree',
    order_by     TEXT    NOT NULL DEFAULT 'tuple()',
    partition_by TEXT,
    primary_key  TEXT,
    settings     TEXT,
    CONSTRAINT ch_options_pkey PRIMARY KEY (relation_id)
);

COMMENT ON TABLE engine.ch_options
    IS 'Per-relation options for chcompress Table AM.  Populated by '
       'ALTER TABLE ... SET (ch_engine = ..., order_by = ...) or by '
       'the chcompress CREATE TABLE hook.  relation_id is the pg_class OID.';

COMMENT ON COLUMN engine.ch_options.ch_engine
    IS 'ClickHouse engine family (default: MergeTree).  '
       'Also accepted: ReplacingMergeTree, CollapsingMergeTree, SummingMergeTree, etc.';
COMMENT ON COLUMN engine.ch_options.order_by
    IS 'ClickHouse ORDER BY expression.  Default tuple() = no physical sort.';
COMMENT ON COLUMN engine.ch_options.partition_by
    IS 'ClickHouse PARTITION BY expression (optional).';
COMMENT ON COLUMN engine.ch_options.primary_key
    IS 'ClickHouse PRIMARY KEY expression (optional; defaults to ORDER BY when not set).';
COMMENT ON COLUMN engine.ch_options.settings
    IS 'Additional ClickHouse ENGINE SETTINGS key=value pairs (optional).';

-- -----------------------------------------------------------------------
-- 2. chcompress_handler() — TAM handler
-- -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION engine.chcompress_handler(internal)
    RETURNS table_am_handler
    LANGUAGE C STRICT
    AS '$libdir/storage_engine', 'chcompress_handler';

COMMENT ON FUNCTION engine.chcompress_handler(internal)
    IS 'Table AM handler for chcompress — embedded ClickHouse storage engine.  '
       'Requires libchdb to be installed at runtime (curl -sL https://lib.chdb.io | bash).';

-- -----------------------------------------------------------------------
-- 3. CREATE ACCESS METHOD chcompress
-- -----------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_am WHERE amname = 'chcompress'
    ) THEN
        CREATE ACCESS METHOD chcompress TYPE TABLE HANDLER engine.chcompress_handler;
    END IF;
END
$$;

COMMENT ON ACCESS METHOD chcompress
    IS 'chcompress Table AM: embedded ClickHouse (chDB) storage.  '
       'Append-optimised, non-transactional, supports MergeTree family engines.  '
       'Requires libchdb: curl -sL https://lib.chdb.io | bash';

-- -----------------------------------------------------------------------
-- 4. engine.chcompress_options view
-- -----------------------------------------------------------------------
CREATE OR REPLACE VIEW engine.chcompress_options AS
SELECT
    c.oid                                    AS relation_id,
    n.nspname || '.' || c.relname           AS table_name,
    COALESCE(o.ch_engine,    'MergeTree')   AS ch_engine,
    COALESCE(o.order_by,     'tuple()')     AS order_by,
    o.partition_by,
    o.primary_key,
    o.settings
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN engine.ch_options o ON o.relation_id = c.oid
WHERE c.relam = (SELECT oid FROM pg_am WHERE amname = 'chcompress');

COMMENT ON VIEW engine.chcompress_options
    IS 'Per-table options for all chcompress tables in the current database.';

GRANT SELECT ON engine.chcompress_options TO PUBLIC;
