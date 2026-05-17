-- =============================================================
-- bench_rowcompress_index_cache_probe.sql
-- Reproduz o caminho de Index Scan do rowcompress e mostra os
-- contadores de cache/metadata expostos por
-- engine.rowcompress_scan_stats().
--
-- Uso:
--   psql -p 5432 -d tpch -f bench_rowcompress_index_cache_probe.sql
--
-- Esperado após a correção:
--   - a tabela aparece em engine.rowcompress_scan_stats()
--   - meta_cache_misses / batch_cache_misses / batch_decompresses > 0
-- =============================================================

\set ON_ERROR_STOP on
SET client_min_messages = notice;

DROP SCHEMA IF EXISTS cache_probe CASCADE;
CREATE SCHEMA cache_probe;

CREATE TABLE cache_probe.rc_docs (
    id bigint PRIMARY KEY,
    grp int NOT NULL,
    payload text NOT NULL
) USING rowcompress;

SELECT engine.alter_rowcompress_table_set(
    'cache_probe.rc_docs'::regclass,
    batch_size => 1000,
    compression => 'zstd',
    compression_level => 1,
    index_scan => true,
    orderby => 'id'
);

INSERT INTO cache_probe.rc_docs
SELECT gs,
       gs % 100,
       repeat(md5(gs::text), 4)
FROM generate_series(1, 50000) AS gs;

SELECT engine.rowcompress_repack('cache_probe.rc_docs'::regclass);
SELECT engine.rowcompress_reset_scan_stats();

SET storage_engine.enable_engine_index_scan = on;
SET enable_seqscan = off;
SET enable_bitmapscan = off;

EXPLAIN (ANALYZE, COSTS)
SELECT payload
FROM cache_probe.rc_docs
WHERE id = 4242;

DO $$
DECLARE
    lookup_id bigint;
    outer_batch int;
    repeat_idx int;
BEGIN
    FOR outer_batch IN 0..49 LOOP
        FOR repeat_idx IN 1..20 LOOP
            lookup_id := (outer_batch * 1000) + 42;
            PERFORM payload FROM cache_probe.rc_docs WHERE id = lookup_id;
            PERFORM payload FROM cache_probe.rc_docs WHERE id = lookup_id + 1;
            PERFORM payload FROM cache_probe.rc_docs WHERE id = lookup_id + 2;
            PERFORM payload FROM cache_probe.rc_docs WHERE id = lookup_id + 500;
        END LOOP;
    END LOOP;
END
$$;

SELECT *
FROM engine.rowcompress_scan_stats()
WHERE table_name = 'rc_docs';