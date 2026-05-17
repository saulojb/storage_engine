-- =============================================================
-- bench_rowcompress_index_cache_bench.sql
-- Benchmark cold/warm do cache backend-local de rowcompress
-- no caminho de Index Scan.
--
-- Uso:
--   psql -p 5432 -d tpch -f bench_rowcompress_index_cache_bench.sql
--
-- Saida esperada apos o redesign:
--   - fase cold: batch_cache_misses ~= numero de batches tocados
--   - fase warm: batch_cache_misses = 0 e batch_cache_hits = total de lookups
-- =============================================================

\set ON_ERROR_STOP on
\timing on
SET client_min_messages = notice;

DROP SCHEMA IF EXISTS cache_probe_bench CASCADE;
CREATE SCHEMA cache_probe_bench;

CREATE TABLE cache_probe_bench.rc_docs (
    id bigint PRIMARY KEY,
    grp int NOT NULL,
    payload text NOT NULL
) USING rowcompress;

SELECT engine.alter_rowcompress_table_set(
    'cache_probe_bench.rc_docs'::regclass,
    batch_size => 1000,
    compression => 'zstd',
    compression_level => 1,
    index_scan => true,
    orderby => 'id'
);

INSERT INTO cache_probe_bench.rc_docs
SELECT gs,
       gs % 100,
       repeat(md5(gs::text), 4)
FROM generate_series(1, 50000) AS gs;

SELECT engine.rowcompress_repack('cache_probe_bench.rc_docs'::regclass);

SET storage_engine.enable_engine_index_scan = on;
SET enable_seqscan = off;
SET enable_bitmapscan = off;

CREATE TEMP TABLE rc_cache_bench_stats (
    phase text,
    elapsed_ms numeric,
    table_name text,
    total_scans bigint,
    batches_total bigint,
    batches_scanned bigint,
    batches_pruned bigint,
    pruning_ratio numeric,
    meta_cache_hits bigint,
    meta_cache_misses bigint,
    batch_cache_hits bigint,
    batch_cache_misses bigint,
    batch_cache_evictions bigint,
    batch_decompresses bigint
);

CREATE OR REPLACE FUNCTION pg_temp.run_rc_lookup_phase(phase_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    started timestamptz;
    finished timestamptz;
    lookup_id bigint;
    outer_batch int;
    repeat_idx int;
BEGIN
    PERFORM engine.rowcompress_reset_scan_stats();

    started := clock_timestamp();
    FOR outer_batch IN 0..49 LOOP
        FOR repeat_idx IN 1..20 LOOP
            lookup_id := (outer_batch * 1000) + 42;
            PERFORM payload FROM cache_probe_bench.rc_docs WHERE id = lookup_id;
            PERFORM payload FROM cache_probe_bench.rc_docs WHERE id = lookup_id + 1;
            PERFORM payload FROM cache_probe_bench.rc_docs WHERE id = lookup_id + 2;
            PERFORM payload FROM cache_probe_bench.rc_docs WHERE id = lookup_id + 500;
        END LOOP;
    END LOOP;
    finished := clock_timestamp();

    INSERT INTO pg_temp.rc_cache_bench_stats
    SELECT phase_name,
           round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3),
           table_name,
           total_scans,
           batches_total,
           batches_scanned,
           batches_pruned,
           pruning_ratio,
           meta_cache_hits,
           meta_cache_misses,
           batch_cache_hits,
           batch_cache_misses,
           batch_cache_evictions,
           batch_decompresses
    FROM engine.rowcompress_scan_stats()
    WHERE table_name = 'rc_docs';

    RAISE NOTICE 'rowcompress index cache phase=% elapsed_ms=%',
        phase_name,
        round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3);
END
$$;

SELECT pg_temp.run_rc_lookup_phase('cold');
SELECT pg_temp.run_rc_lookup_phase('warm');

SELECT *
FROM pg_temp.rc_cache_bench_stats
ORDER BY CASE phase WHEN 'cold' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END;
