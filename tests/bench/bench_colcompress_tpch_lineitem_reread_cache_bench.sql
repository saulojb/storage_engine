-- =============================================================
-- bench_colcompress_tpch_lineitem_reread_cache_bench.sql
-- Benchmark cold/warm do cache backend-local de colcompress
-- sobre uma tabela real do TPC-H: col.lineitem.
--
-- Cada caso roda em uma nova conexao psql via \connect para
-- resetar o cache backend-local entre casos, preservando o par
-- cold/warm dentro do mesmo backend.
--
-- Uso:
--   psql -p 5432 -d tpch -f bench_colcompress_tpch_lineitem_reread_cache_bench.sql
-- =============================================================

\set ON_ERROR_STOP on
\timing on

DROP SCHEMA IF EXISTS cache_probe_tpch_col_bench CASCADE;
CREATE SCHEMA cache_probe_tpch_col_bench;

CREATE TABLE cache_probe_tpch_col_bench.cc_tpch_reread_results (
    case_name text NOT NULL,
    phase text NOT NULL,
    elapsed_ms numeric NOT NULL
);

CREATE OR REPLACE FUNCTION cache_probe_tpch_col_bench.run_case(case_name text, sql_text text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    started timestamptz;
    finished timestamptz;
    sink record;
BEGIN
    started := clock_timestamp();
    EXECUTE sql_text INTO sink;
    finished := clock_timestamp();

    INSERT INTO cache_probe_tpch_col_bench.cc_tpch_reread_results(case_name, phase, elapsed_ms)
    VALUES (
        case_name,
        'cold',
        round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3)
    );

    RAISE NOTICE 'case=% phase=cold elapsed_ms=%',
        case_name,
        round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3);

    started := clock_timestamp();
    EXECUTE sql_text INTO sink;
    finished := clock_timestamp();

    INSERT INTO cache_probe_tpch_col_bench.cc_tpch_reread_results(case_name, phase, elapsed_ms)
    VALUES (
        case_name,
        'warm',
        round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3)
    );

    RAISE NOTICE 'case=% phase=warm elapsed_ms=%',
        case_name,
        round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3);
END
$$;

\echo === narrow_range ===
\connect tpch
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET storage_engine.enable_page_cache = on;
SET storage_engine.enable_vectorization = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT cache_probe_tpch_col_bench.run_case(
    'narrow_range',
    'SELECT sum(l_quantity), count(*) FROM col.lineitem WHERE l_orderkey BETWEEN 100000 AND 120000'
);

\echo === mid_range ===
\connect tpch
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET storage_engine.enable_page_cache = on;
SET storage_engine.enable_vectorization = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT cache_probe_tpch_col_bench.run_case(
    'mid_range',
    'SELECT sum(l_quantity), count(*) FROM col.lineitem WHERE l_orderkey BETWEEN 100000 AND 300000'
);

\echo === wide_range ===
\connect tpch
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET storage_engine.enable_page_cache = on;
SET storage_engine.enable_vectorization = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT cache_probe_tpch_col_bench.run_case(
    'wide_range',
    'SELECT sum(l_quantity), count(*) FROM col.lineitem WHERE l_orderkey BETWEEN 100000 AND 1000000'
);

\echo === full_scan ===
\connect tpch
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET storage_engine.enable_page_cache = on;
SET storage_engine.enable_vectorization = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT cache_probe_tpch_col_bench.run_case(
    'full_scan',
    'SELECT sum(l_quantity), count(*) FROM col.lineitem WHERE l_orderkey BETWEEN 1 AND 6000000'
);

SELECT *
FROM cache_probe_tpch_col_bench.cc_tpch_reread_results
ORDER BY CASE case_name
             WHEN 'narrow_range' THEN 1
             WHEN 'mid_range' THEN 2
             WHEN 'wide_range' THEN 3
             WHEN 'full_scan' THEN 4
             ELSE 5
         END,
         CASE phase WHEN 'cold' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END;