-- =============================================================
-- bench_colcompress_tpch_q14_reread_cache_bench.sql
-- Benchmark cold/warm do cache backend-local de colcompress
-- usando a consulta real TPC-H Q14 sobre col.lineitem e col.part.
--
-- Uso:
--   psql -p 5432 -d tpch -f bench_colcompress_tpch_q14_reread_cache_bench.sql
-- =============================================================

\set ON_ERROR_STOP on
\timing on

DROP SCHEMA IF EXISTS cache_probe_tpch_q14_bench CASCADE;
CREATE SCHEMA cache_probe_tpch_q14_bench;

CREATE TABLE cache_probe_tpch_q14_bench.results (
    phase text NOT NULL,
    elapsed_ms numeric NOT NULL
);

CREATE OR REPLACE FUNCTION cache_probe_tpch_q14_bench.run_bench()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    started timestamptz;
    finished timestamptz;
    sink numeric;
BEGIN
    started := clock_timestamp();
    SELECT
        100.00 * sum(CASE
            WHEN p_type LIKE 'PROMO%'
                THEN l_extendedprice * (1 - l_discount)
            ELSE 0
        END) / sum(l_extendedprice * (1 - l_discount))
    INTO sink
    FROM col.lineitem,
         col.part
    WHERE l_partkey = p_partkey
      AND l_shipdate >= date '1995-09-01'
      AND l_shipdate < date(date '1995-09-01' + interval '1month');
    finished := clock_timestamp();

    INSERT INTO cache_probe_tpch_q14_bench.results(phase, elapsed_ms)
    VALUES (
        'cold',
        round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3)
    );

    RAISE NOTICE 'phase=cold elapsed_ms=% result=%',
        round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3),
        sink;

    started := clock_timestamp();
    SELECT
        100.00 * sum(CASE
            WHEN p_type LIKE 'PROMO%'
                THEN l_extendedprice * (1 - l_discount)
            ELSE 0
        END) / sum(l_extendedprice * (1 - l_discount))
    INTO sink
    FROM col.lineitem,
         col.part
    WHERE l_partkey = p_partkey
      AND l_shipdate >= date '1995-09-01'
      AND l_shipdate < date(date '1995-09-01' + interval '1month');
    finished := clock_timestamp();

    INSERT INTO cache_probe_tpch_q14_bench.results(phase, elapsed_ms)
    VALUES (
        'warm',
        round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3)
    );

    RAISE NOTICE 'phase=warm elapsed_ms=% result=%',
        round((EXTRACT(epoch FROM finished - started) * 1000.0)::numeric, 3),
        sink;
END
$$;

SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET storage_engine.enable_page_cache = on;
SET storage_engine.enable_vectorization = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

SELECT cache_probe_tpch_q14_bench.run_bench();

SELECT *
FROM cache_probe_tpch_q14_bench.results
ORDER BY CASE phase WHEN 'cold' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END;