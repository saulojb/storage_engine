-- =============================================================
-- bench_colcompress_reread_cache_bench.sql
-- Benchmark cold/warm do cache backend-local de colcompress
-- em rereads no mesmo backend.
--
-- Cada caso usa uma relacao propria para manter o par cold/warm
-- isolado sem precisar reiniciar o cluster entre casos.
--
-- Uso:
--   psql -p 5432 -d tpch -f bench_colcompress_reread_cache_bench.sql
-- =============================================================

\set ON_ERROR_STOP on
\timing on

SET client_min_messages = notice;
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET storage_engine.enable_page_cache = on;
SET storage_engine.enable_vectorization = off;

DROP SCHEMA IF EXISTS cache_probe_col_bench CASCADE;
CREATE SCHEMA cache_probe_col_bench;

CREATE TEMP TABLE cc_reread_bench_results (
    case_name text,
    phase text,
    elapsed_ms numeric
);

CREATE OR REPLACE FUNCTION pg_temp.setup_case_table(case_name text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format(
        'CREATE TABLE cache_probe_col_bench.%I (
             id bigint,
             grp int NOT NULL,
             payload text NOT NULL
         ) USING colcompress WITH (orderby = ''id'')',
        case_name);

    EXECUTE format(
        'INSERT INTO cache_probe_col_bench.%I
         SELECT gs, gs %% 100, repeat(md5(gs::text), 4)
         FROM generate_series(1, 50000) AS gs',
        case_name);

    EXECUTE format('ANALYZE cache_probe_col_bench.%I', case_name);
END
$$;

CREATE OR REPLACE FUNCTION pg_temp.run_case(case_name text, sql_text text)
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

    INSERT INTO pg_temp.cc_reread_bench_results(case_name, phase, elapsed_ms)
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

    INSERT INTO pg_temp.cc_reread_bench_results(case_name, phase, elapsed_ms)
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

SELECT pg_temp.setup_case_table('cc_docs_mid_range');
SELECT pg_temp.run_case(
    'mid_range',
    'SELECT sum(grp), count(*) FROM cache_probe_col_bench.cc_docs_mid_range WHERE id BETWEEN 1000 AND 40000'
);

SELECT pg_temp.setup_case_table('cc_docs_narrow_range');
SELECT pg_temp.run_case(
    'narrow_range',
    'SELECT sum(grp), count(*) FROM cache_probe_col_bench.cc_docs_narrow_range WHERE id BETWEEN 1000 AND 5000'
);

SELECT pg_temp.setup_case_table('cc_docs_tail_range');
SELECT pg_temp.run_case(
    'tail_range',
    'SELECT sum(grp), count(*) FROM cache_probe_col_bench.cc_docs_tail_range WHERE id BETWEEN 45000 AND 50000'
);

SELECT pg_temp.setup_case_table('cc_docs_full_scan');
SELECT pg_temp.run_case(
    'full_scan',
    'SELECT sum(grp), count(*) FROM cache_probe_col_bench.cc_docs_full_scan WHERE id BETWEEN 1 AND 50000'
);

SELECT *
FROM pg_temp.cc_reread_bench_results
ORDER BY CASE case_name
             WHEN 'mid_range' THEN 1
             WHEN 'narrow_range' THEN 2
             WHEN 'tail_range' THEN 3
             WHEN 'full_scan' THEN 4
             ELSE 5
         END,
         CASE phase WHEN 'cold' THEN 1 WHEN 'warm' THEN 2 ELSE 3 END;