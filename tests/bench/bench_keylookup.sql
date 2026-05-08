-- =============================================================
-- bench_keylookup.sql
-- Benchmark de leitura por chave de índice — heap vs rowcompress vs colcompress
--
-- Cenários testados:
--   K1: PK lookup — 1 linha por id (único)
--   K2: PK lookup — IN list de 10 ids aleatórios
--   K3: PK lookup — IN list de 100 ids aleatórios
--   K4: PK lookup — IN list de 1000 ids aleatórios
--   K5: FK lookup — user_id = X (1 usuário, ~20 linhas)
--   K6: FK lookup — user_id IN (10 usuários, ~200 linhas)
--   K7: FK lookup — user_id IN (100 usuários, ~2000 linhas)
--   K8: FK lookup — user_id IN (1000 usuários, ~20000 linhas)
--   K9:  GROUP BY baixa cardinalidade  — event_type (6 valores)
--   K10: GROUP BY média cardinalidade  — country_code (10 valores)
--   K11: GROUP BY alta cardinalidade   — user_id (50k valores) com LIMIT
--   K12: GROUP BY + filtro de data     — GROUP BY event_type WHERE 1 mês
--
-- Pré-requisito: banco bench_am com events_heap, events_row, events_col
-- populados (sem necessidade de ordered tables)
--
-- Uso:
--   sudo -u postgres psql -p 5432 -d bench_am \
--     -f bench_keylookup.sql > /tmp/bench_key.txt 2>&1
-- =============================================================
\set ON_ERROR_STOP on
\timing on
SET jit = off;
SET max_parallel_workers_per_gather = 0;

-- ============================================================
-- PARTE 1 — LOOKUP POR CHAVE PRIMÁRIA (id)
-- heap: acesso direto via heap-fetch por TID do índice
-- rowcompress: precisa descomprimir 1 batch por id encontrado
-- colcompress: não suporta index scan eficiente (acesso aleatório)
-- ============================================================
\echo ''
\echo '============================================================'
\echo 'PARTE 1 — PK LOOKUP (id): 1, 10, 100, 1000 chaves'
\echo '============================================================'

\echo ''
\echo 'K1: PK lookup — 1 id'
\echo 'K1 heap'
SELECT id, user_id, amount, event_type FROM events_heap WHERE id = 500000;
\echo 'K1 rowcompress'
SELECT id, user_id, amount, event_type FROM events_row  WHERE id = 500000;
\echo 'K1 colcompress (index_scan=off — full scan)'
SELECT id, user_id, amount, event_type FROM events_col  WHERE id = 500000;

\echo ''
\echo 'K2: PK lookup — IN list 10 ids'
\echo 'K2 heap'
SELECT COUNT(*), SUM(amount) FROM events_heap
WHERE id IN (1000,50000,100000,200000,300000,400000,500000,700000,850000,999000);
\echo 'K2 rowcompress'
SELECT COUNT(*), SUM(amount) FROM events_row
WHERE id IN (1000,50000,100000,200000,300000,400000,500000,700000,850000,999000);
\echo 'K2 colcompress'
SELECT COUNT(*), SUM(amount) FROM events_col
WHERE id IN (1000,50000,100000,200000,300000,400000,500000,700000,850000,999000);

\echo ''
\echo 'K3: PK lookup — IN list 100 ids (spread uniforme)'
\echo 'K3 heap'
SELECT COUNT(*), SUM(amount), AVG(duration_ms) FROM events_heap
WHERE id IN (
  SELECT (generate_series * 10000)::bigint FROM generate_series(1,100)
);
\echo 'K3 rowcompress'
SELECT COUNT(*), SUM(amount), AVG(duration_ms) FROM events_row
WHERE id IN (
  SELECT (generate_series * 10000)::bigint FROM generate_series(1,100)
);
\echo 'K3 colcompress'
SELECT COUNT(*), SUM(amount), AVG(duration_ms) FROM events_col
WHERE id IN (
  SELECT (generate_series * 10000)::bigint FROM generate_series(1,100)
);

\echo ''
\echo 'K4: PK lookup — IN list 1000 ids (spread uniforme)'
\echo 'K4 heap'
SELECT COUNT(*), SUM(amount), AVG(duration_ms) FROM events_heap
WHERE id IN (
  SELECT (generate_series * 1000)::bigint FROM generate_series(1,1000)
);
\echo 'K4 rowcompress'
SELECT COUNT(*), SUM(amount), AVG(duration_ms) FROM events_row
WHERE id IN (
  SELECT (generate_series * 1000)::bigint FROM generate_series(1,1000)
);
\echo 'K4 colcompress'
SELECT COUNT(*), SUM(amount), AVG(duration_ms) FROM events_col
WHERE id IN (
  SELECT (generate_series * 1000)::bigint FROM generate_series(1,1000)
);

-- ============================================================
-- PARTE 2 — LOOKUP POR CHAVE ESTRANGEIRA (user_id)
-- user_id tem ~20 linhas por valor (1M / 50k = 20)
-- IN list de N users = N*20 linhas espalhadas aleatoriamente
-- ============================================================
\echo ''
\echo '============================================================'
\echo 'PARTE 2 — FK LOOKUP (user_id): 1, 10, 100, 1000 usuários'
\echo '============================================================'

\echo ''
\echo 'K5: user_id = 1 usuário (~20 linhas)'
\echo 'K5 heap'
SELECT COUNT(*), SUM(amount) FROM events_heap WHERE user_id = 25000;
\echo 'K5 rowcompress'
SELECT COUNT(*), SUM(amount) FROM events_row  WHERE user_id = 25000;
\echo 'K5 colcompress'
SELECT COUNT(*), SUM(amount) FROM events_col  WHERE user_id = 25000;

\echo ''
\echo 'K6: user_id IN 10 usuários (~200 linhas)'
\echo 'K6 heap'
SELECT COUNT(*), SUM(amount), MAX(duration_ms) FROM events_heap
WHERE user_id IN (1000,5000,10000,15000,20000,25000,30000,35000,40000,45000);
\echo 'K6 rowcompress'
SELECT COUNT(*), SUM(amount), MAX(duration_ms) FROM events_row
WHERE user_id IN (1000,5000,10000,15000,20000,25000,30000,35000,40000,45000);
\echo 'K6 colcompress'
SELECT COUNT(*), SUM(amount), MAX(duration_ms) FROM events_col
WHERE user_id IN (1000,5000,10000,15000,20000,25000,30000,35000,40000,45000);

\echo ''
\echo 'K7: user_id IN 100 usuários (~2000 linhas)'
\echo 'K7 heap'
SELECT COUNT(*), SUM(amount), AVG(price) FROM events_heap
WHERE user_id IN (SELECT generate_series * 500 FROM generate_series(1,100));
\echo 'K7 rowcompress'
SELECT COUNT(*), SUM(amount), AVG(price) FROM events_row
WHERE user_id IN (SELECT generate_series * 500 FROM generate_series(1,100));
\echo 'K7 colcompress'
SELECT COUNT(*), SUM(amount), AVG(price) FROM events_col
WHERE user_id IN (SELECT generate_series * 500 FROM generate_series(1,100));

\echo ''
\echo 'K8: user_id IN 1000 usuários (~20000 linhas)'
\echo 'K8 heap'
SELECT COUNT(*), SUM(amount), AVG(price) FROM events_heap
WHERE user_id IN (SELECT generate_series * 50 FROM generate_series(1,1000));
\echo 'K8 rowcompress'
SELECT COUNT(*), SUM(amount), AVG(price) FROM events_row
WHERE user_id IN (SELECT generate_series * 50 FROM generate_series(1,1000));
\echo 'K8 colcompress'
SELECT COUNT(*), SUM(amount), AVG(price) FROM events_col
WHERE user_id IN (SELECT generate_series * 50 FROM generate_series(1,1000));

-- ============================================================
-- PARTE 3 — GROUP BY com diferentes cardinalidades
-- Mede custo de agregação + full scan comprimido
-- ============================================================
\echo ''
\echo '============================================================'
\echo 'PARTE 3 — GROUP BY: baixa / média / alta cardinalidade'
\echo '============================================================'

\echo ''
\echo 'K9: GROUP BY event_type (6 valores) — full scan'
\echo 'K9 heap'
SELECT event_type, COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_heap GROUP BY event_type ORDER BY event_type;
\echo 'K9 rowcompress'
SELECT event_type, COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_row  GROUP BY event_type ORDER BY event_type;
\echo 'K9 colcompress'
SELECT event_type, COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_col  GROUP BY event_type ORDER BY event_type;

\echo ''
\echo 'K10: GROUP BY country_code (10 valores) — full scan'
\echo 'K10 heap'
SELECT country_code, COUNT(*), SUM(amount), AVG(price)
FROM events_heap GROUP BY country_code ORDER BY country_code;
\echo 'K10 rowcompress'
SELECT country_code, COUNT(*), SUM(amount), AVG(price)
FROM events_row  GROUP BY country_code ORDER BY country_code;
\echo 'K10 colcompress'
SELECT country_code, COUNT(*), SUM(amount), AVG(price)
FROM events_col  GROUP BY country_code ORDER BY country_code;

\echo ''
\echo 'K11: GROUP BY user_id (50k valores) LIMIT 20 — HashAgg'
\echo 'K11 heap'
SELECT user_id, COUNT(*), SUM(amount)
FROM events_heap GROUP BY user_id ORDER BY SUM(amount) DESC LIMIT 20;
\echo 'K11 rowcompress'
SELECT user_id, COUNT(*), SUM(amount)
FROM events_row  GROUP BY user_id ORDER BY SUM(amount) DESC LIMIT 20;
\echo 'K11 colcompress'
SELECT user_id, COUNT(*), SUM(amount)
FROM events_col  GROUP BY user_id ORDER BY SUM(amount) DESC LIMIT 20;

\echo ''
\echo 'K12: GROUP BY event_type WHERE event_date = 1 mês (filtro + agg)'
\echo 'K12 heap'
SELECT event_type, COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_heap
WHERE event_date BETWEEN '2024-06-01' AND '2024-06-30'
GROUP BY event_type ORDER BY event_type;
\echo 'K12 rowcompress'
SELECT event_type, COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_row
WHERE event_date BETWEEN '2024-06-01' AND '2024-06-30'
GROUP BY event_type ORDER BY event_type;
\echo 'K12 colcompress'
SELECT event_type, COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_col
WHERE event_date BETWEEN '2024-06-01' AND '2024-06-30'
GROUP BY event_type ORDER BY event_type;

\echo ''
\echo '=== Benchmark de chave concluído ==='
