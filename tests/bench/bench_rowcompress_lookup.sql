-- =============================================================
-- bench_rowcompress_lookup.sql
-- Benchmark: rowcompress vs heap vs colcompress
-- Foco: busca indexada, batch por data, lote recente
--
-- Dois cenários:
--   A) Tabelas com inserção ALEATÓRIA (pior caso rowcompress)
--   B) Tabelas com inserção ORDENADA por event_date (caso ideal)
--
-- Pré-requisito: bench_rowcompress_lookup_setup.sql executado
--
-- Uso:
--   sudo -u postgres psql -p 5432 -d bench_am \
--     -f bench_rowcompress_lookup.sql 2>&1 | grep -E "^[QL]|Time"
-- =============================================================
\set ON_ERROR_STOP on
\timing on
SET jit = off;
SET max_parallel_workers_per_gather = 0;

-- ================================================================
-- CENÁRIO A: tabelas com inserção aleatória
-- (events_heap / events_col / events_row — já existentes)
-- Demonstra o CUSTO do rowcompress quando não há localidade
-- ================================================================

\echo ''
\echo '========================================================='
\echo 'CENÁRIO A — Inserção aleatória (sem pruning efetivo)'
\echo '========================================================='

-- L-A1: Lookup por user_id (1 usuário ~20 linhas)
-- heap  → btree index scan rápido
-- row   → index encontra TIDs espalhados em muitos batches
-- col   → full scan (sem índice em user_id)
\echo ''
\echo 'L-A1: SELECT * WHERE user_id = X  (índice, ~20 linhas)'
\echo 'L-A1 heap'
SELECT * FROM events_heap WHERE user_id = 12345;
\echo 'L-A1 rowcompress (aleatório)'
SELECT * FROM events_row  WHERE user_id = 12345;
\echo 'L-A1 colcompress (sem índice user_id — full scan)'
SELECT * FROM events_col  WHERE user_id = 12345;

-- L-A2: Lookup faixa de 50 usuários (~1 000 linhas)
\echo ''
\echo 'L-A2: SELECT * WHERE user_id BETWEEN X AND X+49  (~1000 linhas)'
\echo 'L-A2 heap'
SELECT COUNT(*), SUM(amount) FROM events_heap WHERE user_id BETWEEN 1000 AND 1049;
\echo 'L-A2 rowcompress (aleatório)'
SELECT COUNT(*), SUM(amount) FROM events_row  WHERE user_id BETWEEN 1000 AND 1049;
\echo 'L-A2 colcompress (sem índice user_id)'
SELECT COUNT(*), SUM(amount) FROM events_col  WHERE user_id BETWEEN 1000 AND 1049;

-- L-A3: Date range 1 dia — tabelas aleatórias (sem pruning nos batches)
\echo ''
\echo 'L-A3: SELECT * WHERE event_date = 1 dia  (~2700 linhas)'
\echo 'L-A3 heap'
SELECT COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_heap WHERE event_date = '2024-06-15';
\echo 'L-A3 rowcompress (aleatório — pruning_attnum inativo/ineficaz)'
SELECT COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_row  WHERE event_date = '2024-06-15';
\echo 'L-A3 colcompress (orderby não definido — sem stripe pruning)'
SELECT COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_col  WHERE event_date = '2024-06-15';

-- ================================================================
-- CENÁRIO B: tabelas ordenadas por event_date
-- (events_row_ord / events_col_ord / events_heap — mesmo dado)
-- Demonstra a VANTAGEM do rowcompress com pruning ativo
-- ================================================================

\echo ''
\echo '========================================================='
\echo 'CENÁRIO B — Inserção ordenada por event_date'
\echo 'rowcompress: pruning ativo  |  colcompress: orderby ativo'
\echo '========================================================='

-- L-B1: 1 dia — ponto ideal do pruning de batch rowcompress
--   ~30M/365 dias ≈ 82k linhas = ~8-9 batches (de 10k) a descomprimir
--   vs varrer todos os 3000 batches
\echo ''
\echo 'L-B1: SELECT WHERE event_date = 1 dia  (~82k linhas)'
\echo 'L-B1 heap'
SELECT COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_heap   WHERE event_date = '2024-06-15';
\echo 'L-B1 rowcompress ordenado (pruning por event_date)'
SELECT COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_row_ord WHERE event_date = '2024-06-15';
\echo 'L-B1 colcompress ordenado (stripe pruning por event_date)'
SELECT COUNT(*), SUM(amount), AVG(duration_ms)
FROM events_col_ord WHERE event_date = '2024-06-15';

-- L-B2: 1 semana (~574k linhas para 30M, ~19k para 1M)
\echo ''
\echo 'L-B2: SELECT WHERE event_date BETWEEN 7 dias  (~1/52 do total)'
\echo 'L-B2 heap'
SELECT COUNT(*), SUM(amount), MIN(event_date), MAX(event_date)
FROM events_heap   WHERE event_date BETWEEN '2024-06-01' AND '2024-06-07';
\echo 'L-B2 rowcompress ordenado'
SELECT COUNT(*), SUM(amount), MIN(event_date), MAX(event_date)
FROM events_row_ord WHERE event_date BETWEEN '2024-06-01' AND '2024-06-07';
\echo 'L-B2 colcompress ordenado'
SELECT COUNT(*), SUM(amount), MIN(event_date), MAX(event_date)
FROM events_col_ord WHERE event_date BETWEEN '2024-06-01' AND '2024-06-07';

-- L-B3: 1 mês (~8% do total)
\echo ''
\echo 'L-B3: SELECT WHERE event_date = 1 mês  (~8% do total)'
\echo 'L-B3 heap'
SELECT COUNT(*), SUM(amount), AVG(price)
FROM events_heap   WHERE event_date BETWEEN '2024-06-01' AND '2024-06-30';
\echo 'L-B3 rowcompress ordenado'
SELECT COUNT(*), SUM(amount), AVG(price)
FROM events_row_ord WHERE event_date BETWEEN '2024-06-01' AND '2024-06-30';
\echo 'L-B3 colcompress ordenado'
SELECT COUNT(*), SUM(amount), AVG(price)
FROM events_col_ord WHERE event_date BETWEEN '2024-06-01' AND '2024-06-30';

-- L-B4: "últimas N linhas inseridas" — 1 batch (10 000 linhas)
--   Representa o cenário de fila/log: lê sempre o lote mais recente
\echo ''
\echo 'L-B4: Último batch inserido (10 000 linhas — 1 decompress)'
\echo 'L-B4 heap'
SELECT COUNT(*), SUM(amount)
FROM events_heap
WHERE id >= (SELECT MAX(id) - 9999 FROM events_heap);
\echo 'L-B4 rowcompress ordenado'
SELECT COUNT(*), SUM(amount)
FROM events_row_ord
WHERE id >= (SELECT MAX(id) - 9999 FROM events_row_ord);
\echo 'L-B4 colcompress ordenado'
SELECT COUNT(*), SUM(amount)
FROM events_col_ord
WHERE id >= (SELECT MAX(id) - 9999 FROM events_col_ord);

-- L-B5: Lookup por user_id em tabela ordenada
--   rowcompress: índice scattter-scan — ainda ruim (rows não agrupadas por user_id)
--   Útil para comparar com L-A1 e mostrar que a ordenação por date não ajuda user_id
\echo ''
\echo 'L-B5: SELECT * WHERE user_id = X  (em tabela ordenada por date)'
\echo 'L-B5 heap'
SELECT COUNT(*), SUM(amount) FROM events_heap    WHERE user_id = 12345;
\echo 'L-B5 rowcompress ordenado'
SELECT COUNT(*), SUM(amount) FROM events_row_ord  WHERE user_id = 12345;
\echo 'L-B5 colcompress ordenado'
SELECT COUNT(*), SUM(amount) FROM events_col_ord  WHERE user_id = 12345;

\echo ''
\echo '=== Benchmark concluído ==='
