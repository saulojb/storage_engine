-- =============================================================
-- bench_rowcompress_lookup_setup.sql
-- Cria tabelas ordenadas para demonstrar o ponto forte do
-- rowcompress: busca indexada + pruning de batch em dados
-- inseridos em ordem (e.g. série temporal).
--
-- Pré-requisito: banco bench_am com events_heap já populado
-- (30 M ou 1 M de linhas — funciona em qualquer tamanho).
--
-- Uso:
--   sudo -u postgres psql -p 5432 -d bench_am \
--     -f bench_rowcompress_lookup_setup.sql
-- =============================================================
\set ON_ERROR_STOP on
SET client_min_messages = notice;

\echo '=== Criando tabelas ordenadas para benchmark de lookup ==='

-- ---------------------------------------------------------------
-- 1. events_row_ord  — rowcompress inserido em ordem de event_date
--    pruning ativo em event_date (attnum 3)
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS events_row_ord;
CREATE TABLE events_row_ord (LIKE events_heap INCLUDING DEFAULTS)
    USING rowcompress;

SELECT engine.alter_rowcompress_table_set('events_row_ord'::regclass,
    batch_size => 10000, compression => 'zstd', compression_level => 3);

\echo 'Carregando events_row_ord (INSERT ORDER BY event_date) ...'
INSERT INTO events_row_ord SELECT * FROM events_heap ORDER BY event_date;

\echo 'Ativando pruning por event_date em events_row_ord ...'
SELECT engine.alter_rowcompress_table_set('events_row_ord'::regclass,
    pruning_column => 'event_date');

\echo 'Criando índices em events_row_ord ...'
CREATE INDEX events_row_ord_event_date_idx ON events_row_ord (event_date);
CREATE INDEX events_row_ord_user_id_idx    ON events_row_ord (user_id);

ANALYZE events_row_ord;

-- ---------------------------------------------------------------
-- 2. events_col_ord  — colcompress inserido em ordem de event_date
--    orderby ativo → stripe pruning efetivo
-- ---------------------------------------------------------------
DROP TABLE IF EXISTS events_col_ord;
CREATE TABLE events_col_ord (LIKE events_heap INCLUDING DEFAULTS)
    USING colcompress
    WITH (orderby = 'event_date');

\echo 'Carregando events_col_ord (INSERT ORDER BY event_date) ...'
INSERT INTO events_col_ord SELECT * FROM events_heap ORDER BY event_date;

ANALYZE events_col_ord;

-- ---------------------------------------------------------------
-- 3. Informações de sanidade
-- ---------------------------------------------------------------
\echo ''
\echo '=== Configurações ==='
SELECT table_name, am_name, total_units, live_rows
FROM engine.storage_health
WHERE table_name IN ('events_row_ord', 'events_col_ord');

\echo ''
\echo '--- rowcompress pruning_attnum ---'
SELECT regclass::text, pruning_attnum
FROM engine.row_options WHERE regclass = 'events_row_ord'::regclass;

\echo ''
\echo '--- Batch stats events_row_ord ---'
SELECT COUNT(*) AS total_batches,
       SUM(row_count) AS total_rows,
       MIN(batch_min_value) IS NOT NULL AS pruning_has_min
FROM engine.row_batch
WHERE storage_id = engine.rowcompress_relation_storageid('events_row_ord'::regclass);

\echo ''
\echo '=== Setup concluído. Execute bench_rowcompress_lookup.sql ==='
