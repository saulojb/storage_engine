#!/bin/bash
# setup-tpch.sh — cria o banco tpch no PG18 e carrega dados
# Engines: pg (heap), col (colcompress), ch (ClickHouse FDW)
# Uso: bash setup-tpch.sh [port]

set -e
PORT=${1:-5432}
PSQL="psql -p $PORT"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== [1/5] Criando banco tpch no PostgreSQL (porta $PORT) ==="
$PSQL -c "DROP DATABASE IF EXISTS tpch;" postgres 2>/dev/null || true
$PSQL -c "CREATE DATABASE tpch;" postgres

echo "=== [2/5] Instalando extensões ==="
$PSQL tpch -c "CREATE EXTENSION IF NOT EXISTS storage_engine;"
$PSQL tpch -c "CREATE EXTENSION IF NOT EXISTS pg_clickhouse;"

echo "=== [3/5] Criando schema ch (ClickHouse FDW) ==="
$PSQL tpch <<'EOSQL'
CREATE SERVER IF NOT EXISTS ch_tpch_svr
    FOREIGN DATA WRAPPER clickhouse_fdw
    OPTIONS (dbname 'tpch', driver 'binary', host 'localhost');
CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
    SERVER ch_tpch_svr
    OPTIONS (user 'default', password 'sjb');
CREATE SCHEMA IF NOT EXISTS ch;
IMPORT FOREIGN SCHEMA tpch FROM SERVER ch_tpch_svr INTO ch;
EOSQL

echo "=== [4/5] Criando schema pg (heap) e carregando dados ==="
$PSQL tpch -f "$DIR/tpch-pg.sql"

echo "=== [5/5] Criando schema col (colcompress, orderby = ClickHouse) e carregando dados ==="
$PSQL tpch -f "$DIR/tpch-col.sql"

echo ""
echo "=== Setup concluído! ==="
$PSQL tpch -c "
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname IN ('pg','col')
ORDER BY schemaname, tablename;"
