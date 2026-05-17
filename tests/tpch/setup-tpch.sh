#!/bin/bash
# setup-tpch.sh — cria o banco tpch e carrega dados
# Engines: pg (heap), col (colcompress), ch (ClickHouse FDW opcional)
# Uso: bash setup-tpch.sh [port] [--no-ch]

set -e
PORT=5432
WITH_CH=1

for arg in "$@"; do
    case "$arg" in
        --no-ch)
            WITH_CH=0
            ;;
        ''|*[!0-9]*)
            if [[ "$arg" != "--no-ch" ]]; then
                echo "Uso: bash setup-tpch.sh [port] [--no-ch]" >&2
                exit 1
            fi
            ;;
        *)
            PORT="$arg"
            ;;
    esac
done

PSQL="psql -p $PORT"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== [1/5] Criando banco tpch no PostgreSQL (porta $PORT) ==="
$PSQL -c "DROP DATABASE IF EXISTS tpch;" postgres 2>/dev/null || true
$PSQL -c "CREATE DATABASE tpch;" postgres

echo "=== [2/5] Instalando extensões ==="
$PSQL tpch -c "CREATE EXTENSION IF NOT EXISTS storage_engine;"

if [[ "$WITH_CH" -eq 1 ]]; then
    if ! $PSQL -d tpch -Atqc "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_clickhouse'" | grep -q 1; then
        echo "[AVISO] pg_clickhouse não disponível na porta $PORT; seguindo com pg+col apenas." >&2
        WITH_CH=0
    fi
fi

if [[ "$WITH_CH" -eq 1 ]]; then
    $PSQL tpch -c "CREATE EXTENSION IF NOT EXISTS pg_clickhouse;"
fi

if [[ "$WITH_CH" -eq 1 ]]; then
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
else
echo "=== [3/5] Pulando schema ch (ClickHouse FDW desabilitado) ==="
fi

echo "=== [4/5] Criando schema pg (heap) e carregando dados ==="
$PSQL tpch -v with_ch="$WITH_CH" -f "$DIR/tpch-pg.sql"

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
