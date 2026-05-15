#!/bin/bash
# run-bench.sh — TPC-H benchmark: pg vs colcompress vs colcompress+vec vs ClickHouse
# Uso: bash run-bench.sh [port]
# Mostra cada linha da tabela assim que a query termina nos 4 engines.

PORT=${1:-5432}
DIR="$(cd "$(dirname "$0")" && pwd)"
PSQL="psql -p $PORT -d tpch -qAXt"
mkdir -p "$DIR/result"

TIMEOUT_MS=300000
RUNS=3

# ---------------------------------------------------------------------------
# wait_pg — aguarda o servidor voltar após crash (até 30s)
# ---------------------------------------------------------------------------
function wait_pg {
    local tries=0
    while (( tries < 30 )); do
        psql -p "$PORT" -d tpch -qAXtc "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 1; (( tries++ ))
    done
    echo "[AVISO] servidor não voltou após 30s" >&2
    return 1
}

# ---------------------------------------------------------------------------
# run_pg <schema_path> <result_prefix> <extra_opts> <query_i> <run_j>
# ---------------------------------------------------------------------------
function run_pg {
    local spath="$1" prefix="$2" extra="$3" i="$4" j="$5"
    PGOPTIONS="-c search_path=${spath},public -c statement_timeout=${TIMEOUT_MS} ${extra}" \
        $PSQL -f "$DIR/queries/$i.sql" > "$DIR/result/${prefix}${i}.${j}" 2>&1
    # se o servidor crashou, espera ele voltar
    if grep -q "conexão com o servidor foi perdida\|server closed the connection\|connection to server" \
            "$DIR/result/${prefix}${i}.${j}" 2>/dev/null; then
        wait_pg
    fi
}

# ---------------------------------------------------------------------------
# run_ch <query_i> <run_j>
# ---------------------------------------------------------------------------
function run_ch {
    local i="$1" j="$2"
    # remove a linha EXPLAIN e comentários; junta tudo em uma string
    local sql
    sql=$(tail -n +2 "$DIR/queries/$i.sql" | grep -v '^--' | tr '\n' ' ')
    # captura APENAS o stderr (onde --time escreve o tempo em segundos)
    # stdout (linhas de resultado) vai para /dev/null
    local elapsed
    elapsed=$( { clickhouse-client --user default --password 'sjb' \
        --database tpch --time \
        --query "$sql" >/dev/null; } 2>&1 | grep -E '^[0-9]+\.?[0-9]*$' | head -1 )
    echo "Execution time: ${elapsed:-0} sec" > "$DIR/result/ch${i}.${j}"
}

# ---------------------------------------------------------------------------
# avg_pg <prefix> <query_i>  → "NNN ms" ou "TIMEOUT" ou "-"
# ---------------------------------------------------------------------------
function avg_pg {
    local prefix="$1" i="$2"
    local -a vals
    for j in $(seq 1 $RUNS); do
        local f="$DIR/result/${prefix}${i}.${j}"
        if grep -q "canceling statement due to statement timeout\|cancelar instrução devido ao timeout\|ERROR\|ERRO" "$f" 2>/dev/null; then
            echo "TIMEOUT"; return
        fi
        local ms
        ms=$(grep "Execution Time" "$f" 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+' | head -1)
        [[ -n "$ms" ]] && vals+=("$ms")
    done
    [[ ${#vals[@]} -eq 0 ]] && echo "-" && return
    local sum=0
    for v in "${vals[@]}"; do sum=$(echo "$sum + $v" | bc); done
    echo "$(echo "scale=0; ($sum + 0.5) / ${#vals[@]}" | bc) ms"
}

# ---------------------------------------------------------------------------
# avg_ch <query_i>  → "NNN ms" ou "-"
# ---------------------------------------------------------------------------
function avg_ch {
    local i="$1"
    local -a vals
    for j in $(seq 1 $RUNS); do
        local sec
        sec=$(grep "Execution time:" "$DIR/result/ch${i}.${j}" 2>/dev/null \
              | grep -Eo '[0-9]+\.[0-9]+' | head -1)
        [[ -n "$sec" ]] && vals+=("$(echo "scale=3; $sec * 1000" | bc)")
    done
    [[ ${#vals[@]} -eq 0 ]] && echo "-" && return
    local sum=0
    for v in "${vals[@]}"; do sum=$(echo "$sum + $v" | bc); done
    echo "$(echo "scale=0; ($sum + 0.5) / ${#vals[@]}" | bc) ms"
}

# ---------------------------------------------------------------------------
# cabeçalho
# ---------------------------------------------------------------------------
printf '\n'
printf '| %-5s | %19s | %13s | %15s | %12s |\n' \
    "Query" "PostgreSQL (heap)" "colcompress" "colcompress+vec" "ClickHouse"
printf '|%s|%s|%s|%s|%s|\n' \
    "------:" "--------------------:" "--------------:" "----------------:" "-------------:"

# ---------------------------------------------------------------------------
# loop principal — processa uma query de cada vez e imprime a linha
# ---------------------------------------------------------------------------
for i in $(seq 1 22); do
    # --- pg (heap) ---
    printf "  Q%-2s ... pg" "$i" >&2
    for j in $(seq 1 $RUNS); do
        run_pg "pg" "pg" "" "$i" "$j"
        printf "." >&2
    done
    t_pg=$(avg_pg "pg" "$i")

    # --- colcompress (sem vec) ---
    printf "  col" >&2
    for j in $(seq 1 $RUNS); do
        run_pg "col" "col" "-c storage_engine.enable_vectorization=off" "$i" "$j"
        printf "." >&2
    done
    t_col=$(avg_pg "col" "$i")

    # --- colcompress+vec ---
    printf "  vec" >&2
    for j in $(seq 1 $RUNS); do
        run_pg "col" "colvec" "-c storage_engine.enable_vectorization=on -c max_parallel_workers_per_gather=0" "$i" "$j"
        printf "." >&2
    done
    t_vec=$(avg_pg "colvec" "$i")

    # --- ClickHouse ---
    printf "  ch" >&2
    for j in $(seq 1 $RUNS); do
        run_ch "$i" "$j"
        printf "." >&2
    done
    t_ch=$(avg_ch "$i")

    printf "\r" >&2
    # imprime a linha da tabela imediatamente
    printf '| Q%-4s | %19s | %13s | %15s | %12s |\n' \
        "$i" "$t_pg" "$t_col" "$t_vec" "$t_ch"
done

printf '\n'
echo "Resultados salvos em $DIR/result/"
