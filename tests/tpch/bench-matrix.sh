#!/bin/bash
# bench-matrix.sh — TPC-H Q1..Q22: PG | Col (vec) | ClickHouse × Frio / Quente
#
# Frio  = 1ª execução  (cache frio)
# Quente = média de 3 execuções seguintes (cache quente)
#
# Uso: bash bench-matrix.sh [port] [--no-ch]
#        port padrão: 5433

export LC_ALL=C

PORT=5433
WITH_CH=1
DBUSER="${PGUSER:-postgres}"
CACHE_OPTS=""

for arg in "$@"; do
    case "$arg" in
        --no-ch)
            WITH_CH=0
            ;;
        --cache-on)
            CACHE_OPTS="-c storage_engine.enable_page_cache=on"
            ;;
        ''|*[!0-9]*)
            if [[ "$arg" != "--no-ch" && "$arg" != "--cache-on" ]]; then
                echo "Uso: bash bench-matrix.sh [port] [--no-ch] [--cache-on]" >&2
                exit 1
            fi
            ;;
        *)
            PORT="$arg"
            ;;
    esac
done

DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ "$DBUSER" == "postgres" && "$(id -un)" != "postgres" ]]; then
    USE_SUDO_POSTGRES=1
else
    USE_SUDO_POSTGRES=0
fi
mkdir -p "$DIR/result"


# ---------------------------------------------------------------------------
# run_psql <pgoptions> <extra args...>
# ---------------------------------------------------------------------------
function run_psql {
    local pgoptions="$1"
    shift

    if [[ "$USE_SUDO_POSTGRES" -eq 1 ]]; then
        sudo -u postgres env PGOPTIONS="$pgoptions" \
            psql -p "$PORT" -d tpch -qAXt "$@"
    else
        PGOPTIONS="$pgoptions" \
            psql -U "$DBUSER" -p "$PORT" -d tpch -qAXt "$@"
    fi
}


# ---------------------------------------------------------------------------
# wait_pg — aguarda servidor voltar após crash (até 30 s)
# ---------------------------------------------------------------------------
function wait_pg {
    local tries=0
    while (( tries < 30 )); do
        run_psql "" -c "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 1; (( tries++ ))
    done
    echo "[AVISO] servidor não voltou após 30 s" >&2
    return 1
}

# ---------------------------------------------------------------------------
# run_pg <prefix> <search_path> <extra_pgopts> <query_i> <run_j>
# ---------------------------------------------------------------------------
function run_pg {
    local prefix="$1" spath="$2" extra="$3" i="$4" j="$5"
    run_psql "-c search_path=${spath},public ${extra}" -f "$DIR/queries/${i}.sql" \
        > "$DIR/result/${prefix}${i}.${j}" 2>&1

    if grep -qE "conexão com o servidor foi perdida|server closed the connection|connection to server" \
            "$DIR/result/${prefix}${i}.${j}" 2>/dev/null; then
        wait_pg
    fi
}

# ---------------------------------------------------------------------------
# ms_pg <file> → ms float  |  "TIMEOUT"  |  "-"
# ---------------------------------------------------------------------------
function ms_pg {
    local f="$1"
    if grep -qE "canceling statement|cancelar instrução|ERROR|ERRO" "$f" 2>/dev/null; then
        echo "TIMEOUT"; return
    fi
    local ms
    ms=$(grep -E "Execution Time" "$f" 2>/dev/null \
         | grep -Eo '[0-9]+\.[0-9]+' | head -1)
    [[ -n "$ms" ]] && echo "$ms" || echo "-"
}

# ---------------------------------------------------------------------------
# avg3 <v1> <v2> <v3> → ms float médio  |  "TIMEOUT"  |  "-"
# ---------------------------------------------------------------------------
function avg3 {
    local v1="$1" v2="$2" v3="$3"
    for v in "$v1" "$v2" "$v3"; do
        [[ "$v" == "TIMEOUT" ]] && echo "TIMEOUT" && return
        [[ "$v" == "-"       ]] && echo "-"       && return
    done
    echo "$(echo "scale=6; ($v1 + $v2 + $v3) / 3" | bc)"
}

# ---------------------------------------------------------------------------
# fmt_sec <ms_float | TIMEOUT | -> → "N.NNN"  |  "TIMEOUT"  |  "-"
# ---------------------------------------------------------------------------
function fmt_sec {
    local ms="$1"
    [[ "$ms" == "TIMEOUT" ]] && echo "TIMEOUT" && return
    [[ "$ms" == "-"       ]] && echo "-"       && return
    local sec
    sec=$(echo "scale=6; $ms / 1000" | bc)
    # bc omits leading zero for values < 1 (e.g. ".723" → "0.723")
    [[ "$sec" =~ ^\. ]] && sec="0${sec}"
    printf "%.3f" "$sec"
}

# ---------------------------------------------------------------------------
# Cabeçalho
# ---------------------------------------------------------------------------
printf '\n'
if [[ "$WITH_CH" -eq 1 ]]; then
    printf '| %-5s | %9s | %9s | %9s | %9s | %9s | %9s |\n' \
        "Query" "PG Frio" "PG Quente" "Col Frio" "Col Quente" "CH Frio" "CH Quente"
    printf '|%s|%s|%s|%s|%s|%s|%s|\n' \
        "------:" "----------:" "----------:" "----------:" "----------:" "----------:" "----------:"
else
    printf '| %-5s | %9s | %9s | %9s | %9s |\n' \
        "Query" "PG Frio" "PG Quente" "Col Frio" "Col Quente"
    printf '|%s|%s|%s|%s|%s|\n' \
        "------:" "----------:" "----------:" "----------:" "----------:"
fi

# acumuladores (ms)
total_pf=0 total_pw=0 total_cf=0 total_cw=0 total_hf=0 total_hw=0
any_timeout=0

# ---------------------------------------------------------------------------
# Loop principal Q1..Q22
# ---------------------------------------------------------------------------
VEC="-c storage_engine.enable_vectorization=on ${CACHE_OPTS}"
BASE_PG_OPTS="${CACHE_OPTS}"

for i in $(seq 1 22); do

    printf "  Q%-2s ..." "$i" >&2

    # ---- PostgreSQL heap ----
    printf " pg" >&2
    run_pg "pg" "pg" "$BASE_PG_OPTS" "$i" 1
    ms_pf=$(ms_pg "$DIR/result/pg${i}.1")
    for j in 2 3 4; do
        run_pg "pg" "pg" "$BASE_PG_OPTS" "$i" $j
        printf "." >&2
    done
    ms_pw=$(avg3 "$(ms_pg "$DIR/result/pg${i}.2")" \
                 "$(ms_pg "$DIR/result/pg${i}.3")" \
                 "$(ms_pg "$DIR/result/pg${i}.4")")

    # ---- colcompress + vectorização ----
    printf " col" >&2
    run_pg "col" "col" "$VEC" "$i" 1
    ms_cf=$(ms_pg "$DIR/result/col${i}.1")
    for j in 2 3 4; do
        run_pg "col" "col" "$VEC" "$i" $j
        printf "." >&2
    done
    ms_cw=$(avg3 "$(ms_pg "$DIR/result/col${i}.2")" \
                 "$(ms_pg "$DIR/result/col${i}.3")" \
                 "$(ms_pg "$DIR/result/col${i}.4")")

    if [[ "$WITH_CH" -eq 1 ]]; then
        # ---- ClickHouse (via pg_clickhouse FDW, schema ch) ----
        printf " ch" >&2
        run_pg "ch" "ch" "" "$i" 1
        ms_hf=$(ms_pg "$DIR/result/ch${i}.1")
        for j in 2 3 4; do
            run_pg "ch" "ch" "" "$i" $j
            printf "." >&2
        done
        ms_hw=$(avg3 "$(ms_pg "$DIR/result/ch${i}.2")" \
                     "$(ms_pg "$DIR/result/ch${i}.3")" \
                     "$(ms_pg "$DIR/result/ch${i}.4")")
    else
        ms_hf="-"
        ms_hw="-"
    fi

    printf "\r" >&2

    # formata em segundos
    spf=$(fmt_sec "$ms_pf"); spw=$(fmt_sec "$ms_pw")
    scf=$(fmt_sec "$ms_cf"); scw=$(fmt_sec "$ms_cw")
    shf=$(fmt_sec "$ms_hf"); shw=$(fmt_sec "$ms_hw")

    if [[ "$WITH_CH" -eq 1 ]]; then
        printf '| Q%-4s | %9s | %9s | %9s | %9s | %9s | %9s |\n' \
            "$i" "$spf" "$spw" "$scf" "$scw" "$shf" "$shw"
    else
        printf '| Q%-4s | %9s | %9s | %9s | %9s |\n' \
            "$i" "$spf" "$spw" "$scf" "$scw"
    fi

    # acumula totais
    for pair in \
        "total_pf:$ms_pf" "total_pw:$ms_pw" \
        "total_cf:$ms_cf" "total_cw:$ms_cw" \
        "total_hf:$ms_hf" "total_hw:$ms_hw"
    do
        tvar="${pair%%:*}"; tval="${pair##*:}"
        if   [[ "$tval" == "TIMEOUT" ]]; then any_timeout=1
        elif [[ "$tval" != "-" ]]; then
            eval "$tvar=\$(echo \"scale=6; \${$tvar} + $tval\" | bc)"
        fi
    done
done

# ---------------------------------------------------------------------------
# Linha de Totais
# ---------------------------------------------------------------------------
if [[ "$WITH_CH" -eq 1 ]]; then
    printf '|%s|%s|%s|%s|%s|%s|%s|\n' \
        "------:" "----------:" "----------:" "----------:" "----------:" "----------:" "----------:"
    printf '| %-5s | %9s | %9s | %9s | %9s | %9s | %9s |\n' \
        "Total" \
        "$(fmt_sec "$total_pf")" "$(fmt_sec "$total_pw")" \
        "$(fmt_sec "$total_cf")" "$(fmt_sec "$total_cw")" \
        "$(fmt_sec "$total_hf")" "$(fmt_sec "$total_hw")"
else
    printf '|%s|%s|%s|%s|%s|\n' \
        "------:" "----------:" "----------:" "----------:" "----------:"
    printf '| %-5s | %9s | %9s | %9s | %9s |\n' \
        "Total" \
        "$(fmt_sec "$total_pf")" "$(fmt_sec "$total_pw")" \
        "$(fmt_sec "$total_cf")" "$(fmt_sec "$total_cw")"
fi

printf '\n'
[[ $any_timeout -eq 1 ]] && \
    echo "[AVISO] Algumas queries sofreram TIMEOUT — totais podem estar incompletos." >&2
echo "Resultados salvos em $DIR/result/" >&2
