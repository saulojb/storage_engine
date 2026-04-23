#!/usr/bin/env bash
# ============================================================
# storage_engine Benchmark — Parallel (real-world simulation)
#
# Queries run with JIT=on and up to 16 parallel workers to
# simulate production workloads where the query executor
# exploits multi-core CPUs. This scenario shows the benefit
# of columnar vectorized execution in parallel contexts.
#
# Usage:
#   bash run_parallel.sh [N_RUNS]   (default 3)
#
# Outputs:
#   results_parallel.csv            — raw median timings
#   benchmark_parallel.png / .svg   — chart (requires chart_parallel.py)
# ============================================================
set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS="$SCRIPT_DIR/results_parallel.csv"
RUNS=${1:-3}
AM_DB="bench_am"
PG_USER="$(whoami)"

declare -a Q_LABELS=(
  "Q1 count(*)"
  "Q2 SUM/AVG numeric+double"
  "Q3 GROUP BY country (10)"
  "Q4 GROUP BY event_type+p95"
  "Q5 date range 1 month"
  "Q6 JSONB @> GIN"
  "Q7 JSONB key + GROUP BY"
  "Q8 array @> GIN"
  "Q9 LIKE text scan"
  "Q10 heavy multi-agg"
)

declare -a QUERIES=(
  "SELECT COUNT(*) FROM __TBL__"
  "SELECT SUM(amount),AVG(amount),SUM(price),AVG(price) FROM __TBL__"
  "SELECT country_code,COUNT(*) AS n,AVG(score) FROM __TBL__ GROUP BY country_code ORDER BY n DESC"
  "SELECT event_type,COUNT(*),SUM(amount),AVG(duration_ms),percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_ms) FROM __TBL__ GROUP BY event_type ORDER BY SUM(amount) DESC NULLS LAST"
  "SELECT event_date,COUNT(*),SUM(amount),AVG(price) FROM __TBL__ WHERE event_date BETWEEN '2024-01-01' AND '2024-01-31' GROUP BY event_date ORDER BY event_date"
  "SELECT COUNT(*),AVG(amount) FROM __TBL__ WHERE metadata @> '{\"os\":\"android\"}'"
  "SELECT metadata->>'campaign',COUNT(*),SUM(amount) FROM __TBL__ WHERE metadata ? 'campaign' GROUP BY 1 ORDER BY 3 DESC NULLS LAST"
  "SELECT COUNT(*),AVG(price) FROM __TBL__ WHERE tags @> ARRAY['tag_5']"
  "SELECT COUNT(*),SUM(amount) FROM __TBL__ WHERE url LIKE '/page/1%'"
  "SELECT browser,is_mobile,COUNT(*),SUM(amount),AVG(amount),MIN(amount),MAX(amount),SUM(price*quantity),AVG(duration_ms),COUNT(DISTINCT user_id),SUM(CASE WHEN event_type='purchase' THEN amount END) FROM __TBL__ GROUP BY browser,is_mobile ORDER BY COUNT(*) DESC"
)

declare -A AM_TARGETS=(
  ["heap"]="$AM_DB:events_heap"
  ["colcompress"]="$AM_DB:events_col"
  ["rowcompress"]="$AM_DB:events_row"
  ["citus_columnar"]="$AM_DB:events_cit"
)
declare -a AM_ORDER=("heap" "colcompress" "rowcompress" "citus_columnar")

psql_ms() {
    local db="$1" tbl="$2" q_tmpl="$3"
    local q="${q_tmpl//__TBL__/$tbl}"
    local tmpf
    tmpf=$(mktemp /tmp/bench_XXXXXX.sql)
    printf '\\timing on\nSET jit=on;\nSET max_parallel_workers_per_gather=4;\n%s;\n' "$q" > "$tmpf"

    local -a times=()
    for ((i=0; i<RUNS; i++)); do
        local t
        t=$(psql -U "$PG_USER" -d "$db" --no-psqlrc -P pager=off -f "$tmpf" 2>&1 \
            | grep -oP '(?:Time|Tempo):\s*\K[0-9]+[.,][0-9]+' | tail -1 | tr ',' '.')
        times+=("${t:-9999}")
    done
    rm -f "$tmpf"
    printf '%s\n' "${times[@]}" | LC_ALL=C sort -n | LC_NUMERIC=C awk "NR==$(( (RUNS+1)/2 )) {printf \"%.3f\", \$1}"
}

echo "AM,query,median_ms" > "$RESULTS"

echo "============================================================"
echo " Benchmark — PostgreSQL 18  •  1 000 000 rows"
echo " Runs per query: $RUNS   JIT: on   Parallelism: 4 workers"
echo "============================================================"

for am in "${AM_ORDER[@]}"; do
    target="${AM_TARGETS[$am]}"
    db="${target%%:*}"
    tbl="${target##*:}"
    echo ""
    echo "── $am  (db: $db  table: $tbl) ─────────────────────────"
    for i in "${!Q_LABELS[@]}"; do
        ms=$(psql_ms "$db" "$tbl" "${QUERIES[$i]}")
        label="${Q_LABELS[$i]}"
        printf "  %-35s  %s ms\n" "$label" "$ms"
        echo "$am,\"$label\",$ms" >> "$RESULTS"
    done
done

echo ""
echo "Results saved → $RESULTS"
echo "Generate chart: python3 $SCRIPT_DIR/chart_parallel.py $RESULTS"
