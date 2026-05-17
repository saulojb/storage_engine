#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${1:-${PGPORT:-5432}}"
DB="${DB:-tpch}"
RESULT_DIR="${RESULT_DIR:-$SCRIPT_DIR/result}"
TS="$(date +%Y%m%d_%H%M%S)"

mkdir -p "$RESULT_DIR"

benchmarks=(
  "bench_colcompress_reread_cache_bench.sql"
  "bench_colcompress_tpch_lineitem_reread_cache_bench.sql"
  "bench_colcompress_tpch_q14_reread_cache_bench.sql"
)

echo "============================================================"
echo " colcompress reread benchmarks"
echo " port: $PORT   db: $DB"
echo " results: $RESULT_DIR"
echo "============================================================"

for bench in "${benchmarks[@]}"; do
    log_file="$RESULT_DIR/${bench%.sql}_${TS}.log"
    echo
    echo "==> running $bench"
    if ! psql -p "$PORT" -d "$DB" -v ON_ERROR_STOP=1 -f "$SCRIPT_DIR/$bench" > "$log_file" 2>&1; then
        echo "benchmark failed: $bench" >&2
        echo "log: $log_file" >&2
        tail -n 80 "$log_file" >&2 || true
        exit 1
    fi

    echo "log: $log_file"
    awk '
        /^  case_name   \| phase \| elapsed_ms / {capture=1}
        /^  case_name   \| phase \| elapsed_ms\s*$/ {capture=1}
        /^ phase \| elapsed_ms / {capture=1}
        capture {print}
        capture && /^\([0-9]+ linhas?\)|^\([0-9]+ rows?\)$/ {capture=0}
    ' "$log_file"
done

echo
echo "completed: ${#benchmarks[@]} benchmark(s)"