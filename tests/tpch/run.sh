#!/bin/bash

function run_queries {
    engine="$1"
    name="$2"
    mkdir -p result
    export PGOPTIONS="-c search_path=$engine,public -c statement_timeout=60000"

    for i in {1..22}; do
        printf "$name Query %s:" "$i"
        for j in {1..3}; do
            psql -d tpch -qAXtf "queries/$i.sql" > "result/$engine$i.$j" 2>&1
            printf " %s" "$j"
        done
        printf '\n'
    done
}

function join_by {
    local d=${1-} f=${2-}
    if shift 2; then
        printf %s "$f" "${@/#/$d}"
    fi
}

function print_query {
    local i="$1"
    local -a pg_times
    local -a ch_times
    for j in {1..3}; do
        ms=$(grep Execution "result/pg$i.$j" | grep -Eo '[0-9]*\.[0-9]*')
        if [ -n "$ms" ]; then pg_times+=("$ms"); fi
        ms=$(grep Execution "result/ch$i.$j" | grep -Eo '[0-9]*\.[0-9]*')
        if [ -n "$ms" ]; then ch_times+=("$ms"); fi
    done
    if [ -z "${pg_times[*]}" ]; then
        pg="-"
    else
        pg=$(psql -c "SELECT round(AVG(x)) || ' ms' FROM unnest(ARRAY[$(join_by ', ' "${pg_times[@]}")]) AS x" --tuples-only --no-psqlrc --quiet --no-align)
    fi
    if [ -z "${ch_times[*]}" ]; then
        ch="-"
    else
        ch=$(psql -c "SELECT round(AVG(x)) || ' ms' FROM unnest(ARRAY[$(join_by ', ' "${ch_times[@]}")]) AS x" --tuples-only --no-psqlrc --quiet --no-align)
    fi
    # Full pushdown means outer most plan node is a foreign scan, and no inner
    # (indented) nodes are foreign scans.
    check=$(grep -q '^Foreign Scan' "result/ch$i.1" && ! grep -q ' Foreign Scan' "result/ch$i.1" && printf '✔︎' || printf ' ')
    printf "| %10s | %10s | %13s |     %s    |\n" "[Query $i]" "$pg" "$ch" "$check"
}

function print_header {
    printf '\n'
    echo "|    Query   | PostgreSQL | pg_clickhouse | Pushdown |"
    echo "| ----------:| ----------:| -------------:|:--------:|"
}

run_queries pg PostgreSQL
run_queries ch pg_clickhouse

print_header
for i in {1..22}; do  print_query "$i"; done
