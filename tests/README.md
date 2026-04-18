# storage_engine — Benchmark Replication Guide

This folder contains everything needed to reproduce the official storage_engine
benchmark results independently.

---

## Test Environment

| Component | Details |
|---|---|
| **CPU** | AMD Ryzen 7 5800H (8 cores / 16 threads @ 3.2–4.4 GHz) |
| **RAM** | 40 GB DDR4 |
| **OS** | Ubuntu 24.04 LTS (x86_64) |
| **PostgreSQL** | 18.3 (Ubuntu 18.3-1.pgdg24.04+1) |
| **storage_engine** | 1.0 |
| **citus** | 14.0.0 (for `citus_columnar` comparison) |
| **pg_cron** | 1.6 |
| **shared_buffers** | 10 GB |
| **work_mem** | 256 MB |
| **Dataset** | 1 000 000 rows, ~388 MB heap / ~78 MB colcompress / ~106 MB rowcompress / ~48 MB columnar |

### `postgresql.conf` settings used

```
shared_preload_libraries = 'pg_cron,citus,storage_engine'
shared_buffers = 10GB
work_mem = 256MB
max_parallel_workers_per_gather = 8   # system default; overridden per test
jit = on                              # system default; overridden per test
```

---

## Prerequisites

0. **Install build dependencies**:
  Ubuntu/Debian:
  ```bash
  sudo apt update
  sudo apt install -y build-essential libcurl4-openssl-dev liblz4-dev libzstd-dev postgresql-server-dev-18 python3-pip
  ```

  RPM-based (dnf):
  ```bash
  sudo dnf install -y gcc make libcurl-devel lz4-devel libzstd-devel postgresql18-devel python3-pip
  ```

  > If you build against PostgreSQL 16 or 17, replace the PostgreSQL dev package
  > with the matching version (`postgresql-server-dev-16` /
  > `postgresql-server-dev-17` on Debian/Ubuntu, or `postgresql16-devel` /
  > `postgresql17-devel` on RPM-based distributions).

1. **Build and install storage_engine** (from this repository root):
  ```bash
  ./configure
  sudo make -j$(nproc) install
  ```

2. **Install citus** *(optional — needed for the `citus_columnar` column)*:
   ```bash
   # Ubuntu / Debian (example for PostgreSQL 18):
   sudo apt install postgresql-18-citus-14.0
   ```

3. **Configure `shared_preload_libraries`** in `postgresql.conf` or
   `postgresql.auto.conf`:
   ```
   shared_preload_libraries = 'pg_cron,citus,storage_engine'
   ```
   > **Order matters:** citus must appear before storage_engine. PostgreSQL
   > will fail to start if storage_engine is listed first.

4. **Restart PostgreSQL**:
   ```bash
   sudo systemctl restart postgresql@18-main
   ```

5. **Install Python dependencies** (for chart generation):
  ```bash
  pip install -r tests/bench/requirements.txt
  ```

---

## Step-by-step Replication

### 1 — Create the database

```bash
createdb bench_am
```

### 2 — Load schema and data

```bash
psql -d bench_am -f tests/bench/setup.sql
```

This creates four tables (`events_heap`, `events_col`, `events_row`, `events_cit`)
with 1 000 000 rows each and builds the appropriate indexes.

> **Note about `events_cit`:** The `events_cit` table uses `citus_columnar` and
> is commented out in `setup.sql` by default. If citus is installed, uncomment
> the relevant blocks before running the script.

> **Note about btree indexes on `events_col`:** The setup script intentionally
> does **not** create btree indexes on `events_col`. A btree index causes
> PostgreSQL's planner to prefer `IndexScan` with `randomAccess=true`, which
> disables stripe pruning in the columnar engine and makes date-range queries
> (Q5) approximately 10× slower. Only GIN indexes are created for JSONB/array
> columns.

### 3 — Run the serial benchmark (baseline)

```bash
cd tests/bench/
bash run.sh 3
```

Disable JIT and parallelism to isolate raw storage/decompression costs:
- `SET jit = off`
- `SET max_parallel_workers_per_gather = 0`

Results are saved to `results_serial.csv`.

### 4 — Generate the serial chart

```bash
python3 chart.py
# → benchmark.png
# → benchmark.svg
```

### 5 — Run the parallel benchmark (real-world simulation)

```bash
bash run_parallel.sh 3
```

Enables JIT and 16 parallel workers to simulate production multi-core workloads:
- `SET jit = on`
- `SET max_parallel_workers_per_gather = 16`

Results are saved to `results_parallel.csv`.

### 6 — Generate the parallel chart

```bash
python3 chart_parallel.py
# → benchmark_parallel.png
# → benchmark_parallel.svg
```

---

## Query Descriptions

| Query | SQL pattern | What it measures |
|---|---|---|
| Q1 | `COUNT(*)` | Full-scan decompression throughput |
| Q2 | `SUM/AVG` on numeric + double | Numeric decompression + aggregation |
| Q3 | `GROUP BY country_code` | Low-cardinality group-by |
| Q4 | `GROUP BY event_type` + `percentile_disc(0.95)` | Ordered-set aggregate |
| Q5 | Date range filter (1 month) | **Stripe pruning** on sorted columnar data |
| Q6 | `JSONB @> GIN` | GIN index seek + columnar fetch |
| Q7 | `JSONB ->>'key'` + `GROUP BY` | JSONB extraction + aggregation |
| Q8 | `array @> GIN` | Array GIN index + columnar fetch |
| Q9 | `LIKE '/page/1%'` | Text scan |
| Q10 | Heavy multi-aggregate (10 agg functions) | Vectorized aggregate throughput |

---

## Key Observations

### Serial mode (JIT off, no parallelism)

- **Q1–Q3, Q9–Q10**: colcompress is 1.2–2× faster than heap due to
  decompression reducing I/O (data fits in fewer cache lines).
- **Q5 (date range)**: colcompress achieves ~10× speedup over heap when data is
  physically sorted by `event_date` (set via `ALTER TABLE events_col SET
  (orderby = 'event_date ASC')`). The columnar engine's stripe-level chunk
  filter skips stripes whose min/max date range does not overlap the query
  predicate, reading only the one relevant stripe out of seven.
- **Q6, Q8**: heap wins — GIN index seeks return scattered row IDs that map to
  random stripe reads, negating columnar compression benefits.

### Parallel mode (JIT on, 16 workers)

- Most queries see significant speedups across all AMs due to parallelism.
- **Q5** shows no stripe-pruning benefit in parallel mode: each parallel worker
  receives a disjoint block range and scans its assigned blocks without the
  global stripe-pruning pass, so all stripes are read.
- colcompress still wins on aggregate-heavy queries (Q10: ~3× vs heap) due to
  vectorized execution in parallel workers.

---

## Troubleshooting

**`EXPLAIN` crashes with citus loaded**
This was a known bug in storage_engine ≤ 1.0 where `IsCreateTableAs(NULL)`
was called with a NULL `query_string` passed internally by citus, causing a
SIGSEGV. Fixed in storage_engine 1.0 (commit: add NULL guard in
`IsCreateTableAs` in `engine_planner_hook.c`).

**Q5 is slow on `events_col`**
Ensure: (a) there are no btree indexes on `events_col`, (b) `index_scan=false`
is set, and (c) data was inserted in `event_date` order (as `setup.sql` does
via the `ORDER BY event_date` clause in the `INSERT … SELECT` from
`events_heap`).

**`citus` must appear before `storage_engine`**
PostgreSQL enforces strict `shared_preload_libraries` ordering for extensions
that register planner hooks. citus registers its hook at startup and expects to
be the outermost hook in the chain. Placing storage_engine before citus causes
PostgreSQL to refuse to start.
