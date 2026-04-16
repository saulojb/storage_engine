# storage_engine

A PostgreSQL extension providing two high-performance Table Access Methods designed for analytical and HTAP workloads.

- **`colcompress`** — column-oriented compressed storage with vectorized execution and parallel scan
- **`rowcompress`** — row-oriented batch-compressed storage with parallel scan

Both AMs coexist alongside standard heap tables in the same database. All catalog objects are isolated in the `engine` schema, making the extension safe to install alongside `citus_columnar` or any other columnar extension (all exported C symbols carry the `se_` prefix to avoid linker conflicts).

> **Lineage:** `storage_engine` is a fork of [Hydra Columnar](https://github.com/hydradatabase/hydra) (itself derived from [citus_columnar](https://github.com/citusdata/citus)), extended with `rowcompress`, full DELETE/UPDATE support, stripe-level min/max pruning, and a redesigned parallel scan. The MergeTree-style `orderby` option and zone-map pruning are directly inspired by [ClickHouse](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree).

---

## Table of Contents

- [Quick Start](#quick-start)
- [colcompress AM](#colcompress-am)
  - [How It Works](#how-it-works)
  - [Column-Level Caching](#column-level-caching)
  - [Vectorized Execution](#vectorized-execution)
  - [Parallel Scan](#parallel-scan)
  - [Stripe-Level and Chunk-Level Min/Max Pruning](#stripe-level-and-chunk-level-minmax-pruning)
  - [Index-Backed Scan](#index-backed-scan)
  - [DELETE and UPDATE Support](#delete-and-update-support)
  - [ON CONFLICT / Upserts](#on-conflict--upserts)
  - [MergeTree-Like Ordering and colcompress_merge](#mergetree-like-ordering-and-colcompress_merge)
  - [Compression Options](#compression-options)
  - [Configuration GUCs](#configuration-gucs)
  - [Per-Table Options](#per-table-options)
- [rowcompress AM](#rowcompress-am)
  - [How It Works](#how-it-works-1)
  - [Parallel Scan](#parallel-scan-1)
  - [Per-Table Options](#per-table-options-1)
- [Management Functions](#management-functions)
- [Catalog Views](#catalog-views)
- [Installation](#installation)
- [PostgreSQL Version Compatibility](#postgresql-version-compatibility)
- [Known Limitations](#known-limitations)
- [Benchmarks](#benchmarks)
- [Attribution](#attribution)

---

## Quick Start

```sql
CREATE EXTENSION storage_engine;

-- Column-oriented analytics table
CREATE TABLE events (
    ts          timestamptz NOT NULL,
    user_id     bigint,
    event_type  text,
    value       float8
) USING colcompress;

-- Row-oriented compressed table (good for append-heavy workloads)
CREATE TABLE logs (
    id          bigserial,
    logged_at   timestamptz NOT NULL,
    message     text
) USING rowcompress;

-- Insert normally
INSERT INTO events
SELECT now(), g, 'click', random()
FROM generate_series(1, 1000000) g;

-- Query normally — column projection, vectorized execution,
-- and parallel scan are transparent and automatic.
SELECT event_type, count(*), avg(value)
FROM events
WHERE ts > now() - interval '1 day'
GROUP BY 1;
```

---

## colcompress AM

### How It Works

Data is stored **column by column** on disk. Each column is split into *stripes* (default 150,000 rows each), and each stripe is further divided into *chunk groups* (default 10,000 rows). Every chunk records the minimum and maximum value of its column, enabling zone-map pruning at query time.

```
Table file
├── Stripe 1  (rows 1 – 150,000)
│   ├── Chunk group 0  (rows 1 – 10,000)
│   │   ├── Column A  [min, max, compressed values…]
│   │   ├── Column B  [min, max, compressed values…]
│   │   └── …
│   └── Chunk group 1  (rows 10,001 – 20,000)  …
└── Stripe 2  (rows 150,001 – 300,000)  …
```

A scan only reads the columns referenced by the query, skipping all others entirely. This dramatically reduces I/O for wide tables with selective column access patterns.

### Column-Level Caching

The AM maintains an in-memory **column cache** that stores decompressed column chunks across executor iterations. When the same stripe region is accessed more than once (nested loops, repeated plans, self-joins), the decompressed data is served from cache without re-reading or re-decompressing the file.

```sql
SET storage_engine.enable_column_cache = on;   -- default: on
```

### Vectorized Execution

`colcompress` ships a **vectorized expression evaluation engine** that processes WHERE clauses and aggregates in column-oriented batches of up to 10,000 values per call, instead of row-at-a-time evaluation. This maps naturally onto column chunks and eliminates per-row interpreter overhead.

Supported vectorized operations:

| Category | Types |
|---|---|
| Comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`) | `int2`, `int4`, `int8`, `float4`, `float8`, `date`, `timestamp`, `timestamptz`, `char`, `bpchar`, `text`, `varchar`, `name`, `bool`, `oid` |
| Aggregates (`count`, `sum`, `avg`, `max`, `min`) | `int2`, `int4`, `int8`, `float8`, `date`, `timestamptz` |

```sql
SET storage_engine.enable_vectorization = on;   -- default: on
```

### Parallel Scan

The AM implements the full PostgreSQL parallel Table AM protocol using **Dynamic Shared Memory (DSM)**. The coordinator divides the stripe range across workers; each worker independently reads and decompresses its assigned stripes, then feeds results back through the Gather node.

Parallel scan stacks on top of vectorized execution — each worker runs its own vectorized evaluation pipeline independently.

```sql
SET storage_engine.enable_parallel_execution = on;  -- default: on
SET storage_engine.min_parallel_processes = 8;       -- minimum workers (default: 8)

-- Standard PostgreSQL parallel knobs also apply:
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
```

### Stripe-Level and Chunk-Level Min/Max Pruning

`colcompress` implements **two layers** of zone-map pruning using the `minimum_value` / `maximum_value` statistics stored per chunk in `engine.chunk`.

**Stripe-level pruning (coarse)** — Before reading any data, the scan aggregates the min/max across all chunks of each stripe and tests the resulting stripe-wide range against the query's WHERE predicates using PostgreSQL's `predicate_refuted_by`. Any stripe whose range is provably disjoint from the predicate is skipped entirely — no decompression, no I/O. The number of stripes removed this way is reported in `EXPLAIN`:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM events WHERE ts > '2025-01-01';

-- Custom Scan (ColumnarScan) on events  ...
--   Engine Stripes Removed by Pruning: 41
--   Engine Stripes Read: 12
```

Stripe pruning is most effective after `engine.colcompress_merge()` has established a global sort order, but it also works on any partially sorted data.

**Chunk-level pruning (fine)** — Within each stripe that survives the coarse filter, the custom scan evaluates each individual chunk group's min/max range against the same predicates. Chunk groups whose range cannot satisfy the predicate are skipped.

The two layers compose: a query on a large, well-sorted table typically eliminates entire stripes before touching them, then further prunes chunk groups within the remaining stripes, resulting in very small I/O amplification even without an index.

Pruning effectiveness scales directly with data sortedness. Use the `orderby` option combined with `engine.colcompress_merge()` to establish global sort order and maximize pruning at both levels.

### Index-Backed Scan

A custom index scan path allows B-tree and other indexes to drive lookups into a `colcompress` table, decompressing only the rows matched by the index.

```sql
-- Session GUC (applies to all colcompress tables in the session)
SET storage_engine.enable_columnar_index_scan = on;  -- default: off

-- Per-table override (persisted in engine.col_options, survives reconnect)
SELECT engine.alter_colcompress_table_set('documents'::regclass, index_scan => true);
```

The right default depends on the access pattern:

| Workload | Recommendation |
|---|---|
| Analytical (aggregates, range scans, high column count) | Keep `off` — the sequential vectorized path + chunk pruning wins |
| Document repository (XML, PDF, JSON blobs stored for compression, fetched by primary key or unique identifier) | Set `on` — you want columnar compression with point-lookup speed, not full-table scan |

Index scan is enabled for a given query if **either** the session GUC is `on` **or** the table's `index_scan` option is `true`. This lets you keep the GUC `off` globally while enabling it selectively per table:

```sql
-- OLAP tables: keep default (index_scan = false)

-- Document table: enable permanently
SELECT engine.alter_colcompress_table_set('adm.documents'::regclass, index_scan => true);

-- Inspect
SELECT table_name, index_scan FROM engine.colcompress_options WHERE table_name = 'adm.documents';
--  table_name    | index_scan
-- ---------------+-----------
--  adm.documents | t
```

For document repositories the combination is compelling: `colcompress` with `zstd` can compress large binary/text documents 3–10×, and with the index scan enabled, retrievals like `WHERE id = $1` or `WHERE document_key = $1` decompress only the matching rows without touching the rest of the stripe.

> **When to use `index_scan = true` vs `colcompress_merge()`**
>
> These are two distinct use cases, not competing strategies:
>
> | Use case | Strategy |
> |---|---|
> | **File/document storage** (XML, PDF, JSON blobs — fetched by primary key or unique key) | `index_scan = true`. You want columnar compression for storage savings and point-lookup speed without full-stripe decompression. Sort order is irrelevant; every fetch targets a specific row. |
> | **Analytics** (aggregations, date ranges, GROUP BY, pattern scans over millions of rows) | `index_scan = false` + `colcompress_merge()`. Sort the data by the query's filter column (`orderby = 'event_date ASC'`), then merge to produce globally ordered stripes. Stripe-level min/max pruning skips irrelevant stripes entirely before any decompression occurs. |
>
> Mixing both on the same table is possible but not ideal: a B-tree index on the `orderby` column will cause the planner to prefer `IndexScan` for range queries, disabling stripe pruning (see [Known Limitations](#b-tree-indexes-on-colcompress-disable-stripe-pruning)). If you need occasional point lookups on an analytical table, rely on the GUC `SET storage_engine.enable_columnar_index_scan = on` at session level rather than creating a B-tree index.

### DELETE and UPDATE Support

`colcompress` fully supports `DELETE` and `UPDATE` via a **row mask** stored in `engine.row_mask`. Each deleted row is marked as a bit in a per-chunk-group bitmask; the scan engine skips masked rows without rewriting the stripe. `UPDATE` is implemented as a delete-then-insert.

```sql
SET storage_engine.enable_dml = on;  -- default: on

DELETE FROM events WHERE ts < now() - interval '1 year';
UPDATE events SET value = value * 1.1 WHERE event_type = 'purchase';
```

Deleted rows are reclaimed during `VACUUM`, which rewrites affected stripes and clears the row mask.

### ON CONFLICT / Upserts

Standard `INSERT … ON CONFLICT` is fully supported, including `DO NOTHING` and `DO UPDATE SET …`. Requires a unique index on the conflict target column(s).

```sql
INSERT INTO events (ts, user_id, event_type, value)
VALUES (now(), 42, 'click', 1.0)
ON CONFLICT (user_id, event_type) DO UPDATE SET value = EXCLUDED.value;
```

### MergeTree-Like Ordering and colcompress_merge

Inspired by ClickHouse's MergeTree engine, `colcompress` supports a **global sort key** per table. When set, every new stripe written to the table is sorted by the key before compression. The `engine.colcompress_merge()` function rewrites the entire table in a single globally sorted pass, making stripe-level and chunk-level min/max pruning maximally effective across all stripes.

```sql
-- Assign a sort key to an existing table
SELECT engine.alter_colcompress_table_set(
    'events'::regclass,
    orderby => 'ts ASC, user_id ASC'
);

-- After a large INSERT or initial load, compact and globally sort:
SELECT engine.colcompress_merge('events');

-- Now WHERE ts BETWEEN ... skips almost all chunks.
```

`colcompress_merge` internally:
1. Copies all live (non-deleted) rows to a temporary heap table
2. Truncates the target table
3. Re-inserts rows in the order defined by `orderby`, writing fresh globally-ordered stripes

### Compression Options

Each chunk is compressed independently. Available algorithms:

| Name | Description |
|---|---|
| `none` | No compression |
| `lz4` | Fast compression/decompression, moderate ratio |
| `zstd` | High ratio, configurable level 1–19 (default level: 3) |
| `pglz` | PostgreSQL's built-in LZ variant |

```sql
SELECT engine.alter_colcompress_table_set('events',
    compression       => 'zstd',
    compression_level => 9);
```

### Configuration GUCs

All parameters can be set per-session or globally in `postgresql.conf`.

> **GUC availability:** The `storage_engine.*` GUCs are registered when the `storage_engine` shared library is loaded. This happens automatically the first time a `colcompress` or `rowcompress` table is accessed in a session. To make GUCs available immediately in every session (including before any table access), add the extension to `shared_preload_libraries`:
> ```
> shared_preload_libraries = 'storage_engine'
> ```
> GUC names use the `storage_engine.` prefix and do not overlap with `citus_columnar` (which uses the `columnar.` prefix), so both extensions can be loaded simultaneously without conflict.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `storage_engine.enable_parallel_execution` | bool | `on` | Enable parallel custom scan via DSM |
| `storage_engine.min_parallel_processes` | int | `8` | Minimum worker count to launch for parallel scan |
| `storage_engine.enable_vectorization` | bool | `on` | Enable vectorized WHERE/aggregate evaluation |
| `storage_engine.enable_custom_scan` | bool | `on` | Enable projection and qual-pushdown custom scan |
| `storage_engine.enable_column_cache` | bool | `on` | Enable in-memory column chunk cache |
| `storage_engine.enable_columnar_index_scan` | bool | `off` | Enable index-driven columnar scan path (recommended `on` for document/point-lookup repositories) |
| `storage_engine.enable_dml` | bool | `on` | Allow DELETE and UPDATE on colcompress tables |
| `storage_engine.stripe_row_limit` | int | `150000` | Default rows per stripe (1,000 – 100,000,000) |
| `storage_engine.chunk_group_row_limit` | int | `10000` | Default rows per chunk group (1,000 – 100,000,000) |
| `storage_engine.compression_level` | int | `3` | Default compression level for zstd (1 – 19) |

### Per-Table Options

Per-table options override the session GUCs for a specific table:

```sql
SELECT engine.alter_colcompress_table_set(
    'events'::regclass,
    chunk_group_row_limit => 10000,    -- NULL = leave unchanged
    stripe_row_limit      => 150000,
    compression           => 'zstd',
    compression_level     => 9,
    orderby               => 'ts ASC, user_id ASC',
    index_scan            => false     -- true = skip cost penalty for index scans on this table
);

-- Reset individual options to system defaults
SELECT engine.alter_colcompress_table_reset(
    'events'::regclass,
    compression       => true,
    compression_level => true,
    index_scan        => true   -- resets to false
);

-- Inspect current options
SELECT * FROM engine.colcompress_options WHERE table_name = 'events';
```

---

## rowcompress AM

### How It Works

`rowcompress` stores rows in fixed-size **batches** (default 10,000 rows per batch). Each batch is serialized using heap tuple format and then compressed as a single unit. Batch metadata (file offset, byte size, first row number, row count) is stored in `engine.row_batch`.

```
Table file (ColumnarStorage layout)
├── Batch 1: [header | row offsets | compressed heap tuple data]
├── Batch 2: [header | row offsets | compressed heap tuple data]
└── …
```

This AM suits **append-heavy workloads where compression matters but column projection is not needed** — event logs, audit trails, time-series with many columns all queried together. Typical storage savings of 2–10× with zstd.

Compared to `colcompress`:
- Reads full rows (no column projection)
- Lower write latency per row (no columnar transposition)
- No vectorized execution or chunk-level pruning
- Supports parallel reads and index scans
- Supports multiple compression algorithms

### Parallel Scan

`rowcompress` implements the PostgreSQL parallel scan protocol via **atomic batch claiming**. Each parallel worker atomically increments a shared counter to claim the next unprocessed batch, decompresses it, and repeats. There is no coordinator or work distribution step — workers self-schedule in a work-stealing fashion with zero contention on most paths.

```sql
-- Standard PostgreSQL parallel knobs apply
SET max_parallel_workers_per_gather = 4;
```

### Per-Table Options

```sql
SELECT engine.alter_rowcompress_table_set(
    'logs'::regclass,
    batch_size        => 10000,   -- rows per compressed batch (default: 10000)
    compression       => 'zstd',
    compression_level => 5
);

-- Reset to defaults
SELECT engine.alter_rowcompress_table_reset('logs'::regclass, compression => true);

-- Rewrite all batches with current options (e.g. after changing compression)
SELECT engine.rowcompress_repack('logs');

-- Inspect
SELECT * FROM engine.rowcompress_options WHERE table_name = 'logs';
SELECT * FROM engine.rowcompress_batches LIMIT 10;
```

---

## Management Functions

| Function | Description |
|---|---|
| `engine.alter_colcompress_table_set(regclass, ...)` | Set one or more options on a colcompress table |
| `engine.alter_colcompress_table_reset(regclass, ...)` | Reset colcompress options to system defaults |
| `engine.colcompress_merge(regclass)` | Rewrite and globally sort a colcompress table by its `orderby` key |
| `engine.colcompress_repack(regclass)` | Alias for `colcompress_merge`; drop-in replacement for `pg_repack` on colcompress tables |
| `engine.alter_rowcompress_table_set(regclass, ...)` | Set one or more options on a rowcompress table |
| `engine.alter_rowcompress_table_reset(regclass, ...)` | Reset rowcompress options to system defaults |
| `engine.rowcompress_repack(regclass)` | Rewrite all batches of a rowcompress table with current options |

---

## Catalog Views

| View | Description |
|---|---|
| `engine.colcompress_options` | Per-table options for all colcompress tables |
| `engine.colcompress_stripes` | Stripe-level metadata (offset, size, row range) per table |
| `engine.rowcompress_options` | Per-table options for all rowcompress tables |
| `engine.rowcompress_batches` | Batch-level metadata for all rowcompress tables |

All views grant `SELECT` to `PUBLIC`.

---

## Installation

### Build from source

Requires PostgreSQL server headers and `pg_config` in `PATH`.

```bash
cd dist/
sudo make -j$(nproc) install
```

Add to `postgresql.conf`:

```
shared_preload_libraries = 'storage_engine'
```

#### Loading with citus

If `citus` or `pg_cron` is also in `shared_preload_libraries`, the load order matters:

```
shared_preload_libraries = 'pg_cron,citus,storage_engine'
```

`citus` **must appear before** `storage_engine`. PostgreSQL registers planner hooks in load order; citus expects to be the outermost hook in the chain. Reversing the order causes PostgreSQL to refuse to start.

Restart PostgreSQL and load the extension:

```sql
CREATE EXTENSION storage_engine;
```

### Docker

```bash
docker compose up
psql postgres://postgres:password@127.0.0.1:5432
```

The `engine` schema and both AMs are created automatically on `CREATE EXTENSION`.

---

## Known Limitations

### No AFTER ROW triggers (and no pg_repack)

`colcompress` and `rowcompress` tables do not support **AFTER ROW triggers** or **foreign keys**. This is a fundamental architectural constraint: columnar storage does not maintain heap tuples in a form that row-level trigger machinery can inspect.

This means **`pg_repack` cannot be used** — it relies internally on an AFTER ROW trigger to capture concurrent changes during its online copy phase:

```
ERROR: Foreign keys and AFTER ROW triggers are not supported for columnar tables
DETAIL: Tools such as pg_repack use AFTER ROW triggers internally and cannot be used with colcompress or rowcompress tables.
HINT: Use engine.colcompress_repack(table) as a drop-in replacement for pg_repack on colcompress tables.
```

Use `engine.colcompress_repack()` instead:

```sql
-- Drop-in replacement for: pg_repack -t mytable
SELECT engine.colcompress_repack('adm.documents'::regclass);

-- Repack all colcompress tables in the database:
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT n.nspname, c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_am a ON c.relam = a.oid
    WHERE a.amname = 'colcompress' AND c.relkind = 'r'
    ORDER BY pg_total_relation_size(c.oid) DESC
  LOOP
    RAISE NOTICE 'Repacking %.%...', r.nspname, r.relname;
    PERFORM engine.colcompress_repack(
      (quote_ident(r.nspname) || '.' || quote_ident(r.relname))::regclass);
  END LOOP;
END;
$$;
```

> **Limitation vs. pg_repack:** `colcompress_repack` acquires `AccessExclusiveLock` for the duration of the operation (reads and writes are blocked). There is no online/concurrent mode. Schedule during a maintenance window for large tables.

### AFTER STATEMENT triggers are supported

Only row-level (`FOR EACH ROW`) AFTER triggers are blocked. Statement-level (`FOR EACH STATEMENT`) AFTER triggers work fine.

### sort-on-write is disabled when indexes exist

When a `colcompress` table has both an `orderby` option and one or more B-tree indexes, the sort-on-write path is automatically disabled during writes. This avoids a TID placeholder problem that would corrupt index entries. The table's stripes are still read in their natural write order; run `engine.colcompress_merge()` (or `colcompress_repack()`) to produce globally sorted stripes after loading data.

### B-tree indexes on colcompress disable stripe pruning

When a B-tree index exists on a `colcompress` column, the PostgreSQL planner may choose `IndexScan` even for queries that would benefit from a full sequential scan with stripe pruning. `IndexScan` opens the table with `randomAccess=true`, which bypasses the stripe pruning code path — causing date-range and similar ordered queries to read all stripes instead of only the relevant one.

**For analytical tables: do not create B-tree indexes on columns covered by the `orderby` key.** Use GIN indexes for JSONB and array columns, and rely on stripe pruning for range predicates on the sort key. If point-lookup access is occasionally needed on an analytical colcompress table, set `index_scan = false` explicitly and use `engine.colcompress_merge()` to keep the data globally sorted.

Examples of problematic and safe index patterns:

```sql
-- BAD: B-tree index on the ordery column defeats stripe pruning on range queries
CREATE INDEX ON events_col (event_date);  -- do NOT do this

-- GOOD: GIN indexes for JSONB / array columns are fine
CREATE INDEX ON events_col USING gin (metadata jsonb_path_ops);
CREATE INDEX ON events_col USING gin (tags);

-- Ensure the planner does not choose IndexScan
SELECT engine.alter_colcompress_table_set('events_col'::regclass, index_scan => false);
```

### No CLUSTER support

`CLUSTER` (index-ordered physical rewrite) is not implemented for columnar tables. Use `engine.colcompress_merge()` with an `orderby` option to achieve equivalent physical ordering.

### No VACUUM FULL / table rewrite

`VACUUM FULL` triggers a table rewrite that is not implemented for colcompress or rowcompress tables. Use `engine.colcompress_repack()` / `engine.rowcompress_repack()` instead, which perform the same compaction without the PostgreSQL rewrite machinery.

### No unlogged tables

`CREATE UNLOGGED TABLE ... USING colcompress` is not supported (`ERRCODE_FEATURE_NOT_SUPPORTED`). Use the default `LOGGED` persistence.

### No speculative insertion (INSERT … ON CONFLICT on non-unique predicates)

Speculative insertion (`INSERT … ON CONFLICT`) requires a unique index on the conflict target. Conflict detection on arbitrary predicates or without an index is not supported.

---

## Benchmarks

Benchmark suite: 1 000 000 rows, PostgreSQL 18.3, AMD Ryzen 7 5800H (8-core), 40 GB RAM, `shared_buffers=10GB`. `colcompress` configured with `lz4` compression and `orderby = 'event_date ASC'` (globally sorted via `colcompress_merge`).

Two scenarios are measured:

| Scenario | Settings |
|---|---|
| **Serial** (storage baseline) | `JIT=off`, `max_parallel_workers_per_gather=0` |
| **Parallel** (real-world simulation) | `JIT=on`, `max_parallel_workers_per_gather=16` |

### Serial results — median of 3 runs

| Query | heap | colcompress | rowcompress | citus_columnar |
|---|---|---|---|---|
| Q1 `count(*)` | 39.8ms | 43.0ms | 313ms | 36.6ms |
| Q2 `SUM/AVG` numeric+double | 188.6ms | 117.4ms | 358ms | 122.9ms |
| Q3 `GROUP BY` country (10 vals) | 219.0ms | 162.0ms | 395ms | 139.4ms |
| Q4 `GROUP BY` event_type + p95 | 538.9ms | 448.4ms | 685ms | 469.7ms |
| **Q5 date range 1 month** | 20.8ms | **22.4ms** | 59.1ms | 20.6ms |
| Q6 JSONB `@>` GIN | 123.1ms | 162.2ms | 326ms | 238.1ms |
| Q7 JSONB key + GROUP BY | 388.5ms | 310.3ms | 550ms | 358.2ms |
| Q8 array `@>` GIN | 63.0ms | 122.7ms | 274ms | 140.9ms |
| Q9 LIKE text scan | 150.9ms | 90.9ms | 338ms | 89.9ms |
| Q10 heavy multi-agg | 1953ms | 1939ms | 2109ms | 1925ms |

Q5 on `colcompress` achieves heap-equivalent performance (22.4ms vs 20.8ms heap) because stripe pruning skips 6 of 7 stripes — data is physically sorted by `event_date` via the `orderby` option. lz4 decompression adds negligible overhead over zstd for this stripe-pruned workload while reducing merge time.

### Parallel results — median of 3 runs (JIT on, 16 workers)

| Query | heap | colcompress | rowcompress | citus_columnar |
|---|---|---|---|---|
| Q1 `count(*)` | 18.3ms | 16.4ms | 148ms | 37.9ms |
| Q2 `SUM/AVG` numeric+double | 53.5ms | 29.7ms | 166ms | 121.5ms |
| Q3 `GROUP BY` country (10 vals) | 61.6ms | 166ms | 161ms | 143ms |
| Q4 `GROUP BY` event_type + p95 | 540ms | 316ms | 674ms | 470ms |
| **Q5 date range 1 month** | 21.4ms | **28.2ms** | 73.3ms | 21.1ms |
| Q6 JSONB `@>` GIN | 84.3ms | 40.4ms | 490ms | 245ms |
| Q7 JSONB key + GROUP BY | 392ms | 65.7ms | 687ms | 362ms |
| Q8 array `@>` GIN | 61.6ms | 32.7ms | 273ms | 146ms |
| Q9 LIKE text scan | 48.7ms | 25.4ms | 157ms | 91.7ms |
| Q10 heavy multi-agg | 1903ms | **641ms** | 2085ms | 1920ms |

Note: Q5 on `colcompress` in parallel mode (28.2ms) is now close to the sequential result (22.4ms). The v1.0.6 fix ensures `disable_cost` is applied to `IndexPath` entries in `rel->partial_pathlist`, so the planner correctly chooses `Parallel Custom Scan (ColcompressScan)` with stripe pruning instead of `Parallel Index Scan`. Stripe pruning (6 of 7 stripes skipped) is therefore active in both sequential and parallel paths when `index_scan=false`.

### Reproducing the results

The full benchmark kit is in `tests/bench/`:

```bash
createdb bench_am
psql -d bench_am -f tests/bench/setup.sql

# Serial run
bash tests/bench/run.sh 3
python3 tests/bench/chart.py

# Parallel run
bash tests/bench/run_parallel.sh 3
python3 tests/bench/chart_parallel.py
```

See [tests/README.md](tests/README.md) for full environment description and step-by-step instructions.

> Benchmark results above correspond to version 1.0.6 with `lz4` compression and globally sorted stripes.

---

## PostgreSQL Version Compatibility

| PostgreSQL | Status |
|---|---|
| 13 | Supported |
| 14 | Supported |
| 15 | Supported |
| 16 | Supported |
| 17 | Supported |
| 18 | Supported (current development target) |

---

## Attribution

`storage_engine` is a fork of **[Hydra Columnar](https://github.com/hydradatabase/hydra)**, which is itself derived from **[citus_columnar](https://github.com/citusdata/citus)** — the columnar extension originally built by Citus Data (now part of Microsoft). The original work is copyright Citus Data / Hydra and licensed under the AGPL-3.0.

Key ideas borrowed or adapted from other projects:

| Inspiration | Feature |
|---|---|
| [ClickHouse MergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree) | Per-table `orderby` sort key; zone-map (min/max) pruning at stripe and chunk level |
| [Apache Parquet](https://parquet.apache.org/) | Row-group statistics; column projection; dictionary encoding |
| [DuckDB](https://duckdb.org/) | Vectorized expression evaluation for columnar batches |

We are grateful to the Hydra and Citus teams for making their work open source.
