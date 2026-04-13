# storage_engine

Copyright (c) 2026 Saulo José Benvenutti — [AGPL-3.0 License](LICENSE)

A PostgreSQL extension providing two high-performance Table Access Methods designed for analytical and HTAP workloads — with **significant storage savings and no meaningful query performance penalty** compared to standard heap tables.

- **`colcompress`** — column-oriented compressed storage with vectorized execution and parallel scan. Typical real-world compression ratios of **3–10× smaller** than heap for analytical datasets, while aggregate and range-scan queries often run *faster* due to reduced I/O and vectorized evaluation.
- **`rowcompress`** — row-oriented batch-compressed storage with parallel scan. Achieves **2–6× smaller** footprint than heap for append-heavy workloads (audit logs, event streams, time-series), with read throughput comparable to heap thanks to parallel work-stealing batch decompression.

Both AMs use `zstd` compression by default (configurable per table to `lz4`, `pglz`, or `none`). Storage savings translate directly to lower I/O, smaller backups, and reduced memory pressure from the buffer cache — benefits that compound as data volumes grow.

Both AMs coexist alongside standard heap tables in the same database. All catalog objects are isolated in the `engine` schema, making the extension safe to install alongside `citus_columnar` or any other columnar extension (all exported C symbols carry the `se_` prefix to avoid linker conflicts).

### Lineage

`storage_engine` is a restructured fork of [Hydra Columnar](https://github.com/hydradatabase/hydra), which is itself a fork of [Citus Columnar](https://github.com/citusdata/citus) (the columnar AM originally developed by Citus Data / Microsoft). The columnar storage model (`colcompress`) was further inspired by [ClickHouse's MergeTree engine](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree), particularly its per-table sort key and min/max chunk-level pruning semantics.

---

## Table of Contents

- [Quick Start](#quick-start)
- [colcompress AM](#colcompress-am)
  - [How It Works](#how-it-works)
  - [Column-Level Caching](#column-level-caching)
  - [Vectorized Execution](#vectorized-execution)
  - [Parallel Scan](#parallel-scan)
  - [Chunk-Level Min/Max Pruning](#chunk-level-minmax-pruning)
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
  - [Dependencies](#dependencies)
  - [Build from source](#build-from-source)
  - [Docker](#docker)
- [PostgreSQL Version Compatibility](#postgresql-version-compatibility)
- [Benchmarks](#benchmarks)

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

### Chunk-Level Min/Max Pruning

Every chunk stores `minimum_value` and `maximum_value` for its column in `engine.chunk`. When a query has a WHERE predicate on a column (e.g. `WHERE ts > '2024-01-01'`), the custom scan node evaluates each chunk's range at planning time and skips any chunk whose range cannot satisfy the predicate. This is the same technique used by Parquet row-group statistics and ClickHouse sparse indexes.

Pruning effectiveness scales directly with data sortedness. Use the `orderby` option combined with `engine.colcompress_merge()` to establish global sort order and maximize pruning.

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

Inspired by ClickHouse's MergeTree engine, `colcompress` supports a **global sort key** per table. When set, every new stripe written to the table is sorted by the key before compression. The `engine.colcompress_merge()` function rewrites the entire table in a single globally sorted pass, making chunk-level min/max pruning maximally effective across all stripes.

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

### Dependencies

The following libraries and tools must be installed before building:

| Dependency | Purpose | Optional |
|---|---|---|
| C compiler (`gcc` or `clang`) | Build toolchain | No |
| `make` | Build system | No |
| PostgreSQL server headers | Extension API | No |
| `liblz4` + headers | lz4 compression support | Yes (`--without-lz4`) |
| `libzstd` + headers | zstd compression (default algorithm) | Yes (`--without-zstd`) |
| `libcurl` + headers | Anonymous statistics scaffolding | Yes (`--without-libcurl`) |
| `autoconf` | Regenerating `./configure` after source changes | Dev only |

> **Note:** `pglz` is built into PostgreSQL itself — no separate package required.

**Debian / Ubuntu:**

```bash
sudo apt-get install -y \
  gcc make autoconf \
  postgresql-server-dev-18 \
  liblz4-dev \
  libzstd-dev \
  libcurl4-openssl-dev
```

Replace `18` with your PostgreSQL major version (`16`, `17`, `18`, …).

**RHEL / Fedora / Rocky Linux:**

```bash
sudo dnf install -y \
  gcc make autoconf \
  postgresql18-server-devel \
  lz4-devel \
  libzstd-devel \
  libcurl-devel
```

**macOS (Homebrew):**

```bash
brew install lz4 zstd curl
```

PostgreSQL from `brew install postgresql@18` already includes server headers in the keg.

---

### Build from source

Requires `pg_config` in `PATH` (provided by `postgresql-server-dev-XX` or the PostgreSQL keg).

```bash
./configure
make -j$(nproc) install
```

To build without optional dependencies:

```bash
./configure --without-libcurl --without-lz4   # zstd only
./configure --without-libcurl --without-zstd  # lz4 only
```

Add to `postgresql.conf`:

```
shared_preload_libraries = 'storage_engine'
```

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

## Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for detailed results comparing `colcompress`, `rowcompress`, and heap across analytical and mixed OLTP/OLAP workloads.
