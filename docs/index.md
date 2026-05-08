---
layout: home
title: Home
nav_order: 1
---

# storage_engine
{: .fs-9 }

High-performance columnar and row-compressed Table Access Methods for PostgreSQL.
{: .fs-6 .fw-300 }

[Get started](#quick-start){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/saulojb/storage_engine){: .btn .fs-5 .mb-4 .mb-md-0 }

---

**storage_engine** is a PostgreSQL extension that ships two Table Access Methods (AMs) designed for analytical and HTAP workloads. Both AMs coexist alongside standard heap tables and with each other in the same database, without conflicts.

| AM | Orientation | Best for |
|---|---|---|
| **`colcompress`** | Column-oriented, compressed | Analytics, GROUP BY, range scans, aggregations |
| **`rowcompress`** | Row-oriented, compressed | Append-heavy logs, audit trails, compressed archives |

---

## Key Features

### colcompress

- **Vectorized GROUP BY aggregation** — `StorageEngineVectorGroupAgg` transparently replaces `HashAggregate`/`GroupAggregate` for GROUP BY queries; no SQL changes required
- **Vectorized filter evaluation** — WHERE clauses evaluated in batches of 10,000 values per column chunk, eliminating per-row interpreter overhead
- **Parallel scan** — full PostgreSQL DSM-based parallel protocol; each worker runs an independent vectorized pipeline
- **Two-layer zone-map pruning** — stripe-level (coarse) + chunk-level (fine); well-sorted tables skip entire stripes before decompressing any data
- **MergeTree-like ordering** — `orderby` option + `engine.colcompress_merge()` establishes global sort order, maximizing pruning effectiveness
- **Index-backed scan** — optional B-tree/GIN index path for point-lookup repositories (document stores, etc.)
- **Full DELETE / UPDATE** — via per-chunk-group row-mask bitmaps; no stripe rewrites at write time
- **ON CONFLICT / upserts** — fully supported
- **`engine.uint8`** — native unsigned 64-bit integer type for ClickBench-style workloads

### rowcompress

- **Batch compression** — rows packed in fixed-size batches (default 10,000 rows), each compressed as a unit with zstd/lz4/deflate/pglz
- **Parallel scan** — atomic batch claiming; workers self-schedule with zero coordinator overhead
- **Full DELETE / UPDATE** — via deleted-row bitmasks per batch
- **Multiple codecs** — zstd, lz4, deflate, pglz; configurable per table

---

## Quick Start

```sql
CREATE EXTENSION storage_engine;
```

```sql
-- Analytics table (column-oriented)
CREATE TABLE events (
    ts          timestamptz NOT NULL,
    user_id     bigint,
    event_type  text,
    amount      numeric(15,4)
) USING colcompress;

-- Set sort key for optimal range query performance
SELECT engine.alter_colcompress_table_set(
    'events'::regclass,
    orderby      => 'ts ASC',
    compression  => 'zstd',
    compression_level => 9
);

-- Load data
INSERT INTO events
SELECT
    now() - (random() * interval '365 days'),
    (random() * 50000)::bigint,
    (ARRAY['click','purchase','pageview'])[ceil(random()*3)::int],
    (random() * 1000)::numeric(15,4)
FROM generate_series(1, 1000000);

-- Globally sort and compact (maximizes stripe pruning)
SELECT engine.colcompress_merge('events');
```

```sql
-- Query — column projection, vectorized execution and parallel scan are automatic
SELECT event_type, COUNT(*), SUM(amount), AVG(amount)
FROM events
WHERE ts > now() - interval '30 days'
GROUP BY event_type
ORDER BY SUM(amount) DESC;
```

```sql
-- Compressed log table (row-oriented)
CREATE TABLE app_logs (
    id         bigserial,
    logged_at  timestamptz NOT NULL,
    level      text,
    message    text,
    payload    jsonb
) USING rowcompress;
```

---

## Performance Overview

All numbers from a single-core serial run (jit=off, parallelism=off) on **1,000,000 rows** (heap 388 MB · colcompress 95 MB · rowcompress 106 MB).

| Query | heap | colcompress | rowcompress |
|---|---:|---:|---:|
| `COUNT(*)` | 38.6 ms | 43.7 ms | 305 ms |
| `SUM/AVG` numeric + double | 182.3 ms | **118.3 ms** | 356 ms |
| `GROUP BY` 10 values | 214.4 ms | **162.3 ms** | 382 ms |
| `GROUP BY` + p95 | 538.2 ms | **452.5 ms** | 680 ms |
| Date range 1 month | 21.1 ms | **23.5 ms** | 60.0 ms |
| LIKE text scan | 147.0 ms | **88.3 ms** | 333 ms |
| Heavy multi-aggregate | 1908 ms | **1902 ms** | 2067 ms |

With **16 parallel workers + JIT**, colcompress achieves up to **×2.8 speedup** over heap on heavy aggregation workloads.

[Full benchmarks →](benchmarks)

---

## PostgreSQL Compatibility

| Version | Supported |
|---|---|
| PostgreSQL 16 | ✓ |
| PostgreSQL 17 | ✓ |
| PostgreSQL 18 | ✓ |

---

## Lineage

`storage_engine` is a fork of [Hydra Columnar](https://github.com/hydradatabase/hydra) (itself derived from [citus_columnar](https://github.com/citusdata/citus)), extended with `rowcompress`, full DELETE/UPDATE support, stripe-level min/max pruning, and a redesigned parallel scan. The MergeTree-style `orderby` option and zone-map pruning are directly inspired by [ClickHouse](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree).

All catalog objects are isolated in the `engine` schema; all exported C symbols carry the `se_` prefix — safe to install alongside `citus_columnar` without conflicts.
