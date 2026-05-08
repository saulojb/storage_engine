---
layout: default
title: colcompress
nav_order: 3
---

# colcompress AM
{: .no_toc }

Column-oriented compressed storage with vectorized execution, parallel scan, and MergeTree-like ordering.
{: .fs-6 .fw-300 }

<details open markdown="block">
<summary>Table of contents</summary>
{: .text-delta }
1. TOC
{:toc}
</details>

---

## How It Works

Data is stored **column by column** on disk. Each column is split into *stripes* (default 150,000 rows each), and each stripe is divided into *chunk groups* (default 10,000 rows). Every chunk records the minimum and maximum value for zone-map pruning.

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

A scan only reads the **columns referenced by the query**, skipping all others. For a wide table where a query touches 3 of 20 columns, I/O is reduced to roughly 15% of a heap scan.

---

## Vectorized GROUP BY Aggregation

`storage_engine` v2.0 introduces **`StorageEngineVectorGroupAgg`** — a custom aggregate executor node that transparently replaces `HashAggregate` and `GroupAggregate` for GROUP BY queries over colcompress tables. No SQL changes are required; the planner hook intercepts eligible plans and substitutes the vectorized path automatically.

```sql
-- These queries automatically use the vectorized path:
SELECT event_type, COUNT(*), SUM(amount), AVG(price)
FROM events
GROUP BY event_type;

SELECT country_code, MIN(score), MAX(score), COUNT(*)
FROM events
GROUP BY country_code
ORDER BY COUNT(*) DESC;
```

**Supported aggregates:** `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG`

**Supported types:** `int2`, `int4`, `int8`, `float4`, `float8`, `numeric`, `money`, `engine.uint8`

Up to 4 GROUP BY keys are supported. Parallel partial mode runs inside parallel workers via `AGGSPLIT_INITIAL_SERIAL`, feeding the native finalize step.

```sql
-- Control vectorized GROUP BY
SET storage_engine.enable_vectorized_groupagg = on;   -- default: on

-- Debug: log when a plan falls back to native aggregation
SET storage_engine.debug_vectorized_groupagg_fallback = on;
```

---

## Vectorized Filter Evaluation

WHERE clauses are evaluated in column-oriented batches of up to 10,000 values per chunk, eliminating per-row interpreter overhead.

| Category | Supported Types |
|---|---|
| Comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`) | `int2`, `int4`, `int8`, `float4`, `float8`, `date`, `timestamp`, `timestamptz`, `char`, `text`, `varchar`, `bool`, `oid` |
| Aggregates (`count`, `sum`, `avg`, `min`, `max`) | `int2`, `int4`, `int8`, `float8`, `numeric`, `date`, `money`, `engine.uint8` |

```sql
SET storage_engine.enable_vectorization = on;  -- default: on
```

---

## Zone-Map Pruning (Two Layers)

colcompress implements **two layers** of min/max pruning using statistics stored per chunk in `engine.chunk`.

### Stripe-level (coarse)
Before reading any data, the scan aggregates min/max across all chunks of each stripe and tests against the query's WHERE predicates. Any stripe whose range is provably disjoint is skipped entirely — no I/O, no decompression.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*) FROM events WHERE ts > '2025-01-01';
-- ...
--   Engine Stripes Removed by Pruning: 41
--   Engine Stripes Read: 12
```

### Chunk-level (fine)
Within each surviving stripe, individual chunk groups are tested. Those whose range cannot satisfy the predicate are skipped.

Pruning effectiveness scales with **data sortedness**. Use `orderby` + `colcompress_merge()` to maximize it.

---

## MergeTree-Like Ordering

Inspired by ClickHouse's MergeTree engine, colcompress supports a global sort key per table.

```sql
-- Set sort key
SELECT engine.alter_colcompress_table_set(
    'events'::regclass,
    orderby => 'ts ASC, user_id ASC'
);

-- After bulk load, compact into globally sorted stripes
SELECT engine.colcompress_merge('events');

-- Now date-range queries skip almost all stripes
SELECT COUNT(*), SUM(amount)
FROM events
WHERE ts BETWEEN '2024-01-01' AND '2024-01-31';
-- Stripes Removed by Pruning: 11
-- Stripes Read: 1
```

`colcompress_merge` internally:
1. Copies all live rows to a temporary heap table
2. Truncates the target table
3. Re-inserts rows in `orderby` order, writing fresh globally-ordered stripes

---

## Parallel Scan

colcompress implements the full PostgreSQL parallel Table AM protocol using **Dynamic Shared Memory (DSM)**. The coordinator pre-loads stripe IDs into DSM; each worker atomically claims the next stripe, decompresses it, and runs its own vectorized evaluation pipeline independently.

```sql
SET storage_engine.enable_parallel_execution = on;   -- default: on
SET storage_engine.min_parallel_processes = 8;       -- minimum workers (default: 8)

-- Standard PostgreSQL parallel knobs also apply
SET max_parallel_workers_per_gather = 8;
```

{: .warning }
> Stripe-level pruning is a **sequential-scan optimization**. In parallel mode, each worker reads its assigned stripes independently without a global pruning pass. For date-range workloads that depend on pruning, run without parallelism or use a GIN / B-tree index on a non-colcompress table.

---

## Index-Backed Scan

An optional index scan path allows B-tree and other indexes to drive lookups into a colcompress table, decompressing only the matched rows.

```sql
-- Enable per-session (for point lookups on analytical tables)
SET storage_engine.enable_engine_index_scan = on;

-- Enable permanently for a specific table (document stores)
SELECT engine.alter_colcompress_table_set('documents'::regclass, index_scan => true);
```

| Workload | Recommendation |
|---|---|
| Analytics (GROUP BY, range scans, full scan) | Keep `off` — sequential + pruning wins |
| Document repository (XML, PDF, JSON blobs; fetched by PK) | Set `on` — columnar compression + point-lookup speed |

{: .important }
> Do **not** create a B-tree index on the `orderby` column of an analytical table. The planner will prefer the index, bypassing stripe pruning. Use the GUC `enable_engine_index_scan = on` at session level for occasional point lookups instead.

---

## DELETE and UPDATE

colcompress fully supports DELETE and UPDATE via a **row mask** stored in `engine.row_mask`. Each deleted row is marked as a bit in a per-chunk-group bitmask; the scan engine skips masked rows without rewriting the stripe. UPDATE is implemented as delete-then-insert.

```sql
SET storage_engine.enable_dml = on;  -- default: on

DELETE FROM events WHERE ts < now() - interval '1 year';
UPDATE events SET amount = amount * 1.1 WHERE event_type = 'purchase';
```

Deleted rows are reclaimed during `VACUUM`, which rewrites affected stripes and clears the row mask.

---

## ON CONFLICT / Upserts

Standard `INSERT … ON CONFLICT` is fully supported:

```sql
INSERT INTO events (ts, user_id, event_type, amount)
VALUES (now(), 42, 'purchase', 99.90)
ON CONFLICT (user_id, event_type) DO UPDATE
    SET amount = EXCLUDED.amount,
        ts     = EXCLUDED.ts;
```

Requires a unique index on the conflict target column(s).

---

## Column Cache

The AM maintains an in-memory column cache that stores decompressed column chunks across executor iterations. Useful for nested loops, repeated plan nodes, or self-joins where the same stripe region is accessed multiple times.

```sql
SET storage_engine.enable_column_cache = on;    -- default: off
SET storage_engine.column_cache_size = 200;     -- MB (default: 200)
```

---

## engine.uint8 — Unsigned 64-bit Integer

`storage_engine` ships a native **unsigned 64-bit integer** type designed for columns that carry values in the full `[0, 2⁶⁴−1]` range, such as ClickBench's `WatchID` and `UserID` columns.

```sql
CREATE TABLE hits (
    WatchID  engine.uint8,
    UserID   engine.uint8,
    EventTime timestamptz
) USING colcompress;

SET search_path TO engine, public;
SELECT min(WatchID), max(WatchID), sum(WatchID) FROM hits;
```

- **Storage**: 8 bytes — identical layout to `bigint`, zero overhead
- **Operators**: full unsigned semantics (`18446744073709551615 > 1` is true)
- **Btree + hash opclasses**: ORDER BY, GROUP BY, DISTINCT, index scans
- **Casts**: `uint8 ↔ bigint`, `uint8 ↔ numeric`, `uint8 ↔ text`
- **Aggregates** (`min`, `max`, `sum`) in the `engine` schema — `sum` returns `numeric` to accommodate values > `INT64_MAX`
- **Vectorized**: `engine.vmin`, `engine.vmax`, `engine.vsum` dispatched automatically by the planner

---

## Per-Table Options

```sql
SELECT engine.alter_colcompress_table_set(
    'events'::regclass,
    stripe_row_limit      => 150000,   -- rows per stripe
    chunk_group_row_limit => 10000,    -- rows per chunk group
    compression           => 'zstd',
    compression_level     => 9,
    orderby               => 'ts ASC, user_id ASC',
    index_scan            => false
);

-- Reset individual options to system defaults
SELECT engine.alter_colcompress_table_reset(
    'events'::regclass,
    compression       => true,
    compression_level => true
);

-- Inspect
SELECT * FROM engine.colcompress_options WHERE table_name = 'events';

-- Stripe-level view
SELECT * FROM engine.colcompress_stripes WHERE table_name = 'events' LIMIT 5;
```

---

## Compression Options

| Codec | Description |
|---|---|
| `pglz` | PostgreSQL built-in LZ (always available) |
| `lz4` | Fast (~500 MB/s decompress). Requires `liblz4-dev` |
| `zstd` ★ | Best ratio + good speed; level 1–19. **Recommended**. Requires `libzstd-dev` |
| `deflate` | zlib-compatible; good middle ground. Requires `libdeflate-dev` |
| `zxc` | Asymmetric SIMD (NEON/AVX2/AVX-512); extremely fast decompress. Ideal for ARM Graviton. |
| `none` | No compression |

---

## Known Limitations

- **No AFTER ROW triggers or foreign keys** — use `engine.colcompress_repack()` instead of `pg_repack`
- **Stripe pruning disabled in parallel mode** — each worker reads its assigned stripes independently
- **B-tree index on `orderby` column disables pruning** — the planner prefers index scan over sequential scan
- **`INSERT … SELECT` with pre-existing indexes** may corrupt index TIDs — workaround: `REINDEX TABLE CONCURRENTLY` after bulk load
