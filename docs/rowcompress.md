---
layout: default
title: rowcompress
nav_order: 4
---

# rowcompress AM
{: .no_toc }

Row-oriented batch-compressed storage with parallel scan, DELETE/UPDATE support, and multiple compression codecs.
{: .fs-6 .fw-300 }

<details open markdown="block">
<summary>Table of contents</summary>
{: .text-delta }
1. TOC
{:toc}
</details>

---

## How It Works

`rowcompress` stores rows in fixed-size **batches** (default 10,000 rows per batch). Each batch is serialized using the PostgreSQL heap tuple format and compressed as a single unit. Batch metadata (file offset, byte size, first row number, row count) is stored in `engine.row_batch`.

```
Table file
├── Batch 1: [header | row offsets | compressed heap tuple data]
├── Batch 2: [header | row offsets | compressed heap tuple data]
└── …
```

This AM suits **append-heavy workloads where compression matters but column projection is not needed** — event logs, audit trails, time-series with many columns always queried together.

### Comparison with colcompress

| Feature | rowcompress | colcompress |
|---|---|---|
| Orientation | Row-oriented | Column-oriented |
| Column projection | No (reads full rows) | Yes (reads only referenced columns) |
| Vectorized execution | No | Yes |
| Chunk-level min/max pruning | No | Yes |
| Write latency per row | Lower | Higher (columnar transposition) |
| GROUP BY / analytics | Slower | Much faster |
| Sequential append + range by insert order | Good | Good |
| Scatter reads (random FK lookup) | Very slow | Full scan (flat latency) |
| Typical storage savings | 2–10× | 3–15× |

---

## Parallel Scan

`rowcompress` implements the PostgreSQL parallel scan protocol via **atomic batch claiming**. Each parallel worker atomically increments a shared counter to claim the next unprocessed batch, decompresses it, and repeats. There is no coordinator or work-distribution step — workers self-schedule in a work-stealing fashion with zero contention on most paths.

```sql
-- Standard PostgreSQL parallel knobs apply
SET max_parallel_workers_per_gather = 4;
```

---

## DELETE and UPDATE

`rowcompress` supports DELETE and UPDATE via **per-batch deleted-row bitmasks** stored in `engine.row_batch`. Deleted rows are masked at read time without rewriting the batch. UPDATE is implemented as delete-then-insert.

```sql
DELETE FROM logs WHERE logged_at < now() - interval '90 days';
UPDATE logs SET level = 'WARN' WHERE level = 'WARNING';
```

Deleted rows are reclaimed during `VACUUM` (batch rewrite).

---

## When to Use rowcompress

rowcompress is the right choice when:

- **Writes are frequent and low-latency matters** — packing rows into batches is cheaper than columnar transposition
- **Queries always select most or all columns** — no benefit from column projection
- **Access pattern is sequential** — append order is the natural read order (e.g., process batch 1, then batch 2, etc.)
- **Table is effectively write-once** — logs, events, immutable records

**Avoid rowcompress when:**
- Queries filter by non-sequential keys (user_id, session_id, etc.) — scatter reads are catastrophic (K8: 1min17s for 20k rows vs colcompress 113ms)
- Heavy GROUP BY / aggregation — no vectorized execution
- Storage efficiency matters most — colcompress with zstd compresses better

---

## Per-Table Options

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

-- Inspect options
SELECT * FROM engine.rowcompress_options WHERE table_name = 'logs';

-- Inspect batches
SELECT * FROM engine.rowcompress_batches WHERE table_name = 'logs' LIMIT 10;
```

---

## Example: Audit Log Table

```sql
CREATE TABLE audit_log (
    id         bigserial,
    logged_at  timestamptz NOT NULL DEFAULT now(),
    user_id    bigint,
    action     text NOT NULL,
    table_name text,
    old_data   jsonb,
    new_data   jsonb,
    ip_address inet
) USING rowcompress;

-- Tune for write-heavy audit workload
SELECT engine.alter_rowcompress_table_set(
    'audit_log'::regclass,
    batch_size        => 10000,
    compression       => 'zstd',
    compression_level => 3  -- low level = fast writes
);

-- Read recent logs (sequential scan, all batches)
SELECT action, COUNT(*)
FROM audit_log
WHERE logged_at > now() - interval '7 days'
GROUP BY action
ORDER BY COUNT(*) DESC;

-- Check storage efficiency
SELECT table_name, total_units, live_rows,
       pg_size_pretty(pg_total_relation_size('audit_log')) AS disk_size
FROM engine.storage_health
WHERE table_name = 'audit_log';
```

---

## Compression Options

| Codec | Notes |
|---|---|
| `pglz` | Always available |
| `lz4` | Fast, good for write-heavy. Requires `liblz4-dev` |
| `zstd` ★ | Best ratio. Requires `libzstd-dev` |
| `deflate` | Middle ground. Requires `libdeflate-dev` |
| `none` | No compression |
