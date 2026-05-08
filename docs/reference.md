---
layout: default
title: Reference
nav_order: 7
---

# Reference
{: .no_toc }

<details open markdown="block">
<summary>Table of contents</summary>
{: .text-delta }
1. TOC
{:toc}
</details>

---

## Management Functions

### colcompress

| Function | Description |
|---|---|
| `engine.alter_colcompress_table_set(regclass, ...)` | Set one or more options on a colcompress table |
| `engine.alter_colcompress_table_reset(regclass, ...)` | Reset colcompress options to system defaults |
| `engine.colcompress_merge(regclass)` | Rewrite and globally sort a colcompress table by its `orderby` key |
| `engine.colcompress_repack(regclass)` | Alias for `colcompress_merge`; drop-in replacement for `pg_repack` |
| `engine.colcompress_merge_incremental(regclass)` | Incremental merge: only rewrites stripes with dirty_ratio above threshold |

```sql
-- Full options signature
SELECT engine.alter_colcompress_table_set(
    'mytable'::regclass,
    stripe_row_limit      => 150000,   -- rows per stripe (1000 – 100000000)
    chunk_group_row_limit => 10000,    -- rows per chunk group (1000 – 100000000)
    compression           => 'zstd',   -- none | pglz | lz4 | zstd | deflate | zxc
    compression_level     => 9,        -- 1–19 for zstd
    orderby               => 'ts ASC', -- sort key for colcompress_merge
    index_scan            => false     -- true = enable index scan path for this table
);

-- Reset individual options
SELECT engine.alter_colcompress_table_reset(
    'mytable'::regclass,
    stripe_row_limit      => true,
    compression           => true,
    compression_level     => true,
    orderby               => true,
    index_scan            => true
);

-- Compact and globally sort
SELECT engine.colcompress_merge('mytable');

-- Incremental maintenance (merge only tables above dirty threshold)
SELECT engine.colcompress_merge_incremental('mytable');
```

### rowcompress

| Function | Description |
|---|---|
| `engine.alter_rowcompress_table_set(regclass, ...)` | Set one or more options on a rowcompress table |
| `engine.alter_rowcompress_table_reset(regclass, ...)` | Reset rowcompress options to system defaults |
| `engine.rowcompress_repack(regclass)` | Rewrite all batches with current compression options |
| `engine.rowcompress_merge_incremental(regclass)` | Incremental repack: only rewrites batches with high tombstone ratio |

```sql
SELECT engine.alter_rowcompress_table_set(
    'mylogs'::regclass,
    batch_size        => 10000,   -- rows per batch (100 – 100000000)
    compression       => 'zstd',
    compression_level => 5
);

SELECT engine.alter_rowcompress_table_reset(
    'mylogs'::regclass,
    compression => true,
    batch_size  => true
);

SELECT engine.rowcompress_repack('mylogs');
```

### Storage Maintenance

| Function / Procedure | Description |
|---|---|
| `engine.storage_maintenance_auto(dry_run, max_tables, am_filter, p_verbose)` | Iterates `engine.storage_health` and dispatches merge/repack for every table whose `recommended_action != 'ok'` |

```sql
-- Dry run: show what would be done
CALL engine.storage_maintenance_auto(dry_run => true);

-- Execute maintenance for colcompress tables only
CALL engine.storage_maintenance_auto(am_filter => 'colcompress', p_verbose => true);

-- Via pg_cron (runs every 5 minutes)
SELECT cron.schedule('storage-maintenance', '*/5 * * * *',
    $$CALL engine.storage_maintenance_auto()$$);
```

---

## Catalog Views

### colcompress_options

Per-table options for all colcompress tables.

```sql
SELECT * FROM engine.colcompress_options;
```

| Column | Type | Description |
|---|---|---|
| `table_name` | text | Schema-qualified table name |
| `stripe_row_limit` | int | Rows per stripe |
| `chunk_group_row_limit` | int | Rows per chunk group |
| `compression` | text | Compression codec |
| `compression_level` | int | Codec level |
| `orderby` | text | Sort key (NULL if not set) |
| `index_scan` | bool | Index scan enabled for this table |

### colcompress_stripes

Stripe-level metadata per table.

```sql
SELECT table_name, stripe_num, row_count, file_size_bytes, pruning_valid
FROM engine.colcompress_stripes
WHERE table_name = 'events'
ORDER BY stripe_num;
```

### rowcompress_options

Per-table options for all rowcompress tables.

```sql
SELECT * FROM engine.rowcompress_options;
```

| Column | Type | Description |
|---|---|---|
| `table_name` | text | Schema-qualified table name |
| `batch_size` | int | Rows per compressed batch |
| `compression` | text | Compression codec |
| `compression_level` | int | Codec level |

### rowcompress_batches

Batch-level metadata for all rowcompress tables.

```sql
SELECT table_name, batch_num, row_count, deleted_count, pruning_valid
FROM engine.rowcompress_batches
WHERE table_name = 'logs'
ORDER BY batch_num;
```

### storage_health

Unified health view for all colcompress and rowcompress tables.

```sql
SELECT
    table_name,
    am_name,
    total_units,
    dirty_units,
    tombstone_rows,
    live_rows,
    effective_pruning_ratio_est,
    recommended_action
FROM engine.storage_health
ORDER BY table_name;
```

| Column | Description |
|---|---|
| `table_name` | Schema-qualified table name |
| `am_name` | `colcompress` or `rowcompress` |
| `total_units` | Total stripes (col) or batches (row) |
| `dirty_units` | Units with deleted/tombstone rows |
| `tombstone_rows` | Total deleted rows not yet vacuumed |
| `live_rows` | Estimated live rows |
| `effective_pruning_ratio_est` | Fraction of units prunable by current sort order |
| `recommended_action` | `ok`, `merge`, `repack`, or `vacuum` |

---

## Configuration GUCs

All parameters can be set in `postgresql.conf` (global) or `SET` (per-session).

### Storage

| Parameter | Type | Default | Description |
|---|---|---|---|
| `storage_engine.compression` | enum | `zstd` | Default codec: `none`, `pglz`, `zstd`, `lz4`, `deflate` |
| `storage_engine.compression_level` | int | `3` | Default zstd level (1–19) |
| `storage_engine.stripe_row_limit` | int | `150000` | Max rows per stripe |
| `storage_engine.chunk_group_row_limit` | int | `10000` | Max rows per chunk group |

### Execution

| Parameter | Type | Default | Description |
|---|---|---|---|
| `storage_engine.enable_parallel_execution` | bool | `on` | Enable parallel scan via DSM |
| `storage_engine.min_parallel_processes` | int | `8` | Minimum parallel workers |
| `storage_engine.enable_vectorization` | bool | `on` | Enable vectorized WHERE/aggregate evaluation |
| `storage_engine.enable_vectorized_groupagg` | bool | `on` | Enable `StorageEngineVectorGroupAgg` |
| `storage_engine.enable_automatic_plan` | bool | `on` | Auto-compare serial vs parallel aggregate plans |
| `storage_engine.enable_dml` | bool | `on` | Allow DELETE and UPDATE |

### Custom Scan / Pushdown

| Parameter | Type | Default | Description |
|---|---|---|---|
| `storage_engine.enable_custom_scan` | bool | `on` | Enable projection + qual pushdown |
| `storage_engine.enable_qual_pushdown` | bool | `on` | Push WHERE quals into columnar scan layer |
| `storage_engine.qual_pushdown_correlation_threshold` | real | `0.4` | Min column correlation for qual pushdown |
| `storage_engine.enable_engine_index_scan` | bool | `off` | Enable index-driven columnar scan |

### Cache

| Parameter | Type | Default | Description |
|---|---|---|---|
| `storage_engine.enable_column_cache` | bool | `off` | Enable in-memory column chunk cache |
| `storage_engine.column_cache_size` | int | `200` | Column cache size in MB |

### Debug

| Parameter | Type | Default | Description |
|---|---|---|---|
| `storage_engine.debug_vectorized_groupagg_fallback` | bool | `off` | Log when `VectorGroupAgg` falls back to native `HashAggregate` |
| `storage_engine.planner_debug_level` | enum | `debug3` | Log level for planner diagnostics |

### Auto-Maintenance Background Worker

| Parameter | Type | Default | Description |
|---|---|---|---|
| `storage_engine.maintenance_auto_enabled` | bool | `off` | Enable background maintenance worker |
| `storage_engine.maintenance_auto_database` | string | `''` | Database to connect to (empty = disabled) |
| `storage_engine.maintenance_auto_naptime` | int | `300` | Seconds between maintenance runs |

---

## Known Limitations

### No AFTER ROW triggers / no foreign keys
colcompress and rowcompress do not support AFTER ROW triggers or foreign keys. This means `pg_repack` cannot be used. Use `engine.colcompress_repack()` instead:

```sql
SELECT engine.colcompress_repack('mytable'::regclass);
```

`colcompress_repack` acquires `AccessExclusiveLock` for the duration — schedule during a maintenance window for large tables.

### AFTER STATEMENT triggers are supported
Only row-level (`FOR EACH ROW`) AFTER triggers are blocked. Statement-level (`FOR EACH STATEMENT`) AFTER triggers work fine.

### Stripe pruning disabled in parallel mode
Each parallel worker reads its assigned stripes independently without a global pruning pass. Stripe pruning only applies in the sequential (non-parallel) scan path.

### B-tree index on orderby column disables pruning
If a B-tree index exists on the `orderby` column, the planner prefers the index path over the sequential scan, bypassing stripe pruning. For analytical tables, avoid B-tree indexes on the sort key. Use `SET storage_engine.enable_engine_index_scan = on` at session level for occasional point lookups instead.

### INSERT … SELECT with pre-existing indexes
Index TIDs may be corrupted when tables are populated via `INSERT INTO … SELECT` with pre-existing indexes. Workaround:

```sql
REINDEX TABLE CONCURRENTLY mytable;
```

---

## engine.uint8 Type Reference

| Attribute | Value |
|---|---|
| Type name | `engine.uint8` |
| Storage | 8 bytes, pass-by-value |
| Range | `[0, 18446744073709551615]` |
| Operators | `<`, `<=`, `=`, `<>`, `>=`, `>` (unsigned semantics) |
| Opclasses | btree (`engine.uint8_ops`), hash |
| Casts | `↔ bigint` (assignment), `→ numeric` (implicit), `↔ text` (assignment) |
| Aggregates | `engine.min`, `engine.max`, `engine.sum` (returns `numeric`) |
| Vectorized agg | `engine.vmin`, `engine.vmax`, `engine.vsum` |
