# CHANGELOG

## 1.0.2

* fix: **index corruption during `COPY` into colcompress tables** — `engine_multi_insert`
  was calling `ExecInsertIndexTuples()` internally, while COPY's
  `CopyMultiInsertBufferFlush` also calls it after `table_multi_insert` returns.
  The double insertion corrupted every B-tree index on tables loaded via `COPY`.
  Fixed by removing all executor infrastructure from the per-tuple loop; index
  insertion is the caller's responsibility, matching `heap_multi_insert` semantics.
* fix: **index corruption when `orderby` and indexes coexist** — when sort-on-write
  is active, `ColumnarWriteRow()` buffers rows and returns `COLUMNAR_FIRST_ROW_NUMBER`
  (= 1) as a placeholder for every row. The executor then indexed all rows with
  TID `(0,1)`, making every index lookup return the first row. Fixed in
  `engine_init_write_state()`: sort-on-write is disabled when the target relation
  has `relhasindex = true`. Tables with indexes already have fast key access;
  sort ordering is redundant and was silently lethal.
* perf: fast `ANALYZE` via chunk-group stride sampling — samples at most
  `N / stride` chunk groups (`stride = max(1, nchunks / 300)`) instead of
  reading the entire table, making `ANALYZE` on large colcompress tables
  milliseconds instead of minutes.

> **Migration note (1.0.1 → 1.0.2):** any colcompress table that has indexes
> and was written with `COPY` or `colcompress_merge` using a prior version must
> be rebuilt: `REINDEX TABLE CONCURRENTLY <table>;`

## 1.0.1

* fix: `multi_insert` now sets `tts_tid` before opening indexes, and explicitly
  calls `ExecInsertIndexTuples()` — previously B-tree entries received garbage
  TIDs during `INSERT INTO ... SELECT`, causing index scans to return wrong rows.
  Tables populated before this fix require `REINDEX TABLE CONCURRENTLY`.
* fix: `orderby` syntax is now validated at `ALTER TABLE SET (orderby=...)` time
  instead of at merge time, giving an immediate error on bad input.
* fix: CustomScan node names renamed to avoid symbol collision with `columnar.so`
  when both extensions are loaded simultaneously.
* fix: corrected SQL function names for `se_alter_engine_table_set` /
  `se_alter_engine_table_reset` (C symbols were mismatched).
* fix: added `safeclib` symlink under `vendor/` so `memcpy_s` resolves correctly
  at link time.
* add: `META.json` for PGXN publication.

## 1.0.0

Initial release of **storage_engine** — a PostgreSQL table access method extension
derived from [Hydra Columnar](https://github.com/hydradatabase/hydra) and extended
with two independent access methods:

* **colcompress** — column-oriented storage with vectorized execution, parallel
  DSM scan, chunk pruning, and a MergeTree-style per-table sort key (`orderby`).
* **rowcompress** — row-compressed batch storage with parallel work-stealing scan
  and full DELETE/UPDATE support via a row-level mask.

Additional features added beyond the upstream:

* per-table `index_scan` option (GUC `storage_engine.enable_index_scan`)
* full DELETE/UPDATE support for colcompress via row mask
* parallel columnar scan wired through DSM
* GUCs under the `storage_engine.*` namespace
* support for PostgreSQL 16, 17, and 18

