# CHANGELOG

## 1.0.9

* docs: **pg_search 0.23 (ParadeDB) compatibility** — colcompress tables are fully
  compatible with pg_search BM25 full-text search. The BM25 index (`CREATE INDEX
  USING bm25`) works transparently via `index_fetch_tuple`; `@@@`, `===`,
  `pdb.score()`, and `pdb.snippet()` all function correctly. To avoid
  `ColcompressScan` intercepting the planner before pg_search's `ParadeDB Base Scan`
  path is selected, use `SET storage_engine.enable_custom_scan = false` for queries
  that use `@@@`. A future release will auto-detect the `@@@` operator in
  `ColumnarSetRelPathlistHook` and skip the hook transparently.
* docs: **native regex alternative to BM25 for analytics** — `~*` (POSIX
  case-insensitive regex) on colcompress tables uses `ColcompressScan` with full
  parallelism and stripe-level projection pushdown, achieving the same recall as
  BM25 at 3× lower latency (60 ms vs ~200 ms for 150k rows, 8 parallel workers).
  Prefer `~*` over `@@@` for counter/aggregation patterns; reserve BM25 for ranked
  retrieval and fuzzy matching.
* bench: updated serial and parallel benchmark results; added baseline CSV for
  regression tracking.

## 1.0.8

* fix: **`UPDATE` duplicate-key error on colcompress tables with unique indexes** —
  `engine_index_fetch_tuple` now consults the in-memory `RowMaskWriteStateMap`
  bitmask before falling back to `ColumnarReadRowByRowNumber` for flushed stripes.
  Previously, `engine_tuple_update()` marked the old row deleted (via `UpdateRowMask`)
  and immediately inserted the new version; the unique-constraint recheck via
  `index_fetch_tuple` read a stale pre-deletion snapshot from the B-tree entry's old
  TID and returned "tuple still alive", causing a spurious duplicate-key error on
  every `UPDATE`.
* fix: **deleted rows visible within same command** — `engine_tuple_satisfies_snapshot`
  now also consults `RowMaskWriteStateMap`, so rows deleted within the current
  transaction are correctly reported as invisible during the same command, preventing
  false positives in constraint checks.
* fix: **OOM crash in `engine_tuple_update` with large VARLENA columns** —
  `ColumnarWriteRowInternal` adds a memory-based flush guard: if the
  `stripeWriteContext` exceeds 256 MB (`SE_MAX_STRIPE_MEM_BYTES`), the current stripe
  is flushed before buffering the next row. This prevents OOM crashes when stripe
  row-count limits are generous but rows carry large VARLENA columns (XML, JSON, PDF).

## 1.0.7

* fix: **GIN `BitmapHeapScan` bypasses `ColcompressScan` with `random_page_cost=1.1`**
  — On NVMe-tuned servers (`random_page_cost=1.1`), the planner preferred a GIN
  `Bitmap Heap Scan` over `Custom Scan (ColcompressScan)` for analytical queries
  with JSONB `@>` or array `@>` predicates when `index_scan=false`. This caused
  +195–237% regression in serial mode vs baseline (Q6 JSONB: 163ms→479ms,
  Q8 array: 123ms→414ms). Fixed by adding a `disable_cost` (1e10) penalty to every
  `BitmapHeapPath` in `CostColumnarPaths` when `index_scan=false`, symmetric with the
  existing penalty for `IndexPath`. Tables with `index_scan=true` are unaffected.
  Fix confirmed: serial Q6 175ms (-63%), Q8 141ms (-66%).
* fix: **`index_scan=false` gate missing in `engine_reader.c` chunk loader** —
  The single-chunk targeted loading optimisation (`ColumnarReadRowByRowNumber`)
  was activating unconditionally, including on analytics tables where
  `index_scan=false`. Added `indexScanEnabled` field to `ColumnarReadState`,
  populated from `ReadColumnarOptions` in `ColumnarBeginRead`, and gated the
  single-chunk optimisation on `readState->indexScanEnabled`.
* fix: **`BitmapHeapPath` penalty also applied to `partial_pathlist`** — parallel
  bitmap heap paths were not being penalised, allowing GIN scans via parallel
  workers to bypass `ColcompressScan` even with `index_scan=false`.
* fix: **infinite loop in index scan point lookup** — `ColumnarReadRowByRowNumber`
  could loop forever when the requested row number fell beyond the last stripe,
  producing a hang with no error output.
* fix: **index scan cost at chunk granularity** — `ColumnarIndexScanAdditionalCost`
  now computes `perChunkCost` instead of `perStripeCost`, eliminating the ~15×
  cost inflation that caused the planner to always reject `IndexScan` over
  `ColcompressScan` for selective point lookups on wide columnar tables.
* fix: **use projected column count in `ColumnarIndexScanAdditionalCost`** — replaced
  `RelationIdGetNumberOfAttributes` with `list_length(rel->reltarget->exprs)`, so
  wide tables with large blob columns (XML/JSON) no longer inflate index scan cost
  beyond the full-scan cost, restoring planner choice for `index_scan=true` tables.
* fix: **remove stray `randomAccessPenalty` from `ColumnarIndexScanAdditionalCost`**
  — the per-row penalty (`estimatedRows * cpu_tuple_cost * 100`) was dead code when
  `index_scan=false` (path already blocked by `disable_cost`) but was still evaluated
  when `index_scan=true`, causing the planner to always choose `SeqScan` over
  `IndexScan` regardless of selectivity. Removed entirely.

## 1.0.6

* fix: **`index_scan=false` bypassed by `Parallel Index Scan`** — `CostColumnarPaths`
  only iterated `rel->pathlist`, leaving `rel->partial_pathlist` (parallel paths)
  untouched. When a B-tree index existed on a colcompress table, the planner chose
  `Parallel Index Scan` even with `index_scan=false`, bypassing stripe pruning
  entirely. Fixed by iterating `rel->partial_pathlist` in `CostColumnarPaths` and
  applying `disable_cost` (1e10) to every `IndexPath` found there.
* fix: **`disable_cost` for `index_scan=false` serial paths** — replaced the
  proportional penalty (`estimatedRows * cpu_tuple_cost * 100.0`) with PostgreSQL's
  canonical `disable_cost` constant (1e10), matching the behaviour of
  `SET enable_indexscan = off`. The old penalty was smaller than the seq-scan cost
  for low-selectivity queries (~4% of rows), so the planner still preferred
  `IndexScan` over `ColcompressScan`.
* bench: updated serial and parallel benchmark results and charts (1M rows,
  PostgreSQL 18, 4 access methods).

## 1.0.5

* fix: **EXPLAIN + citus SIGSEGV** — `IsCreateTableAs(NULL)` called `strlen(NULL)` when
  citus passed `query_string=NULL` internally; added NULL guard. Added `IsExplainQuery`
  guard to skip `PlanTreeMutator` for EXPLAIN statements. Fixed `T_CustomScan` else
  branch to recurse into `custom_plans` instead of `elog(ERROR)`.
* fix: **stripe pruning bypassed by btree indexes** — when a btree index existed on a
  colcompress table, the planner chose `IndexScan` with `randomAccess=true`, which
  disabled stripe pruning entirely. Fixed by strengthening
  `ColumnarIndexScanAdditionalCost` with a per-row random-access penalty
  (`estimatedRows * cpu_tuple_cost * 100.0`), steering the planner back to seq scan.
* perf: **`ColumnarIndexScanAdditionalCost` per-row penalty** — discourages index scans
  on large colcompress tables where full-stripe pruning is more efficient.
* docs: **benchmark kit** — added `tests/bench/` with setup SQL, serial/parallel run
  scripts, chart generators, and result PNGs; added `BENCHMARKS.md` with full analysis.
* docs: **README** — citus load order note, btree/stripe-pruning Known Limitation,
  Benchmarks section, corrected install path.

## 1.0.4

* chore: bump version to 1.0.4 (PGXN meta).
* docs: benchmark results — heap vs colcompress vs rowcompress vs citus_columnar.

## 1.0.3

* perf: **stripe-level min/max pruning for colcompress scans** — before reading
  any stripe, the scan aggregates the per-column min/max statistics from
  `engine.chunk` across all chunks of the stripe and tests the resulting
  stripe-wide ranges against the query's WHERE predicates using
  `predicate_refuted_by`. Any stripe whose range is provably disjoint from the
  predicate is skipped entirely — no decompression, no I/O. The pruned count is
  shown in `EXPLAIN`:

  ```
  Engine Stripes Removed by Pruning: N
  ```

  Pruning applies to both the serial scan path and the parallel DSM path
  (parallel workers only receive stripe IDs that survive the filter).
  Effectiveness scales directly with data sortedness; combine with
  `engine.colcompress_merge()` and the `orderby` table option to maximise it.

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

