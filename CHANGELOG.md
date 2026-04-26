# CHANGELOG

## 1.2.6

* feat: **Vectorized aggregates for `float8`, `numeric`, and `money`** —
  extends `StorageEngineVectorAgg` to cover three more numeric types:
  - **`float8`**: `vsum`, `vavg`, `vmin`, `vmax` (`se_vfloat8pl`,
    `se_vfloat8_accum`, `se_vfloat8larger`, `se_vfloat8smaller`)
  - **`numeric`**: `vsum`, `vavg`, `vmin`, `vmax` (`se_vnumericavg_accum`,
    `se_vnumericavg_final`, `se_vnumericsum_final`, `se_vnumericlarger`,
    `se_vnumericsmaller`)
  - **`money`**: `vsum`, `vmin`, `vmax` (`se_vcashpl`, `se_vcashsmaller`,
    `se_vcashlarger`) — no `avg` because PostgreSQL has no `avg(money)`
* fix: **Parallel aggregate correctness** — added `AGGSPLIT_SIMPLE` guard in
  the planner hook so the vectorized path is only substituted for non-split
  (non-partial) aggregates, preventing incorrect results in parallel queries.

## 1.2.5

* fix: **Restore continuous upgrade chain** — added missing upgrade scripts
  `1.2.1→1.2.2` and `1.2.2→1.2.3`, which caused `CREATE EXTENSION storage_engine`
  (and `ALTER EXTENSION ... UPDATE`) to fail with "no installation script nor update
  path for version" on systems that had installed any version from 1.2.1 onward.
  No catalog changes; upgrade from 1.2.4 is a no-op.

## 1.2.4

* feat: **Vectorized aggregates fully operational** — `vmin`, `vmax`, `vsum`, `vavg`,
  and `vcount` are now registered in the `engine` schema (18 C functions + 16 aggregate
  definitions). `SELECT min(col), max(col), sum(col), count(*) FROM colcompress_table`
  transparently uses `StorageEngineVectorAgg` when `storage_engine.enable_vectorization = on`,
  yielding ~**1.4× speedup** over standard heap-style evaluation on 1M-row tables.
* feat: **EXPLAIN ANALYZE shows VectorAgg node** — previously `IsExplainQuery` blocked
  vectorization for both plain `EXPLAIN` and `EXPLAIN ANALYZE`; now only plain `EXPLAIN`
  is blocked. `EXPLAIN ANALYZE` correctly shows `Custom Scan (StorageEngineVectorAgg)`
  with `Engine Vectorized Aggregate: enabled` annotation.
* feat: **Schema-qualified vectorized function lookup** — `GetVectorizedProcedureOid()`
  now searches `engine.vXXX` (schema-qualified) instead of unqualified names, preventing
  false positives with similarly-named functions in other schemas.
* fix: **NULL-safe vmin/vmax** — vectorized min/max transition functions now return
  `NULL` when scanning an empty result set (previously returned `INT_MIN`/`INT_MAX`).
  Affects `vint2smaller/larger`, `vint4smaller/larger`, `vint8smaller/larger`,
  `vdatesmaller/larger`.
* fix: **C function name mismatches** — `PG_FUNCTION_INFO_V1` declarations now match
  their `Datum` body names (`se_vXXX`) for all 9 affected functions in `aggregates.c`.
* fix: **Missing closing brace in `se_vint8smaller`** — caused a compilation error
  on GCC (`static declaration follows non-static declaration`) for date aggregate
  functions when building for PostgreSQL 18.

## 1.2.3

* fix: **`CREATE EXTENSION` failure on fresh install** — `default_version` in
  `storage_engine.control` was incorrectly bumped to `1.2.2` in the previous
  release, causing `ERROR: extension "storage_engine" has no installation
  script nor update path for version "1.2.2"`. The SQL extension version only
  needs to change when catalog objects (tables, functions, views in the
  `engine` schema) actually change. 1.2.2 was a C/build-only release —
  `default_version` has been corrected back to `1.2.1`.
  Reported by user after installing from PGXN.

## 1.2.2

* feat: **ZXC compression** (`compression='zxc'`) — adds support for the
  [ZXC asymmetric codec](https://github.com/hellobertrand/zxc) (BSD-3-Clause).
  Write-Once Read-Many design: encoder is slow; decoder is SIMD-maximized
  (NEON on ARMv8+, AVX2/AVX-512 on x86_64). Decompression throughput vs LZ4:
  Neoverse-V2 +24%, x86_64 AMD EPYC +18%, Apple M2 +46%.
  Not yet in apt — build from source. Auto-detected by `Makefile.global`.

* feat: **libdeflate compression** (`compression='deflate'`) — adds support for
  [libdeflate](https://github.com/ebiggers/libdeflate), a zlib-compatible codec
  with better throughput than the standard zlib. Available as `libdeflate-dev`
  on Ubuntu/Debian. Auto-detected by `Makefile.global`.

* build: **all compression libraries are now optional** — previously LZ4, ZSTD
  and libdeflate were hardcoded in `citus_config.h`, causing link failures on
  systems without those libraries. All four codecs (LZ4, ZSTD, Deflate, ZXC)
  are now detected dynamically at build time via `Makefile.global` header
  detection. The extension falls back to PostgreSQL's built-in `pglz` when no
  external library is present. Default precedence when available:
  `ZSTD > ZXC > LZ4 > Deflate > pglz`.

* bench: **aarch64 benchmark area** (`tests/bench/aarch64/`) — new directory
  with serial and parallel benchmark results on ARM Neoverse-N1 / Graviton2
  (PostgreSQL 18.1, 1M rows). Includes results for all four compression codecs
  (ZSTD, LZ4, Deflate, ZXC) with comparison charts.
  Key finding: ZXC achieves fastest analytical read performance on aarch64 in
  6/10 queries, beating even LZ4 despite slightly larger disk size (123 MB vs
  118 MB), confirming its SIMD NEON advantage on ARM.

## 1.2.1

* fix: **GUC visibility** — `storage_engine.enable_vectorization`,
  `enable_parallel_execution`, `enable_dml`, and `enable_engine_index_scan`
  were registered with `GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE`, hiding them
  from `\dconfig` and psql tab-completion. Removed — all operational GUCs
  are now discoverable. Note: GUCs only take effect when the extension is
  listed in `shared_preload_libraries`.

* fix: **`-Wmissing-variable-declarations`** — `ColumnarScanPathMethods`,
  `ColumnarScanScanMethods`, and `ColumnarScanExecuteMethods` lacked
  `extern` declarations in `engine_customscan.h`, causing warnings (fatal
  with `-Werror`) under stricter compiler settings.

* fix: **`table_beginscan` 5-argument compile error on PG16–18** — The PG19
  API added a 5th `flags` argument to `table_beginscan`. The call site in
  `RCScan_BeginCustomScan` is now guarded with
  `#if PG_VERSION_NUM >= PG_VERSION_19`. This error affected builds from
  the `v1.2.0` tag against PG16–18.

* fix: **`statement_timeout` cancels `engine.smart_update` /
  `engine.colcompress_bulk_update` mid-run** — `set_config('statement_timeout',
  '0', false)` was applied once before the stripe loop, but PostgreSQL
  resets session-level GUCs at each `COMMIT` inside a procedure. Both
  procedures now re-apply the timeout overrides at the top of every loop
  iteration.

* feat: **`engine.smart_update` parallel worker cap** —
  `max_parallel_workers_per_gather` is set to `max_parallel_workers / 2`
  at procedure start, preventing the maintenance procedure from consuming
  the full parallel worker pool. Integer division: 0→0 (serial), 1→0
  (serial), 2→1, 4→2, 16→8.

## 1.2.0
* feat: **`index_scan` per-table option for `rowcompress`** —
  `rowcompress` now supports `index_scan` as a per-table flag, providing
  feature parity with `colcompress`. Default (`false`) keeps the analytical
  mode: range index paths are removed by the planner hook so queries use
  the batch-compressed sequential scan with batch-level min/max pruning.
  When set to `true`, index scans are allowed (OLTP / document-store mode).
  New 6th argument to `engine.alter_rowcompress_table_set()` and new 5th
  argument to `engine.alter_rowcompress_table_reset()`. The `engine.rowcompress_options`
  view now exposes the `index_scan` column. Upgrade via
  `ALTER EXTENSION storage_engine UPDATE TO '1.2'`.

## 1.1.5

* compat: **PostgreSQL 19 support** — `storage_engine.so` now compiles and
  runs on PostgreSQL 19 (devel). README compatibility table updated.
* fix: **META.json PGXN license field** — changed `license` value to the
  PGXN-recognized string `agpl_3`.

## 1.1.4

* fix: **`ORDER BY` silently dropped with parallel `ColcompressScan`** —
  When a query had `ORDER BY` and the planner chose a parallel `ColcompressScan`,
  PostgreSQL emitted `Gather(ColcompressScan)` without any `Sort` node above it,
  returning rows in arbitrary worker-completion order instead of the requested
  order. Root cause: `ColcompressScan` paths have `pathkeys = NIL` (columnar
  data has no inherent physical order), so `generate_useful_gather_paths()`
  found no pre-sorted partial paths and could not build `Gather Merge`. Fix:
  when `root->query_pathkeys != NIL`, a `Sort(ColcompressScan)` partial path is
  added to `partial_pathlist` alongside the unsorted one. The planner can now
  choose `Gather Merge(Sort(ColcompressScan))` and correctly satisfies `ORDER BY`.
* fix: **double `_PG_init()` when Citus is in `shared_preload_libraries`** —
  On PG15 the Citus APT package dynamically loads `citus_columnar.so` via
  `dlopen()` at load time, which re-entered `_PG_init()` for any co-loaded
  extension. This caused:
  `ERROR: attempt to redefine parameter "storage_engine.compression"` and
  `ERROR: extensible node type "ColumnarScan" already exists`.
  Fix: added `GetConfigOption()` early-return guard in `engine_guc_init()` and
  an `if (GetConfigOption(...) == NULL)` block guard in `engine_customscan_init()`,
  mirroring the `GetCustomScanMethods()` guard already in place for
  `RegisterCustomScanMethods`. The init functions are now idempotent.

## 1.1.3

* fix: **remove `citus_config.h` dependency from vendored safeclib** —
  `safeclib/safeclib_private.h` included `citus_config.h` (generated by Citus
  `./configure`), causing a fatal compile error on clean clones:
  `fatal error: citus_config.h: No such file or directory`. Replaced with
  inline `#define` macros for the standard POSIX feature flags it provided.
* fix: **suppress `-Wdeclaration-after-statement` warnings** — added
  `-Wno-declaration-after-statement` to `Makefile.global`; the codebase uses
  C99 mixed declarations which are valid for PostgreSQL extensions.
* cleanup: **remove unused static functions** — `IsIndexPath`,
  `RCFindBatchForRowNumber`, `rowcompress_estimate_rel_size`, and
  `rowcompress_relation_set_new_filenode_compat` were declared/defined but
  never called, producing `-Wunused-function` warnings.

## 1.1.2

* fix: **remove stray `#include "citus_version.h"` from source files** —
  `citus_version.h` is a file generated by the Citus `./configure` step and is
  not present in a clean clone. Its absence caused a fatal compile error:
  `fatal error: citus_version.h: No such file or directory`. Removed from all
  eight translation units that referenced it. The `HAVE_CITUS_LIBLZ4` macro
  (also defined in that header) was replaced with the standard PostgreSQL
  `HAVE_LIBLZ4` macro throughout.

## 1.1.1

* fix: **remove Citus autoconf build artifacts** — the root `Makefile` was the
  Citus 11.1devel toplevel Makefile and required `./configure` (a Citus-specific
  autoconf script) to be run before any build could proceed. This caused
  `configure: error: C compiler cannot create executables` and other
  Citus-specific probe failures for users with non-standard toolchains (ccache
  without a backing compiler, aarch64/ARM Linux, NixOS, etc.).
  The root `Makefile` is now a simple delegator to `src/backend/engine`.
  A portable, pre-generated `Makefile.global` is now tracked in the repository
  and uses `pg_config` from `PATH` — no `./configure` step is needed.
  The six Citus autoconf artifacts (`configure`, `configure.in`, `autogen.sh`,
  `aclocal.m4`, `Makefile.global.in`, `src/include/citus_config.h.in`) are
  removed from the repository.
  Build is now simply:
  ```bash
  sudo make -j$(nproc) install
  # or with an explicit pg_config:
  PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config sudo make install
  ```

## 1.1.0

* feat: **`RowcompressScan` custom scan node with batch-level min/max pruning** —
  `rowcompress` tables now support a `pruning_column` parameter
  (`engine.alter_rowcompress_table_set(tbl, pruning_column := 'col')`).
  When set, `RowcompressScan` records the serialised min/max value of the pruning
  column per batch during `engine.rowcompress_repack()` or bulk inserts, storing
  them in `engine.row_batch.batch_min_value` / `batch_max_value`. At scan time,
  batches whose range does not intersect the query predicate are skipped entirely —
  no decompression, no I/O. The new GUC `storage_engine.enable_custom_scan` (default
  `on`) controls whether `RowcompressScan` is injected by the planner hook.
* feat: **`engine.rowcompress_repack(tbl)`** — utility function that rewrites all
  batches of a `rowcompress` table in sorted order by the `pruning_column`, maximising
  pruning efficiency for range queries (e.g. date, timestamp, bigint sequences).
* schema: **`engine.row_options.pruning_attnum`** — new nullable `int2` column; stores
  the 1-based attribute number of the pruning column.
* schema: **`engine.row_batch.batch_min_value` / `batch_max_value`** — new nullable
  `bytea` columns; store serialised type-agnostic min/max statistics per batch.
* upgrade: `ALTER EXTENSION storage_engine UPDATE TO '1.1'` applies the schema changes
  via `storage_engine--1.0--1.1.sql`.

## 1.0.10

* fix: **pg_search (ParadeDB) BM25 transparent compatibility** — `IsNotIndexPath` in
  `engine_customscan.c` now preserves `CustomPath` nodes whose `CustomName` equals
  `"ParadeDB Base Scan"`. Previously, `RemovePathsByPredicate(rel, IsNotIndexPath)`
  discarded pg_search's planner path, causing the `@@@` operator to fall through as a
  `Filter` inside `ColcompressScan`, which then failed with "Unsupported query shape".
  BM25 full-text search on colcompress tables now works **transparently** — no need for
  `SET storage_engine.enable_custom_scan = false`. `pdb.score()`, `pdb.snippet()`, `===`,
  and multi-field `AND @@@` all work correctly. `ColcompressScan` continues to handle all
  other query shapes (projection pushdown, stripe pruning, parallel scan) without change.

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

