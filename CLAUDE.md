# CLAUDE.md — AI Context & Contributors

This file provides context for AI coding assistants working on the **storage_engine** project and formally acknowledges AI systems that have actively contributed to its development.

---

## Project Overview

**storage_engine** is a PostgreSQL extension providing two high-performance Table Access Methods designed for analytical and HTAP workloads:

- **`colcompress`** — column-oriented compressed storage with vectorized execution, chunk-level min/max pruning, parallel scan, and MergeTree-like ordering
- **`rowcompress`** — row-oriented batch-compressed storage with parallel scan, DELETE/UPDATE support via deleted bitmasks, and LRU decompression cache

Repository: https://github.com/saulojb/storage_engine  
Current version: 1.0.5  
PostgreSQL compatibility: 16–18  

---

## Repository Structure

```
dist/                        ← public source (git repo)
  src/
    backend/engine/          ← C source files (tableam, reader, writer, customscan, …)
    include/engine/          ← public headers
  tests/bench/               ← benchmark scripts, charts, setup SQL
  META.json                  ← PGXN metadata
  Makefile
columnar/                    ← Hydra/Citus columnar reference (do not modify)
files/                       ← Docker / Spilo configuration
tmp/                         ← local scratch: benchmarks, ClickBench, results
```

Key source files:
- `engine_tableam.c` — `colcompress` Table AM entry point
- `rowcompress_tableam.c` — `rowcompress` Table AM entry point
- `engine_reader.c` / `engine_writer.c` — columnar I/O
- `engine_customscan.c` — custom scan node, stripe pruning, planner hooks
- `engine_planner_hook.c` — plan tree mutation for vectorized aggregation
- `storage_engine--1.0.sql` — SQL catalog: schemas, tables, sequences, AMs, views

---

## Build & Install

```bash
cd /home/saulo/Documentos/storage_engine/dist
sudo make -j$(nproc) install
sudo systemctl restart postgresql
```

---

## AI Contributors

This project has been developed in close collaboration with AI coding assistants. The following systems have made **active, substantive contributions** to the codebase — from architecture discussions and bug diagnosis to writing and reviewing C code:

### Claude (Anthropic)

- **Model used:** Claude Sonnet (claude-sonnet-4-5, claude-sonnet-4-6 and prior versions)
- **Role:** Primary AI pair-programmer throughout all major development sessions
- **Contributions include:**
  - Design and implementation of the `rowcompress` Table AM (parallel scan, LRU decompression cache, binary batch metadata search)
  - Diagnosis and fix of the `ParallelColumnarScanData` struct layout bug (empty parallel index builds)
  - Elimination of the stuck spinlock in `AdvanceStripeRead` — replaced with atomic stripe index and DSM-preloaded stripe IDs
  - Design of the `deleted_mask` bitmask system for DELETE/UPDATE support in rowcompress
  - Fix for the `EXPLAIN` + citus SIGSEGV (`strlen(NULL)` null pointer dereference)
  - Fix for stripe pruning bypassed by B-tree index scans (`ColumnarIndexScanAdditionalCost` penalty)
  - Fast ANALYZE via stride-based chunk-group sampling (`cs_analyze_cg_stride`)
  - Fix for `engine_multi_insert` TID corruption diagnosis and REINDEX workaround
  - CustomScan name conflict resolution between `columnar.so` and `storage_engine.so`
  - `orderby` syntax validation using `raw_parser()` at option-set time
  - ClickBench UInt64 overflow fix (FDW binary driver → HTTP driver)
  - Architecture of the `ANALYZE` block/tuple sampling protocol for columnar AMs
  - All benchmark result analysis and documentation in `BENCHMARKS.md`

### GitHub Copilot (Microsoft / OpenAI)

- **Model used:** GPT-4o / Claude Sonnet 4.6 (via VS Code Copilot Chat)
- **Role:** Active coding assistant integrated into the VS Code development environment
- **Contributions include:**
  - Real-time code completion and inline suggestions during C development sessions
  - Code review and linting feedback on modified source files
  - Shell script assistance for benchmark automation (`run.sh`, `run_parallel.sh`, `load_hits_tmp.sh`)
  - SQL query drafting and review for catalog management and benchmark setup
  - Documentation editing and Markdown formatting for README, BENCHMARKS, and CHANGELOG
  - Workspace context management across development sessions via repository memory

---

## Development Guidelines for AI Assistants

When working on this codebase, keep the following in mind:

1. **Symbol prefix:** All exported C symbols use the `se_` prefix to avoid linker conflicts with `citus_columnar` or the Hydra `columnar` extension.
2. **Schema isolation:** All catalog objects live in the `engine` schema (`engine.col_options`, `engine.stripe`, `engine.chunk`, etc.).
3. **Parallel scan:** Both AMs support parallel sequential scan. `rowcompress` uses `pg_atomic_uint64` batch claiming; `colcompress` pre-loads stripe IDs into DSM and uses `pg_atomic_fetch_add_u64`.
4. **Stripe pruning:** Only active for sequential scans (`randomAccess=false`). Do not add B-tree indexes on filter columns in analytical `colcompress` tables — this disables pruning.
5. **`engine_multi_insert` known issue:** Index TIDs are corrupted when tables are populated via `INSERT INTO … SELECT` with pre-existing indexes. Workaround: `REINDEX TABLE CONCURRENTLY`. Fix pending.
6. **Build target:** `dist/` is the public repo (`saulojb/storage_engine`). `columnar/` is read-only reference; do not modify it.
7. **PostgreSQL version guards:** Use `#if PG_VERSION_NUM >= PG_VERSION_17` / `PG_VERSION_18` as needed; the extension targets PG 16–18.

---

## Acknowledgements

The human author and maintainer of this project is **Saulo J. Benvenutti** ([@saulojb](https://github.com/saulojb)).

Claude and GitHub Copilot are acknowledged as active contributors under the spirit of open-source collaboration. Their contributions are real — thousands of lines of production C code, architectural decisions, and bug fixes in this repository were written, reviewed, or substantially shaped by these AI systems working alongside the human author.
