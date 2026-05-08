# Vectorization + Parallel Checklist

Objective: guarantee that vectorized execution is not only correct, but actively used in parallel plans where supported.

## 1. Build and deployment hygiene

- [ ] For each target PG version, run a clean build before install:
  - `make -C dist/src/backend/engine clean`
  - `make -C dist clean`
  - `make -C dist PG_CONFIG=/usr/lib/postgresql/<v>/bin/pg_config`
  - `sudo make -C dist PG_CONFIG=/usr/lib/postgresql/<v>/bin/pg_config install`
- [ ] Restart cluster after install when `shared_preload_libraries` includes `storage_engine`.
- [ ] Validate no cross-version symbol contamination in installed library.

## 2. Correctness matrix

- [ ] Run full suite on PG 15, 16, 17, 18, 19.
- [ ] Keep matrix artifacts in `dist/tests/bench/matrix_YYYYMMDD[_tag]/`.
- [ ] Require `ALL 155 TESTS PASSED` for each supported version before release.

## 3. Plan-shape gates (must be explicit)

- [ ] Parallel grouped query with `count/sum/min/max` must show `StorageEngineVectorGroupAgg` in EXPLAIN.
- [ ] The same grouped query must show parallel execution in EXPLAIN (contains `Parallel`).
- [ ] Parallel grouped `avg(...)` must fallback (no `StorageEngineVectorGroupAgg`).
- [ ] PG15 numeric plain aggregate must fallback (no `StorageEngineVectorAgg`).

## 4. Result-equivalence gates

- [ ] For vectorized-supported shapes, enforce `VEC OFF == VEC ON`.
- [ ] For fallback shapes, enforce `VEC ON` result equals native/fallback result.
- [ ] Add no-crash checks (`rc == 0`) for all fallback-sensitive shapes.

## 5. Performance sanity gates (non-flaky)

- [ ] Run `EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON)` for a representative parallel grouped shape.
- [ ] Parse execution time for vectorized and native plans.
- [ ] Enforce wide catastrophe threshold only (current: vectorized <= 5x native).
- [ ] Keep stricter SLO checks in benchmark pipelines, not in unit/regression suite.

## 6. Release evidence

- [ ] Add plan excerpts proving vectorized parallel path to release notes.
- [ ] Add fallback excerpts proving safe degradation for unsupported shapes.
- [ ] Link matrix summary file and benchmark artifacts in CHANGELOG entry.
