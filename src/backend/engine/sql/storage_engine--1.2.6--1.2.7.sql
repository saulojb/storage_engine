-- storage_engine upgrade: 1.2.6 → 1.2.7
-- Bug fix release:
--   • vsum(smallint) and vsum(integer) now return NULL for empty input,
--     matching the standard behaviour of sum(smallint) and sum(integer).
--     Previously both returned 0 (the aggregate's initcond) for empty sets.
--
-- Root cause: the aggregate definitions carried initcond = '0', so the
-- state was never NULL even when no rows were processed.  The C transition
-- functions (se_vint2sum, se_vint4sum) also lacked a NULL-state guard,
-- which would have caused undefined behaviour if called with a NULL state.
--
-- Fix: the initcond is dropped from both aggregates, and the C functions
-- now properly propagate NULL when state is NULL and no non-NULL values
-- were encountered in the vector.

-- =========================================================
-- vsum(smallint) — remove initcond = '0'
-- =========================================================

-- CREATE OR REPLACE replaces the definition in-place, preserving
-- the extension membership.  Omitting initcond means the initial
-- state is NULL → empty set returns NULL (SQL standard).
CREATE OR REPLACE AGGREGATE engine.vsum(smallint) (
    sfunc = engine.vint2sum,
    stype = bigint
);

-- =========================================================
-- vsum(integer) — remove initcond = '0'
-- =========================================================

CREATE OR REPLACE AGGREGATE engine.vsum(integer) (
    sfunc = engine.vint4_sum,
    stype = bigint
);
