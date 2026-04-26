-- storage_engine upgrade: 1.2.5 → 1.2.6
-- Feature release:
--   • Vectorized aggregates for float8 (double precision):
--       vsum, vavg, vmin, vmax
--   • Vectorized aggregates for numeric:
--       vsum, vavg, vmin, vmax
--   This extends the vectorization speedup beyond COUNT(*) to
--   SUM/AVG/MIN/MAX queries on float8 and numeric columns.

-- =========================================================
-- FLOAT8 (double precision) VECTORIZED AGGREGATE FUNCTIONS
-- =========================================================

-- Transition function for sum(float8):
--   State = double precision, no initcond → NULL for empty set
CREATE FUNCTION engine.vfloat8pl(double precision, double precision)
    RETURNS double precision
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vfloat8pl';

-- Transition function for avg(float8):
--   State = double precision[3] = {N, sumX, sumX²} — matches float8_accum
--   convention so pg_catalog.float8_avg can be reused as the final function.
CREATE FUNCTION engine.vfloat8_accum(double precision[], double precision)
    RETURNS double precision[]
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vfloat8_accum';

-- Transition function for max(float8)
CREATE FUNCTION engine.vfloat8larger(double precision, double precision)
    RETURNS double precision
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vfloat8larger';

-- Transition function for min(float8)
CREATE FUNCTION engine.vfloat8smaller(double precision, double precision)
    RETURNS double precision
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vfloat8smaller';

-- =========================================================
-- FLOAT8 VECTORIZED AGGREGATES
-- =========================================================

-- vsum(float8) maps to sum(float8)
CREATE AGGREGATE engine.vsum(double precision) (
    sfunc    = engine.vfloat8pl,
    stype    = double precision
);

-- vavg(float8) maps to avg(float8)
--   Final function is pg_catalog.float8_avg which computes state[1]/state[0]
CREATE AGGREGATE engine.vavg(double precision) (
    sfunc     = engine.vfloat8_accum,
    stype     = double precision[],
    finalfunc = pg_catalog.float8_avg,
    initcond  = '{0,0,0}'
);

-- vmin(float8) maps to min(float8)
CREATE AGGREGATE engine.vmin(double precision) (
    sfunc = engine.vfloat8smaller,
    stype = double precision
);

-- vmax(float8) maps to max(float8)
CREATE AGGREGATE engine.vmax(double precision) (
    sfunc = engine.vfloat8larger,
    stype = double precision
);

-- =========================================================
-- NUMERIC VECTORIZED AGGREGATE FUNCTIONS
-- =========================================================

-- Transition function for sum(numeric): kept for standalone use
-- (aggregate vsum(numeric) uses vnumeric_avg_accum+vnumeric_sum instead,
--  so it shares stype=internal with vavg(numeric) and the planner assigns
--  both the same aggtransno, allowing correct pertrans sharing)
CREATE FUNCTION engine.vnumeric_add(numeric, numeric)
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vnumeric_add';

-- Transition function for avg(numeric) and sum(numeric): state = internal
CREATE FUNCTION engine.vnumeric_avg_accum(internal, numeric)
    RETURNS internal
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vnumericavg_accum';

-- Final function for avg(numeric)
CREATE FUNCTION engine.vnumeric_avg(internal)
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vnumericavg_final';

-- Final function for sum(numeric): extracts just the sum from the shared state
CREATE FUNCTION engine.vnumeric_sum(internal)
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vnumericsum_final';

-- Transition function for max(numeric)
CREATE FUNCTION engine.vnumeric_larger(numeric, numeric)
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vnumericlarger';

-- Transition function for min(numeric)
CREATE FUNCTION engine.vnumeric_smaller(numeric, numeric)
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vnumericsmaller';

-- =========================================================
-- NUMERIC VECTORIZED AGGREGATES
-- =========================================================

-- vsum(numeric) maps to sum(numeric).
-- Uses stype=internal with the same sfunc as vavg(numeric) so that the
-- planner assigns both the same aggtransno (they share a single pertrans).
CREATE AGGREGATE engine.vsum(numeric) (
    sfunc     = engine.vnumeric_avg_accum,
    stype     = internal,
    finalfunc = engine.vnumeric_sum
);

-- vavg(numeric) maps to avg(numeric)
CREATE AGGREGATE engine.vavg(numeric) (
    sfunc     = engine.vnumeric_avg_accum,
    stype     = internal,
    finalfunc = engine.vnumeric_avg
);

-- vmin(numeric) maps to min(numeric)
CREATE AGGREGATE engine.vmin(numeric) (
    sfunc = engine.vnumeric_smaller,
    stype = numeric
);

-- vmax(numeric) maps to max(numeric)
CREATE AGGREGATE engine.vmax(numeric) (
    sfunc = engine.vnumeric_larger,
    stype = numeric
);

-- =========================================================
-- MONEY (Cash = int64) VECTORIZED AGGREGATE FUNCTIONS
-- PostgreSQL has no avg(money), so only vsum/vmin/vmax.
-- =========================================================

-- Transition function for max(money)
CREATE FUNCTION engine.vcashlarger(money, money)
    RETURNS money
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vcashlarger';

-- Transition function for min(money)
CREATE FUNCTION engine.vcashsmaller(money, money)
    RETURNS money
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vcashsmaller';

-- Transition function for sum(money)
CREATE FUNCTION engine.vcashpl(money, money)
    RETURNS money
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vcashpl';

-- =========================================================
-- MONEY VECTORIZED AGGREGATES
-- =========================================================

-- vsum(money) maps to sum(money)
CREATE AGGREGATE engine.vsum(money) (
    sfunc = engine.vcashpl,
    stype = money
);

-- vmin(money) maps to min(money)
CREATE AGGREGATE engine.vmin(money) (
    sfunc = engine.vcashsmaller,
    stype = money
);

-- vmax(money) maps to max(money)
CREATE AGGREGATE engine.vmax(money) (
    sfunc = engine.vcashlarger,
    stype = money
);
