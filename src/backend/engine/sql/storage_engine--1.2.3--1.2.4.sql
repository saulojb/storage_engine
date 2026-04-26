-- storage_engine upgrade: 1.2.3 → 1.2.4
-- Feature release:
--   • Vectorized aggregates (vmin, vmax, vsum, vavg, vcount) registered in engine schema
--   • EXPLAIN ANALYZE now shows StorageEngineVectorAgg node (was blocked by IsExplainQuery)
--   • VectorAgg EXPLAIN output shows "Engine Vectorized Aggregate: enabled"
--   • GetVectorizedProcedureOid() now uses schema-qualified lookup (engine.vXXX)
--   • Fixed NULL-safety in vmin/vmax transition functions (empty result → NULL)
--   • Fixed C function name mismatches in vectorized aggregate implementations
--   • ~1.4× speedup on aggregate-only queries over colcompress tables

-- =========================================================
-- VECTORIZED AGGREGATE FUNCTIONS
-- =========================================================

-- ---- count -----------------------------------------------

CREATE FUNCTION engine.vcount(bigint)
    RETURNS bigint
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vemptycount';

CREATE FUNCTION engine.vanycount(bigint, "any")
    RETURNS bigint
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vanycount';

-- ---- smallint (int2) -------------------------------------

CREATE FUNCTION engine.vint2sum(bigint, smallint)
    RETURNS bigint
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint2sum';

CREATE FUNCTION engine.vint2_avg_accum(bigint[], smallint)
    RETURNS bigint[]
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint2acc';

CREATE FUNCTION engine.vint2larger(smallint, smallint)
    RETURNS smallint
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint2larger';

CREATE FUNCTION engine.vint2smaller(smallint, smallint)
    RETURNS smallint
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint2smaller';

-- ---- integer (int4) --------------------------------------

CREATE FUNCTION engine.vint4_sum(bigint, integer)
    RETURNS bigint
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint4sum';

CREATE FUNCTION engine.vint4_avg_accum(bigint[], integer)
    RETURNS bigint[]
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint4acc';

CREATE FUNCTION engine.vint4larger(integer, integer)
    RETURNS integer
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint4larger';

CREATE FUNCTION engine.vint4smaller(integer, integer)
    RETURNS integer
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint4smaller';

-- ---- int2/int4 avg final ---------------------------------

CREATE FUNCTION engine.vint8_avg(bigint[])
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint2int4avg';

-- ---- bigint (int8) ---------------------------------------

CREATE FUNCTION engine.vint8_avg_accum(internal, bigint)
    RETURNS internal
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint8acc';

CREATE FUNCTION engine.vnumeric_poly_sum(internal)
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint8sum';

CREATE FUNCTION engine.vnumeric_poly_avg(internal)
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint8avg';

CREATE FUNCTION engine.vint8larger(bigint, bigint)
    RETURNS bigint
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint8larger';

CREATE FUNCTION engine.vint8smaller(bigint, bigint)
    RETURNS bigint
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vint8smaller';

-- ---- date ------------------------------------------------

CREATE FUNCTION engine.vdate_larger(date, date)
    RETURNS date
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vdatelarger';

CREATE FUNCTION engine.vdate_smaller(date, date)
    RETURNS date
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vdatesmaller';

-- =========================================================
-- VECTORIZED AGGREGATES
-- Named vXXX so GetVectorizedProcedureOid() finds them by
-- prepending "v" to the original aggregate name.
-- =========================================================

-- vcount(*) — maps to count(*)
CREATE AGGREGATE engine.vcount(*) (
    sfunc    = engine.vcount,
    stype    = bigint,
    initcond = '0'
);

-- vcount(any) — maps to count(col)
CREATE AGGREGATE engine.vcount("any") (
    sfunc    = engine.vanycount,
    stype    = bigint,
    initcond = '0'
);

-- vmin / vmax for smallint
CREATE AGGREGATE engine.vmin(smallint) (
    sfunc = engine.vint2smaller,
    stype = smallint
);
CREATE AGGREGATE engine.vmax(smallint) (
    sfunc = engine.vint2larger,
    stype = smallint
);

-- vmin / vmax for integer
CREATE AGGREGATE engine.vmin(integer) (
    sfunc = engine.vint4smaller,
    stype = integer
);
CREATE AGGREGATE engine.vmax(integer) (
    sfunc = engine.vint4larger,
    stype = integer
);

-- vmin / vmax for bigint
CREATE AGGREGATE engine.vmin(bigint) (
    sfunc = engine.vint8smaller,
    stype = bigint
);
CREATE AGGREGATE engine.vmax(bigint) (
    sfunc = engine.vint8larger,
    stype = bigint
);

-- vmin / vmax for date
CREATE AGGREGATE engine.vmin(date) (
    sfunc = engine.vdate_smaller,
    stype = date
);
CREATE AGGREGATE engine.vmax(date) (
    sfunc = engine.vdate_larger,
    stype = date
);

-- vsum for smallint
CREATE AGGREGATE engine.vsum(smallint) (
    sfunc    = engine.vint2sum,
    stype    = bigint,
    initcond = '0'
);

-- vsum for integer
CREATE AGGREGATE engine.vsum(integer) (
    sfunc    = engine.vint4_sum,
    stype    = bigint,
    initcond = '0'
);

-- vsum for bigint
CREATE AGGREGATE engine.vsum(bigint) (
    sfunc     = engine.vint8_avg_accum,
    stype     = internal,
    finalfunc = engine.vnumeric_poly_sum
);

-- vavg for smallint
CREATE AGGREGATE engine.vavg(smallint) (
    sfunc     = engine.vint2_avg_accum,
    stype     = bigint[],
    finalfunc = engine.vint8_avg,
    initcond  = '{0,0}'
);

-- vavg for integer
CREATE AGGREGATE engine.vavg(integer) (
    sfunc     = engine.vint4_avg_accum,
    stype     = bigint[],
    finalfunc = engine.vint8_avg,
    initcond  = '{0,0}'
);

-- vavg for bigint
CREATE AGGREGATE engine.vavg(bigint) (
    sfunc     = engine.vint8_avg_accum,
    stype     = internal,
    finalfunc = engine.vnumeric_poly_avg
);
