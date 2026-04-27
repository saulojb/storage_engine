-- storage_engine upgrade: 1.2.7 → 1.2.8
-- Feature release:
--   • New type engine.uint8 — unsigned 64-bit integer
--   • I/O, binary protocol, comparison operators, btree opclass
--   • Casts: uint8 ↔ bigint, uint8 ↔ numeric, uint8 ↔ text
--   • Standard aggregates: min, max, sum, count  (in engine schema)
--   • Vectorized aggregates: vmin, vmax, vsum for colcompress tables
--   • Use engine.uint8 for ClickBench UInt64 columns (WatchID, UserID, etc.)
--     to avoid signed-overflow with bigint.
--
-- NOTE: Standard min/max/sum are defined in the engine schema.
--       To use them without schema qualification, add engine to your
--       search_path:
--         SET search_path TO engine, public;
--       or reference explicitly: engine.min(col), engine.max(col), etc.

-- =========================================================
-- SHELL TYPE (required before I/O functions reference it)
-- =========================================================

CREATE TYPE engine.uint8;

-- =========================================================
-- I/O FUNCTIONS
-- =========================================================

CREATE FUNCTION engine.uint8in(cstring)
    RETURNS engine.uint8
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8in';

CREATE FUNCTION engine.uint8out(engine.uint8)
    RETURNS cstring
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8out';

CREATE FUNCTION engine.uint8recv(internal)
    RETURNS engine.uint8
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8recv';

CREATE FUNCTION engine.uint8send(engine.uint8)
    RETURNS bytea
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8send';

-- =========================================================
-- COMPLETE TYPE DEFINITION
-- 8 bytes, passedbyvalue, double alignment — same storage as bigint.
-- =========================================================

CREATE TYPE engine.uint8 (
    INPUT          = engine.uint8in,
    OUTPUT         = engine.uint8out,
    RECEIVE        = engine.uint8recv,
    SEND           = engine.uint8send,
    INTERNALLENGTH = 8,
    ALIGNMENT      = double,
    PASSEDBYVALUE,
    CATEGORY       = 'N'
);

-- =========================================================
-- COMPARISON FUNCTIONS
-- =========================================================

CREATE FUNCTION engine.uint8lt(engine.uint8, engine.uint8)
    RETURNS boolean
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8lt';

CREATE FUNCTION engine.uint8le(engine.uint8, engine.uint8)
    RETURNS boolean
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8le';

CREATE FUNCTION engine.uint8eq(engine.uint8, engine.uint8)
    RETURNS boolean
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8eq';

CREATE FUNCTION engine.uint8ne(engine.uint8, engine.uint8)
    RETURNS boolean
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8ne';

CREATE FUNCTION engine.uint8ge(engine.uint8, engine.uint8)
    RETURNS boolean
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8ge';

CREATE FUNCTION engine.uint8gt(engine.uint8, engine.uint8)
    RETURNS boolean
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8gt';

CREATE FUNCTION engine.uint8cmp(engine.uint8, engine.uint8)
    RETURNS integer
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8cmp';

CREATE FUNCTION engine.uint8hash(engine.uint8)
    RETURNS integer
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8hash';

CREATE FUNCTION engine.uint8hash_extended(engine.uint8, bigint)
    RETURNS bigint
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8hash_extended';

-- =========================================================
-- COMPARISON OPERATORS
-- =========================================================

CREATE OPERATOR engine.< (
    FUNCTION    = engine.uint8lt,
    LEFTARG     = engine.uint8,
    RIGHTARG    = engine.uint8,
    COMMUTATOR  = OPERATOR(engine.>),
    NEGATOR     = OPERATOR(engine.>=),
    RESTRICT    = scalarltsel,
    JOIN        = scalarltjoinsel
);

CREATE OPERATOR engine.<= (
    FUNCTION    = engine.uint8le,
    LEFTARG     = engine.uint8,
    RIGHTARG    = engine.uint8,
    COMMUTATOR  = OPERATOR(engine.>=),
    NEGATOR     = OPERATOR(engine.>),
    RESTRICT    = scalarlesel,
    JOIN        = scalarlejoinsel
);

CREATE OPERATOR engine.= (
    FUNCTION    = engine.uint8eq,
    LEFTARG     = engine.uint8,
    RIGHTARG    = engine.uint8,
    COMMUTATOR  = OPERATOR(engine.=),
    NEGATOR     = OPERATOR(engine.<>),
    RESTRICT    = eqsel,
    JOIN        = eqjoinsel,
    HASHES,
    MERGES
);

CREATE OPERATOR engine.<> (
    FUNCTION    = engine.uint8ne,
    LEFTARG     = engine.uint8,
    RIGHTARG    = engine.uint8,
    COMMUTATOR  = OPERATOR(engine.<>),
    NEGATOR     = OPERATOR(engine.=),
    RESTRICT    = neqsel,
    JOIN        = neqjoinsel
);

CREATE OPERATOR engine.>= (
    FUNCTION    = engine.uint8ge,
    LEFTARG     = engine.uint8,
    RIGHTARG    = engine.uint8,
    COMMUTATOR  = OPERATOR(engine.<=),
    NEGATOR     = OPERATOR(engine.<),
    RESTRICT    = scalargesel,
    JOIN        = scalargejoinsel
);

CREATE OPERATOR engine.> (
    FUNCTION    = engine.uint8gt,
    LEFTARG     = engine.uint8,
    RIGHTARG    = engine.uint8,
    COMMUTATOR  = OPERATOR(engine.<),
    NEGATOR     = OPERATOR(engine.<=),
    RESTRICT    = scalargtsel,
    JOIN        = scalargtjoinsel
);

-- =========================================================
-- BTREE OPERATOR CLASS
-- =========================================================

CREATE OPERATOR CLASS engine.uint8_ops
    DEFAULT FOR TYPE engine.uint8 USING btree AS
        OPERATOR 1  engine.<,
        OPERATOR 2  engine.<=,
        OPERATOR 3  engine.=,
        OPERATOR 4  engine.>=,
        OPERATOR 5  engine.>,
        FUNCTION 1  engine.uint8cmp(engine.uint8, engine.uint8);

-- =========================================================
-- HASH OPERATOR CLASS
-- =========================================================

CREATE OPERATOR CLASS engine.uint8_hash_ops
    FOR TYPE engine.uint8 USING hash AS
        OPERATOR 1  engine.=,
        FUNCTION 1  engine.uint8hash(engine.uint8),
        FUNCTION 2  engine.uint8hash_extended(engine.uint8, bigint);

-- =========================================================
-- CAST FUNCTIONS & CASTS
-- =========================================================

-- uint8 ↔ bigint (int8) — same bit pattern, no data loss
CREATE FUNCTION engine.uint8_to_int8(engine.uint8)
    RETURNS bigint
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8_to_int8';

CREATE FUNCTION engine.int8_to_uint8(bigint)
    RETURNS engine.uint8
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_int8_to_uint8';

CREATE CAST (engine.uint8 AS bigint)
    WITH FUNCTION engine.uint8_to_int8(engine.uint8)
    AS ASSIGNMENT;

CREATE CAST (bigint AS engine.uint8)
    WITH FUNCTION engine.int8_to_uint8(bigint)
    AS ASSIGNMENT;

-- uint8 ↔ numeric
CREATE FUNCTION engine.uint8_to_numeric(engine.uint8)
    RETURNS numeric
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8_to_numeric';

CREATE FUNCTION engine.numeric_to_uint8(numeric)
    RETURNS engine.uint8
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_numeric_to_uint8';

CREATE CAST (engine.uint8 AS numeric)
    WITH FUNCTION engine.uint8_to_numeric(engine.uint8)
    AS IMPLICIT;

CREATE CAST (numeric AS engine.uint8)
    WITH FUNCTION engine.numeric_to_uint8(numeric)
    AS ASSIGNMENT;

-- uint8 ↔ text
CREATE FUNCTION engine.uint8_to_text(engine.uint8)
    RETURNS text
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8_to_text';

CREATE FUNCTION engine.text_to_uint8(text)
    RETURNS engine.uint8
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_text_to_uint8';

CREATE CAST (engine.uint8 AS text)
    WITH FUNCTION engine.uint8_to_text(engine.uint8)
    AS ASSIGNMENT;

CREATE CAST (text AS engine.uint8)
    WITH FUNCTION engine.text_to_uint8(text)
    AS ASSIGNMENT;

-- =========================================================
-- SCALAR TRANSITION FUNCTIONS (non-vectorized aggregates)
-- =========================================================

-- Transition for min(engine.uint8): returns the smaller of two uint8 values
CREATE FUNCTION engine.uint8smaller(engine.uint8, engine.uint8)
    RETURNS engine.uint8
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8smaller';

-- Transition for max(engine.uint8): returns the larger of two uint8 values
CREATE FUNCTION engine.uint8larger(engine.uint8, engine.uint8)
    RETURNS engine.uint8
    LANGUAGE c STRICT IMMUTABLE
    AS '$libdir/storage_engine', 'se_uint8larger';

-- Accumulator for sum(engine.uint8): uses Int128 state to avoid overflow
CREATE FUNCTION engine.uint8_acc(internal, engine.uint8)
    RETURNS internal
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_uint8_acc';

-- Final function for sum(engine.uint8): extracts numeric result from state
CREATE FUNCTION engine.uint8sum_final(internal)
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vuint8sum';

-- =========================================================
-- STANDARD AGGREGATES (non-vectorized, engine schema)
-- =========================================================

-- min(engine.uint8)
CREATE AGGREGATE engine.min(engine.uint8) (
    sfunc   = engine.uint8smaller,
    stype   = engine.uint8,
    sortop  = OPERATOR(engine.<)
);

-- max(engine.uint8)
CREATE AGGREGATE engine.max(engine.uint8) (
    sfunc   = engine.uint8larger,
    stype   = engine.uint8,
    sortop  = OPERATOR(engine.>)
);

-- sum(engine.uint8) — returns numeric to handle large values
CREATE AGGREGATE engine.sum(engine.uint8) (
    sfunc     = engine.uint8_acc,
    stype     = internal,
    finalfunc = engine.uint8sum_final
);

-- count(engine.uint8) — reuse standard count(*)
-- (the generic vcount("any") handles this in the vectorized path)

-- =========================================================
-- VECTORIZED AGGREGATE TRANSITION FUNCTIONS
-- =========================================================

-- Vectorized max: arg[1] is VectorColumn* of uint64 at runtime
CREATE FUNCTION engine.vuint8larger(engine.uint8, engine.uint8)
    RETURNS engine.uint8
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vuint8larger';

-- Vectorized min: arg[1] is VectorColumn* of uint64 at runtime
CREATE FUNCTION engine.vuint8smaller(engine.uint8, engine.uint8)
    RETURNS engine.uint8
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vuint8smaller';

-- Vectorized sum accumulator: arg[1] is VectorColumn* of uint64 at runtime
CREATE FUNCTION engine.vuint8_acc(internal, engine.uint8)
    RETURNS internal
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vuint8_acc';

-- Vectorized sum final: same C function as non-vectorized finalfunc
CREATE FUNCTION engine.vuint8sum_final(internal)
    RETURNS numeric
    LANGUAGE c CALLED ON NULL INPUT
    AS '$libdir/storage_engine', 'se_vuint8sum';

-- =========================================================
-- VECTORIZED AGGREGATES
-- Named vXXX so GetVectorizedProcedureOid() replaces min/max/sum
-- on engine.uint8 columns in colcompress tables.
-- =========================================================

-- vmin(engine.uint8) — dispatched from min(engine.uint8)
CREATE AGGREGATE engine.vmin(engine.uint8) (
    sfunc = engine.vuint8smaller,
    stype = engine.uint8
);

-- vmax(engine.uint8) — dispatched from max(engine.uint8)
CREATE AGGREGATE engine.vmax(engine.uint8) (
    sfunc = engine.vuint8larger,
    stype = engine.uint8
);

-- vsum(engine.uint8) — dispatched from sum(engine.uint8)
CREATE AGGREGATE engine.vsum(engine.uint8) (
    sfunc     = engine.vuint8_acc,
    stype     = internal,
    finalfunc = engine.vuint8sum_final
);
