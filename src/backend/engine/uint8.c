/*-------------------------------------------------------------------------
 *
 * uint8.c
 *
 * engine.uint8 — unsigned 64-bit integer type for storage_engine.
 *
 * Provides I/O, binary protocol, comparison operators, casts, btree
 * support, and scalar transition functions for min/max/sum aggregates.
 * The vectorized aggregate transitions (se_vuint8*) live in
 * vectorization/types/aggregates.c so they share the Int128AggState
 * infrastructure already defined there.
 *
 * Storage: 8 bytes, passedbyvalue, double alignment — identical layout
 * to int8 (bigint).  The only difference is unsigned comparison semantics.
 *
 * Copyright (c) Saulo J. Benvenutti
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/fmgrprotos.h"

#include "engine/vectorization/types/types.h"
#include "engine/vectorization/types/numeric.h"

/* Convenience macros – uint64 lives inside a Datum as int64 bits.
 * PG18+ defines these natively in fmgr.h; guard to avoid redefinition. */
#ifndef PG_GETARG_UINT64
#define PG_GETARG_UINT64(n)   ((uint64) PG_GETARG_INT64(n))
#endif
#ifndef PG_RETURN_UINT64
#define PG_RETURN_UINT64(x)   PG_RETURN_INT64((int64)(x))
#endif


/* =========================================================
 * I/O functions
 * ========================================================= */

PG_FUNCTION_INFO_V1(se_uint8in);
Datum
se_uint8in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	uint64		result;
	char	   *endptr;

	if (str == NULL || str[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type engine.uint8: \"%s\"",
						str ? str : "")));

	errno = 0;
	result = (uint64) strtoull(str, &endptr, 10);

	if (errno != 0 || *endptr != '\0' || endptr == str)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type engine.uint8: \"%s\"",
						str)));

	PG_RETURN_UINT64(result);
}

PG_FUNCTION_INFO_V1(se_uint8out);
Datum
se_uint8out(PG_FUNCTION_ARGS)
{
	uint64		val = PG_GETARG_UINT64(0);
	char		buf[21];		/* max 20 digits + NUL */

	snprintf(buf, sizeof(buf), UINT64_FORMAT, val);
	PG_RETURN_CSTRING(pstrdup(buf));
}

/*
 * Binary receive: read 8 bytes big-endian from the wire.
 * We reuse pq_getmsgint64 since the bit pattern is identical.
 */
PG_FUNCTION_INFO_V1(se_uint8recv);
Datum
se_uint8recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	uint64		result = (uint64) pq_getmsgint64(buf);

	PG_RETURN_UINT64(result);
}

PG_FUNCTION_INFO_V1(se_uint8send);
Datum
se_uint8send(PG_FUNCTION_ARGS)
{
	uint64			val = PG_GETARG_UINT64(0);
	StringInfoData	buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, (int64) val);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/* =========================================================
 * Comparison operators
 * ========================================================= */

PG_FUNCTION_INFO_V1(se_uint8lt);
Datum
se_uint8lt(PG_FUNCTION_ARGS)
{
	uint64 a = PG_GETARG_UINT64(0);
	uint64 b = PG_GETARG_UINT64(1);
	PG_RETURN_BOOL(a < b);
}

PG_FUNCTION_INFO_V1(se_uint8le);
Datum
se_uint8le(PG_FUNCTION_ARGS)
{
	uint64 a = PG_GETARG_UINT64(0);
	uint64 b = PG_GETARG_UINT64(1);
	PG_RETURN_BOOL(a <= b);
}

PG_FUNCTION_INFO_V1(se_uint8eq);
Datum
se_uint8eq(PG_FUNCTION_ARGS)
{
	uint64 a = PG_GETARG_UINT64(0);
	uint64 b = PG_GETARG_UINT64(1);
	PG_RETURN_BOOL(a == b);
}

PG_FUNCTION_INFO_V1(se_uint8ne);
Datum
se_uint8ne(PG_FUNCTION_ARGS)
{
	uint64 a = PG_GETARG_UINT64(0);
	uint64 b = PG_GETARG_UINT64(1);
	PG_RETURN_BOOL(a != b);
}

PG_FUNCTION_INFO_V1(se_uint8ge);
Datum
se_uint8ge(PG_FUNCTION_ARGS)
{
	uint64 a = PG_GETARG_UINT64(0);
	uint64 b = PG_GETARG_UINT64(1);
	PG_RETURN_BOOL(a >= b);
}

PG_FUNCTION_INFO_V1(se_uint8gt);
Datum
se_uint8gt(PG_FUNCTION_ARGS)
{
	uint64 a = PG_GETARG_UINT64(0);
	uint64 b = PG_GETARG_UINT64(1);
	PG_RETURN_BOOL(a > b);
}

/*
 * Three-way comparison for btree: returns -1, 0, or 1.
 */
PG_FUNCTION_INFO_V1(se_uint8cmp);
Datum
se_uint8cmp(PG_FUNCTION_ARGS)
{
	uint64 a = PG_GETARG_UINT64(0);
	uint64 b = PG_GETARG_UINT64(1);
	int32  res = (a < b) ? -1 : (a > b) ? 1 : 0;
	PG_RETURN_INT32(res);
}

/*
 * Hash function — reuse int8 hash (same bit pattern).
 */
PG_FUNCTION_INFO_V1(se_uint8hash);
Datum
se_uint8hash(PG_FUNCTION_ARGS)
{
	/* Forward to int8hash which has the right 8-byte hash logic */
	return DirectFunctionCall1(hashint8, PG_GETARG_DATUM(0));
}

PG_FUNCTION_INFO_V1(se_uint8hash_extended);
Datum
se_uint8hash_extended(PG_FUNCTION_ARGS)
{
	return DirectFunctionCall2(hashint8extended,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}


/* =========================================================
 * Cast functions
 * ========================================================= */

/* engine.uint8 → bigint (int8) — direct bit reinterpretation */
PG_FUNCTION_INFO_V1(se_uint8_to_int8);
Datum
se_uint8_to_int8(PG_FUNCTION_ARGS)
{
	/* same bit pattern, just different semantics */
	PG_RETURN_INT64(PG_GETARG_INT64(0));
}

/* bigint (int8) → engine.uint8 — same */
PG_FUNCTION_INFO_V1(se_int8_to_uint8);
Datum
se_int8_to_uint8(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(PG_GETARG_INT64(0));
}

/* engine.uint8 → numeric */
PG_FUNCTION_INFO_V1(se_uint8_to_numeric);
Datum
se_uint8_to_numeric(PG_FUNCTION_ARGS)
{
	uint64 val = PG_GETARG_UINT64(0);
	/* Reuse int128_to_numeric with a non-negative int128 value */
	Numeric res = int128_to_numeric((int128) val);
	PG_RETURN_NUMERIC(res);
}

/* numeric → engine.uint8 */
PG_FUNCTION_INFO_V1(se_numeric_to_uint8);
Datum
se_numeric_to_uint8(PG_FUNCTION_ARGS)
{
	Numeric		n = PG_GETARG_NUMERIC(0);
	/*
	 * Convert via text representation to handle values > INT64_MAX.
	 * numeric_int8 only covers the signed int64 range, but we need
	 * the full [0, 2^64-1] range.
	 */
	char	   *str = DatumGetCString(DirectFunctionCall1(numeric_out,
													 NumericGetDatum(n)));
	uint64		result;
	char	   *endptr;

	errno = 0;
	result = (uint64) strtoull(str, &endptr, 10);

	if (errno != 0 || endptr == str || *endptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("uint8 out of range: \"%s\"", str)));

	pfree(str);
	PG_RETURN_UINT64(result);
}

/* engine.uint8 → text */
PG_FUNCTION_INFO_V1(se_uint8_to_text);
Datum
se_uint8_to_text(PG_FUNCTION_ARGS)
{
	uint64		val = PG_GETARG_UINT64(0);
	char		buf[21];

	snprintf(buf, sizeof(buf), UINT64_FORMAT, val);
	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

/* text → engine.uint8 */
PG_FUNCTION_INFO_V1(se_text_to_uint8);
Datum
se_text_to_uint8(PG_FUNCTION_ARGS)
{
	text	   *t = PG_GETARG_TEXT_PP(0);
	char	   *str = text_to_cstring(t);

	/* Reuse the input function */
	return DirectFunctionCall1(se_uint8in, CStringGetDatum(str));
}


/* =========================================================
 * Scalar transition functions for non-vectorized aggregates.
 * These are called row-by-row by the standard executor.
 * ========================================================= */

/*
 * se_uint8smaller — transition for min(engine.uint8)
 * SQL: (engine.uint8, engine.uint8) → engine.uint8  STRICT
 */
PG_FUNCTION_INFO_V1(se_uint8smaller);
Datum
se_uint8smaller(PG_FUNCTION_ARGS)
{
	uint64 a = PG_GETARG_UINT64(0);
	uint64 b = PG_GETARG_UINT64(1);
	PG_RETURN_UINT64(a < b ? a : b);
}

/*
 * se_uint8larger — transition for max(engine.uint8)
 * SQL: (engine.uint8, engine.uint8) → engine.uint8  STRICT
 */
PG_FUNCTION_INFO_V1(se_uint8larger);
Datum
se_uint8larger(PG_FUNCTION_ARGS)
{
	uint64 a = PG_GETARG_UINT64(0);
	uint64 b = PG_GETARG_UINT64(1);
	PG_RETURN_UINT64(a > b ? a : b);
}

/*
 * se_uint8_acc — scalar transition for sum(engine.uint8).
 *
 * Uses Int128AggState (same as bigint) to accumulate without overflow.
 * Allocated in aggContext so it survives across rows.
 */
PG_FUNCTION_INFO_V1(se_uint8_acc);
Datum
se_uint8_acc(PG_FUNCTION_ARGS)
{
	Int128AggState *state;
	uint64			val;
	MemoryContext	aggContext;
	MemoryContext	oldContext;

	if (!AggCheckCallContext(fcinfo, &aggContext))
		elog(ERROR, "se_uint8_acc called in non-aggregate context");

	state = PG_ARGISNULL(0) ? NULL : (Int128AggState *) PG_GETARG_POINTER(0);

	/* NULL input — just return state unchanged */
	if (PG_ARGISNULL(1))
	{
		if (state == NULL)
			PG_RETURN_NULL();
		PG_RETURN_POINTER(state);
	}

	val = PG_GETARG_UINT64(1);

	oldContext = MemoryContextSwitchTo(aggContext);

	if (state == NULL)
	{
		state = palloc0(sizeof(Int128AggState));
		state->calcSumX2 = false;
	}

	state->N++;
	state->sumX += (int128) val;

	MemoryContextSwitchTo(oldContext);

	PG_RETURN_POINTER(state);
}
