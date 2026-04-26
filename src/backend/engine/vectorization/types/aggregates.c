
#include "postgres.h"

#include "fmgr.h"
#include "nodes/execnodes.h"
#include "utils/date.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/numeric.h"
#include "utils/fmgrprotos.h"

#include "pg_version_constants.h"
#include "engine/vectorization/types/types.h"
#include "engine/vectorization/types/numeric.h"

/*
 * PG19 renamed numeric_xxx_opt_error() → numeric_xxx_safe().
 * Provide a compat shim so callers need not be ifdef'd individually.
 */
#if PG_VERSION_NUM >= PG_VERSION_19
#define numeric_div_opt_error(a, b, err) numeric_div_safe((a), (b), NULL)
#endif

/* count */

PG_FUNCTION_INFO_V1(se_vemptycount);
Datum se_vemptycount(PG_FUNCTION_ARGS)
{
	int64 arg = PG_GETARG_INT64(0);
	PG_RETURN_INT64(arg);
}

PG_FUNCTION_INFO_V1(se_vanycount);
Datum
se_vanycount(PG_FUNCTION_ARGS)
{
	int64 arg = PG_GETARG_INT64(0);
	int64 result = arg;
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	int i;

	for (i = 0; i <  arg1->dimension; i++) 
	{
		if (arg1->isnull[i])
			continue;

		result++;
	}

	PG_RETURN_INT64(result);
}

/* int2 */

PG_FUNCTION_INFO_V1(se_vint2sum);
Datum
se_vint2sum(PG_FUNCTION_ARGS)
{
	bool  state_isnull = PG_ARGISNULL(0);
	int64 sumX = state_isnull ? 0 : PG_GETARG_INT64(0);
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);
	bool  had_values = false;
	int   i;

	int16 *vectorValue = (int16*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			sumX += (int64) vectorValue[i];
			had_values = true;
		}
	}

	/* Return NULL when state was NULL and no non-NULL values were found.
	 * This matches the behaviour of PostgreSQL's sum(smallint) for empty sets.
	 */
	if (state_isnull && !had_values)
		PG_RETURN_NULL();

	PG_RETURN_INT64(sumX);
}

PG_FUNCTION_INFO_V1(se_vint2acc);
Datum
se_vint2acc(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);
	Int64AggState *transdata;
	int i;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	transdata = (Int64AggState *) ARR_DATA_PTR(transarray);

	int16 *vectorValue = (int16*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			transdata->N++;
			transdata->sumX += (int64) vectorValue[i];
		}
	}

	PG_RETURN_ARRAYTYPE_P(transarray);
}

PG_FUNCTION_INFO_V1(se_vint2larger);
Datum se_vint2larger(PG_FUNCTION_ARGS)
{
	int16 maxValue = PG_ARGISNULL(0) ? INT16_MIN : PG_GETARG_INT16(0);
	bool anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i = 0;

	int16 *vectorValue = (int16*) arg2->value;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		maxValue = Max(maxValue, vectorValue[i]);
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT16(maxValue);
}

PG_FUNCTION_INFO_V1(se_vint2smaller);
Datum se_vint2smaller(PG_FUNCTION_ARGS)
{
	int16 minValue = PG_ARGISNULL(0) ? INT16_MAX : PG_GETARG_INT16(0);
	bool anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i = 0;

	int16 *vectorValue = (int16*) arg2->value;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		minValue = Min(minValue, vectorValue[i]);
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT16(minValue);
}

/* int4 */

PG_FUNCTION_INFO_V1(se_vint4sum);
Datum
se_vint4sum(PG_FUNCTION_ARGS)
{
	bool  state_isnull = PG_ARGISNULL(0);
	int64 sumX = state_isnull ? 0 : PG_GETARG_INT64(0);
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);
	bool  had_values = false;
	int   i;

	int32 *vectorValue = (int32*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			sumX += (int64) vectorValue[i];
			had_values = true;
		}
	}

	/* Return NULL when state was NULL and no non-NULL values were found.
	 * This matches the behaviour of PostgreSQL's sum(integer) for empty sets.
	 */
	if (state_isnull && !had_values)
		PG_RETURN_NULL();

	PG_RETURN_INT64(sumX);
}

PG_FUNCTION_INFO_V1(se_vint4acc);
Datum
se_vint4acc(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);
	Int64AggState *transdata;
	int i;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	transdata = (Int64AggState *) ARR_DATA_PTR(transarray);

	int32 *vectorValue = (int32*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			transdata->N++;
			transdata->sumX += (int64) vectorValue[i];
		}
	}

	PG_RETURN_ARRAYTYPE_P(transarray);
}

PG_FUNCTION_INFO_V1(se_vint4larger);
Datum se_vint4larger(PG_FUNCTION_ARGS)
{
	int32 maxValue = PG_ARGISNULL(0) ? INT32_MIN : PG_GETARG_INT32(0);
	bool anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i = 0;

	int32 *vectorValue = (int32*) arg2->value;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		maxValue = Max(maxValue, vectorValue[i]);
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT32(maxValue);
}

PG_FUNCTION_INFO_V1(se_vint4smaller);
Datum se_vint4smaller(PG_FUNCTION_ARGS)
{
	int32 minValue = PG_ARGISNULL(0) ? INT32_MAX : PG_GETARG_INT32(0);
	bool anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i = 0;

	int32 *vectorValue = (int32*) arg2->value;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		minValue = Min(minValue, vectorValue[i]);
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT32(minValue);
}

/* int2 / int4 */

PG_FUNCTION_INFO_V1(se_vint2int4avg);
Datum
se_vint2int4avg(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	Int64AggState *transdata;

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int64AggState))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int64AggState *) ARR_DATA_PTR(transarray);

	/* SQL defines AVG of no values to be NULL */
	if (transdata->N == 0)
		PG_RETURN_NULL();

#if PG_VERSION_NUM >= PG_VERSION_14
	Numeric sumNumeric, nNumeric, res;
	nNumeric = int64_to_numeric(transdata->N);
	sumNumeric = int64_to_numeric(transdata->sumX);
	res = numeric_div_opt_error(sumNumeric, nNumeric, NULL);
	PG_RETURN_NUMERIC(res);
#else
	Datum countd, sumd;
	countd = DirectFunctionCall1(int8_numeric,
								 Int64GetDatumFast(transdata->N));
	sumd = DirectFunctionCall1(int8_numeric,
							   Int64GetDatumFast(transdata->sumX));
	PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumd, countd));
#endif

}


/* int8 */

PG_FUNCTION_INFO_V1(se_vint8acc);
Datum
se_vint8acc(PG_FUNCTION_ARGS)
{
	Int128AggState *state;
	int i;

	state = PG_ARGISNULL(0) ? NULL : (Int128AggState *) PG_GETARG_POINTER(0);
	VectorColumn *arg1 = (VectorColumn*) PG_GETARG_POINTER(1);

	MemoryContext aggContext;
	MemoryContext oldContext;

	if (!AggCheckCallContext(fcinfo, &aggContext))
		elog(ERROR, "aggregate function called in non-aggregate context");

	oldContext = MemoryContextSwitchTo(aggContext);

	/* Create the state data on the first call */
	if (state == NULL)
	{
		state = palloc0(sizeof(Int128AggState));
		state->calcSumX2 = false;
	}

	int64 *vectorValue = (int64*) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			state->N++;
			state->sumX += (int128) vectorValue[i];
		}
	}

	MemoryContextSwitchTo(oldContext);

	PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(se_vint8sum);
Datum
se_vint8sum(PG_FUNCTION_ARGS)
{
	Int128AggState *state;

	state = PG_ARGISNULL(0) ? NULL : (Int128AggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	Numeric res = int128_to_numeric(state->sumX);

	PG_RETURN_NUMERIC(res);
}

PG_FUNCTION_INFO_V1(se_vint8avg);
Datum
se_vint8avg(PG_FUNCTION_ARGS)
{
	Int128AggState *state;
	Numeric sumNumeric, nNumeric, res;

	state = PG_ARGISNULL(0) ? NULL : (Int128AggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	sumNumeric = int128_to_numeric(state->sumX);
	nNumeric = int128_to_numeric(state->N);
	res = numeric_div_opt_error(sumNumeric, nNumeric, NULL);

	PG_RETURN_NUMERIC(res);
}

PG_FUNCTION_INFO_V1(se_vint8larger);
Datum se_vint8larger(PG_FUNCTION_ARGS)
{
	int64 maxValue = PG_ARGISNULL(0) ? INT64_MIN : PG_GETARG_INT64(0);
	bool anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i = 0;

	int64 *vectorValue = (int64*) arg2->value;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		maxValue = Max(maxValue, vectorValue[i]);
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT64(maxValue);
}

PG_FUNCTION_INFO_V1(se_vint8smaller);
Datum se_vint8smaller(PG_FUNCTION_ARGS)
{
	int64 minValue = PG_ARGISNULL(0) ? INT64_MAX : PG_GETARG_INT64(0);
	bool anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i = 0;

	int64 *vectorValue = (int64*) arg2->value;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		minValue = Min(minValue, vectorValue[i]);
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT64(minValue);
}

/* =========================================================
 * float8 (double precision) vectorized aggregates
 * ========================================================= */

/*
 * se_vfloat8pl — vectorized transition for sum(float8).
 *
 * SQL signature: vfloat8pl(double precision, double precision) → double precision
 * At runtime arg[1] is a VectorColumn* masquerading as double precision.
 * Called with CALLED ON NULL INPUT so we handle NULL initial state.
 */
PG_FUNCTION_INFO_V1(se_vfloat8pl);
Datum
se_vfloat8pl(PG_FUNCTION_ARGS)
{
	float8		state = PG_ARGISNULL(0) ? 0.0 : PG_GETARG_FLOAT8(0);
	bool		hasState = !PG_ARGISNULL(0);
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	float8	   *vectorValue = (float8 *) arg1->value;
	int			i;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			state += vectorValue[i];
			hasState = true;
		}
	}

	if (!hasState)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(state);
}

/*
 * se_vfloat8_accum — vectorized transition for avg(float8).
 *
 * State: float8[3] = {N, sumX, sumX2} — matches PG's float8_accum convention
 * so we can reuse pg_catalog.float8_avg as the final function.
 */
PG_FUNCTION_INFO_V1(se_vfloat8_accum);
Datum
se_vfloat8_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	float8	   *transdata;
	float8	   *vectorValue;
	float8		N = 0.0,
				sumX = 0.0,
				sumX2 = 0.0;
	int			i;

	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + 3 * sizeof(float8))
		elog(ERROR, "expected 3-element float8 array for float8_accum state");

	transdata = (float8 *) ARR_DATA_PTR(transarray);
	vectorValue = (float8 *) arg1->value;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			float8 val = vectorValue[i];

			N += 1.0;
			sumX += val;
			sumX2 += val * val;
		}
	}

	transdata[0] += N;
	transdata[1] += sumX;
	transdata[2] += sumX2;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

/*
 * se_vfloat8larger — vectorized transition for max(float8).
 */
PG_FUNCTION_INFO_V1(se_vfloat8larger);
Datum
se_vfloat8larger(PG_FUNCTION_ARGS)
{
	float8		maxValue = PG_ARGISNULL(0) ? 0.0 : PG_GETARG_FLOAT8(0);
	bool		anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	float8	   *vectorValue = (float8 *) arg1->value;
	int			i;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (arg1->isnull[i])
			continue;
		if (!anyValue || vectorValue[i] > maxValue)
			maxValue = vectorValue[i];
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(maxValue);
}

/*
 * se_vfloat8smaller — vectorized transition for min(float8).
 */
PG_FUNCTION_INFO_V1(se_vfloat8smaller);
Datum
se_vfloat8smaller(PG_FUNCTION_ARGS)
{
	float8		minValue = PG_ARGISNULL(0) ? 0.0 : PG_GETARG_FLOAT8(0);
	bool		anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	float8	   *vectorValue = (float8 *) arg1->value;
	int			i;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (arg1->isnull[i])
			continue;
		if (!anyValue || vectorValue[i] < minValue)
			minValue = vectorValue[i];
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(minValue);
}

/* =========================================================
 * numeric vectorized aggregates
 *
 * numeric is varlena; each element in VectorColumn.value is
 * a Datum (pointer) to a palloc'd Numeric copy.
 * ========================================================= */

/*
 * State for vectorized avg(numeric): maintained in aggContext via internal.
 */
typedef struct NumericVecAvgState
{
	int64		N;
	Numeric		sumX;			/* NULL until first non-null value */
} NumericVecAvgState;

/*
 * se_vnumeric_add — vectorized transition for sum(numeric).
 *
 * State starts NULL (no initcond).  All allocations must happen in
 * aggContext because VectorAgg does not call ExecAggCopyTransValue;
 * allocating in the per-tuple context would leave a dangling pointer
 * on the next batch call.
 */
PG_FUNCTION_INFO_V1(se_vnumeric_add);
Datum
se_vnumeric_add(PG_FUNCTION_ARGS)
{
	Numeric		state = PG_ARGISNULL(0) ? NULL : PG_GETARG_NUMERIC(0);
	bool		anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	Datum	   *vectorValues = (Datum *) arg1->value;
	int			i;
	MemoryContext aggContext;
	MemoryContext oldContext;

	if (!AggCheckCallContext(fcinfo, &aggContext))
		elog(ERROR, "se_vnumeric_add called in non-aggregate context");

	oldContext = MemoryContextSwitchTo(aggContext);

	for (i = 0; i < arg1->dimension; i++)
	{
		if (arg1->isnull[i])
			continue;

		Numeric		val = DatumGetNumeric(vectorValues[i]);

		if (!anyValue)
		{
			/* Copy first value into aggContext so it outlives this batch. */
			state = DatumGetNumeric(datumCopy(NumericGetDatum(val), false, -1));
			anyValue = true;
		}
		else
		{
			state = DatumGetNumeric(
				DirectFunctionCall2(numeric_add,
									NumericGetDatum(state),
									NumericGetDatum(val)));
		}
	}

	MemoryContextSwitchTo(oldContext);

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_NUMERIC(state);
}

/*
 * se_vnumericavg_accum — vectorized transition for avg(numeric).
 *
 * Uses an internal NumericVecAvgState allocated in aggContext.
 */
PG_FUNCTION_INFO_V1(se_vnumericavg_accum);
Datum
se_vnumericavg_accum(PG_FUNCTION_ARGS)
{
	NumericVecAvgState *state;
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	MemoryContext aggContext;
	MemoryContext oldContext;
	Datum	   *vectorValues = (Datum *) arg1->value;
	int			i;

	if (!AggCheckCallContext(fcinfo, &aggContext))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = PG_ARGISNULL(0) ? NULL : (NumericVecAvgState *) PG_GETARG_POINTER(0);

	oldContext = MemoryContextSwitchTo(aggContext);

	if (state == NULL)
	{
		state = palloc0(sizeof(NumericVecAvgState));
		state->N = 0;
		state->sumX = NULL;
	}

	for (i = 0; i < arg1->dimension; i++)
	{
		if (arg1->isnull[i])
			continue;

		Numeric		val = DatumGetNumeric(vectorValues[i]);

		state->N++;

		if (state->sumX == NULL)
		{
			/* Copy first value into aggContext */
			state->sumX = DatumGetNumeric(
				datumCopy(NumericGetDatum(val), false, -1));
		}
		else
		{
			Numeric		oldSum = state->sumX;

			state->sumX = DatumGetNumeric(
				DirectFunctionCall2(numeric_add,
									NumericGetDatum(oldSum),
									NumericGetDatum(val)));
			pfree(oldSum);
		}
	}

	MemoryContextSwitchTo(oldContext);

	PG_RETURN_POINTER(state);
}

/*
 * se_vnumericavg_final — final function for vectorized avg(numeric).
 */
PG_FUNCTION_INFO_V1(se_vnumericavg_final);
Datum
se_vnumericavg_final(PG_FUNCTION_ARGS)
{
	NumericVecAvgState *state;
	Numeric		nNumeric,
				res;

	state = PG_ARGISNULL(0) ? NULL : (NumericVecAvgState *) PG_GETARG_POINTER(0);

	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	nNumeric = int128_to_numeric((int128) state->N);
	res = numeric_div_opt_error(state->sumX, nNumeric, NULL);

	PG_RETURN_NUMERIC(res);
}

/*
 * se_vnumericsum_final — final function for vectorized sum(numeric).
 *
 * Extracts just the sum from the shared NumericVecAvgState, allowing
 * vsum(numeric) to use the same stype=internal / sfunc=vnumericavg_accum
 * as vavg(numeric).  This ensures the planner assigns both the same
 * aggtransno so they share a single pertrans entry correctly.
 */
PG_FUNCTION_INFO_V1(se_vnumericsum_final);
Datum
se_vnumericsum_final(PG_FUNCTION_ARGS)
{
	NumericVecAvgState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericVecAvgState *) PG_GETARG_POINTER(0);

	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	PG_RETURN_NUMERIC(state->sumX);
}

/*
 * se_vnumericlarger — vectorized transition for max(numeric).
 */
PG_FUNCTION_INFO_V1(se_vnumericlarger);
Datum
se_vnumericlarger(PG_FUNCTION_ARGS)
{
	Numeric		maxValue = PG_ARGISNULL(0) ? NULL : PG_GETARG_NUMERIC(0);
	bool		anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	Datum	   *vectorValues = (Datum *) arg1->value;
	int			i;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (arg1->isnull[i])
			continue;

		Numeric		val = DatumGetNumeric(vectorValues[i]);

		if (!anyValue ||
			DatumGetInt32(DirectFunctionCall2(numeric_cmp,
											  NumericGetDatum(maxValue),
											  NumericGetDatum(val))) < 0)
		{
			maxValue = val;
		}
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_NUMERIC(maxValue);
}

/*
 * se_vnumericsmaller — vectorized transition for min(numeric).
 */
PG_FUNCTION_INFO_V1(se_vnumericsmaller);
Datum
se_vnumericsmaller(PG_FUNCTION_ARGS)
{
	Numeric		minValue = PG_ARGISNULL(0) ? NULL : PG_GETARG_NUMERIC(0);
	bool		anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	Datum	   *vectorValues = (Datum *) arg1->value;
	int			i;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (arg1->isnull[i])
			continue;

		Numeric		val = DatumGetNumeric(vectorValues[i]);

		if (!anyValue ||
			DatumGetInt32(DirectFunctionCall2(numeric_cmp,
											  NumericGetDatum(minValue),
											  NumericGetDatum(val))) > 0)
		{
			minValue = val;
		}
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_NUMERIC(minValue);
}

// date

PG_FUNCTION_INFO_V1(se_vdatelarger);
Datum se_vdatelarger(PG_FUNCTION_ARGS)
{
	int32 maxValue = PG_ARGISNULL(0) ? INT32_MIN : PG_GETARG_INT32(0);
	bool anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i = 0;

	DateADT *vectorValue = (DateADT*) arg2->value;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		maxValue = Max(maxValue, vectorValue[i]);
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT32(maxValue);
}

PG_FUNCTION_INFO_V1(se_vdatesmaller);
Datum se_vdatesmaller(PG_FUNCTION_ARGS)
{
	int32 minValue = PG_ARGISNULL(0) ? INT32_MAX : PG_GETARG_INT32(0);
	bool anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn*) PG_GETARG_POINTER(1);
	int i = 0;

	DateADT *vectorValue = (DateADT*) arg2->value;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		minValue = Min(minValue, vectorValue[i]);
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT32(minValue);
}

/* =========================================================
 * money (Cash = int64) vectorized aggregates
 *
 * PostgreSQL money is stored as int64 — no avg aggregate exists.
 * We implement vmax, vmin, and vsum only.
 * ========================================================= */

/*
 * se_vcashlarger — vectorized transition for max(money).
 */
PG_FUNCTION_INFO_V1(se_vcashlarger);
Datum
se_vcashlarger(PG_FUNCTION_ARGS)
{
	int64		maxValue = PG_ARGISNULL(0) ? INT64_MIN : PG_GETARG_INT64(0);
	bool		anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn *) PG_GETARG_POINTER(1);
	int64	   *vectorValue = (int64 *) arg2->value;
	int			i;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		if (!anyValue || vectorValue[i] > maxValue)
			maxValue = vectorValue[i];
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT64(maxValue);
}

/*
 * se_vcashsmaller — vectorized transition for min(money).
 */
PG_FUNCTION_INFO_V1(se_vcashsmaller);
Datum
se_vcashsmaller(PG_FUNCTION_ARGS)
{
	int64		minValue = PG_ARGISNULL(0) ? INT64_MAX : PG_GETARG_INT64(0);
	bool		anyValue = !PG_ARGISNULL(0);
	VectorColumn *arg2 = (VectorColumn *) PG_GETARG_POINTER(1);
	int64	   *vectorValue = (int64 *) arg2->value;
	int			i;

	for (i = 0; i < arg2->dimension; i++)
	{
		if (arg2->isnull[i])
			continue;
		if (!anyValue || vectorValue[i] < minValue)
			minValue = vectorValue[i];
		anyValue = true;
	}

	if (!anyValue)
		PG_RETURN_NULL();

	PG_RETURN_INT64(minValue);
}

/*
 * se_vcashpl — vectorized transition for sum(money).
 *
 * Accumulates int64 values; returns NULL for an all-NULL or empty set.
 */
PG_FUNCTION_INFO_V1(se_vcashpl);
Datum
se_vcashpl(PG_FUNCTION_ARGS)
{
	int64		state = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT64(0);
	bool		hasState = !PG_ARGISNULL(0);
	VectorColumn *arg1 = (VectorColumn *) PG_GETARG_POINTER(1);
	int64	   *vectorValue = (int64 *) arg1->value;
	int			i;

	for (i = 0; i < arg1->dimension; i++)
	{
		if (!arg1->isnull[i])
		{
			state += vectorValue[i];
			hasState = true;
		}
	}

	if (!hasState)
		PG_RETURN_NULL();

	PG_RETURN_INT64(state);
}