/*-------------------------------------------------------------------------
 *
 * engine_groupagg_node.c
 *      Vectorized GROUP BY aggregation custom scan node.
 *
 *      Replaces HashAggregate → ColcompressScan with a single CustomScan
 *      that reads VectorColumn batches from ColcompressScan and accumulates
 *      per-group results using a HTAB, then emits one tuple per group.
 *
 *      Only engaged when:
 *        - Single GROUP BY key
 *        - Key type: int4, int8, float4, float8, or text (low-cardinality)
 *        - Aggregates: count(*), sum, min, max, avg over int4/int8/float4/float8
 *        - n_distinct estimate < VECGROUPAGG_MAX_GROUPS
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pg_version_constants.h"

#include "access/tupmacs.h"
#include "commands/explain.h"
/* explain_format.h was split from explain.h in PG 18 */
#if PG_VERSION_NUM >= PG_VERSION_18
#include "commands/explain_format.h"
#endif
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "common/hashfn.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/varlena.h"

/* INT4OID / INT8OID / FLOAT8OID — pg_type_d.h is required from PG 19 onwards */
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"		/* int8_numeric, float8_numeric */
#include "utils/numeric.h"		/* NUMERICOID numeric conversion */

#include "engine/engine_customscan.h"
#include "engine/vectorization/engine_vector_types.h"
#include "engine/vectorization/nodes/engine_groupagg_node.h"

/* ----------------------------------------------------------------
 *  Custom scan method forward declarations
 * ---------------------------------------------------------------- */
static Node  *CreateVecGroupAggState(CustomScan *custom_plan);
static void   BeginVecGroupAgg(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *ExecVecGroupAgg(CustomScanState *node);
static void   EndVecGroupAgg(CustomScanState *node);
static void   ReScanVecGroupAgg(CustomScanState *node);
static void   ExplainVecGroupAgg(CustomScanState *node, List *ancestors,
								 ExplainState *es);
Datum  eval_vec_expr_node(VecExprNode *nodes, VecExprNode *cur,
						  TupleTableSlot *batch_slot, int row_i, bool *isnull);

static CustomScanMethods VecGroupAggScanMethods = {
	"StorageEngineVectorGroupAgg",
	CreateVecGroupAggState,
};

static CustomExecMethods VecGroupAggExecMethods = {
	.CustomName			= "StorageEngineVectorGroupAgg",
	.BeginCustomScan	= BeginVecGroupAgg,
	.ExecCustomScan		= ExecVecGroupAgg,
	.EndCustomScan		= EndVecGroupAgg,
	.ReScanCustomScan	= ReScanVecGroupAgg,
	.ExplainCustomScan	= ExplainVecGroupAgg,
};

/* VecGroupKey and VecGroupEntry are defined in engine_groupagg_node.h */

/* ----------------------------------------------------------------
 *  Helpers
 * ---------------------------------------------------------------- */

/*
 * Map a Postgres type OID to our internal VECGAGG_TYPE_* code.
 * Returns -1 if not supported.
 */
static int
type_oid_to_vectype(Oid typeoid)
{
	switch (typeoid)
	{
		case INT4OID:	return VECGAGG_TYPE_INT4;
		case INT8OID:	return VECGAGG_TYPE_INT8;
		case FLOAT4OID:	return VECGAGG_TYPE_FLOAT4;
		case FLOAT8OID:	return VECGAGG_TYPE_FLOAT8;
		case NUMERICOID:	return VECGAGG_TYPE_NUMERIC;
		case BPCHAROID:	return VECGAGG_TYPE_BPCHAR;
		case TEXTOID:	return VECGAGG_TYPE_TEXT;
		case CASHOID:	return VECGAGG_TYPE_INT8;
		case BOOLOID:	return VECGAGG_TYPE_INT4; /* bool: Datum 0/1, same as int4 */
		default:		return -1;
	}
}

static void
ensure_numeric_fmgr(VecGroupAggState *state)
{
	Oid numeric_add_oid;
	Oid numeric_cmp_oid;
	Oid numeric_avg_oid;
	Oid numeric_sum_oid;

	if (state->numeric_fmgr_ready)
		return;

	numeric_add_oid = fmgr_internal_function("numeric_add");
	numeric_cmp_oid = fmgr_internal_function("numeric_cmp");
	numeric_avg_oid = fmgr_internal_function("numeric_avg");
	numeric_sum_oid = fmgr_internal_function("numeric_sum");
	if (!OidIsValid(numeric_add_oid) || !OidIsValid(numeric_cmp_oid) ||
		!OidIsValid(numeric_avg_oid) || !OidIsValid(numeric_sum_oid))
		elog(ERROR, "failed to resolve required numeric functions");

	fmgr_info_cxt(numeric_add_oid, &state->numeric_add_fmgr, state->agg_context);
	fmgr_info_cxt(numeric_cmp_oid, &state->numeric_cmp_fmgr, state->agg_context);
	fmgr_info_cxt(fmgr_internal_function("numeric_avg_accum"),
			  &state->numeric_avg_accum_fmgr,
			  state->agg_context);
	fmgr_info_cxt(fmgr_internal_function("numeric_avg_serialize"),
			  &state->numeric_avg_serialize_fmgr,
			  state->agg_context);
	fmgr_info_cxt(numeric_avg_oid, &state->numeric_avg_fmgr, state->agg_context);
	fmgr_info_cxt(numeric_sum_oid, &state->numeric_sum_fmgr, state->agg_context);

	state->numeric_fmgr_ready = true;
}

static void
ensure_int8_avg_serialize_fmgr(VecGroupAggState *state)
{
	Oid		fn_oid;

	if (state->int8_avg_serialize_ready)
		return;

	fn_oid = fmgr_internal_function("int8_avg_serialize");
	if (!OidIsValid(fn_oid))
		elog(ERROR, "failed to resolve int8_avg_serialize");
	fmgr_info_cxt(fn_oid, &state->int8_avg_serialize_fmgr, state->agg_context);

	state->int8_avg_serialize_ready = true;
}

static Datum
call_numeric_binary_fmgr(FmgrInfo *flinfo,
				 Datum arg0,
				 bool arg0isnull,
				 Datum arg1,
				 bool arg1isnull,
				 Node *context)
{
	LOCAL_FCINFO(inner_fcinfo, 2);

	InitFunctionCallInfoData(*inner_fcinfo, flinfo, 2,
				 InvalidOid,
				 context, NULL);
	inner_fcinfo->args[0].value = arg0;
	inner_fcinfo->args[0].isnull = arg0isnull;
	inner_fcinfo->args[1].value = arg1;
	inner_fcinfo->args[1].isnull = arg1isnull;

	return FunctionCallInvoke(inner_fcinfo);
}

static Datum
call_numeric_unary_fmgr(FmgrInfo *flinfo,
				Datum arg0,
				bool arg0isnull,
				Node *context)
{
	LOCAL_FCINFO(inner_fcinfo, 1);

	InitFunctionCallInfoData(*inner_fcinfo, flinfo, 1,
				 InvalidOid,
				 context, NULL);
	inner_fcinfo->args[0].value = arg0;
	inner_fcinfo->args[0].isnull = arg0isnull;

	return FunctionCallInvoke(inner_fcinfo);
}

static Datum
coerce_value_to_numeric(VecGroupAggTarget *tgt, Datum val)
{
	switch (tgt->col_type)
	{
		case VECGAGG_TYPE_INT4:
			return DirectFunctionCall1(int8_numeric,
							   Int64GetDatum((int64) DatumGetInt32(val)));
		case VECGAGG_TYPE_INT8:
			return DirectFunctionCall1(int8_numeric, Int64GetDatum(DatumGetInt64(val)));
		case VECGAGG_TYPE_FLOAT4:
			return DirectFunctionCall1(float8_numeric,
							   Float8GetDatum((float8) DatumGetFloat4(val)));
		case VECGAGG_TYPE_FLOAT8:
			return DirectFunctionCall1(float8_numeric, Float8GetDatum(DatumGetFloat8(val)));
		case VECGAGG_TYPE_NUMERIC:
			return val;
		default:
			return (Datum) 0;
	}
}

/*
 * Build a composite VecGroupKey from arrays of key values.
 * text/bpchar keys are stored inline in text_key[ki]; all keys also store
 * the original Datum in key[ki] for use by fill_and_store_slot.
 */
static void
build_composite_key(VecGroupAggState *state, Datum keys[], bool isnulls[],
					VecGroupKey *hkey)
{
	int ki;
	MemSet(hkey, 0, sizeof(*hkey));
	hkey->num_keys = state->num_keys;

	for (ki = 0; ki < state->num_keys; ki++)
	{
		hkey->key_type[ki] = state->key_col_type[ki];
		hkey->isnull[ki]   = isnulls[ki];

		if (isnulls[ki])
			continue;

		hkey->key[ki] = keys[ki];	/* store original Datum for all types */

		if (state->key_col_type[ki] == VECGAGG_TYPE_BPCHAR ||
			state->key_col_type[ki] == VECGAGG_TYPE_TEXT)
		{
			text   *txt = DatumGetTextPP(keys[ki]);
			int		len = VARSIZE_ANY_EXHDR(txt);

			if (len > (int) sizeof(hkey->text_key[ki]))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("VectorGroupAgg: GROUP BY key too long (%d > %zu)",
								len, sizeof(hkey->text_key[ki]))));
			hkey->text_len[ki] = (int16) len;
			memcpy(hkey->text_key[ki], VARDATA_ANY(txt), len);
		}
	}
}

/* ----------------------------------------------------------------
 *  Custom HTAB hash and compare callbacks for composite keys
 * ---------------------------------------------------------------- */

static uint32
vecgroupkey_hash(const void *key, Size keysize)
{
	const VecGroupKey *gk = (const VecGroupKey *) key;
	uint32	hash = 0;
	int		ki;

	for (ki = 0; ki < gk->num_keys; ki++)
	{
		uint32 kh;

		if (gk->isnull[ki])
		{
			kh = 0xdeadbeef;  /* NULLs hash to a fixed value */
		}
		else if (gk->key_type[ki] == VECGAGG_TYPE_BPCHAR ||
				 gk->key_type[ki] == VECGAGG_TYPE_TEXT)
		{
			kh = hash_bytes((const unsigned char *) gk->text_key[ki],
							(int) gk->text_len[ki]);
		}
		else
		{
			kh = hash_bytes((const unsigned char *) &gk->key[ki],
							sizeof(Datum));
		}
		/* combine per standard hash mixing */
		hash ^= kh + 0x9e3779b9u + (hash << 6) + (hash >> 2);
	}
	return hash;
}

static int
vecgroupkey_compare(const void *key1, const void *key2, Size keysize)
{
	const VecGroupKey *a = (const VecGroupKey *) key1;
	const VecGroupKey *b = (const VecGroupKey *) key2;
	int		ki;

	if (a->num_keys != b->num_keys)
		return a->num_keys - b->num_keys;

	for (ki = 0; ki < a->num_keys; ki++)
	{
		if (a->isnull[ki] && b->isnull[ki])
			continue;
		if (a->isnull[ki] != b->isnull[ki])
			return a->isnull[ki] ? 1 : -1;

		if (a->key_type[ki] == VECGAGG_TYPE_BPCHAR ||
			a->key_type[ki] == VECGAGG_TYPE_TEXT)
		{
			int minlen = (a->text_len[ki] < b->text_len[ki])
						 ? a->text_len[ki] : b->text_len[ki];
			int cmp = memcmp(a->text_key[ki], b->text_key[ki], minlen);
			if (cmp != 0) return cmp;
			if (a->text_len[ki] != b->text_len[ki])
				return (a->text_len[ki] < b->text_len[ki]) ? -1 : 1;
		}
		else
		{
			/* by-value or by-ref (Datum comparison) */
			if (a->key[ki] != b->key[ki])
				return (a->key[ki] < b->key[ki]) ? -1 : 1;
		}
	}
	return 0;
}

/*
 * Initialize per-group HTAB with custom hash+compare for composite keys.
 */
static HTAB *
create_group_htab(VecGroupAggState *state, MemoryContext ctx)
{
	HASHCTL		ctl;
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize	  = sizeof(VecGroupKey);
	ctl.entrysize = sizeof(VecGroupEntry);
	ctl.hcxt	  = ctx;
	ctl.hash	  = vecgroupkey_hash;
	ctl.match	  = vecgroupkey_compare;
	return hash_create("VecGroupAgg groups",
					   256,
					   &ctl,
					   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
}

/*
 * Lookup or create a group entry for a composite key.
 * hkey must already be filled by build_composite_key().
 */
static VecGroupEntry *
lookup_or_create_group(VecGroupAggState *state, VecGroupKey *hkey)
{
	VecGroupEntry  *entry;
	bool			found;

	entry = (VecGroupEntry *) hash_search(state->group_htab,
										  hkey,
										  HASH_ENTER,
										  &found);
	if (!found)
	{
		int			ki, i;
		MemoryContext	oldctx;

		if (state->num_groups >= VECGROUPAGG_MAX_GROUPS)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("VectorGroupAgg: too many distinct groups (limit %d)",
							VECGROUPAGG_MAX_GROUPS)));

		/*
		 * For by-reference key types, make stable copies in agg_context.
		 * For text/bpchar, also re-extract inline text from the stable copy
		 * so the hash/compare inline bytes stay valid.
		 */
		oldctx = MemoryContextSwitchTo(state->agg_context);
		for (ki = 0; ki < state->num_keys; ki++)
		{
			if (!hkey->isnull[ki] && !state->key_typbyval[ki])
			{
				entry->k.key[ki] = datumCopy(entry->k.key[ki],
											 false,
											 state->key_typlen[ki]);
				if (hkey->key_type[ki] == VECGAGG_TYPE_BPCHAR ||
					hkey->key_type[ki] == VECGAGG_TYPE_TEXT)
				{
					text *txt = DatumGetTextPP(entry->k.key[ki]);
					int   len = VARSIZE_ANY_EXHDR(txt);
					entry->k.text_len[ki] = (int16) len;
					memcpy(entry->k.text_key[ki], VARDATA_ANY(txt), len);
				}
			}
		}
		MemoryContextSwitchTo(oldctx);

		for (i = 0; i < state->num_targets; i++)
		{
			entry->int64_acc[i]			= 0;
			entry->avg_count_acc[i]		= 0;
			entry->float8_acc[i]		= 0.0;
			entry->numeric_acc[i]		= (Datum) 0;
			entry->numeric_state_acc[i]	= (Datum) 0;
			entry->acc_isnull[i]		= true;
			entry->distinct_htab[i]		= NULL;

			/* For COUNT(DISTINCT col), create a per-group hash set */
			if (state->targets[i].agg_kind == VECGAGG_COUNT_DISTINCT)
			{
				HASHCTL		ctl;
				char		htab_name[64];
				int			col_type = state->targets[i].col_type;
				Size		key_size;

				/*
				 * Key size depends on the column type.
				 * For text/bpchar we store a fixed-width hash of the bytes
				 * to avoid pointer lifetime issues — use uint64 hash key.
				 * For by-value types we store the raw Datum (always 8 bytes).
				 */
				if (col_type == VECGAGG_TYPE_TEXT || col_type == VECGAGG_TYPE_BPCHAR)
					key_size = sizeof(uint64);	/* hash of the text bytes */
				else
					key_size = sizeof(int64);	/* raw int64/float8/etc Datum */

				memset(&ctl, 0, sizeof(ctl));
				ctl.keysize   = key_size;
				ctl.entrysize = key_size;
				ctl.hcxt      = state->agg_context;
				snprintf(htab_name, sizeof(htab_name), "distinct_set_t%d_g%d",
						 i, state->num_groups);
				entry->distinct_htab[i] = hash_create(htab_name, 64, &ctl,
										  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
			}
		}

		state->num_groups++;
	}

	return entry;
}

/*
 * Recursively evaluate an inline VecExprNode expression tree against
 * a single row (row_i) of a VectorTupleTableSlot batch.
 *
 * Returns the result Datum and sets *isnull.  Any NULL input propagates
 * as NULL output (SQL 3-valued logic).
 */
Datum
eval_vec_expr_node(VecExprNode *nodes, VecExprNode *cur,
				   TupleTableSlot *batch_slot, int row_i, bool *isnull)
{
	if (cur->is_var)
	{
		/* Constant leaf (Const node in expression, value set at Begin time) */
		if (cur->slot_idx == VECEXPR_CONST_SENTINEL)
		{
			*isnull = cur->const_isnull;
			return cur->const_val;
		}

		VectorColumn *col = (VectorColumn *) batch_slot->tts_values[cur->slot_idx];

		if (col == NULL)
		{
			*isnull = true;
			return (Datum) 0;
		}
		*isnull = col->isnull[row_i];
		if (*isnull)
			return (Datum) 0;

		{
			int8 *rawptr = (int8 *) col->value +
						   (int) col->columnTypeLen * row_i;
			return fetch_att(rawptr, col->columnIsVal, col->columnTypeLen);
		}
	}
	else
	{
		bool	lnull;
		Datum	lval;

		lval = eval_vec_expr_node(nodes, &nodes[cur->left],
								  batch_slot, row_i, &lnull);
		if (lnull)
		{
			*isnull = true;
			return (Datum) 0;
		}

		if (cur->right >= 0)
		{
			/* Binary operator */
			bool	rnull;
			Datum	rval;

			rval = eval_vec_expr_node(nodes, &nodes[cur->right],
									  batch_slot, row_i, &rnull);
			if (rnull)
			{
				*isnull = true;
				return (Datum) 0;
			}
			*isnull = false;
			return FunctionCall2(&cur->opfmgr, lval, rval);
		}
		else
		{
			/* Unary operator (type cast) — passthrough when opfuncid=InvalidOid */
			*isnull = false;
			if (!OidIsValid(cur->opfuncid))
				return lval;
			return FunctionCall1(&cur->opfmgr, lval);
		}
	}
}

/*
 * Accumulate one value into a group entry for aggregate target t.
 */
static void
accumulate_value(VecGroupAggState *state, VecGroupEntry *entry,
			 int t_idx, VecGroupAggTarget *tgt,
				 Datum val, bool isnull)
{
	if (tgt->agg_kind == VECGAGG_COUNT_STAR)
	{
		/* count(*) counts all rows regardless of nullness */
		entry->int64_acc[t_idx]++;
		entry->acc_isnull[t_idx] = false;
		return;
	}

	if (tgt->agg_kind == VECGAGG_COUNT_COL)
	{
		/* count(col) counts only non-NULL rows */
		if (!isnull)
		{
			entry->int64_acc[t_idx]++;
			entry->acc_isnull[t_idx] = false;
		}
		return;
	}

	if (tgt->agg_kind == VECGAGG_COUNT_DISTINCT)
	{
		/*
		 * count(distinct col): insert the value into the per-group hash set.
		 * NULL values are ignored (SQL semantics).
		 * For text/bpchar we hash the bytes to uint64 so the key is always
		 * fixed-width and has no pointer lifetime issue.
		 * For numeric types we store the raw int64 Datum.
		 */
		if (!isnull && entry->distinct_htab[t_idx] != NULL)
		{
			bool found;
			if (tgt->col_type == VECGAGG_TYPE_TEXT ||
				tgt->col_type == VECGAGG_TYPE_BPCHAR)
			{
				text	*tv  = DatumGetTextPP(val);
				uint64	 hv  = hash_any_extended(
								(const unsigned char *) VARDATA_ANY(tv),
								VARSIZE_ANY_EXHDR(tv), 0);
				hash_search(entry->distinct_htab[t_idx], &hv, HASH_ENTER, &found);
			}
			else
			{
				int64	 kv = DatumGetInt64(val);	/* float/int all fit */
				hash_search(entry->distinct_htab[t_idx], &kv, HASH_ENTER, &found);
			}
			entry->acc_isnull[t_idx] = false;
		}
		return;
	}

	if (isnull)
		return;		/* other aggregates skip NULLs */

	switch (tgt->agg_kind)
	{
		case VECGAGG_SUM_EXPR:	/* fall through: val was pre-evaluated from expr */
		case VECGAGG_SUM:
			if (tgt->result_typeoid == NUMERICOID)
			{
				Datum numeric_val;
				MemoryContext oldctx;

				ensure_numeric_fmgr(state);
				oldctx = MemoryContextSwitchTo(state->agg_context);
				numeric_val = coerce_value_to_numeric(tgt, val);
				entry->numeric_state_acc[t_idx] =
					call_numeric_binary_fmgr(&state->numeric_avg_accum_fmgr,
						entry->numeric_state_acc[t_idx],
						entry->numeric_state_acc[t_idx] == (Datum) 0,
						numeric_val,
						false,
						(Node *) &state->numeric_fake_aggstate);
				entry->acc_isnull[t_idx] = false;
				MemoryContextSwitchTo(oldctx);
				break;
			}

			switch (tgt->col_type)
			{
				case VECGAGG_TYPE_INT4:
					entry->int64_acc[t_idx] += (int64) DatumGetInt32(val);
					entry->acc_isnull[t_idx] = false;
					break;
				case VECGAGG_TYPE_INT8:
					entry->int64_acc[t_idx] += DatumGetInt64(val);
					entry->acc_isnull[t_idx] = false;
					break;
				case VECGAGG_TYPE_FLOAT4:
					entry->float8_acc[t_idx] += (float8) DatumGetFloat4(val);
					entry->acc_isnull[t_idx] = false;
					break;
				case VECGAGG_TYPE_FLOAT8:
					entry->float8_acc[t_idx] += DatumGetFloat8(val);
					entry->acc_isnull[t_idx] = false;
					break;
			}
			break;

		case VECGAGG_AVG:
			if (tgt->use_int8_avg_path)
			{
				/*
				 * avg(int/bigint) partial: call the real aggtransfn resolved
				 * from pg_aggregate at plan time.  The fmgr was pre-loaded in
				 * BeginVecGroupAgg via avg_transfn_fmgr[t_idx].
				 */
				MemoryContext	oldctx;

				oldctx = MemoryContextSwitchTo(state->agg_context);
				entry->numeric_state_acc[t_idx] =
					call_numeric_binary_fmgr(&state->avg_transfn_fmgr[t_idx],
						entry->numeric_state_acc[t_idx],
						entry->numeric_state_acc[t_idx] == (Datum) 0,
						val, false,
						(Node *) &state->numeric_fake_aggstate);
				entry->acc_isnull[t_idx] = false;
				MemoryContextSwitchTo(oldctx);
				break;
			}
			if (tgt->result_typeoid == NUMERICOID)
			{
				Datum numeric_val;
				MemoryContext oldctx;

				ensure_numeric_fmgr(state);
				oldctx = MemoryContextSwitchTo(state->agg_context);
				numeric_val = coerce_value_to_numeric(tgt, val);
				entry->numeric_state_acc[t_idx] =
					call_numeric_binary_fmgr(&state->numeric_avg_accum_fmgr,
						entry->numeric_state_acc[t_idx],
						entry->numeric_state_acc[t_idx] == (Datum) 0,
						numeric_val,
						false,
						(Node *) &state->numeric_fake_aggstate);
				entry->acc_isnull[t_idx] = false;
				MemoryContextSwitchTo(oldctx);
				break;
			}

			switch (tgt->col_type)
			{
				case VECGAGG_TYPE_INT4:
					if (tgt->avg_input_as_float8)
						entry->float8_acc[t_idx] += (float8) DatumGetInt32(val);
					else
						entry->int64_acc[t_idx] += (int64) DatumGetInt32(val);
					entry->avg_count_acc[t_idx]++;
					entry->acc_isnull[t_idx] = false;
					break;
				case VECGAGG_TYPE_INT8:
					if (tgt->avg_input_as_float8)
						entry->float8_acc[t_idx] += (float8) DatumGetInt64(val);
					else
						entry->int64_acc[t_idx] += DatumGetInt64(val);
					entry->avg_count_acc[t_idx]++;
					entry->acc_isnull[t_idx] = false;
					break;
				case VECGAGG_TYPE_FLOAT4:
					entry->float8_acc[t_idx] += (float8) DatumGetFloat4(val);
					entry->avg_count_acc[t_idx]++;
						entry->acc_isnull[t_idx] = false;
					break;
				case VECGAGG_TYPE_FLOAT8:
					entry->float8_acc[t_idx] += DatumGetFloat8(val);
					entry->avg_count_acc[t_idx]++;
						entry->acc_isnull[t_idx] = false;
					break;
				default:
					break;
			}
			break;

		case VECGAGG_MIN:
			if (tgt->result_typeoid == NUMERICOID)
			{
				Datum numeric_val;
				MemoryContext oldctx;

				ensure_numeric_fmgr(state);
				oldctx = MemoryContextSwitchTo(state->agg_context);
				numeric_val = coerce_value_to_numeric(tgt, val);
				if (entry->acc_isnull[t_idx])
				{
					entry->numeric_acc[t_idx] = datumCopy(numeric_val, false, -1);
					entry->acc_isnull[t_idx] = false;
				}
				else if (DatumGetInt32(call_numeric_binary_fmgr(
						 &state->numeric_cmp_fmgr,
						 entry->numeric_acc[t_idx],
						 false,
						 numeric_val,
						 false,
						 (Node *) &state->numeric_fake_aggstate)) > 0)
				{
					entry->numeric_acc[t_idx] = datumCopy(numeric_val, false, -1);
				}
				MemoryContextSwitchTo(oldctx);
				break;
			}

			switch (tgt->col_type)
			{
				case VECGAGG_TYPE_INT4:
				{
					int64 v = (int64) DatumGetInt32(val);
					if (entry->acc_isnull[t_idx])
					{
						entry->int64_acc[t_idx] = v;
						entry->acc_isnull[t_idx] = false;
					}
					else if (v < entry->int64_acc[t_idx])
						entry->int64_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_INT8:
				{
					int64 v = DatumGetInt64(val);
					if (entry->acc_isnull[t_idx])
					{
						entry->int64_acc[t_idx] = v;
						entry->acc_isnull[t_idx] = false;
					}
					else if (v < entry->int64_acc[t_idx])
						entry->int64_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_FLOAT4:
				{
					float8 v = (float8) DatumGetFloat4(val);
					if (entry->acc_isnull[t_idx])
					{
						entry->float8_acc[t_idx] = v;
						entry->acc_isnull[t_idx] = false;
					}
					else if (v < entry->float8_acc[t_idx])
						entry->float8_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_FLOAT8:
				{
					float8 v = DatumGetFloat8(val);
					if (entry->acc_isnull[t_idx])
					{
						entry->float8_acc[t_idx] = v;
						entry->acc_isnull[t_idx] = false;
					}
					else if (v < entry->float8_acc[t_idx])
						entry->float8_acc[t_idx] = v;
					break;
				}
			}
			break;

		case VECGAGG_MAX:
			if (tgt->result_typeoid == NUMERICOID)
			{
				Datum numeric_val;
				MemoryContext oldctx;

				ensure_numeric_fmgr(state);
				oldctx = MemoryContextSwitchTo(state->agg_context);
				numeric_val = coerce_value_to_numeric(tgt, val);
				if (entry->acc_isnull[t_idx])
				{
					entry->numeric_acc[t_idx] = datumCopy(numeric_val, false, -1);
					entry->acc_isnull[t_idx] = false;
				}
				else if (DatumGetInt32(call_numeric_binary_fmgr(
						 &state->numeric_cmp_fmgr,
						 entry->numeric_acc[t_idx],
						 false,
						 numeric_val,
						 false,
						 (Node *) &state->numeric_fake_aggstate)) < 0)
				{
					entry->numeric_acc[t_idx] = datumCopy(numeric_val, false, -1);
				}
				MemoryContextSwitchTo(oldctx);
				break;
			}

			switch (tgt->col_type)
			{
				case VECGAGG_TYPE_INT4:
				{
					int64 v = (int64) DatumGetInt32(val);
					if (entry->acc_isnull[t_idx])
					{
						entry->int64_acc[t_idx] = v;
						entry->acc_isnull[t_idx] = false;
					}
					else if (v > entry->int64_acc[t_idx])
						entry->int64_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_INT8:
				{
					int64 v = DatumGetInt64(val);
					if (entry->acc_isnull[t_idx])
					{
						entry->int64_acc[t_idx] = v;
						entry->acc_isnull[t_idx] = false;
					}
					else if (v > entry->int64_acc[t_idx])
						entry->int64_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_FLOAT4:
				{
					float8 v = (float8) DatumGetFloat4(val);
					if (entry->acc_isnull[t_idx])
					{
						entry->float8_acc[t_idx] = v;
						entry->acc_isnull[t_idx] = false;
					}
					else if (v > entry->float8_acc[t_idx])
						entry->float8_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_FLOAT8:
				{
					float8 v = DatumGetFloat8(val);
					if (entry->acc_isnull[t_idx])
					{
						entry->float8_acc[t_idx] = v;
						entry->acc_isnull[t_idx] = false;
					}
					else if (v > entry->float8_acc[t_idx])
						entry->float8_acc[t_idx] = v;
					break;
				}
			}
			break;
	}
}

/*
 * Comparison context for qsort-based sorted emission (single-key only).
 * Only used when sort_output=true and num_keys==1.
 */
static int	g_sort_key_type;	/* set before qsort call */

static int
vecgroup_entry_cmp(const void *a, const void *b)
{
	const VecGroupEntry *ea = *(const VecGroupEntry **) a;
	const VecGroupEntry *eb = *(const VecGroupEntry **) b;

	/* NULLs sort last */
	if (ea->k.isnull[0] && eb->k.isnull[0])
		return 0;
	if (ea->k.isnull[0])
		return 1;
	if (eb->k.isnull[0])
		return -1;

	switch (g_sort_key_type)
	{
		case VECGAGG_TYPE_INT4:
		{
			int64 va = DatumGetInt32(ea->k.key[0]);
			int64 vb = DatumGetInt32(eb->k.key[0]);
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case VECGAGG_TYPE_INT8:
		{
			int64 va = DatumGetInt64(ea->k.key[0]);
			int64 vb = DatumGetInt64(eb->k.key[0]);
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case VECGAGG_TYPE_FLOAT4:
		{
			float4 va = DatumGetFloat4(ea->k.key[0]);
			float4 vb = DatumGetFloat4(eb->k.key[0]);
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case VECGAGG_TYPE_FLOAT8:
		{
			float8 va = DatumGetFloat8(ea->k.key[0]);
			float8 vb = DatumGetFloat8(eb->k.key[0]);
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case VECGAGG_TYPE_BPCHAR:
		case VECGAGG_TYPE_TEXT:
		{
			int	 lena = ea->k.text_len[0];
			int	 lenb = eb->k.text_len[0];
			int	 minlen = (lena < lenb) ? lena : lenb;
			int	 cmp = memcmp(ea->k.text_key[0], eb->k.text_key[0], minlen);

			if (cmp != 0)
				return cmp;
			return (lena < lenb) ? -1 : (lena > lenb) ? 1 : 0;
		}
		default:
			return 0;
	}
}

/*
 * Process one VectorTupleTableSlot batch: update per-group accumulators.
 * The slot has one VectorColumn per projected attribute.
 *
 * Attribute layout in the slot matches ColumnarReadNextVector output:
 *   key_attnum[ki]  - 0-based slot output position of the i-th GROUP BY key
 *   col_attnum      - 0-based slot output position of the aggregate column
 *                     (-1 for count(*) which needs no column access)
 */
static void
process_vector_batch(VecGroupAggState *state, TupleTableSlot *slot)
{
	VectorTupleTableSlot *vslot = (VectorTupleTableSlot *) slot;
	uint32	dim = vslot->dimension;
	uint32	i;
	int		ki;

	/* Pre-fetch all key columns */
	VectorColumn *key_cols[VECGROUPAGG_MAX_KEYS];
	for (ki = 0; ki < state->num_keys; ki++)
	{
		int key_idx = state->key_attnum[ki];
		key_cols[ki] = (state->key_is_const[ki] || key_idx < 0)
			? NULL
			: (VectorColumn *) slot->tts_values[key_idx];
	}

	for (i = 0; i < dim; i++)
	{
		if (!vslot->keep[i])
			continue;

		Datum			key_vals[VECGROUPAGG_MAX_KEYS];
		bool			key_nulls[VECGROUPAGG_MAX_KEYS];
		VecGroupKey		hkey;
		VecGroupEntry  *entry;
		int				t;

		for (ki = 0; ki < state->num_keys; ki++)
		{
			if (state->key_is_const[ki])
			{
				key_vals[ki]  = state->key_const[ki];
				key_nulls[ki] = state->key_const_isnull[ki];
			}
			else if (key_cols[ki])
			{
				int8 *rawPtr = (int8 *) key_cols[ki]->value +
							  (int) key_cols[ki]->columnTypeLen * i;
				key_vals[ki]  = fetch_att(rawPtr, key_cols[ki]->columnIsVal,
										  key_cols[ki]->columnTypeLen);
				key_nulls[ki] = key_cols[ki]->isnull[i];
			}
			else
			{
				key_vals[ki]  = (Datum) 0;
				key_nulls[ki] = true;
			}
		}

		build_composite_key(state, key_vals, key_nulls, &hkey);
		entry = lookup_or_create_group(state, &hkey);

		for (t = 0; t < state->num_targets; t++)
		{
			VecGroupAggTarget *tgt = &state->targets[t];
			Datum	val = (Datum) 0;
			bool	val_null = true;

			if (tgt->agg_kind == VECGAGG_SUM_EXPR && tgt->expr_num_nodes > 0)
			{
				/*
				 * VECGAGG_SUM_EXPR: evaluate the arithmetic expression tree
				 * row-by-row against the VectorColumn batch.
				 */
				val = eval_vec_expr_node(tgt->expr_nodes,
										 &tgt->expr_nodes[tgt->expr_root_idx],
										 slot, i, &val_null);
			}
			else if (tgt->agg_kind != VECGAGG_COUNT_STAR && tgt->col_attnum >= 0)
			{
				int val_idx = tgt->col_attnum;
				VectorColumn *val_col = (VectorColumn *) slot->tts_values[val_idx];
				if (val_col)
				{
					int8 *vrawPtr = (int8 *) val_col->value +
									(int) val_col->columnTypeLen * i;
					val      = fetch_att(vrawPtr, val_col->columnIsVal,
										 val_col->columnTypeLen);
					val_null = val_col->isnull[i];
				}
			}

			/*
			 * CASE WHEN col = const THEN val END filter.
			 * If the filter condition is false (or NULL), treat the value as
			 * NULL so that accumulate_value skips it.
			 */
			if (tgt->has_case_filter && !val_null)
			{
				VectorColumn *fcol = (tgt->filter_col_attnum >= 0)
					? (VectorColumn *) slot->tts_values[tgt->filter_col_attnum]
					: NULL;

				if (fcol == NULL || fcol->isnull[i])
				{
					val_null = true;	/* NULL filter column → skip */
				}
				else
				{
					int8  *frawPtr = (int8 *) fcol->value +
									 (int) fcol->columnTypeLen * i;
					Datum  fval    = fetch_att(frawPtr, fcol->columnIsVal,
											   fcol->columnTypeLen);
					bool   matches;

					if (tgt->filter_col_type == VECGAGG_TYPE_TEXT ||
						tgt->filter_col_type == VECGAGG_TYPE_BPCHAR)
					{
						/*
						 * Varlena text comparison: compare raw bytes after
						 * stripping any varlena header (VARDATA_ANY / VARSIZE_ANY_EXHDR).
						 */
						text *fa = DatumGetTextPP(fval);
						text *fb = DatumGetTextPP(tgt->filter_eq_value);
						Size  la = VARSIZE_ANY_EXHDR(fa);
						Size  lb = VARSIZE_ANY_EXHDR(fb);

						matches  = (la == lb &&
									memcmp(VARDATA_ANY(fa), VARDATA_ANY(fb), la) == 0);
					}
					else
					{
						/* By-value comparison (int4, int8, float8, bool, etc.) */
						matches = (fval == tgt->filter_eq_value);
					}

					if (!matches)
						val_null = true;	/* condition false → skip */
				}
			}

			accumulate_value(state, entry, t, tgt, val, val_null);
		}
	}
}

/* Store values directly into slot */
static TupleTableSlot *
fill_and_store_slot(VecGroupAggState *state, VecGroupEntry *entry,
					TupleTableSlot *slot)
{
	int natt = slot->tts_tupleDescriptor->natts;
	int t;

	ExecClearTuple(slot);

	/* zero everything */
	for (int i = 0; i < natt; i++)
	{
		slot->tts_values[i] = (Datum) 0;
		slot->tts_isnull[i] = true;
	}

	/* Slot atts for all GROUP BY keys */
	for (int ki = 0; ki < state->num_keys; ki++)
	{
		int kra = state->key_result_attnum[ki];
		if (kra >= 0 && kra < natt)
		{
			slot->tts_values[kra] = entry->k.key[ki];
			slot->tts_isnull[kra] = entry->k.isnull[ki];
		}
	}

	/* Aggregate result atts */
	for (t = 0; t < state->num_targets; t++)
	{
		VecGroupAggTarget *tgt = &state->targets[t];
		int ra = tgt->result_attnum;	/* 0-based */

		if (ra >= natt)
			continue;

		if (entry->acc_isnull[t])
		{
			slot->tts_values[ra] = (Datum) 0;
			slot->tts_isnull[ra] = true;
			continue;
		}

		slot->tts_isnull[ra] = false;

		switch (tgt->agg_kind)
		{
			case VECGAGG_COUNT_STAR:
			case VECGAGG_COUNT_COL:
				slot->tts_values[ra] = Int64GetDatum(entry->int64_acc[t]);
				break;
			case VECGAGG_COUNT_DISTINCT:
				/*
				 * Count the number of unique keys in the per-group hash set.
				 * hash_get_num_entries returns the current fill count.
				 */
				slot->tts_values[ra] = Int64GetDatum(
					entry->distinct_htab[t] != NULL
					? (int64) hash_get_num_entries(entry->distinct_htab[t])
					: 0);
				break;
			case VECGAGG_SUM:
			case VECGAGG_SUM_EXPR:
			case VECGAGG_MIN:
			case VECGAGG_MAX:
					if (tgt->result_typeoid == NUMERICOID)
					{
						if (state->is_partial_serial &&
							(tgt->agg_kind == VECGAGG_SUM || tgt->agg_kind == VECGAGG_SUM_EXPR))
						{
							slot->tts_values[ra] =
								call_numeric_unary_fmgr(
									&state->numeric_avg_serialize_fmgr,
									entry->numeric_state_acc[t],
									entry->numeric_state_acc[t] == (Datum) 0,
									(Node *) &state->numeric_fake_aggstate);
						}
						else if (tgt->agg_kind == VECGAGG_SUM ||
								 tgt->agg_kind == VECGAGG_SUM_EXPR)
						{
							slot->tts_values[ra] =
								call_numeric_unary_fmgr(
									&state->numeric_sum_fmgr,
									entry->numeric_state_acc[t],
									entry->numeric_state_acc[t] == (Datum) 0,
									(Node *) &state->numeric_fake_aggstate);
						}
						else
						{
							slot->tts_values[ra] = entry->numeric_acc[t];
						}
						break;
					}

				switch (tgt->col_type)
				{
					case VECGAGG_TYPE_INT4:
					case VECGAGG_TYPE_INT8:
						/*
						 * sum(int8) returns numeric in SQL (OID 1700).
						 * Convert int64 accumulator to numeric Datum so
						 * downstream nodes (Sort, Materialize, etc.) can
						 * safely copy the slot into a heap / minimal tuple.
						 */
						if (tgt->result_typeoid == NUMERICOID)
							slot->tts_values[ra] =
								DirectFunctionCall1(int8_numeric,
													Int64GetDatum(entry->int64_acc[t]));
						else
							slot->tts_values[ra] =
								Int64GetDatum(entry->int64_acc[t]);
						break;
					case VECGAGG_TYPE_FLOAT4:
						if (tgt->result_typeoid == FLOAT4OID)
							slot->tts_values[ra] = Float4GetDatum((float4) entry->float8_acc[t]);
						else
							slot->tts_values[ra] = Float8GetDatum(entry->float8_acc[t]);
						break;
					case VECGAGG_TYPE_FLOAT8:
						slot->tts_values[ra] = Float8GetDatum(entry->float8_acc[t]);
						break;
				}
				break;
			case VECGAGG_AVG:
					if (tgt->result_typeoid == NUMERICOID)
					{
						if (tgt->use_int8_avg_path && state->is_partial_serial)
						{
							/* avg(int/bigint) partial: emit int8_avg_serialize bytea */
							ensure_int8_avg_serialize_fmgr(state);
							slot->tts_values[ra] =
								call_numeric_unary_fmgr(
									&state->int8_avg_serialize_fmgr,
									entry->numeric_state_acc[t],
									entry->numeric_state_acc[t] == (Datum) 0,
									(Node *) &state->numeric_fake_aggstate);
						}
						else if (state->is_partial_serial)
						{
							slot->tts_values[ra] =
								call_numeric_unary_fmgr(
									&state->numeric_avg_serialize_fmgr,
									entry->numeric_state_acc[t],
									entry->numeric_state_acc[t] == (Datum) 0,
									(Node *) &state->numeric_fake_aggstate);
						}
						else
						{
							slot->tts_values[ra] =
								call_numeric_unary_fmgr(
									&state->numeric_avg_fmgr,
									entry->numeric_state_acc[t],
									entry->numeric_state_acc[t] == (Datum) 0,
									(Node *) &state->numeric_fake_aggstate);
						}
						break;
					}

				if (state->is_partial_serial)
				{
					if ((tgt->col_type == VECGAGG_TYPE_INT4 ||
						 tgt->col_type == VECGAGG_TYPE_INT8) &&
						!tgt->avg_input_as_float8)
					{
						Datum elems[2];

						/*
						 * avg(int4/int8) transition state is bigint[] with [count, sum].
						 */
						elems[0] = Int64GetDatum(entry->avg_count_acc[t]);
						elems[1] = Int64GetDatum(entry->int64_acc[t]);

						slot->tts_values[ra] = PointerGetDatum(
							construct_array(elems,
										2,
										INT8OID,
										sizeof(int64),
										true,
										TYPALIGN_DOUBLE));
					}
					else
					{
						Datum elems[3];

						/*
						 * avg(float8) transition state is float8[] with
						 * [count_nonnull, sum, reserved].
						 */
						elems[0] = Float8GetDatum((float8) entry->avg_count_acc[t]);
						elems[1] = Float8GetDatum(entry->float8_acc[t]);
						elems[2] = Float8GetDatum(0.0);

						slot->tts_values[ra] = PointerGetDatum(
							construct_array(elems,
										3,
										FLOAT8OID,
										sizeof(float8),
										FLOAT8PASSBYVAL,
										TYPALIGN_DOUBLE));
					}
				}
				else
				{
					if (tgt->result_typeoid == NUMERICOID)
					{
						Datum sum_numeric;
						Datum count_numeric =
							DirectFunctionCall1(int8_numeric,
											Int64GetDatum(entry->avg_count_acc[t]));

						if (tgt->col_type == VECGAGG_TYPE_INT4 &&
							!tgt->avg_input_as_float8)
						{
							sum_numeric =
								DirectFunctionCall1(int8_numeric,
												Int64GetDatum(entry->int64_acc[t]));
						}
						else
						{
							sum_numeric =
								DirectFunctionCall1(float8_numeric,
												Float8GetDatum(entry->float8_acc[t]));
						}

						slot->tts_values[ra] =
							DirectFunctionCall2(numeric_div,
											sum_numeric,
											count_numeric);
					}
					else
					{
						slot->tts_values[ra] =
							Float8GetDatum(entry->float8_acc[t] /
										   (float8) entry->avg_count_acc[t]);
					}
				}
				break;
		}
	}

	return ExecStoreVirtualTuple(slot);
}

/* ----------------------------------------------------------------
 *  Custom scan node callbacks
 * ---------------------------------------------------------------- */

static Node *
CreateVecGroupAggState(CustomScan *custom_plan)
{
	VecGroupAggState *state = (VecGroupAggState *)
		newNode(sizeof(VecGroupAggState), T_CustomScanState);

	state->css.methods = &VecGroupAggExecMethods;
	return (Node *) state;
}

static void
BeginVecGroupAgg(CustomScanState *css, EState *estate, int eflags)
{
	VecGroupAggState *state = (VecGroupAggState *) css;
	CustomScan		 *cscan = (CustomScan *) css->ss.ps.plan;

	elog(DEBUG1, "VecGroupAgg: BeginVecGroupAgg entered, eflags=%d", eflags);

	/*
	 * Unpack parameters from custom_private (new multi-key format):
	 *   [0]              num_keys
	 *   [1..num_keys]    key_attnums[ki]
	 *   [num_keys+1..2*num_keys]  key_typeoids[ki]
	 *   [2*num_keys+1..3*num_keys] key_is_consts[ki]
	 *   [3*num_keys+1..4*num_keys] key_result_atts[ki]
	 *   [4*num_keys+1]  num_targets
	 *   [4*num_keys+2]  sort_output
	 *   [4*num_keys+3]  aggsplit_mode
	 *   [Const nodes for ki where key_is_consts[ki]=true, in order]
	 *   [8 Ints per target: kind, col_type, col_attnum, result_attnum,
	 *    avg_input_as_float8, result_typeoid, use_int8_avg_path, avg_transfn_oid,
	 *    has_case_filter, filter_col_attnum, filter_col_type, <Const filter_value>]
	 */
	List   *priv = cscan->custom_private;
	int		target_idx;
	int		ki;

	#define PRIV_INT(nodeptr) ((int) DatumGetInt32(((Const *) (nodeptr))->constvalue))

	state->num_keys = PRIV_INT(list_nth(priv, 0));

	for (ki = 0; ki < state->num_keys; ki++)
		state->key_attnum[ki] = PRIV_INT(list_nth(priv, 1 + ki));
	for (ki = 0; ki < state->num_keys; ki++)
		state->key_typeoid[ki] = (Oid) PRIV_INT(list_nth(priv, state->num_keys + 1 + ki));
	for (ki = 0; ki < state->num_keys; ki++)
		state->key_is_const[ki] = (bool) PRIV_INT(list_nth(priv, 2 * state->num_keys + 1 + ki));
	for (ki = 0; ki < state->num_keys; ki++)
		state->key_result_attnum[ki] = PRIV_INT(list_nth(priv, 3 * state->num_keys + 1 + ki));

	state->num_targets  = PRIV_INT(list_nth(priv, 4 * state->num_keys + 1));
	state->sort_output  = (bool) PRIV_INT(list_nth(priv, 4 * state->num_keys + 2));
	state->is_partial_serial =
		(PRIV_INT(list_nth(priv, 4 * state->num_keys + 3)) == (int) AGGSPLIT_INITIAL_SERIAL);

	/* Initialize per-key type info */
	for (ki = 0; ki < state->num_keys; ki++)
	{
		state->key_col_type[ki] = type_oid_to_vectype(state->key_typeoid[ki]);
		get_typlenbyval(state->key_typeoid[ki],
						&state->key_typlen[ki],
						&state->key_typbyval[ki]);
		state->key_const[ki] = (Datum) 0;
		state->key_const_isnull[ki] = true;
	}

	/* Const key values (optional, one per const key in order) */
	target_idx = 4 * state->num_keys + 4;	/* first slot after the 4 fixed ints */
	for (ki = 0; ki < state->num_keys; ki++)
	{
		if (state->key_is_const[ki])
		{
			Const *kconst = (Const *) list_nth(priv, target_idx++);
			state->key_const_isnull[ki] = kconst->constisnull;
			if (!kconst->constisnull)
				state->key_const[ki] = datumCopy(kconst->constvalue,
												 state->key_typbyval[ki],
												 state->key_typlen[ki]);
		}
	}

	for (int tno = 0; tno < state->num_targets && tno < VECGROUPAGG_MAX_TARGETS; tno++)
	{
		state->targets[tno].agg_kind = PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].col_type = PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].col_attnum = PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].result_attnum = PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].avg_input_as_float8 = (bool) PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].result_typeoid = (Oid) PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].use_int8_avg_path = (bool) PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].avg_transfn_oid = (Oid) PRIV_INT(list_nth(priv, target_idx++));
		/* CASE WHEN filter deserialization */
		state->targets[tno].has_case_filter   = (bool) PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].filter_col_attnum = PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].filter_col_type   = PRIV_INT(list_nth(priv, target_idx++));
		{
			Const *fc = (Const *) list_nth(priv, target_idx++);
			state->targets[tno].filter_const_plan = NULL; /* runtime: unused */
			if (state->targets[tno].has_case_filter && !fc->constisnull)
			{
				state->targets[tno].filter_eq_typeoid = fc->consttype;
				get_typlenbyval(fc->consttype,
								&state->targets[tno].filter_typlen,
								&state->targets[tno].filter_typbyval);
				state->targets[tno].filter_eq_value =
					datumCopy(fc->constvalue,
							  state->targets[tno].filter_typbyval,
							  state->targets[tno].filter_typlen);
			}
			else
			{
				state->targets[tno].filter_eq_typeoid = InvalidOid;
				state->targets[tno].filter_typlen     = 0;
				state->targets[tno].filter_typbyval   = true;
				state->targets[tno].filter_eq_value   = (Datum) 0;
			}
		}

		/* VECGAGG_SUM_EXPR: inline expression tree */
		state->targets[tno].expr_num_nodes = PRIV_INT(list_nth(priv, target_idx++));
		state->targets[tno].expr_root_idx  = PRIV_INT(list_nth(priv, target_idx++));
		for (int en = 0; en < state->targets[tno].expr_num_nodes; en++)
		{
			VecExprNode *n = &state->targets[tno].expr_nodes[en];

			n->is_var    = (bool) PRIV_INT(list_nth(priv, target_idx++));
			n->slot_idx  = PRIV_INT(list_nth(priv, target_idx++));
			n->col_type  = PRIV_INT(list_nth(priv, target_idx++));
			n->left      = PRIV_INT(list_nth(priv, target_idx++));
			n->right     = PRIV_INT(list_nth(priv, target_idx++));
			n->opfuncid  = (Oid) PRIV_INT(list_nth(priv, target_idx++));
			n->rettype   = (Oid) PRIV_INT(list_nth(priv, target_idx++));

			/* Load runtime function info for operator nodes */
			if (!n->is_var && OidIsValid(n->opfuncid))
				fmgr_info_cxt(n->opfuncid, &n->opfmgr, state->agg_context);

			/* Load type info for the node's return type */
			if (OidIsValid(n->rettype))
				get_typlenbyval(n->rettype, &n->rettyplen, &n->retbyval);
		}
	}

	#undef PRIV_INT

	/* Create memory context for per-group data */
	state->agg_context = AllocSetContextCreate(CurrentMemoryContext,
											   "VecGroupAgg",
											   ALLOCSET_DEFAULT_SIZES);
	MemSet(&state->numeric_fake_aggstate, 0, sizeof(AggState));
	MemSet(&state->numeric_fake_aggexpr, 0, sizeof(ExprContext));
	state->numeric_fake_aggexpr.ecxt_per_tuple_memory = state->agg_context;
	((Node *) &state->numeric_fake_aggstate)->type = T_AggState;
	state->numeric_fake_aggstate.curaggcontext = &state->numeric_fake_aggexpr;
	state->numeric_fmgr_ready = false;
 	ensure_numeric_fmgr(state);

	/* Initialize per-target transition fmgrs for avg(int/bigint) partial path */
	state->int8_avg_serialize_ready = false;
	for (int tno = 0; tno < state->num_targets && tno < VECGROUPAGG_MAX_TARGETS; tno++)
	{
		if (state->targets[tno].use_int8_avg_path &&
			OidIsValid(state->targets[tno].avg_transfn_oid))
		{
			fmgr_info_cxt(state->targets[tno].avg_transfn_oid,
						  &state->avg_transfn_fmgr[tno],
						  state->agg_context);
		}
	}

	/* Initialize group hash table */
	state->group_htab = create_group_htab(state, state->agg_context);
	state->num_groups = 0;
	state->scan_done  = false;
	state->seq_started = false;
	state->sorted_arr  = NULL;
	state->sorted_idx  = 0;

	/*
	 * Initialize the child ColcompressScan.
	 * Child plan is in lefttree (not custom_plans) so that
	 * deparse_context_for_plan_tree can resolve OUTER_VAR references.
	 */
	{
		Plan *child_plan = outerPlan(cscan);
		outerPlanState(css) = ExecInitNode(child_plan, estate, eflags);
	}
}

static TupleTableSlot *
ExecVecGroupAgg(CustomScanState *css)
{
	VecGroupAggState *state = (VecGroupAggState *) css;
	PlanState		 *child_ps = outerPlanState(css);

	/* Phase 1: consume all batches from ColcompressScan */
	if (!state->scan_done)
	{
		for (;;)
		{
			TupleTableSlot *batch = ExecProcNode(child_ps);

			if (TupIsNull(batch))
				break;

			/* batch is a VectorTupleTableSlot */
			process_vector_batch(state, batch);
		}

		state->scan_done = true;

		if (state->sort_output)
		{
			/*
			 * AGG_SORTED replacement: build a sorted array of all group
			 * entries so we can emit them in ascending key order (to satisfy
			 * an ORDER BY clause that matched the GROUP BY key).
			 */
			HASH_SEQ_STATUS	seq;
			VecGroupEntry  *e;
			int				n = 0;

			state->sorted_arr = (VecGroupEntry **)
				MemoryContextAlloc(state->agg_context,
								   state->num_groups * sizeof(VecGroupEntry *));

			hash_seq_init(&seq, state->group_htab);
			while ((e = (VecGroupEntry *) hash_seq_search(&seq)) != NULL)
				state->sorted_arr[n++] = e;

			Assert(n == state->num_groups);

			/* Sort ascending by key */
			g_sort_key_type = state->key_col_type[0];
			if (n > 1)
				qsort(state->sorted_arr, n, sizeof(VecGroupEntry *),
					  vecgroup_entry_cmp);

			state->sorted_idx = 0;
		}
		else
		{
			hash_seq_init(&state->hash_seq, state->group_htab);
			state->seq_started = true;
		}
	}

	/* Phase 2: emit result tuples one per group */
	if (state->sort_output)
	{
		/* Emit from pre-sorted array */
		if (state->sorted_idx >= state->num_groups)
			return ExecClearTuple(css->ss.ss_ScanTupleSlot);

		VecGroupEntry *entry = state->sorted_arr[state->sorted_idx++];
		return fill_and_store_slot(state, entry, css->ss.ss_ScanTupleSlot);
	}

	VecGroupEntry *entry = (VecGroupEntry *) hash_seq_search(&state->hash_seq);

	if (entry == NULL)
	{
		/* Done — return empty scan slot, mark seq as fully consumed */
		state->seq_started = false;
		return ExecClearTuple(css->ss.ss_ScanTupleSlot);
	}

	/*
	 * Fill the scan tuple slot (ss_ScanTupleSlot), which PG's projection
	 * machinery will map to the result slot via the pass-through Var tlist.
	 */
	return fill_and_store_slot(state, entry, css->ss.ss_ScanTupleSlot);
}

static void
EndVecGroupAgg(CustomScanState *css)
{
	VecGroupAggState *state = (VecGroupAggState *) css;

	if (state->seq_started)
	{
		/* If seq not fully consumed, terminate the scan */
		VecGroupEntry *entry;
		while ((entry = (VecGroupEntry *) hash_seq_search(&state->hash_seq)) != NULL)
			;
		state->seq_started = false;
	}

	if (state->group_htab)
	{
		hash_destroy(state->group_htab);
		state->group_htab = NULL;
	}

	if (outerPlanState(css) != NULL)
		ExecEndNode(outerPlanState(css));

	MemoryContextDelete(state->agg_context);
}

static void
ReScanVecGroupAgg(CustomScanState *css)
{
	VecGroupAggState *state = (VecGroupAggState *) css;

	/* Re-scan: discard accumulated data and start over */
	if (state->seq_started)
	{
		VecGroupEntry *e;
		while ((e = (VecGroupEntry *) hash_seq_search(&state->hash_seq)) != NULL)
			;
		state->seq_started = false;
	}

	if (state->group_htab)
		hash_destroy(state->group_htab);

	state->group_htab = create_group_htab(state, state->agg_context);
	state->num_groups = 0;
	state->scan_done  = false;
	state->sorted_arr = NULL;
	state->sorted_idx = 0;

	ExecReScan(outerPlanState(css));
}

static void
ExplainVecGroupAgg(CustomScanState *css, List *ancestors, ExplainState *es)
{
	VecGroupAggState *state = (VecGroupAggState *) css;
	char		 buf[64];

	snprintf(buf, sizeof(buf), "%d", state->num_groups);
	ExplainPropertyText("Engine Vectorized Group Aggregate", "enabled", es);
	if (state->scan_done)
		ExplainPropertyText("Engine Groups Found", buf, es);
}

/* ----------------------------------------------------------------
 *  Plan-time node creation
 * ---------------------------------------------------------------- */

/*
 * engine_create_groupagg_node
 *
 * Build a CustomScan plan node for VectorGroupAgg (multi-key version).
 *
 * New custom_private format:
 *   [0]              num_keys
 *   [1..num_keys]    key_attnums[ki]
 *   [num_keys+1..2*num_keys]  key_typeoids[ki]
 *   [2*num_keys+1..3*num_keys] key_is_consts[ki]
 *   [3*num_keys+1..4*num_keys] key_result_atts[ki]
 *   [4*num_keys+1]  num_targets
 *   [4*num_keys+2]  sort_output
 *   [4*num_keys+3]  aggsplit_mode
 *   [Const nodes for each ki where key_is_consts[ki]=true, in order]
 *   [12 items per target: 11 ints + 1 Const:
 *    kind, col_type, col_attnum, result_attnum,
 *    avg_input_as_float8, result_typeoid, use_int8_avg_path, avg_transfn_oid,
 *    has_case_filter, filter_col_attnum, filter_col_type, <Const filter_value>]
 */
CustomScan *
engine_create_groupagg_node(int num_keys,
							int key_attnums[],
							Oid key_typeoids[],
							bool key_is_consts[],
							Const *key_consts[],
							int key_result_atts[],
							bool sort_output,
							int aggsplit_mode,
							int num_targets,
							VecGroupAggTarget *targets)
{
	CustomScan *cscan = makeNode(CustomScan);
	List	   *priv = NIL;
	int			t;
	int			ki;

	cscan->methods = &VecGroupAggScanMethods;
	cscan->flags   = 0;

#define MKINT(v) \
	do { \
		Const *_c = makeNode(Const); \
		_c->consttype   = INT4OID; \
		_c->consttypmod = -1; \
		_c->constbyval  = true; \
		_c->constlen    = sizeof(int32); \
		_c->constvalue  = Int32GetDatum(v); \
		_c->constisnull = false; \
		priv = lappend(priv, _c); \
	} while (0)

	MKINT(num_keys);
	for (ki = 0; ki < num_keys; ki++) MKINT(key_attnums[ki]);
	for (ki = 0; ki < num_keys; ki++) MKINT((int) key_typeoids[ki]);
	for (ki = 0; ki < num_keys; ki++) MKINT((int) key_is_consts[ki]);
	for (ki = 0; ki < num_keys; ki++) MKINT(key_result_atts[ki]);
	MKINT(num_targets);
	MKINT((int) sort_output);
	MKINT(aggsplit_mode);

	/* Const key values (in order, only for const keys) */
	for (ki = 0; ki < num_keys; ki++)
	{
		if (key_is_consts[ki] && key_consts[ki] != NULL)
			priv = lappend(priv, copyObject(key_consts[ki]));
	}

	for (t = 0; t < num_targets; t++)
	{
		MKINT(targets[t].agg_kind);
		MKINT(targets[t].col_type);
		MKINT(targets[t].col_attnum);
		MKINT(targets[t].result_attnum);
		MKINT((int) targets[t].avg_input_as_float8);
		MKINT((int) targets[t].result_typeoid);
		MKINT((int) targets[t].use_int8_avg_path);
		MKINT((int) targets[t].avg_transfn_oid);
		/* CASE WHEN filter: 3 ints + 1 Const node */
		MKINT((int) targets[t].has_case_filter);
		MKINT(targets[t].filter_col_attnum);
		MKINT(targets[t].filter_col_type);
		if (targets[t].has_case_filter && targets[t].filter_const_plan != NULL)
			priv = lappend(priv, copyObject(targets[t].filter_const_plan));
		else
			priv = lappend(priv, makeNullConst(INT4OID, -1, InvalidOid));

		/* VECGAGG_SUM_EXPR: inline expression tree (2 ints + 7 ints per node) */
		MKINT(targets[t].expr_num_nodes);
		MKINT(targets[t].expr_root_idx);
		for (int en = 0; en < targets[t].expr_num_nodes; en++)
		{
			const VecExprNode *n = &targets[t].expr_nodes[en];
			MKINT((int) n->is_var);
			MKINT(n->slot_idx);
			MKINT(n->col_type);
			MKINT(n->left);
			MKINT(n->right);
			MKINT((int) n->opfuncid);
			MKINT((int) n->rettype);
		}
	}

#undef MKINT

	cscan->custom_private = priv;

	return cscan;
}

void
engine_register_groupagg_node(void)
{
	if (GetCustomScanMethods(VecGroupAggScanMethods.CustomName, true) == NULL)
		RegisterCustomScanMethods(&VecGroupAggScanMethods);
}

/*
 * Return true if the given plan node is a VecGroupAgg node.
 */
bool
engine_is_groupagg_node(Plan *plan)
{
	CustomScan *cs;

	if (plan == NULL || plan->type != T_CustomScan)
		return false;

	cs = (CustomScan *) plan;
	return cs->methods == &VecGroupAggScanMethods;
}

/*
 * Enable sorted output for an already-built VecGroupAgg plan node.
 * (Used when an outer Sort node is absorbed by PlanTreeMutator.)
 * sort_output is serialized at index [5] in custom_private.
 */
void
engine_groupagg_enable_sort_output(CustomScan *cscan)
{
	int		num_keys;
	int		sort_output_idx;
	Const  *c;

	/* num_keys is at index 0 */
	num_keys = (int) DatumGetInt32(
				((Const *) list_nth(cscan->custom_private, 0))->constvalue);
	/* sort_output is at index 4*num_keys+2 */
	sort_output_idx = 4 * num_keys + 2;
	c = (Const *) list_nth(cscan->custom_private, sort_output_idx);
	c->constvalue = Int32GetDatum(1);
}
