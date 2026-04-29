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
 *        - Key type: int4, int8, float8, or text (low-cardinality)
 *        - Aggregates: count(*), sum, min, max over int4/int8/float8
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
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

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

/* ----------------------------------------------------------------
 *  HTAB key structure for group lookup
 * ---------------------------------------------------------------- */
typedef struct VecGroupKey
{
	Datum	key;
	bool	isnull;
} VecGroupKey;

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
		case FLOAT8OID:	return VECGAGG_TYPE_FLOAT8;
		default:		return -1;
	}
}

/*
 * Initialize per-group HTAB.
 */
static HTAB *
create_group_htab(MemoryContext ctx)
{
	HASHCTL		ctl;
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize		= sizeof(VecGroupKey);
	ctl.entrysize	= sizeof(VecGroupEntry);
	ctl.hcxt		= ctx;
	return hash_create("VecGroupAgg groups",
					   256,
					   &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Lookup or create a group entry for the given key.
 */
static VecGroupEntry *
lookup_or_create_group(VecGroupAggState *state, Datum key, bool isnull)
{
	VecGroupKey		hkey;
	VecGroupEntry  *entry;
	bool			found;

	MemSet(&hkey, 0, sizeof(hkey));	/* zero padding for HASH_BLOBS memcmp */
	hkey.key	= key;
	hkey.isnull	= isnull;

	entry = (VecGroupEntry *) hash_search(state->group_htab,
										  &hkey,
										  HASH_ENTER,
										  &found);
	if (!found)
	{
		int i;

		if (state->num_groups >= VECGROUPAGG_MAX_GROUPS)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("VectorGroupAgg: too many distinct groups (limit %d)",
							VECGROUPAGG_MAX_GROUPS)));

		/* Initialize new entry */
		entry->key		 = isnull ? (Datum) 0 : datumCopy(key, true, -1);
		entry->key_isnull = isnull;

		for (i = 0; i < state->num_targets; i++)
		{
			entry->int64_acc[i]		= 0;
			entry->float8_acc[i]	= 0.0;
			entry->acc_isnull[i]	= true;	/* NULL until first non-null input */
		}

		state->num_groups++;
	}

	return entry;
}

/*
 * Accumulate one value into a group entry for aggregate target t.
 */
static void
accumulate_value(VecGroupEntry *entry, int t_idx, VecGroupAggTarget *tgt,
				 Datum val, bool isnull)
{
	if (tgt->agg_kind == VECGAGG_COUNT_STAR)
	{
		/* count(*) counts all rows regardless of nullness */
		entry->int64_acc[t_idx]++;
		entry->acc_isnull[t_idx] = false;
		return;
	}

	if (isnull)
		return;		/* other aggregates skip NULLs */

	entry->acc_isnull[t_idx] = false;

	switch (tgt->agg_kind)
	{
		case VECGAGG_SUM:
			switch (tgt->col_type)
			{
				case VECGAGG_TYPE_INT4:
					entry->int64_acc[t_idx] += (int64) DatumGetInt32(val);
					break;
				case VECGAGG_TYPE_INT8:
					entry->int64_acc[t_idx] += DatumGetInt64(val);
					break;
				case VECGAGG_TYPE_FLOAT8:
					entry->float8_acc[t_idx] += DatumGetFloat8(val);
					break;
			}
			break;

		case VECGAGG_MIN:
			switch (tgt->col_type)
			{
				case VECGAGG_TYPE_INT4:
				{
					int64 v = (int64) DatumGetInt32(val);
					if (entry->acc_isnull[t_idx] || v < entry->int64_acc[t_idx])
						entry->int64_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_INT8:
				{
					int64 v = DatumGetInt64(val);
					if (entry->acc_isnull[t_idx] || v < entry->int64_acc[t_idx])
						entry->int64_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_FLOAT8:
				{
					float8 v = DatumGetFloat8(val);
					if (entry->acc_isnull[t_idx] || v < entry->float8_acc[t_idx])
						entry->float8_acc[t_idx] = v;
					break;
				}
			}
			break;

		case VECGAGG_MAX:
			switch (tgt->col_type)
			{
				case VECGAGG_TYPE_INT4:
				{
					int64 v = (int64) DatumGetInt32(val);
					if (entry->acc_isnull[t_idx] || v > entry->int64_acc[t_idx])
						entry->int64_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_INT8:
				{
					int64 v = DatumGetInt64(val);
					if (entry->acc_isnull[t_idx] || v > entry->int64_acc[t_idx])
						entry->int64_acc[t_idx] = v;
					break;
				}
				case VECGAGG_TYPE_FLOAT8:
				{
					float8 v = DatumGetFloat8(val);
					if (entry->acc_isnull[t_idx] || v > entry->float8_acc[t_idx])
						entry->float8_acc[t_idx] = v;
					break;
				}
			}
			break;
	}
}

/*
 * Comparison context for qsort-based sorted emission.
 * We sort VecGroupEntry pointers by key value (NULLs last).
 */
static int	g_key_col_type;	/* set before qsort call */

static int
vecgroup_entry_cmp(const void *a, const void *b)
{
	const VecGroupEntry *ea = *(const VecGroupEntry **) a;
	const VecGroupEntry *eb = *(const VecGroupEntry **) b;

	/* NULLs sort last */
	if (ea->key_isnull && eb->key_isnull)
		return 0;
	if (ea->key_isnull)
		return 1;
	if (eb->key_isnull)
		return -1;

	switch (g_key_col_type)
	{
		case VECGAGG_TYPE_INT4:
		{
			int64 va = DatumGetInt32(ea->key);
			int64 vb = DatumGetInt32(eb->key);
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case VECGAGG_TYPE_INT8:
		{
			int64 va = DatumGetInt64(ea->key);
			int64 vb = DatumGetInt64(eb->key);
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		case VECGAGG_TYPE_FLOAT8:
		{
			float8 va = DatumGetFloat8(ea->key);
			float8 vb = DatumGetFloat8(eb->key);
			return (va < vb) ? -1 : (va > vb) ? 1 : 0;
		}
		default:
			return 0;
	}
}

/*
 * Process one VectorTupleTableSlot batch: update per-group accumulators.
 * The slot has one VectorColumn per projected attribute.
 *
 * Attribute layout in the slot matches the ColcompressScan projection:
 *   col_attnum-1 → group key VectorColumn
 *   targets[i].col_attnum-1 → aggregate value VectorColumn
 */
static void
process_vector_batch(VecGroupAggState *state, TupleTableSlot *slot)
{
	VectorTupleTableSlot *vslot = (VectorTupleTableSlot *) slot;
	uint32	dim = vslot->dimension;
	uint32	i;

	/* Key column: 0-based index in slot->tts_values */
	int		key_idx = state->key_attnum - 1;
	VectorColumn *key_col;

	key_col = (VectorColumn *) slot->tts_values[key_idx];

	for (i = 0; i < dim; i++)
	{
		/*
		 * VectorColumn.value is a compact byte array where element i is at
		 * byte offset (i * columnTypeLen), NOT at Datum pointer offset i*8.
		 * Use fetch_att() to read correctly, matching ExtractTupleFromVectorSlot.
		 */
		Datum		key_val;
		bool		key_null;
		VecGroupEntry *entry;
		int			t;

		if (key_col)
		{
			int8 *rawPtr = (int8 *) key_col->value + (int) key_col->columnTypeLen * i;
			key_val  = fetch_att(rawPtr, key_col->columnIsVal, key_col->columnTypeLen);
			key_null = key_col->isnull[i];
		}
		else
		{
			key_val  = (Datum) 0;
			key_null = true;
		}

		entry = lookup_or_create_group(state, key_val, key_null);

		for (t = 0; t < state->num_targets; t++)
		{
			VecGroupAggTarget *tgt = &state->targets[t];
			Datum	val = (Datum) 0;
			bool	val_null = true;

			if (tgt->agg_kind != VECGAGG_COUNT_STAR && tgt->col_attnum > 0)
			{
				int val_idx = tgt->col_attnum - 1;
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

			accumulate_value(entry, t, tgt, val, val_null);
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

	/* Slot att key_result_attnum: group key */
	slot->tts_values[state->key_result_attnum] = entry->key;
	slot->tts_isnull[state->key_result_attnum] = entry->key_isnull;

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
				slot->tts_values[ra] = Int64GetDatum(entry->int64_acc[t]);
				break;
			case VECGAGG_SUM:
			case VECGAGG_MIN:
			case VECGAGG_MAX:
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
					case VECGAGG_TYPE_FLOAT8:
						slot->tts_values[ra] = Float8GetDatum(entry->float8_acc[t]);
						break;
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
	ListCell		 *lc;


	/*
	 * Unpack parameters from custom_private:
	 *   [0] key_attnum   (Int)
	 *   [1] key_typeoid  (Int)
	 *   [2] num_targets  (Int)
	 *   [3] key_result_att (Int) — 0-based position of GROUP BY key in output
	 *   [4] sort_output   (Int) — 1 if output must be sorted by key (AGG_SORTED)
	 *   [5..] encoded targets: 5 Ints each (kind, col_type, col_attnum, result_attnum, result_typeoid)
	 */
	List   *priv = cscan->custom_private;
	int		idx = 0;

	foreach(lc, priv)
	{
		Const *c = (Const *) lfirst(lc);
		int	   v = (int) DatumGetInt32(c->constvalue);

		if (idx == 0)		state->key_attnum  = v;
		else if (idx == 1)	state->key_typeoid = (Oid) v;
		else if (idx == 2)	state->num_targets = v;
		else if (idx == 3)	state->key_result_attnum = v;
		else if (idx == 4)	state->sort_output = (bool) v;
		else
		{
			int tbase = (idx - 5);
			int tno   = tbase / 5;
			int toff  = tbase % 5;

			if (tno < VECGROUPAGG_MAX_TARGETS)
			{
				switch (toff)
				{
					case 0: state->targets[tno].agg_kind       = v; break;
					case 1: state->targets[tno].col_type       = v; break;
					case 2: state->targets[tno].col_attnum     = v; break;
					case 3: state->targets[tno].result_attnum  = v; break;
					case 4: state->targets[tno].result_typeoid = (Oid) v; break;
				}
			}
		}
		idx++;
	}

	state->key_col_type = type_oid_to_vectype(state->key_typeoid);

	/* Create memory context for per-group data */
	state->agg_context = AllocSetContextCreate(CurrentMemoryContext,
											   "VecGroupAgg",
											   ALLOCSET_DEFAULT_SIZES);

	/* Initialize group hash table */
	state->group_htab = create_group_htab(state->agg_context);
	state->num_groups = 0;
	state->scan_done  = false;
	state->seq_started = false;
	state->sorted_arr  = NULL;
	state->sorted_idx  = 0;

	/*
	 * Initialize the child ColcompressScan.
	 * PG does not auto-initialize custom_plans in ExecInitCustomScan,
	 * so we must call ExecInitNode ourselves.
	 */
	{
		Plan *child_plan = (Plan *) linitial(cscan->custom_plans);
		PlanState *child_ps = ExecInitNode(child_plan, estate, eflags);
		state->css.custom_ps = lappend(state->css.custom_ps, child_ps);
	}
}

static TupleTableSlot *
ExecVecGroupAgg(CustomScanState *css)
{
	VecGroupAggState *state = (VecGroupAggState *) css;
	PlanState		 *child_ps = (PlanState *) linitial(state->css.custom_ps);

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
			g_key_col_type = state->key_col_type;
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

	if (state->css.custom_ps != NIL)
		ExecEndNode((PlanState *) linitial(state->css.custom_ps));

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

	state->group_htab = create_group_htab(state->agg_context);
	state->num_groups = 0;
	state->scan_done  = false;
	state->sorted_arr = NULL;
	state->sorted_idx = 0;

	ExecReScan((PlanState *) linitial(state->css.custom_ps));
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
 * Build a CustomScan plan node for VectorGroupAgg.
 * The caller must have already built the child ColcompressScan plan
 * and added it to custom_plans.
 *
 * Parameters are packed into custom_private as a flat list of Int Consts:
 *   key_attnum, key_typeoid, num_targets,
 *   [kind, col_type, col_attnum, result_attnum, result_typeoid] × num_targets
 */
CustomScan *
engine_create_groupagg_node(int key_attnum,
							Oid key_typeoid,
							int key_result_att,
							bool sort_output,
							int num_targets,
							VecGroupAggTarget *targets)
{
	CustomScan *cscan = makeNode(CustomScan);
	List	   *priv = NIL;
	int			t;

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

	MKINT(key_attnum);
	MKINT((int) key_typeoid);
	MKINT(num_targets);
	MKINT(key_result_att);
	MKINT((int) sort_output);

	for (t = 0; t < num_targets; t++)
	{
		MKINT(targets[t].agg_kind);
		MKINT(targets[t].col_type);
		MKINT(targets[t].col_attnum);
		MKINT(targets[t].result_attnum);
		MKINT((int) targets[t].result_typeoid);
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
 * sort_output is serialized at index [4] in custom_private.
 */
void
engine_groupagg_enable_sort_output(CustomScan *cscan)
{
	Const *c = (Const *) list_nth(cscan->custom_private, 4);

	c->constvalue = Int32GetDatum(1);
}
