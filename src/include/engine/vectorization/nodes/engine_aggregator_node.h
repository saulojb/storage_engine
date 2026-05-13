/*-------------------------------------------------------------------------
 *
 * engine_aggregator_node.h
 *	Custom scan method for aggregation
 * 
 * IDENTIFICATION
 *	src/backend/columnar/vectorization/nodes/engine_aggregator_node.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef COLUMNAR_AGGEREGATOR_NODE_H
#define COLUMNAR_AGGEREGATOR_NODE_H

#include "postgres.h"

#include "nodes/execnodes.h"
#include "utils/numeric.h"
#include "engine/vectorization/nodes/engine_groupagg_node.h"

/* ----------------------------------------------------------------
 * Multi-target VECGAGG_SUM_EXPR constants
 * ---------------------------------------------------------------- */

/*
 * Magic value at custom_private[0] to select multi-target mode.
 * Distinguishes from single-target format (where [0] = num_nodes >= 2).
 */
#define VECGAGG_MULTI_MAGIC         (-1)

/* Per-target kind codes */
#define VMSEXPR_COUNT_STAR          1   /* count(*) */
#define VMSEXPR_SUM_EXPR            2   /* sum(arithmetic expression) */
#define VMSEXPR_SUM_COL             3   /* sum(plain_var) — direct column read */

/* Maximum aggregate targets in multi-target mode */
#define VECGAGG_MULTI_MAX_TARGETS   8

/*
 * Per-target descriptor for multi-target ExecVecMultiSumExpr.
 * Stores the kind (count / sum_expr / sum_col), the inline expression tree for
 * SUM_EXPR targets, and per-target accumulators.
 */
typedef struct VecMultiExprTarget
{
	int			kind;				/* VMSEXPR_COUNT_STAR / VMSEXPR_SUM_EXPR / VMSEXPR_SUM_COL */
	int			col_type;			/* VECGAGG_TYPE_* for SUM targets */
	int			aggsplit;			/* AggSplit for this target */

	/* Inline expression tree (SUM_EXPR only) */
	VecExprNode	expr_nodes[VECGAGG_EXPR_MAX_NODES];
	int			expr_num_nodes;
	int			expr_root_idx;

	/* SUM_COL only: direct VectorColumn slot index in the batch */
	int			sum_col_slot_idx;

	/* Accumulators */
	float8		sum_float8;
	int64		sum_int64;		/* for INT4/INT8/money SUM_COL */
	Numeric		sum_numeric;
	int64		count;				/* COUNT_STAR accumulator */
	bool		has_value;

	/* Parallel NUMERIC partial accumulation (AGGSPLIT_INITIAL_SERIAL) */
	Datum		numeric_partial_state;
	bool		numeric_partial_null;
	FmgrInfo	numeric_avg_accum_fn;
	FmgrInfo	numeric_serialize_fn;
} VecMultiExprTarget;

/* ----------------------------------------------------------------
 * Post-aggregate scalar expression evaluator
 *
 * After all batches are scanned and accumulator slots are computed,
 * each output column may be an arithmetic combination of slot results
 * (e.g. sum(a) + count(*)).  VecMultiOutput encodes this expression
 * as a small flat tree over VPAE_SLOT, VPAE_OP, and VPAE_CONST nodes.
 * ---------------------------------------------------------------- */
#define VPAE_SLOT    0  /* reference to an accumulator slot result */
#define VPAE_OP      1  /* binary or unary operator */
#define VPAE_CONST   2  /* literal constant */
#define VPAE_MAX_NODES  8

typedef struct VecPostAggNode
{
	int8	kind;			/* VPAE_SLOT / VPAE_OP / VPAE_CONST */
	int8	left;			/* OP: left child index in same array */
	int8	right;			/* OP: right child index; -1 = unary / leaf */
	int8	slot_idx;		/* SLOT: which vecMultiTargets[] slot */
	Oid		opfuncid;		/* OP: operator/function OID */
	Oid		rettype;		/* result type OID of this node */
	FmgrInfo opfmgr;		/* loaded at Begin time, not serialized */
	bool	retbyval;
	int16	rettyplen;
	Datum	const_val;		/* CONST: constant value (loaded at Begin) */
	bool	const_isnull;
	Const  *const_ptr;		/* CONST: original Const* for serialization (planner only) */
} VecPostAggNode;

typedef struct VecMultiOutput
{
	VecPostAggNode	nodes[VPAE_MAX_NODES];
	int				num_nodes;
	int				root_idx;	/* -1 → NULL result */
} VecMultiOutput;

typedef struct VectorAggState
{
	CustomScanState css;
	AggState	   *aggstate;

	/* VECGAGG_SUM_EXPR: arithmetic expression in SUM (e.g. sum(a+b)) */
	bool			vecExprActive;
	VecExprNode		vecExprNodes[VECGAGG_EXPR_MAX_NODES];
	int				vecExprNumNodes;
	int				vecExprRootIdx;
	int				vecExprColType;		/* VECGAGG_TYPE_* of expression result */
	int				vecExprAggsplit;	/* AggSplit value (AGGSPLIT_SIMPLE or INITIAL_SERIAL) */

	/* Accumulators for ExecVecSumExpr */
	float8			sumFloat8;
	Numeric			sumNumeric;
	bool			sumHasValue;
	bool			done;

	/*
	 * Parallel NUMERIC partial aggregate: accumulate via numeric_avg_accum
	 * then emit bytea via numeric_serialize.
	 * numericPartialState is the internal AggState opaque pointer.
	 */
	Datum			numericPartialState;	/* internal state from numeric_avg_accum */
	bool			numericPartialStateNull;
	FmgrInfo		numericAvgAccumFn;		/* numeric_avg_accum(internal, numeric) */
	FmgrInfo		numericSerializeFn;		/* numeric_serialize(internal)          */

	/*
	 * Fake AggState/ExprContext so that numeric_avg_accum passes
	 * AggCheckCallContext (it inspects fcinfo->context->type == T_AggState).
	 */
	AggState		numericFakeAggState;
	ExprContext		numericFakeAggExpr;

	/* Memory context for accumulation (numeric datums) */
	MemoryContext	aggContext;

	/* ----------------------------------------------------------------
	 * Multi-target VECGAGG_SUM_EXPR (N simultaneous sum(expr)/count(*))
	 * Activated when custom_private starts with VECGAGG_MULTI_MAGIC.
	 * ---------------------------------------------------------------- */
	bool					vecMultiActive;
	int						vecMultiNumTargets;		/* # accumulator slots */
	int						vecMultiAggsplit;
	VecMultiExprTarget		vecMultiTargets[VECGAGG_MULTI_MAX_TARGETS];

	/*
	 * Post-aggregate output expressions.
	 * vecMultiNumOutputs = # of non-resjunk TL entries (output columns).
	 * When vecMultiHasPostAgg is false, output[i] = slot[i] directly.
	 */
	bool					vecMultiHasPostAgg;
	int						vecMultiNumOutputs;
	VecMultiOutput			vecMultiOutputs[VECGAGG_MULTI_MAX_TARGETS];
} VectorAggState;

extern CustomScan *engine_create_aggregator_node(void);
extern void engine_register_aggregator_node(void);

#endif
