/*-------------------------------------------------------------------------
 *
 * engine_planner_hook.c
 *
 * Copyright (c) Hydra, Inc.
 *
 * Modify top plan and change aggregate function to provided ones that can execute on 
 * column vector.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/amapi.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "parser/parser.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/spccache.h"

#include "engine/engine.h"
#include "engine/engine_customscan.h"
#include "engine/engine_indexscan.h"
#include "engine/vectorization/engine_vector_execution.h"
#include "engine/vectorization/nodes/engine_aggregator_node.h"
#include "engine/vectorization/nodes/engine_groupagg_node.h"

#include "engine/utils/listutils.h"

static planner_hook_type PreviousPlannerHook = NULL;

static PlannedStmt * ColumnarPlannerHook(Query *parse,
#if PG_VERSION_NUM >= PG_VERSION_13
                                         const char *query_string,
#endif
									 int cursorOptions, ParamListInfo boundParams
#if PG_VERSION_NUM >= PG_VERSION_19
									 , ExplainState *es
#endif
									 );
static bool IsCreateTableAs(const char *query);

#if PG_VERSION_NUM >= PG_VERSION_14
static Oid engine_tableam_oid = InvalidOid;
static bool QueryStringHasPlainExplain(const char *query);
static bool QueryHasVectorizableAggregate(Query *parse);
static bool QueryHasSingleLowCardinalityColumnarGroupBy(Query *parse);
static Oid GetRelationTableAmOid(Oid relid);
static bool PlanHasColumnarCustomScan(Plan *plan);
static bool PlanHasPathologicalSortedColumnarAgg(Plan *plan);
static bool PlanHasHashColumnarAgg(Plan *plan);
static void MutatePlannedStmt(PlannedStmt *stmt);
static bool TryVectorizeSerialPlan(PlannedStmt *stmt_serial,
								   MemoryContext saved_context,
								   Plan **savedPlanTree,
								   List **savedSubplan);
static void CollectProjectedAttnos(Plan *scan_plan, AttrNumber *attnos, int *count);

typedef struct PlanTreeMutatorContext
{
	bool vectorizedAggregation;
	bool vectorizedAggStarOnly;
	List *rtable;
} PlanTreeMutatorContext;

static double EstimateGroupByDistinct(PlanTreeMutatorContext *ctx, Plan *scan_plan,
								   AttrNumber key_table_attno, double fallback);

typedef struct
{
	bool foundAggref;
	bool allAggrefsAreStar;
} AggrefStarContext;

typedef struct
{
	bool foundNumericAgg;
	bool foundMoneyAgg;
} MixedAggTypeContext;

static bool
AggrefStarWalker(Node *node, void *context)
{
	AggrefStarContext *ctx = (AggrefStarContext *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;

		ctx->foundAggref = true;
		if (!aggref->aggstar)
			ctx->allAggrefsAreStar = false;
	}

	return expression_tree_walker(node, AggrefStarWalker, context);
}

static bool
PlanAllAggrefsAreStar(Plan *plan)
{
	AggrefStarContext ctx;

	ctx.foundAggref = false;
	ctx.allAggrefsAreStar = true;
	(void) AggrefStarWalker((Node *) plan->targetlist, &ctx);
	(void) AggrefStarWalker((Node *) plan->qual, &ctx);

	return ctx.foundAggref && ctx.allAggrefsAreStar;
}

static bool
MixedAggTypeWalker(Node *node, void *context)
{
	MixedAggTypeContext *ctx = (MixedAggTypeContext *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;

		if (!aggref->aggstar && list_length(aggref->args) == 1)
		{
			TargetEntry *arg_te = (TargetEntry *) linitial(aggref->args);

			if (arg_te != NULL && IsA(arg_te->expr, Var))
			{
				Var *arg_var = (Var *) arg_te->expr;

				if (arg_var->vartype == NUMERICOID)
					ctx->foundNumericAgg = true;
				else if (arg_var->vartype == CASHOID)
					ctx->foundMoneyAgg = true;
			}
		}

		if (ctx->foundNumericAgg && ctx->foundMoneyAgg)
			return true;
	}

	return expression_tree_walker(node, MixedAggTypeWalker, context);
}

static bool
PlanHasMixedNumericMoneyAggrefs(Plan *plan)
{
	MixedAggTypeContext ctx;

	ctx.foundNumericAgg = false;
	ctx.foundMoneyAgg = false;
	(void) MixedAggTypeWalker((Node *) plan->targetlist, &ctx);
	(void) MixedAggTypeWalker((Node *) plan->qual, &ctx);

	return ctx.foundNumericAgg && ctx.foundMoneyAgg;
}

/*
 * FindFirstAggref_walker — finds the first Aggref node at any depth in an
 * expression tree.  Used to detect whether a plain-aggregate targetlist entry
 * has an aggregate with an arithmetic expression input (sum(price + 1)),
 * even when the Aggref is buried inside a multi-arg outer function
 * (e.g. round(sum(price+1)::numeric, 4)).
 */
static bool
FindFirstAggref_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		*((Aggref **) context) = (Aggref *) node;
		return true;	/* stop at first found */
	}
	return expression_tree_walker(node, FindFirstAggref_walker, context);
}

#if PG_VERSION_NUM < PG_VERSION_16
static bool
PlanHasNumericAggrefs(Plan *plan)
{
	MixedAggTypeContext ctx;

	ctx.foundNumericAgg = false;
	ctx.foundMoneyAgg = false;
	(void) MixedAggTypeWalker((Node *) plan->targetlist, &ctx);
	(void) MixedAggTypeWalker((Node *) plan->qual, &ctx);

	return ctx.foundNumericAgg;
}
#endif

/*
 * Estimate GROUP BY cardinality for a single table key.
 *
 * Priority:
 *   1) Planner-provided fallback (Agg.numGroups / plan_rows)
 *   2) pg_statistic stadistinct for the key column (if available)
 *
 * Result is bounded by scan input rows when available.
 */
static double
EstimateGroupByDistinct(PlanTreeMutatorContext *ctx, Plan *scan_plan,
								 AttrNumber key_table_attno, double fallback)
{
	CustomScan *cscan;
	Index		scanrelid;
	RangeTblEntry *rte;
	HeapTuple statTuple;
	double		estimate = fallback;

	if (ctx == NULL || scan_plan == NULL || key_table_attno <= 0)
		return fallback;

	if (!IsA(scan_plan, CustomScan))
		return fallback;

	cscan = (CustomScan *) scan_plan;
	scanrelid = cscan->scan.scanrelid;
	if (scanrelid <= 0 || ctx->rtable == NIL)
		return fallback;

	rte = rt_fetch(scanrelid, ctx->rtable);
	if (rte == NULL || rte->rtekind != RTE_RELATION)
		return fallback;

	statTuple = SearchSysCache3(STATRELATTINH,
							   ObjectIdGetDatum(rte->relid),
							   Int16GetDatum(key_table_attno),
							   BoolGetDatum(false));
	if (!HeapTupleIsValid(statTuple))
		return fallback;

	{
		Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(statTuple);

		if (stats->stadistinct > 0.0)
			estimate = stats->stadistinct;
		else if (stats->stadistinct < 0.0)
		{
			double reltuples = -1.0;
			HeapTuple classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rte->relid));

			if (HeapTupleIsValid(classTuple))
			{
				Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classTuple);
				reltuples = classForm->reltuples;
				ReleaseSysCache(classTuple);
			}

			if (reltuples > 0.0)
				estimate = (-stats->stadistinct) * reltuples;
		}
	}

	ReleaseSysCache(statTuple);

	if (estimate <= 0.0)
		return fallback;

	if (scan_plan->plan_rows > 0.0 && estimate > scan_plan->plan_rows)
		estimate = scan_plan->plan_rows;

	return estimate;
}

#define FLATCOPY(newnode, node, nodetype)  \
	( (newnode) = (nodetype *) palloc(sizeof(nodetype)), \
	  memcpy((newnode), (node), sizeof(nodetype)) )


static bool
engine_index_table(Oid indexOid, Oid columnarTableAmOid)
{
	HeapTuple ht_idx;
	Form_pg_index idxrec;
	HeapTuple ht_table;
	Form_pg_class tablerec;
	bool index_on_columnar = false;

	/*
	 * Fetch the pg_index tuple by the Oid of the index
	 */
	ht_idx = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
	idxrec = (Form_pg_index) GETSTRUCT(ht_idx);

	ht_table = SearchSysCache1(RELOID, ObjectIdGetDatum(idxrec->indrelid));
	tablerec = (Form_pg_class) GETSTRUCT(ht_table);

	index_on_columnar = tablerec->relam == columnarTableAmOid;

	ReleaseSysCache(ht_idx);
	ReleaseSysCache(ht_table);

	return index_on_columnar;
}

static Node *
AggRefArgsExpressionMutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	Node *previousNode = (Node *) context;

	if (IsA(node, OpExpr) || IsA(node, DistinctExpr) || IsA(node, NullIfExpr) )
	{
		OpExpr *opExprNode = (OpExpr *) node;

		Form_pg_operator operatorForm;
		HeapTuple operatorTuple;

		if (list_length(opExprNode->args) != 2)
			elog(ERROR, "Aggregation vectorizaion works only on two arguments.");

		if (CheckOpExprArgumentRules(opExprNode->args))
			elog(ERROR, "Unsupported aggregate argument combination.");

		operatorTuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opExprNode->opno));
		operatorForm = (Form_pg_operator) GETSTRUCT(operatorTuple);
		Oid procedureOid = operatorForm->oprcode;
		ReleaseSysCache(operatorTuple);

		Oid vectorizedProcedureOid;
		if (!GetVectorizedProcedureOid(procedureOid, &vectorizedProcedureOid))
			elog(ERROR, "Vectorized aggregate not found.");

		opExprNode->opfuncid = vectorizedProcedureOid;

		return (Node *) opExprNode;
	}

	/* This should handle aggregates that have non var(column) as argument*/
	if (previousNode != NULL && IsA(previousNode, TargetEntry) && !IsA(node, Var))
		elog(ERROR, "Vectorized Aggregates accept only valid column argument");

	return expression_tree_mutator(node, AggRefArgsExpressionMutator, (void *) node);
}

static Node *
ExpressionMutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		Aggref *oldAggRefNode = (Aggref *) node;
		Aggref *newAggRefNode = copyObject(oldAggRefNode);

		if (oldAggRefNode->aggdistinct)
		{
			elog(ERROR, "Vectorized aggregate with DISTINCT not supported.");
		}

		if (oldAggRefNode->aggfilter)
		{
			elog(ERROR, "Vectorized aggregate with FILTER not supported");
		}

		newAggRefNode->args = (List *)
			expression_tree_mutator((Node *) oldAggRefNode->args, AggRefArgsExpressionMutator, NULL);
		
		Oid vectorizedProcedureOid = 0;
		if (!GetVectorizedProcedureOid(newAggRefNode->aggfnoid, &vectorizedProcedureOid))
		{
			elog(ERROR, "Vectorized aggregate not found.");
		}

		return (Node *) newAggRefNode;
	}

	return expression_tree_mutator(node, ExpressionMutator, (void *) context);
}

/*
 * Map a scan-output position (1-based resno) to the actual table varattno.
 * Returns 0 if the TargetEntry is not found or not a plain Var.
 */
static AttrNumber
ScanOutputPosToVarAttno(Plan *child_plan, AttrNumber scan_pos)
{
	TargetEntry *te = get_tle_by_resno(child_plan->targetlist, scan_pos);
	if (te == NULL)
		return 0;
	if (!IsA(te->expr, Var))
		return 0;
	return ((Var *) te->expr)->varattno;
}

/*
 * Map a table varattno to its 0-based slot output position.
 *
 * ColumnarReadNextVector writes column data into tts_values[output_idx]
 * where output_idx is the rank of varattno in the sorted (ascending) list of
 * projected table attnos.  This helper computes that rank by collecting all
 * unique varattno values from scan_plan->targetlist, sorting them, and
 * returning the position of table_attno.
 *
 * Returns -1 if table_attno is not projected.
 */
static int
VarAttnoToSlotIdx(Plan *scan_plan, AttrNumber table_attno)
{
	int			n = 0;
	int			i;
	AttrNumber	attnos[MaxTupleAttributeNumber];

	CollectProjectedAttnos(scan_plan, attnos, &n);

	/* Sort ascending (insertion sort; n is tiny) */
	for (i = 1; i < n; i++)
	{
		AttrNumber	key = attnos[i];
		int			j = i - 1;

		while (j >= 0 && attnos[j] > key)
		{
			attnos[j + 1] = attnos[j];
			j--;
		}
		attnos[j + 1] = key;
	}

	/* Return rank of table_attno */
	for (i = 0; i < n; i++)
		if (attnos[i] == table_attno)
			return i;

	return -1;
}

/*
 * Context for VecAggVarAttnoMutator.
 */
typedef struct
{
	Plan *scan_plan;
} VecAggVarCtx;

typedef struct
{
	AttrNumber *attnos;
	int		   *count;
} AttnoCollectorCtx;

static bool
CollectVarAttnosWalker(Node *node, void *context)
{
	AttnoCollectorCtx *ctx = (AttnoCollectorCtx *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		int j;

		if (var->varattno <= 0)
			return false;

		for (j = 0; j < *ctx->count; j++)
			if (ctx->attnos[j] == var->varattno)
				return false;

		ctx->attnos[(*ctx->count)++] = var->varattno;
		return false;
	}

	return expression_tree_walker(node, CollectVarAttnosWalker, context);
}

static void
CollectProjectedAttnos(Plan *scan_plan, AttrNumber *attnos, int *count)
{
	ListCell *lc;
	AttnoCollectorCtx ctx;

	ctx.attnos = attnos;
	ctx.count = count;

	foreach(lc, scan_plan->targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);

		if (te->resjunk || !IsA(te->expr, Var))
			continue;

		(void) CollectVarAttnosWalker((Node *) te->expr, &ctx);
	}

	(void) CollectVarAttnosWalker((Node *) scan_plan->qual, &ctx);

	if (IsA(scan_plan, CustomScan))
	{
		CustomScan *customScan = (CustomScan *) scan_plan;

		(void) CollectVarAttnosWalker((Node *) customScan->custom_exprs, &ctx);
		(void) CollectVarAttnosWalker((Node *) customScan->custom_private, &ctx);
	}
}

/*
 * expression_tree_mutator callback: rewrite Var.varattno from table attno to
 * VectorTupleTableSlot output position (output_pos + 1).
 *
 * ColumnarReadNextVector fills tts_values[output_pos] where output_pos is the
 * rank of varattno in the sorted projected-attno list (NOT varattno-1).
 * The standard EEOP_OUTERVAR evaluator reads tts_values[varattno-1], so we
 * must remap varattno -> output_pos+1 at plan time so both agree.
 */
static Node *
VecAggVarAttnoMutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		if (var->varno == OUTER_VAR && var->varattno > 0)
		{
			VecAggVarCtx *ctx = (VecAggVarCtx *) context;
			int slot_idx = VarAttnoToSlotIdx(ctx->scan_plan, var->varattno);

			if (slot_idx >= 0)
			{
				Var *newvar = copyObject(var);
				newvar->varattno = (AttrNumber) (slot_idx + 1);
				return (Node *) newvar;
			}
		}

		return (Node *) var;
	}

	return expression_tree_mutator(node, VecAggVarAttnoMutator, context);
}

/*
 * Map a Postgres type OID to VECGAGG_TYPE_* code.
 * Returns -1 if unsupported.
 */
static int
TypeOidToVecGaggType(Oid typeoid)
{
	switch (typeoid)
	{
		case INT4OID:	return VECGAGG_TYPE_INT4;
		case INT8OID:	return VECGAGG_TYPE_INT8;
		case FLOAT4OID:	return VECGAGG_TYPE_FLOAT4;
		case FLOAT8OID:	return VECGAGG_TYPE_FLOAT8;
		case NUMERICOID:	return VECGAGG_TYPE_NUMERIC;
		case CASHOID:	return VECGAGG_TYPE_INT8; /* money = int64 internally */
		default:		return -1;
	}
}

/*
 * Map a GROUP BY key type OID to VECGAGG_TYPE_* code.
 * Key support is broader than aggregate-input support because Q3 groups by
 * bpchar/text while aggregating numeric values.
 */
static int
KeyTypeOidToVecGaggType(Oid typeoid)
{
	switch (typeoid)
	{
		case INT4OID:	return VECGAGG_TYPE_INT4;
		case INT8OID:	return VECGAGG_TYPE_INT8;
		case FLOAT4OID:	return VECGAGG_TYPE_FLOAT4;
		case FLOAT8OID:	return VECGAGG_TYPE_FLOAT8;
		case CASHOID:	return VECGAGG_TYPE_INT8; /* money = int64 internally */
		case BOOLOID:	return VECGAGG_TYPE_INT4; /* bool: Datum 0/1, same as int4 */
		case BPCHAROID:	return VECGAGG_TYPE_BPCHAR;
		case TEXTOID:	return VECGAGG_TYPE_TEXT;
		default:		return -1;
	}
}

/*
 * Detect: SUM(CASE WHEN filter_col = const THEN value_col END)
 *
 * Matches a CaseExpr with exactly one CaseWhen whose condition is
 * (Var = Const) or (Const = Var), and whose THEN result is a plain Var.
 * The ELSE branch must be absent or explicitly NULL.
 *
 * On success, fills:
 *   out_value_var        — Var for the value column (THEN branch)
 *   out_filter_scan_pos  — varattno of the filter column (1-based scan pos)
 *   out_filter_const     — the Const node from the equality condition
 *   out_filter_typeoid   — type OID of the filter column
 *
 * Returns false if the expression does not match the pattern.
 */
static bool
ExtractCaseWhenFilter(Node *expr,
					  Var **out_value_var,
					  AttrNumber *out_filter_scan_pos,
					  Const **out_filter_const,
					  Oid *out_filter_typeoid)
{
	CaseExpr   *caseexpr;
	CaseWhen   *casewhen;
	OpExpr	   *opexpr;
	Var		   *filter_var;
	Const	   *filter_const;
	Node	   *then_node;
	Node	   *arg0, *arg1;
	Node	   *cond;

	if (expr == NULL || !IsA(expr, CaseExpr))
		return false;

	caseexpr = (CaseExpr *) expr;

	/* Must have exactly one WHEN clause */
	if (list_length(caseexpr->args) != 1)
		return false;

	/* ELSE must be absent or explicitly NULL */
	if (caseexpr->defresult != NULL)
	{
		if (!IsA(caseexpr->defresult, Const))
			return false;
		if (!((Const *) caseexpr->defresult)->constisnull)
			return false;
	}

	casewhen = (CaseWhen *) linitial(caseexpr->args);
	if (!IsA(casewhen, CaseWhen))
		return false;

	/* Peel any RelabelType wrapping the condition */
	cond = (Node *) casewhen->expr;
	while (IsA(cond, RelabelType))
		cond = (Node *) ((RelabelType *) cond)->arg;

	if (!IsA(cond, OpExpr))
		return false;

	opexpr = (OpExpr *) cond;
	if (list_length(opexpr->args) != 2)
		return false;

	/* Verify operator is equality ("=") */
	{
		HeapTuple  optup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opexpr->opno));
		bool	   is_eq;
		if (!HeapTupleIsValid(optup))
			return false;
		is_eq = (strcmp(NameStr(((Form_pg_operator) GETSTRUCT(optup))->oprname), "=") == 0);
		ReleaseSysCache(optup);
		if (!is_eq)
			return false;
	}

	/* Peel casts from each argument */
	arg0 = (Node *) linitial(opexpr->args);
	arg1 = (Node *) lsecond(opexpr->args);
	while (IsA(arg0, RelabelType)) arg0 = (Node *) ((RelabelType *) arg0)->arg;
	while (IsA(arg1, RelabelType)) arg1 = (Node *) ((RelabelType *) arg1)->arg;

	/* One side must be a Var, the other a Const */
	if (IsA(arg0, Var) && IsA(arg1, Const))
	{
		filter_var   = (Var *) arg0;
		filter_const = (Const *) arg1;
	}
	else if (IsA(arg0, Const) && IsA(arg1, Var))
	{
		filter_var   = (Var *) arg1;
		filter_const = (Const *) arg0;
	}
	else
		return false;

	/* THEN branch must be a plain Var (the value column) */
	then_node = (Node *) casewhen->result;
	while (IsA(then_node, RelabelType))
		then_node = (Node *) ((RelabelType *) then_node)->arg;

	if (!IsA(then_node, Var))
		return false;

	*out_value_var       = (Var *) then_node;
	*out_filter_scan_pos = filter_var->varattno;
	*out_filter_const    = filter_const;
	*out_filter_typeoid  = filter_var->vartype;

	return true;
}

/*
 * Extract a base Var from aggregate argument expressions and detect
 * single-argument cast/coerce wrappers that produce float8.
 */
static bool
ExtractAggInputVar(Node *expr, Var **out_var, Oid *out_expr_type,
				   bool *out_cast_to_float8)
{
	Node   *cur = expr;
	Oid		expr_type = InvalidOid;
	bool	cast_to_float8 = false;

	if (expr == NULL)
		return false;

	expr_type = exprType(expr);

	for (;;)
	{
		if (cur == NULL)
			return false;

		if (IsA(cur, Var))
			break;

		if (IsA(cur, RelabelType))
		{
			RelabelType *r = (RelabelType *) cur;

			if (r->resulttype == FLOAT8OID)
				cast_to_float8 = true;
			cur = (Node *) r->arg;
			continue;
		}

		if (IsA(cur, FuncExpr))
		{
			FuncExpr *f = (FuncExpr *) cur;

			if (list_length(f->args) != 1)
				return false;
			if (f->funcresulttype == FLOAT8OID)
				cast_to_float8 = true;
			cur = (Node *) linitial(f->args);
			continue;
		}

		if (IsA(cur, CoerceViaIO))
		{
			CoerceViaIO *c = (CoerceViaIO *) cur;

			if (c->resulttype == FLOAT8OID)
				cast_to_float8 = true;
			cur = (Node *) c->arg;
			continue;
		}

		if (IsA(cur, CoerceToDomain))
		{
			CoerceToDomain *c = (CoerceToDomain *) cur;

			if (c->resulttype == FLOAT8OID)
				cast_to_float8 = true;
			cur = (Node *) c->arg;
			continue;
		}

		return false;
	}

	*out_var = (Var *) cur;
	*out_expr_type = expr_type;
	*out_cast_to_float8 = (cast_to_float8 && expr_type == FLOAT8OID);
	return true;
}

/*
 * Recursively build an inline VecExprNode tree from an arithmetic expression.
 *
 * Accepted node types:
 *   Var           — scan column reference (leaf)
 *   RelabelType   — lossless relabel (peeled, transparent)
 *   OpExpr        — binary arithmetic operator (both args recursive)
 *   FuncExpr      — unary numeric cast (single arg recursive)
 *
 * All column types and operator/function return types must be supported
 * (TypeOidToVecGaggType returns ≥ 0).
 *
 * Returns the index of the new root node in nodes[], or -1 on failure.
 */
static int
ExtractArithExprNode(Node *expr, Plan *child_plan,
					 VecExprNode *nodes, int *num_nodes, int max_nodes)
{
	if (expr == NULL)
		return -1;

	/* Peel lossless relabels */
	while (IsA(expr, RelabelType))
		expr = (Node *) ((RelabelType *) expr)->arg;

	if (IsA(expr, Var))
	{
		Var		   *v = (Var *) expr;
		AttrNumber	table_attno;
		int			slot_idx;
		int			col_type;

		if (v->varattno <= 0)
			return -1;

		table_attno = ScanOutputPosToVarAttno(child_plan, v->varattno);
		if (table_attno == 0)
			return -1;

		slot_idx = VarAttnoToSlotIdx(child_plan, table_attno);
		if (slot_idx < 0)
			return -1;

		col_type = TypeOidToVecGaggType(v->vartype);
		if (col_type < 0)
			return -1;

		if (*num_nodes >= max_nodes)
			return -1;

		{
			int			idx = (*num_nodes)++;
			VecExprNode *n  = &nodes[idx];

			n->is_var   = true;
			n->slot_idx = slot_idx;
			n->col_type = col_type;
			n->left     = -1;
			n->right    = -1;
			n->opfuncid = InvalidOid;
			n->rettype  = v->vartype;
			return idx;
		}
	}

	if (IsA(expr, OpExpr))
	{
		OpExpr *op = (OpExpr *) expr;
		int		ret_type;
		int		left_idx, right_idx;

		if (list_length(op->args) != 2)
			return -1;
		if (!OidIsValid(op->opfuncid))
			return -1;

		ret_type = TypeOidToVecGaggType(op->opresulttype);
		if (ret_type < 0)
			return -1;

		left_idx = ExtractArithExprNode((Node *) linitial(op->args),
										child_plan, nodes, num_nodes, max_nodes);
		if (left_idx < 0)
			return -1;

		right_idx = ExtractArithExprNode((Node *) lsecond(op->args),
										 child_plan, nodes, num_nodes, max_nodes);
		if (right_idx < 0)
			return -1;

		if (*num_nodes >= max_nodes)
			return -1;

		{
			int			idx = (*num_nodes)++;
			VecExprNode *n  = &nodes[idx];

			n->is_var   = false;
			n->slot_idx = -1;
			n->col_type = ret_type;
			n->left     = left_idx;
			n->right    = right_idx;
			n->opfuncid = op->opfuncid;
			n->rettype  = op->opresulttype;
			return idx;
		}
	}

	if (IsA(expr, Const))
	{
		Const	   *c = (Const *) expr;
		int			col_type;

		col_type = TypeOidToVecGaggType(c->consttype);
		if (col_type < 0)
			return -1;

		if (*num_nodes >= max_nodes)
			return -1;

		{
			int			idx = (*num_nodes)++;
			VecExprNode *n  = &nodes[idx];

			n->is_var   = true;
			n->slot_idx = VECEXPR_CONST_SENTINEL;
			n->col_type = col_type;
			n->left     = -1;
			n->right    = -1;
			n->opfuncid = InvalidOid;
			n->rettype  = c->consttype;
			return idx;
		}
	}

	if (IsA(expr, FuncExpr))
	{
		FuncExpr   *f = (FuncExpr *) expr;
		int			ret_type;
		int			child_idx;
		int			nargs = list_length(f->args);

		/*
		 * Multi-arg functions (e.g. numeric(col, typmod, false)) are typmod-only
		 * casts when all args after the first are Consts.  Treat as passthrough:
		 * opfuncid=InvalidOid; eval_vec_expr_node returns child value unchanged.
		 */
		if (nargs < 1)
			return -1;
		if (nargs > 1)
		{
			ListCell *tlc;
			int       ti = 0;
			foreach(tlc, f->args)
			{
				if (ti > 0 && !IsA(lfirst(tlc), Const))
					return -1;
				ti++;
			}
		}

		ret_type = TypeOidToVecGaggType(f->funcresulttype);
		if (ret_type < 0)
			return -1;

		child_idx = ExtractArithExprNode((Node *) linitial(f->args),
										 child_plan, nodes, num_nodes, max_nodes);
		if (child_idx < 0)
			return -1;

		if (*num_nodes >= max_nodes)
			return -1;

		{
			int			idx = (*num_nodes)++;
			VecExprNode *n  = &nodes[idx];

			n->is_var   = false;
			n->slot_idx = -1;
			n->col_type = ret_type;
			n->left     = child_idx;
			n->right    = -1;		/* unary */
			/* passthrough for multi-arg typmod casts (opfuncid=InvalidOid) */
			n->opfuncid = (nargs == 1) ? f->funcid : InvalidOid;
			n->rettype  = f->funcresulttype;
			return idx;
		}
	}

	return -1;	/* unsupported node type */
}

/*
 * Extract an Aggref from a target expression that may be wrapped by
 * single-argument coercion/finalize nodes.
 */
static bool
ExtractTargetAggref(Node *expr, Aggref **out_aggref)
{
	Node   *cur = expr;

	for (;;)
	{
		if (cur == NULL)
			return false;

		if (IsA(cur, Aggref))
		{
			*out_aggref = (Aggref *) cur;
			return true;
		}

		if (IsA(cur, RelabelType))
		{
			cur = (Node *) ((RelabelType *) cur)->arg;
			continue;
		}

		if (IsA(cur, FuncExpr))
		{
			FuncExpr *f = (FuncExpr *) cur;

			if (list_length(f->args) != 1)
				return false;
			cur = (Node *) linitial(f->args);
			continue;
		}

		if (IsA(cur, CoerceViaIO))
		{
			cur = (Node *) ((CoerceViaIO *) cur)->arg;
			continue;
		}

		if (IsA(cur, CoerceToDomain))
		{
			cur = (Node *) ((CoerceToDomain *) cur)->arg;
			continue;
		}

		return false;
	}
}

/*
 * Classify an Aggref into VECGAGG_COUNT_STAR / SUM / MIN / MAX / AVG.
 * Fills *out_kind, *out_col_varattno, *out_col_typeoid.
 *
 * Optional CASE WHEN output params (all nullable — pass NULL to ignore):
 *   out_has_case_filter   — true when SUM(CASE WHEN col=const THEN val END)
 *   out_filter_scan_pos   — 1-based scan output pos of the filter column
 *   out_filter_const      — Const node for the equality constant
 *   out_filter_typeoid    — type OID of the filter column
 *
 * Optional EXPR output params (pass NULL to disable EXPR classification):
 *   out_expr_nodes        — pre-allocated VecExprNode array (VECGAGG_EXPR_MAX_NODES)
 *   out_expr_num_nodes    — number of nodes filled in out_expr_nodes
 *   out_expr_root_idx     — index of root node in out_expr_nodes
 *
 * Returns false if the aggregate is not supported.
 */
static bool
ClassifyAggref(Aggref *aggref, Plan *child_plan,
			   int *out_kind, AttrNumber *out_col_varattno, Oid *out_col_typeoid,
			   bool *out_avg_input_as_float8,
			   bool *out_has_case_filter,
			   AttrNumber *out_filter_scan_pos,
			   Const **out_filter_const,
			   Oid *out_filter_typeoid,
			   VecExprNode *out_expr_nodes,
			   int *out_expr_num_nodes,
			   int *out_expr_root_idx)
{
	const char *fname;
	Var		   *arg_var = NULL;
	Oid			arg_expr_type = InvalidOid;
	bool		arg_cast_to_float8 = false;
	bool		has_case = false;
	AttrNumber	case_filter_scan_pos = 0;
	Const	   *case_filter_const = NULL;
	Oid			case_filter_typeoid = InvalidOid;

	/* Initialize optional outputs */
	if (out_has_case_filter)  *out_has_case_filter  = false;
	if (out_filter_scan_pos)  *out_filter_scan_pos  = 0;
	if (out_filter_const)     *out_filter_const     = NULL;
	if (out_filter_typeoid)   *out_filter_typeoid   = InvalidOid;
	if (out_expr_num_nodes)   *out_expr_num_nodes   = 0;
	if (out_expr_root_idx)    *out_expr_root_idx    = -1;

	/* FILTER clause not supported */
	if (aggref->aggfilter != NULL)
		return false;

	/* count(*): aggstar = true, no args */
	if (aggref->aggstar)
	{
		*out_kind         = VECGAGG_COUNT_STAR;
		*out_col_varattno = 0;
		*out_col_typeoid  = INT8OID;
		*out_avg_input_as_float8 = false;
		return true;
	}

	/* We only handle single-argument aggregates */
	if (list_length(aggref->args) != 1)
		return false;

	TargetEntry *arg_te = (TargetEntry *) linitial(aggref->args);
	if (!ExtractAggInputVar((Node *) arg_te->expr,
						 &arg_var,
						 &arg_expr_type,
						 &arg_cast_to_float8))
	{
		/*
		 * Not a plain Var — try CASE WHEN col = const THEN value_col END.
		 * Only valid for SUM / MIN / MAX (not AVG or COUNT).
		 */
		Var		   *case_value_var = NULL;

		if (!ExtractCaseWhenFilter((Node *) arg_te->expr,
								   &case_value_var,
								   &case_filter_scan_pos,
								   &case_filter_const,
								   &case_filter_typeoid))
		{
			/*
			 * Not CASE WHEN either — try arithmetic expression for SUM.
			 * Handles SUM(price * quantity), SUM(a + b), SUM(a * b + c), etc.
			 * Only SUM is supported for expressions (not AVG/MIN/MAX).
			 */
			if (out_expr_nodes != NULL && out_expr_num_nodes != NULL &&
				out_expr_root_idx != NULL)
			{
				const char *expr_fname = get_func_name(aggref->aggfnoid);

				if (expr_fname != NULL &&
					(strcmp(expr_fname, "sum") == 0 ||
					 strcmp(expr_fname, "int4_sum") == 0 ||
					 strcmp(expr_fname, "int8_sum") == 0 ||
					 strcmp(expr_fname, "float8pl") == 0))
				{
					int		expr_num = 0;
					int		expr_root = -1;

					expr_root = ExtractArithExprNode(
						(Node *) arg_te->expr, child_plan,
						out_expr_nodes, &expr_num, VECGAGG_EXPR_MAX_NODES);

					if (expr_root >= 0 && expr_num >= 2)
					{
						Oid root_rettype = out_expr_nodes[expr_root].rettype;
						int root_col_type = TypeOidToVecGaggType(root_rettype);

						if (root_col_type >= 0)
						{
							*out_expr_num_nodes       = expr_num;
							*out_expr_root_idx        = expr_root;
							*out_kind                 = VECGAGG_SUM_EXPR;
							*out_col_varattno         = 0;
							*out_col_typeoid          = root_rettype;
							*out_avg_input_as_float8  = false;
							return true;
						}
					}
				}
			}
			return false;
		}

		arg_var            = case_value_var;
		arg_expr_type      = arg_var->vartype;
		arg_cast_to_float8 = false;
		has_case           = true;
	}

	/* Map scan-output position to table varattno */
	AttrNumber scan_pos  = arg_var->varattno;
	AttrNumber col_attno = ScanOutputPosToVarAttno(child_plan, scan_pos);
	if (col_attno == 0)
		return false;

	/* Check type is supported */
	Oid arg_typeoid = arg_var->vartype;
	int arg_vectype = TypeOidToVecGaggType(arg_typeoid);

	fname = get_func_name(aggref->aggfnoid);
	if (fname == NULL)
		return false;

	if (strcmp(fname, "sum") == 0 || strcmp(fname, "int4_sum") == 0 ||
		strcmp(fname, "int8_sum") == 0 || strcmp(fname, "float8pl") == 0)
	{
		if (arg_vectype < 0)
			return false;
		if (arg_cast_to_float8)
			return false;
		*out_kind = VECGAGG_SUM;
	}
	else if (strcmp(fname, "avg") == 0 ||
			 strcmp(fname, "vavg") == 0 ||
			 strcmp(fname, "int8_avg") == 0 ||
			 strcmp(fname, "float8_avg") == 0)
	{
		if (arg_vectype < 0)
			return false;
		/*
		 * Determine the true final result type.  In AGGSPLIT_INITIAL_SERIAL,
		 * aggref->aggtype is the serialized transition type (BYTEAOID), not
		 * the final type.  Use get_func_rettype to always get the final type.
		 */
		Oid final_type = aggref->aggtype;
		if (final_type != NUMERICOID)
			final_type = get_func_rettype(aggref->aggfnoid);

		if (final_type == NUMERICOID)
		{
			if (arg_cast_to_float8)
				return false;
			if (arg_typeoid != INT4OID && arg_typeoid != INT8OID &&
				arg_typeoid != FLOAT4OID && arg_typeoid != FLOAT8OID &&
				arg_typeoid != NUMERICOID)
				return false;
			*out_avg_input_as_float8 = false;
		}
		else
		{
		if (arg_cast_to_float8)
		{
			if (arg_expr_type != FLOAT8OID)
				return false;
			if (arg_typeoid != INT4OID && arg_typeoid != INT8OID &&
				arg_typeoid != FLOAT4OID && arg_typeoid != FLOAT8OID)
				return false;
			*out_avg_input_as_float8 = true;
		}
		else
		{
			if (arg_typeoid != FLOAT4OID && arg_typeoid != FLOAT8OID &&
				arg_typeoid != INT4OID && arg_typeoid != NUMERICOID)
				return false;
			*out_avg_input_as_float8 = false;
		}
		}
		*out_kind = VECGAGG_AVG;
	}
	else if (strcmp(fname, "count") == 0)
	{
		/* count(col): NULL-aware — only count non-null values */
		if (arg_cast_to_float8)
			return false;
		/* CASE WHEN filter is not meaningful for COUNT — fall back */
		if (has_case)
			return false;
		if (aggref->aggdistinct != NIL)
		{
			/*
			 * COUNT(DISTINCT col): executor uses a hash set, so any
			 * by-value type or passable-by-reference type (text, bpchar,
			 * varchar, …) is fine.  The only hard requirement is that we
			 * can read the Datum from a VectorColumn batch slot, which all
			 * supported column types satisfy.  Types that TypeOidToVecGaggType
			 * returns < 0 for (e.g. text, bpchar) are still valid here because
			 * the executor uses VECGAGG_TYPE_TEXT / VECGAGG_TYPE_BPCHAR paths.
			 */
			int cd_col_type = arg_vectype;	/* may be -1 for text types */
			if (cd_col_type < 0)
			{
				/* Allow text-family types via KeyTypeOidToVecGaggType */
				cd_col_type = KeyTypeOidToVecGaggType(arg_typeoid);
				if (cd_col_type < 0)
					return false;	/* truly unsupported type */
			}
			*out_kind = VECGAGG_COUNT_DISTINCT;
		}
		else
		{
			if (arg_vectype < 0)
				return false;
			*out_kind = VECGAGG_COUNT_COL;
		}
	}
	else if (strcmp(fname, "min") == 0 || strcmp(fname, "int4smaller") == 0 ||
			 strcmp(fname, "int8smaller") == 0 || strcmp(fname, "float8smaller") == 0)
	{
		if (arg_cast_to_float8)
			return false;
		*out_kind = VECGAGG_MIN;
	}
	else if (strcmp(fname, "max") == 0 || strcmp(fname, "int4larger") == 0 ||
			 strcmp(fname, "int8larger") == 0 || strcmp(fname, "float8larger") == 0)
	{
		if (arg_cast_to_float8)
			return false;
		*out_kind = VECGAGG_MAX;
	}
	else
		return false;

	/* AVG with CASE WHEN is not supported */
	if (has_case && *out_kind == VECGAGG_AVG)
		return false;

	*out_col_varattno = col_attno;
	*out_col_typeoid  = arg_typeoid;
	if (*out_kind != VECGAGG_AVG)
		*out_avg_input_as_float8 = false;

	/* Propagate CASE WHEN filter info to caller */
	if (has_case)
	{
		if (out_has_case_filter)  *out_has_case_filter  = true;
		if (out_filter_scan_pos)  *out_filter_scan_pos  = case_filter_scan_pos;
		if (out_filter_const)     *out_filter_const     = case_filter_const;
		if (out_filter_typeoid)   *out_filter_typeoid   = case_filter_typeoid;
	}

	return true;
}

static Plan *
PlanTreeMutator(Plan *node, void *context)
{
	if (node == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_CustomScan:
		{
			CustomScan *customScan = (CustomScan *) node;

			if (customScan->methods == engine_customscan_methods())
			{
				PlanTreeMutatorContext *planTreeContext = (PlanTreeMutatorContext *) context;

				Const * vectorizedAggregateExecution = makeNode(Const);
				Const * vectorizedAggStarOnly = makeNode(Const);

				vectorizedAggregateExecution->constbyval = true;
				vectorizedAggregateExecution->consttype = CUSTOM_SCAN_VECTORIZED_AGGREGATE;
				vectorizedAggregateExecution->constvalue =  planTreeContext->vectorizedAggregation;
				vectorizedAggregateExecution->constlen = sizeof(bool);

				vectorizedAggStarOnly->constbyval = true;
				vectorizedAggStarOnly->consttype = CUSTOM_SCAN_VECTORIZED_AGG_STAR_ONLY;
				vectorizedAggStarOnly->constvalue = planTreeContext->vectorizedAggStarOnly;
				vectorizedAggStarOnly->constlen = sizeof(bool);

				customScan->custom_private = lappend(customScan->custom_private, vectorizedAggregateExecution);
				customScan->custom_private = lappend(customScan->custom_private, vectorizedAggStarOnly);
			}
			else
			{
				/*
				 * This is another extension's CustomScan node (e.g. citus router
				 * or local execution plan).  Leave it alone but recurse into its
				 * custom_plans so we can find and mutate any nested storage_engine
				 * CustomScan nodes.
				 */
				ListCell *lc;
				foreach(lc, customScan->custom_plans)
				{
					lfirst(lc) = PlanTreeMutator((Plan *) lfirst(lc), context);
				}
			}

			break;
		}

		case T_Agg:
		{
			Agg *aggNode = (Agg *) node;
			Agg	*newAgg;
			CustomScan *vectorizedAggNode;
			PlanTreeMutatorContext *planTreeContext = (PlanTreeMutatorContext *) context;

			if (!engine_enable_vectorization)
				return node;

			if (aggNode->plan.lefttree->type == T_CustomScan)
			{
				/*
				 * Vectorize stages that still consume raw tuples from the
				 * ColcompressScan.  We keep the original aggregate OID so the
				 * executor preserves the standard combine/serialize/deserial
				 * metadata for parallel partial aggregation.
				 */
				if (aggNode->aggstrategy == AGG_PLAIN &&
					(aggNode->aggsplit == AGGSPLIT_SIMPLE ||
					 aggNode->aggsplit == AGGSPLIT_INITIAL_SERIAL))
				{
#if PG_VERSION_NUM < PG_VERSION_16
					/*
					 * PG15 still crashes for numeric aggregates in the vectorized
					 * plain-aggregate path; keep the regular executor there.
					 */
					if (PlanHasNumericAggrefs((Plan *) aggNode))
						break;
#endif

					/*
					 * Mixed numeric + money plain aggregates still crash in the
					 * vectorized executor on newer PostgreSQL builds. Keep the
					 * regular executor for that combination until the transition
					 * path is fixed.
					 */
					if (PlanHasMixedNumericMoneyAggrefs((Plan *) aggNode))
						break;

					/*
					 * VECGAGG_SUM_EXPR: SUM of an arithmetic expression.
					 * Bypass ExecAgg entirely — ExecVecSumExpr evaluates the
					 * expression tree per row via eval_vec_expr_node.
					 * The node is created with custom_plans=NIL so that
					 * BeginVectorAgg detects this mode at execution time.
					 */
					{
						TargetEntry *_te1 = NULL;
						Aggref      *_sum_agg = NULL;
						int          _kind = -1;
						AttrNumber   _cv = 0;
						Oid          _ct = InvalidOid;
						bool         _af8;
						VecExprNode  _expr_nodes[VECGAGG_EXPR_MAX_NODES];
						int          _expr_num  = 0;
						int          _expr_root = -1;

						if (list_length(aggNode->plan.targetlist) == 1)
						{
							_te1 = (TargetEntry *) linitial(aggNode->plan.targetlist);
							if (_te1 != NULL && !_te1->resjunk)
								ExtractTargetAggref((Node *) _te1->expr, &_sum_agg);
						}

						if (_sum_agg != NULL &&
							ClassifyAggref(_sum_agg, aggNode->plan.lefttree,
										   &_kind, &_cv, &_ct, &_af8,
										   NULL, NULL, NULL, NULL,
										   _expr_nodes, &_expr_num, &_expr_root) &&
							_kind == VECGAGG_SUM_EXPR && _expr_root >= 0)
						{
							int _col_type_expr = TypeOidToVecGaggType(_ct);

							/*
							 * AGGSPLIT safety gate:
							 *   FLOAT8  — safe in SIMPLE and INITIAL_SERIAL
							 *   NUMERIC — safe in SIMPLE and INITIAL_SERIAL
							 *             In INITIAL_SERIAL the executor accumulates
							 *             via numeric_avg_accum then emits bytea via
							 *             numeric_serialize — compatible with Gather.
							 */
							if ((_col_type_expr == VECGAGG_TYPE_FLOAT8 ||
								 _col_type_expr == VECGAGG_TYPE_NUMERIC) &&
								(aggNode->aggsplit == AGGSPLIT_SIMPLE ||
								 aggNode->aggsplit == AGGSPLIT_INITIAL_SERIAL))
							{
								CustomScan *_exprNode = engine_create_aggregator_node();
								List       *_priv     = NIL;
								int         _nconst   = 0;

#define _APPEND_INT(val) \
	do { \
		Const *_pc = makeNode(Const); \
		_pc->consttype   = INT4OID; \
		_pc->constlen    = sizeof(int32); \
		_pc->constvalue  = Int32GetDatum((int32)(val)); \
		_pc->constbyval  = true; \
		_pc->constisnull = false; \
		_priv = lappend(_priv, _pc); \
	} while (0)

								_APPEND_INT(_expr_num);
								_APPEND_INT(_expr_root);
								_APPEND_INT(_col_type_expr);
								_APPEND_INT((int) _ct); /* res_typeoid, reserved */
								_APPEND_INT((int) aggNode->aggsplit); /* aggsplit mode */

								for (int _ni = 0; _ni < _expr_num; _ni++)
								{
									VecExprNode *_n = &_expr_nodes[_ni];
									_APPEND_INT((int) _n->is_var);
									_APPEND_INT(_n->slot_idx);
									_APPEND_INT(_n->col_type);
									_APPEND_INT(_n->left);
									_APPEND_INT(_n->right);
									_APPEND_INT((int) _n->opfuncid);
									_APPEND_INT((int) _n->rettype);
									if (_n->slot_idx == VECEXPR_CONST_SENTINEL)
										_nconst++;
								}
								_APPEND_INT(_nconst);

								/*
								 * Append the actual Const* nodes for each
								 * VECEXPR_CONST_SENTINEL leaf (in node-index order).
								 * BeginVectorAgg reads them back to fill const_val.
								 */
								{
									TargetEntry *_arg_te =
										(TargetEntry *) linitial(_sum_agg->args);
									Node *_arg_expr = (Node *) _arg_te->expr;

									/*
									 * Walk the expr in the same traversal order
									 * as ExtractArithExprNode to collect Consts.
									 */
									Const *_const_buf[VECGAGG_EXPR_MAX_NODES];
									int    _nc = 0;

									struct { Node *e; } _stk[VECGAGG_EXPR_MAX_NODES * 2];
									int _stk_top = 0;
									_stk[_stk_top++].e = _arg_expr;
									while (_stk_top > 0 && _nc < VECGAGG_EXPR_MAX_NODES)
									{
										Node *_e = _stk[--_stk_top].e;
										while (_e && IsA(_e, RelabelType))
											_e = (Node *)((RelabelType *)_e)->arg;
										if (_e == NULL) continue;
										if (IsA(_e, Const))
											_const_buf[_nc++] = (Const *) _e;
										else if (IsA(_e, OpExpr))
										{
											OpExpr *_op = (OpExpr *) _e;
											/* Push right first so left is processed first */
											if (list_length(_op->args) >= 2)
												_stk[_stk_top++].e = (Node *) lsecond(_op->args);
											_stk[_stk_top++].e = (Node *) linitial(_op->args);
										}
										else if (IsA(_e, FuncExpr))
											_stk[_stk_top++].e = (Node *) linitial(((FuncExpr *)_e)->args);
										/* Var nodes: skip */
									}

									for (int _ci = 0; _ci < _nc; _ci++)
										_priv = lappend(_priv, _const_buf[_ci]);
								}

#undef _APPEND_INT

								_exprNode->custom_private = _priv;
								/* custom_plans = NIL — VECGAGG_SUM_EXPR mode */

								Plan *_exprPlan = (Plan *) _exprNode;
								_exprNode->scan.plan.targetlist =
									CustomBuildTargetList(aggNode->plan.targetlist, INDEX_VAR);
								_exprNode->custom_scan_tlist = aggNode->plan.targetlist;
								_exprPlan->startup_cost = aggNode->plan.startup_cost;
								_exprPlan->total_cost   = aggNode->plan.total_cost;
								_exprPlan->plan_rows    = aggNode->plan.plan_rows;
								_exprPlan->plan_width   = aggNode->plan.plan_width;
								_exprPlan->parallel_aware =
									aggNode->plan.lefttree->parallel_aware;

								planTreeContext->vectorizedAggregation = true;
								planTreeContext->vectorizedAggStarOnly  = false;

								node->lefttree = PlanTreeMutator(node->lefttree, context);
								_exprPlan->lefttree = node->lefttree;

								return (Plan *) _exprNode;
							}
							else
							{
								/*
								 * Expression is VECGAGG_SUM_EXPR but AGGSPLIT gate
								 * blocked it (e.g. NUMERIC in AGGSPLIT_INITIAL_SERIAL).
								 * Standard VectorAgg cannot handle arithmetic expressions
								 * on VectorTupleTableSlot either — fall back to PG.
								 */
								break;
							}
						}
					}

					/*
					 * Safety gate: the regular VectorAgg path runs ExpressionMutator
					 * which replaces operators inside Aggref args with their vectorized
					 * counterparts (e.g. float8pl → engine.vfloat8pl).  Those functions
					 * expect arg[1] to be a VectorColumn*, not a scalar Datum.
					 *
					 * When an Aggref's input is an arithmetic expression rather than a
					 * plain Var (e.g. sum(price + 1) wrapped in round(…::numeric, 4)),
					 * ExpressionMutator substitutes vfloat8pl for the inner float8pl,
					 * then at execution time the scalar constant is cast to VectorColumn*
					 * and dereferenced → SIGSEGV.
					 *
					 * The correct path for such expressions is VECGAGG_SUM_EXPR (handled
					 * above).  If VECGAGG_SUM_EXPR was not triggered (e.g. because the
					 * Aggref is buried inside a multi-arg wrapper like round()), detect
					 * the arithmetic-expression input here and fall back to PG standard.
					 */
					{
						bool _has_expr_agg = false;
						ListCell *_guard_lc;

						foreach (_guard_lc, aggNode->plan.targetlist)
						{
							TargetEntry *_guard_te = (TargetEntry *) lfirst(_guard_lc);

							if (_guard_te->resjunk)
								continue;

							Aggref *_found_agg = NULL;
							(void) FindFirstAggref_walker((Node *) _guard_te->expr,
														  &_found_agg);

							if (_found_agg != NULL && list_length(_found_agg->args) == 1)
							{
								TargetEntry *_agg_arg_te =
									(TargetEntry *) linitial(_found_agg->args);

								if (_agg_arg_te != NULL && !IsA(_agg_arg_te->expr, Var))
								{
									_has_expr_agg = true;
									break;
								}
							}
						}

						if (_has_expr_agg)
							break;	/* arithmetic expr in agg — fall back to PG standard */
					}

					vectorizedAggNode = engine_create_aggregator_node();

					FLATCOPY(newAgg, aggNode, Agg);
					{
						VecAggVarCtx varCtx;

						varCtx.scan_plan = aggNode->plan.lefttree;

						newAgg->plan.targetlist =
							(List *) expression_tree_mutator((Node *) newAgg->plan.targetlist,
														 ExpressionMutator,
														 NULL);
						newAgg->plan.targetlist =
							(List *) expression_tree_mutator((Node *) newAgg->plan.targetlist,
														 VecAggVarAttnoMutator,
														 &varCtx);
						newAgg->plan.qual =
							(List *) expression_tree_mutator((Node *) newAgg->plan.qual,
														 VecAggVarAttnoMutator,
														 &varCtx);
					}


					vectorizedAggNode->custom_plans = 
						lappend(vectorizedAggNode->custom_plans, newAgg);
					vectorizedAggNode->scan.plan.targetlist = 
						CustomBuildTargetList(aggNode->plan.targetlist, INDEX_VAR);
					vectorizedAggNode->custom_scan_tlist = newAgg->plan.targetlist;

					// Parallel agg node
					Plan *vectorizedAggNodePlan = (Plan *) vectorizedAggNode;
					vectorizedAggNodePlan->parallel_aware = aggNode->plan.lefttree->parallel_aware;
					vectorizedAggNodePlan->startup_cost = aggNode->plan.startup_cost;
					vectorizedAggNodePlan->total_cost = aggNode->plan.total_cost;
					vectorizedAggNodePlan->plan_rows = aggNode->plan.plan_rows;
					vectorizedAggNodePlan->plan_width = aggNode->plan.plan_width;


					bool oldVectorizedAggStarOnly = planTreeContext->vectorizedAggStarOnly;
					planTreeContext->vectorizedAggregation = true;
					planTreeContext->vectorizedAggStarOnly = PlanAllAggrefsAreStar((Plan *) newAgg);

					node->lefttree  = PlanTreeMutator(node->lefttree, context);
					node->righttree = PlanTreeMutator(node->righttree, context);
					planTreeContext->vectorizedAggStarOnly = oldVectorizedAggStarOnly;

					vectorizedAggNode->scan.plan.lefttree = node->lefttree;
					vectorizedAggNode->scan.plan.righttree = node->righttree;
					newAgg->plan.lefttree = node->lefttree;
					newAgg->plan.righttree = node->righttree;

					return (Plan *) vectorizedAggNode;
				}
			}

			/*
			 * GROUP BY vectorization: replace HashAggregate → ColcompressScan
			 * with VectorGroupAgg → ColcompressScan.
			 *
			 * Requirements:
			 *   - Up to VECGROUPAGG_MAX_KEYS GROUP BY columns
			 *   - Key types: int4/int8/float4/float8/bpchar/text
			 *   - Aggregates: count(*), count(col), sum, min, max, avg over
			 *     int4/int8/float4/float8/numeric
			 *   - Direct ColcompressScan child (no intermediate nodes)
			 *   - No HAVING clause (plan.qual must be empty)
			 */
			if (engine_enable_vectorized_groupagg &&
				(aggNode->aggstrategy == AGG_HASHED ||
				 aggNode->aggstrategy == AGG_SORTED) &&
				(aggNode->aggsplit == AGGSPLIT_SIMPLE ||
				 aggNode->aggsplit == AGGSPLIT_INITIAL_SERIAL) &&
				(aggNode->numCols <= VECGROUPAGG_MAX_KEYS) &&
				engine_enable_vectorization)
			{
				Plan		   *child_plan = node->lefttree;
				Plan		   *scan_plan;  /* the actual ColcompressScan */
				CustomScan	   *childScan;
				const char	   *fallback_reason = "unspecified";
				/* Multi-key support: up to VECGROUPAGG_MAX_KEYS GROUP BY columns */
				int				num_keys = 0;
				AttrNumber		scan_key_pos;
				AttrNumber		key_table_attno;
				int				key_slot_idxs[VECGROUPAGG_MAX_KEYS];
				TargetEntry	   *key_scan_te;
				Oid				key_typeoids[VECGROUPAGG_MAX_KEYS];
				bool			key_is_consts[VECGROUPAGG_MAX_KEYS];
				Const		   *key_consts[VECGROUPAGG_MAX_KEYS];
				int				key_result_atts[VECGROUPAGG_MAX_KEYS];
				VecGroupAggTarget targets[VECGROUPAGG_MAX_TARGETS];
				int				num_targets = 0;
				bool			supported = true;
				ListCell	   *lc;
				int				result_att;
				double			estimated_groups = 0.0;
				int				ki;

				/* Initialize key arrays */
				for (ki = 0; ki < VECGROUPAGG_MAX_KEYS; ki++)
				{
					key_slot_idxs[ki] = -1;
					key_typeoids[ki] = InvalidOid;
					key_is_consts[ki] = false;
					key_consts[ki] = NULL;
					key_result_atts[ki] = -1;
				}
				/*
				 * For AGG_SORTED the planner inserts a Sort node between
				 * the Agg and the scan.  Strip it — VecGroupAgg uses a
				 * hash table and does not need sorted input.
				 *
				 * When the planner uses parallel workers it wraps the scan in
				 * Gather / GatherMerge.  VecGroupAgg has its own internal
				 * parallel scan mechanism, so strip through those nodes too.
				 */
				scan_plan = child_plan;
				if (scan_plan->type == T_Sort)
					scan_plan = ((Sort *) scan_plan)->plan.lefttree;

				/* Strip Gather / GatherMerge inserted for parallel workers */
				if (scan_plan->type == T_Gather || scan_plan->type == T_GatherMerge)
				{
					scan_plan = scan_plan->lefttree;
					if (scan_plan->type == T_Sort)
						scan_plan = ((Sort *) scan_plan)->plan.lefttree;
				}

				/* Child must be our ColcompressScan */
				if (scan_plan->type != T_CustomScan)
				{
					fallback_reason = "child is not CustomScan";
					goto groupagg_fallback;
				}
				childScan = (CustomScan *) scan_plan;
				if (childScan->methods != engine_customscan_methods() &&
					(childScan->methods == NULL ||
					 strcmp(childScan->methods->CustomName, "ColcompressScan") != 0))
				{
					fallback_reason = "child CustomScan is not ColcompressScan";
					goto groupagg_fallback;
				}

				/*
				 * HAVING guard: plan.qual is the HAVING filter.
				 * VecGroupAgg does not evaluate quals during emission,
				 * so fall back to avoid returning wrong results.
				 */
				if (aggNode->plan.qual != NIL)
				{
					fallback_reason = "HAVING clause not supported by VecGroupAgg";
					goto groupagg_fallback;
				}

				/*
				 * Multi-key sorted strategy: VecGroupAgg uses a hash table
				 * internally and does not need sorted input. The sort_output
				 * flag is already false for multi-key (only single-key AGG_SORTED
				 * emits sorted output). Any required output ordering is handled
				 * by the outer Sort node left intact in the plan.
				 * Therefore this case is safe to vectorize.
				 */

				if (aggNode->numCols >= 1)
				{
					/* Normal case: extract each GROUP BY key */
					for (ki = 0; ki < aggNode->numCols; ki++)
					{
						scan_key_pos = aggNode->grpColIdx[ki];
						key_table_attno = ScanOutputPosToVarAttno(scan_plan, scan_key_pos);
						if (key_table_attno == 0)
						{
							fallback_reason = "failed to map GROUP BY key to table attno";
							goto groupagg_fallback;
						}

						key_scan_te = get_tle_by_resno(scan_plan->targetlist, scan_key_pos);
						if (key_scan_te == NULL || !IsA(key_scan_te->expr, Var))
						{
							fallback_reason = "GROUP BY key target is not Var";
							goto groupagg_fallback;
						}
						key_typeoids[ki] = ((Var *) key_scan_te->expr)->vartype;

						if (KeyTypeOidToVecGaggType(key_typeoids[ki]) < 0)
						{
							fallback_reason = "GROUP BY key type unsupported";
							goto groupagg_fallback;
						}

						key_slot_idxs[ki] = VarAttnoToSlotIdx(scan_plan, key_table_attno);
						if (key_slot_idxs[ki] < 0)
						{
							fallback_reason = "failed to map GROUP BY key to vector slot";
							goto groupagg_fallback;
						}
					}
					num_keys = aggNode->numCols;
				}
				else
				{
					/*
					 * Degenerate grouping (numCols==0): infer constant key from
					 * a single Var=Const qual on the scan.
					 */
					ListCell *qlc;
					AttrNumber const_key_attno = 0;

					foreach(qlc, scan_plan->qual)
					{
						Node *q = (Node *) lfirst(qlc);

						if (q != NULL && IsA(q, OpExpr))
						{
							OpExpr *op = (OpExpr *) q;
							Var *v = NULL;
							Const *c = NULL;

							if (list_length(op->args) != 2)
								continue;

							if (IsA(linitial(op->args), Var) && IsA(lsecond(op->args), Const))
							{
								v = (Var *) linitial(op->args);
								c = (Const *) lsecond(op->args);
							}
							else if (IsA(lsecond(op->args), Var) && IsA(linitial(op->args), Const))
							{
								v = (Var *) lsecond(op->args);
								c = (Const *) linitial(op->args);
							}

							if (v != NULL && c != NULL && v->varattno > 0)
							{
								if (const_key_attno == 0)
								{
									const_key_attno = v->varattno;
									key_consts[0] = c;
									key_typeoids[0] = v->vartype;
								}
								else if (const_key_attno != v->varattno)
								{
									fallback_reason = "multiple Var=Const quals with different keys";
									goto groupagg_fallback;
								}
							}
						}
					}

					if (const_key_attno == 0 || key_consts[0] == NULL)
					{
						fallback_reason = "numCols=0 without inferable Var=Const key";
						goto groupagg_fallback;
					}

					key_is_consts[0] = true;
					key_slot_idxs[0] = -1;
					if (KeyTypeOidToVecGaggType(key_typeoids[0]) < 0)
					{
						fallback_reason = "GROUP BY const key type unsupported";
						goto groupagg_fallback;
					}
					num_keys = 1;
				}

				estimated_groups = (aggNode->numGroups > 0.0)
					? aggNode->numGroups
					: aggNode->plan.plan_rows;

				/* Use first key for group-count estimation */
				if (num_keys >= 1 && !key_is_consts[0])
				{
					AttrNumber est_attno = ScanOutputPosToVarAttno(
						scan_plan,
						(aggNode->numCols >= 1) ? aggNode->grpColIdx[0] : 0);

					estimated_groups = EstimateGroupByDistinct(planTreeContext,
												   scan_plan,
												   est_attno,
												   estimated_groups);
				}

				/*
				 * Bail out early if estimated distinct groups exceed VecGroupAgg
				 * safety margin. This avoids runtime overflow at MAX_GROUPS.
				 */
				if (estimated_groups > (double) (VECGROUPAGG_MAX_GROUPS * 3 / 4))
				{
					fallback_reason = "estimated groups exceed safety margin";
					goto groupagg_fallback;
				}

				/*
				 * Some versions expose AVG as a visible Var that references a
				 * hidden (resjunk) Aggref target entry. Pre-scan those resjunk
				 * Aggrefs so visible Vars can be mapped back to aggregate metadata.
				 */
				AttrNumber resjunk_agg_resno[VECGROUPAGG_MAX_TARGETS];
				Aggref *resjunk_agg_expr[VECGROUPAGG_MAX_TARGETS];
				int num_resjunk_aggs = 0;
				ListCell *lc_pre;

				foreach(lc_pre, aggNode->plan.targetlist)
				{
					TargetEntry *te_pre = (TargetEntry *) lfirst(lc_pre);
					Aggref *resjunk_aggref = NULL;

					if (!te_pre->resjunk)
						continue;
					if (num_resjunk_aggs >= VECGROUPAGG_MAX_TARGETS)
						break;
					if (!ExtractTargetAggref((Node *) te_pre->expr, &resjunk_aggref))
						continue;

					resjunk_agg_resno[num_resjunk_aggs] = te_pre->resno;
					resjunk_agg_expr[num_resjunk_aggs] = resjunk_aggref;
					num_resjunk_aggs++;
				}

				/* Walk the visible result targetlist to find aggregate columns */
				result_att = 0;
				foreach(lc, aggNode->plan.targetlist)
				{
					TargetEntry *te = (TargetEntry *) lfirst(lc);
					Aggref *aggref = NULL;
					int kind;
					AttrNumber col_varattno;
					Oid col_typeoid;
					bool avg_input_as_float8 = false;

					if (te->resjunk)
						continue;

					if (IsA(te->expr, Var))
					{
						Var *v = (Var *) te->expr;
						int idx;
						int matched_key = -1;

						for (idx = 0; idx < num_resjunk_aggs; idx++)
						{
							if (v->varattno == resjunk_agg_resno[idx])
							{
								aggref = resjunk_agg_expr[idx];
								break;
							}
						}

						if (aggref != NULL)
						{
							/* Visible Var is a finalize wrapper over hidden Aggref */
						}
						else
						{
							/* Check if this Var is one of the GROUP BY key columns */
							for (ki = 0; ki < num_keys; ki++)
							{
								AttrNumber grp_pos = (aggNode->numCols > 0)
									? aggNode->grpColIdx[ki]
									: 0;
								if (v->varattno == grp_pos)
								{
									matched_key = ki;
									break;
								}
							}
							if (matched_key >= 0)
							{
								/*
								 * GROUP BY key column in output. We handle this
								 * via VecGroupEntry.k.key[ki]; no extra target needed.
								 */
								key_result_atts[matched_key] = result_att;
								result_att++;
								continue;
							}
						else
						{
							fallback_reason = "non-key Var target not linked to aggregate";
							supported = false;
							break;
						}
					}
					}
					else if (!ExtractTargetAggref((Node *) te->expr, &aggref))
					{
						fallback_reason = psprintf("target aggregate shape unsupported (nodeTag=%d)",
									   (int) nodeTag(te->expr));
						supported = false;
						break;
					}

					if (num_targets >= VECGROUPAGG_MAX_TARGETS)
					{
							fallback_reason = psprintf("too many aggregate targets (max %d)", VECGROUPAGG_MAX_TARGETS);
					}

					{
						bool        agg_has_case     = false;
						AttrNumber  agg_filter_spos  = 0;
						Const      *agg_filter_const = NULL;
						Oid         agg_filter_toid  = InvalidOid;
						VecExprNode	expr_nodes_tmp[VECGAGG_EXPR_MAX_NODES];
						int			expr_num_tmp  = 0;
						int			expr_root_tmp = -1;

						if (!ClassifyAggref(aggref, scan_plan,
												&kind, &col_varattno, &col_typeoid,
												&avg_input_as_float8,
												&agg_has_case,
												&agg_filter_spos,
												&agg_filter_const,
												&agg_filter_toid,
												expr_nodes_tmp,
												&expr_num_tmp,
												&expr_root_tmp))
						{
							fallback_reason = psprintf("aggregate target unsupported: %s",
											   get_func_name(aggref->aggfnoid));
							supported = false;
							break;
						}

						targets[num_targets].has_case_filter   = agg_has_case;
						targets[num_targets].filter_const_plan = NULL;

						if (agg_has_case)
						{
							/* Map filter column scan-output pos to table varattno */
							AttrNumber filter_table_attno =
								ScanOutputPosToVarAttno(scan_plan, agg_filter_spos);
							if (filter_table_attno == 0)
							{
								fallback_reason = "CASE WHEN filter column not in scan output";
								supported = false;
								break;
							}
							/* Map table varattno to 0-based slot index */
							int filter_slot_idx = VarAttnoToSlotIdx(scan_plan, filter_table_attno);
							if (filter_slot_idx < 0)
							{
								fallback_reason = "failed to map CASE WHEN filter column to slot";
								supported = false;
								break;
							}
							targets[num_targets].filter_col_attnum  = filter_slot_idx;
							targets[num_targets].filter_col_type    =
								KeyTypeOidToVecGaggType(agg_filter_toid);
							targets[num_targets].filter_const_plan  = agg_filter_const;
						}
						else
						{
							targets[num_targets].filter_col_attnum = -1;
							targets[num_targets].filter_col_type   = -1;
						}

						/* Copy expression tree for VECGAGG_SUM_EXPR targets */
						if (kind == VECGAGG_SUM_EXPR && expr_num_tmp > 0)
						{
							memcpy(targets[num_targets].expr_nodes, expr_nodes_tmp,
								   sizeof(VecExprNode) * expr_num_tmp);
							targets[num_targets].expr_num_nodes = expr_num_tmp;
							targets[num_targets].expr_root_idx  = expr_root_tmp;
						}
						else
						{
							targets[num_targets].expr_num_nodes = 0;
							targets[num_targets].expr_root_idx  = -1;
						}
					}

					/*
					 * Incremental parallel support: AGGSPLIT_INITIAL_SERIAL is
					 * supported here, including AVG. The executor emits the
					 * aggregate transition representation expected by finalize.
					 */

					/*
					 * For non-count(*) aggregates, map table varattno to the
					 * 0-based slot output position (sorted projected attno rank).
					 * Use -1 as the sentinel for count(*) (no slot needed).
					 */
					{
						int col_slot_idx;
						if (kind == VECGAGG_COUNT_STAR || kind == VECGAGG_SUM_EXPR)
							col_slot_idx = -1;  /* no single column for these kinds */
						else
						{
							col_slot_idx = VarAttnoToSlotIdx(scan_plan, col_varattno);
							if (col_slot_idx < 0)
							{
								fallback_reason = "failed to map aggregate input to vector slot";
								supported = false;
								break;
							}
						}
						targets[num_targets].col_attnum = col_slot_idx;
					}

					targets[num_targets].agg_kind       = kind;
					{
						int ct = TypeOidToVecGaggType(col_typeoid);
						if (ct < 0)
							ct = KeyTypeOidToVecGaggType(col_typeoid);
						targets[num_targets].col_type = ct;
					}
					targets[num_targets].result_attnum  = result_att;
					targets[num_targets].avg_input_as_float8 = avg_input_as_float8;
					{
						bool use_int8_path = (kind == VECGAGG_AVG &&
											  aggNode->aggsplit == AGGSPLIT_INITIAL_SERIAL &&
											  col_typeoid == INT8OID);  /* INT8 only: uses internal state + serialize */
						targets[num_targets].use_int8_avg_path = use_int8_path;
						if (use_int8_path)
						{
							/*
							 * Look up the real aggtransfn OID from pg_aggregate
							 * so the executor uses the correct function variant
							 * (internal-transtype vs bigint[]-transtype), which
							 * differ by C function but may share the same SQL name.
							 */
							HeapTuple	agg_tup;
							Form_pg_aggregate agg_form;

							agg_tup = SearchSysCache1(AGGFNOID,
													  ObjectIdGetDatum(aggref->aggfnoid));
							if (!HeapTupleIsValid(agg_tup))
								elog(ERROR, "cache lookup failed for aggregate %u",
									 aggref->aggfnoid);
							agg_form = (Form_pg_aggregate) GETSTRUCT(agg_tup);
							targets[num_targets].avg_transfn_oid = agg_form->aggtransfn;
							ReleaseSysCache(agg_tup);
						}
						else
							targets[num_targets].avg_transfn_oid = InvalidOid;
					}
					/*
					 * In AGGSPLIT_INITIAL_SERIAL, aggref->aggtype is the
					 * actual transition state type emitted to the Finalize
					 * node.  For aggregates with a serialize function
					 * (e.g. numeric_avg), aggref->aggtype = BYTEAOID; in
					 * that case we use get_func_rettype to obtain NUMERICOID
					 * for executor routing.  For aggregates without a
					 * serialize function (avg(int4/int8) → INT8ARRAYOID,
					 * avg(float8) → FLOAT8ARRAYOID), aggref->aggtype IS the
					 * transition format; use it directly so the executor
					 * emits int8[]/{count,sum} or float8[]/{N,sum,0} as
					 * expected by the Finalize GroupAggregate.
					 */
					targets[num_targets].result_typeoid =
						(aggNode->aggsplit == AGGSPLIT_INITIAL_SERIAL)
						? (aggref->aggtype == BYTEAOID
						   ? get_func_rettype(aggref->aggfnoid)  /* numeric_avg → NUMERICOID */
						   : aggref->aggtype)                    /* int8[], float8[], etc. */
						: aggref->aggtype;
					num_targets++;
					result_att++;
				}

				{
					/* Validate: all key result positions must have been found */
					bool keys_ok = true;
					for (ki = 0; ki < num_keys; ki++)
					{
						if (key_result_atts[ki] < 0)
						{
							keys_ok = false;
							break;
						}
					}

					if (!supported || num_targets == 0 || !keys_ok)
					{
						if (num_targets == 0 && strcmp(fallback_reason, "unspecified") == 0)
							fallback_reason = "no vectorizable aggregate targets";
						else if (!keys_ok)
							fallback_reason = "group key not present in output targetlist";
						else if (supported == false && strcmp(fallback_reason, "unspecified") == 0)
							fallback_reason = "targetlist shape incompatible with VecGroupAgg";
						goto groupagg_fallback;
					}
				}

				{
					/* Build VectorGroupAgg plan node */
					CustomScan *vgaNode = engine_create_groupagg_node(
						num_keys,
						key_slot_idxs,
						key_typeoids,
						key_is_consts,
						key_consts,
						key_result_atts,
						(aggNode->aggstrategy == AGG_SORTED && num_keys == 1), /* sort_output */
						(int) aggNode->aggsplit,
						num_targets,
						targets);

					/*
					 * The child ColcompressScan must be signaled to return
					 * VectorTupleTableSlot batches.
					 */
					bool oldVectorizedAggStarOnly = planTreeContext->vectorizedAggStarOnly;
					planTreeContext->vectorizedAggregation = true;
					planTreeContext->vectorizedAggStarOnly = false;

					/* This recurse marks the ColcompressScan child */
					PlanTreeMutator(scan_plan, context);
					planTreeContext->vectorizedAggStarOnly = oldVectorizedAggStarOnly;

					/* Set up node cost/row estimates from original agg */
					Plan *vgaNodePlan = (Plan *) vgaNode;
					vgaNodePlan->parallel_aware = scan_plan->parallel_aware;
					vgaNodePlan->startup_cost = aggNode->plan.startup_cost;
					vgaNodePlan->total_cost   = aggNode->plan.total_cost;
					vgaNodePlan->plan_rows    = aggNode->plan.plan_rows;
					vgaNodePlan->plan_width   = aggNode->plan.plan_width;

					/*
					 * scan.plan.targetlist must contain only simple Var
					 * references to INDEX_VAR, so that PG's projection
					 * machinery in ExecAssignScanProjectionInfoWithVarno
					 * can handle it (Aggref nodes would crash there).
					 *
					 * custom_scan_tlist holds the original agg targetlist
					 * with Aggref nodes — it is used only for type
					 * information via ExecTypeFromTL(custom_scan_tlist).
					 */
					vgaNodePlan->targetlist =
						CustomBuildTargetList(aggNode->plan.targetlist, INDEX_VAR);
					vgaNode->custom_scan_tlist = aggNode->plan.targetlist;

					/*
					 * Store child in lefttree (not custom_plans) so that
					 * deparse_context_for_plan_tree can resolve OUTER_VAR
					 * references in custom_scan_tlist during EXPLAIN VERBOSE.
					 * ExplainNode also walks outerPlanState to show the child.
					 */
					vgaNodePlan->lefttree = scan_plan;

					return (Plan *) vgaNode;
				}

groupagg_fallback:
				if (engine_debug_vectorized_groupagg_fallback)
				{
					elog(DEBUG1,
						 "VecGroupAgg fallback: reason=%s strategy=%d split=%d numCols=%d plan_rows=%.0f est_groups=%.0f",
						 fallback_reason,
						 (int) aggNode->aggstrategy,
						 (int) aggNode->aggsplit,
						 aggNode->numCols,
						 aggNode->plan.plan_rows,
						 estimated_groups);
				}
				/* Fall through to default: recurse normally */
				;
			}

			break;
		}
		case T_IndexScan:
		{
			if (!engine_index_scan)
				return node;

			IndexScan *indexScanNode = (IndexScan *) node;
			IndexScan *newIndexScan;
			CustomScan *columnarIndexScan;

			/* Check if index is build on columnar table */
			if (!engine_index_table(indexScanNode->indexid, engine_tableam_oid))
				return node;

			columnarIndexScan = engine_create_indexscan_node();
			FLATCOPY(newIndexScan, indexScanNode, IndexScan);

			columnarIndexScan->custom_plans = 
						lappend(columnarIndexScan->custom_plans, newIndexScan);
		
			columnarIndexScan->scan.plan.targetlist = 
						CustomBuildTargetList(indexScanNode->scan.plan.targetlist, INDEX_VAR);

			columnarIndexScan->custom_scan_tlist = newIndexScan->scan.plan.targetlist;

			Plan *columnarIndexScanPlan = (Plan *) columnarIndexScan;
			columnarIndexScanPlan->parallel_aware = indexScanNode->scan.plan.parallel_aware;
			columnarIndexScanPlan->startup_cost = indexScanNode->scan.plan.startup_cost;
			columnarIndexScanPlan->total_cost = indexScanNode->scan.plan.total_cost;
			columnarIndexScanPlan->plan_rows = indexScanNode->scan.plan.plan_rows;
			columnarIndexScanPlan->plan_width = indexScanNode->scan.plan.plan_width;

			return (Plan *) columnarIndexScan;
		}
		default:
		{
			break;
		}
		case T_Sort:
		{
			/*
			 * When the planner places a Sort above a HashAggregate (e.g. for
			 * ORDER BY that matches the GROUP BY key), and we replace the
			 * HashAggregate with VecGroupAgg, the outer Sort becomes
			 * redundant.  More importantly, EXPLAIN ANALYZE on this Sort node
			 * can crash in PG 16 (ExplainNode accessing the plan's targetlist
			 * with an out-of-range varno).
			 *
			 * If lefttree becomes VecGroupAgg and the Sort has a single key
			 * at the GROUP BY key output position, absorb the Sort by enabling
			 * sort_output in VecGroupAgg and returning VecGroupAgg directly.
			 */
			node->lefttree = PlanTreeMutator(node->lefttree, context);

			if (engine_is_groupagg_node(node->lefttree))
			{
				Sort	   *sortNode = (Sort *) node;
				CustomScan *vga = (CustomScan *) node->lefttree;

				/*
				 * Only absorb single-key Sorts.  The GROUP BY key is at
				 * 1-based position (key_result_att + 1) in VecGroupAgg's
				 * output; key_result_att is at custom_private[4].
				 */
				if (sortNode->numCols == 1)
				{
					int key_result_att_vga = DatumGetInt32(
						((Const *) list_nth(vga->custom_private, 4))->constvalue);

					if (sortNode->sortColIdx[0] == (AttrNumber)(key_result_att_vga + 1))
					{
						engine_groupagg_enable_sort_output(vga);
						return node->lefttree;  /* Drop the Sort */
					}
				}
			}

			node->righttree = PlanTreeMutator(node->righttree, context);
			return node;
		}
	}

	node->lefttree = PlanTreeMutator(node->lefttree, context);
	node->righttree = PlanTreeMutator(node->righttree, context);

	return node;
}

static void
MutatePlannedStmt(PlannedStmt *stmt)
{
	List		*subplans = NIL;
	ListCell	*cell;
	PlanTreeMutatorContext plainTreeContext;

	plainTreeContext.vectorizedAggregation = false;
	plainTreeContext.vectorizedAggStarOnly = false;
	plainTreeContext.rtable = stmt->rtable;

	stmt->planTree = (Plan *) PlanTreeMutator(stmt->planTree,
										 (void *) &plainTreeContext);

	foreach(cell, stmt->subplans)
	{
		PlanTreeMutatorContext subPlainTreeContext;
		Plan *subplan;

		subPlainTreeContext.vectorizedAggregation = false;
		subPlainTreeContext.vectorizedAggStarOnly = false;
		subPlainTreeContext.rtable = stmt->rtable;
		subplan = (Plan *) PlanTreeMutator(lfirst(cell),
										  (void *) &subPlainTreeContext);
		subplans = lappend(subplans, subplan);
	}

	stmt->subplans = subplans;
}

/*
 * TryVectorizeSerialPlan
 *
 * Attempt to vectorize stmt_serial in-place.  Returns true on success.
 * On failure, restores stmt_serial to its original state and returns false.
 *
 * This helper exists to avoid nesting PG_TRY() blocks inside
 * ColumnarPlannerHook(), which causes -Wshadow=compatible-local warnings
 * from the PG_TRY macro internals.
 */
static bool
TryVectorizeSerialPlan(PlannedStmt *stmt_serial,
					   MemoryContext saved_context,
					   Plan **savedPlanTree,
					   List **savedSubplan)
{
	bool success = false;

	PG_TRY();
	{
		MutatePlannedStmt(stmt_serial);
		success = true;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(saved_context);
		edata = CopyErrorData();
		FlushErrorState();
		ereport(DEBUG1,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Serial plan can't be vectorized. Falling back to native serial execution."),
				 errdetail("%s", edata->message)));
		stmt_serial->planTree = *savedPlanTree;
		stmt_serial->subplans = *savedSubplan;
		success = false;
	}
	PG_END_TRY();

	return success;
}
#endif

static PlannedStmt *
ColumnarPlannerHook(Query *parse,
#if PG_VERSION_NUM >= PG_VERSION_13
					const char *query_string,
#endif
					int cursorOptions,
					ParamListInfo boundParams
#if PG_VERSION_NUM >= PG_VERSION_19
					, ExplainState *es
#endif
					)
{
	PlannedStmt	*stmt;
#if PG_VERSION_NUM >= PG_VERSION_14
	bool pass1Vectorized = false;
	bool pass2Vectorized = false;
	bool run_automatic_plan = false;
	bool lowCardinalityGroupBy = QueryHasSingleLowCardinalityColumnarGroupBy(parse);
	Plan *savedPlanTree;
	List *savedSubplan;
	MemoryContext saved_context;
	Plan *savedParallelPlanTree;
	List *savedParallelSubplan;
	MemoryContext saved_parallel_context;
	int saved_max_parallel_workers_per_gather = max_parallel_workers_per_gather;
	bool saved_enable_sort = enable_sort;
	Query *parse_for_pass2 = NULL;
	Query *parse_for_sortavoid = NULL;
#endif
#if PG_VERSION_NUM < PG_VERSION_13
	/* query_string not passed by PG12 planner hook — not available */
	const char *query_string = NULL;
#endif

	/*
	 * First pass: plan with original parallelism settings.
	 * This is the fallback plan used when vectorization is not possible
	 * (mixed aggregates, non-vectorizable functions, etc.).
	 *
	 * IMPORTANT: standard_planner() modifies the Query tree in-place.
	 * We must save a deep copy BEFORE Pass 1 if we may need Pass 2.
	 */
#if PG_VERSION_NUM >= PG_VERSION_14
	run_automatic_plan = engine_enable_automatic_plan && QueryHasVectorizableAggregate(parse);
	if (run_automatic_plan)
		parse_for_pass2 = copyObject(parse);
	if (lowCardinalityGroupBy)
		parse_for_sortavoid = copyObject(parse);
	if (lowCardinalityGroupBy)
		enable_sort = false;
#endif
	if (PreviousPlannerHook)
#if PG_VERSION_NUM >= PG_VERSION_19
			stmt = PreviousPlannerHook(parse, query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
			stmt = PreviousPlannerHook(parse, query_string, cursorOptions, boundParams);
#else
			stmt = PreviousPlannerHook(parse, cursorOptions, boundParams);
#endif
		else
#if PG_VERSION_NUM >= PG_VERSION_19
			stmt = standard_planner(parse, query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
			stmt = standard_planner(parse, query_string, cursorOptions, boundParams);
#else
			stmt = standard_planner(parse, cursorOptions, boundParams);
#endif
#if PG_VERSION_NUM >= PG_VERSION_14
	enable_sort = saved_enable_sort;
#endif
	/*
	 * In the case of a CREATE TABLE AS query, we are not able to successfully
	 * drop out of a parallel insert situation.  This checks for a CMD_SELECT
	 * and in that case examines the query string to see if it matches the
	 * pattern of a CREATE TABLE AS.  If so, set the parallelism to 0 (off).
	 */
	if (parse->commandType == CMD_SELECT)
	{
		if (IsCreateTableAs(query_string))
		{
			stmt->parallelModeNeeded = 0;
		}
	}

	/*
	 * With citus loaded, mutating plain EXPLAIN plans can segfault because
	 * citus calls ExecutorStart during EXPLAIN processing and does not expect
	 * the plan tree to change after its own planner hook.  In the common case
	 * without citus, keep vectorized plans visible for EXPLAIN and
	 * EXPLAIN ANALYZE so diagnostics match execution.
	 */
	{
		bool citus_loaded = OidIsValid(get_extension_oid("citus", true));
		bool query_has_plain_explain = QueryStringHasPlainExplain(query_string);
		bool debug_has_plain_explain = QueryStringHasPlainExplain(debug_query_string);

		if (citus_loaded && (query_has_plain_explain || debug_has_plain_explain))
		return stmt;
	}

	if (!(engine_enable_vectorization			/* Vectorization should be enabled */
			|| engine_index_scan)				/* or Engine Index Scan */
		|| stmt->commandType != CMD_SELECT)		/* only SELECTS are supported  */
		return stmt;

	if (engine_tableam_oid == InvalidOid)
		engine_tableam_oid = get_table_am_oid("columnar", true);

#if PG_VERSION_NUM >= PG_VERSION_14
	if (parse_for_sortavoid != NULL &&
		PlanHasPathologicalSortedColumnarAgg(stmt->planTree))
	{
		PlannedStmt *stmt_hash = NULL;

		enable_sort = false;
		PG_TRY();
		{
			if (PreviousPlannerHook)
#if PG_VERSION_NUM >= PG_VERSION_19
				stmt_hash = PreviousPlannerHook(parse_for_sortavoid, query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
				stmt_hash = PreviousPlannerHook(parse_for_sortavoid, query_string, cursorOptions, boundParams);
#else
				stmt_hash = PreviousPlannerHook(parse_for_sortavoid, cursorOptions, boundParams);
#endif
			else
#if PG_VERSION_NUM >= PG_VERSION_19
				stmt_hash = standard_planner(parse_for_sortavoid, query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
				stmt_hash = standard_planner(parse_for_sortavoid, query_string, cursorOptions, boundParams);
#else
				stmt_hash = standard_planner(parse_for_sortavoid, cursorOptions, boundParams);
#endif
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(CurrentMemoryContext);
			FlushErrorState();
			stmt_hash = NULL;
		}
		PG_END_TRY();
		enable_sort = saved_enable_sort;

		if (stmt_hash != NULL && PlanHasHashColumnarAgg(stmt_hash->planTree))
			stmt = stmt_hash;
	}

	enable_sort = saved_enable_sort;
#endif

	/*
	 * Automatic plan strategy (two-pass planning, only when enabled):
	 *
	 * Plain vectorized aggregates currently require AGGSPLIT_SIMPLE
	 * (serial T_Agg).
	 * When parallel workers are available the planner generates
	 * AGGSPLIT_INITIAL_SERIAL / AGGSPLIT_FINAL_DESERIAL split nodes instead,
	 * which is generally incompatible with the plain vectorized aggregate path.
	 *
	 * Note: VectorGroupAgg has incremental support for AGGSPLIT_INITIAL_SERIAL
	 * in specific safe shapes (currently count/sum/min/max).
	 *
	 * To handle mixed queries correctly (some aggregates vectorizable, some
	 * not — e.g. SELECT min(a), my_custom_agg(b) FROM foo), we use a
	 * two-pass approach:
	 *
	 *   Pass 1 (done above): plan with original parallelism → parallel plan
	 *                         stored in stmt.  This is the fallback.
	 *   Pass 2 (below, only when hasAggs=true): re-plan with parallelism=0
	 *                         → serial plan → attempt PlanTreeMutator.
	 *         Success → compare the serial vectorized plan cost against the
	 *                   original Pass 1 plan and keep whichever is cheaper.
	 *         Failure → PG_CATCH discards serial plan, returns stmt from
	 *                   Pass 1 (the original parallel plan).
	 *
	 * Cost: one extra planner call only when automatic planning is enabled
	 * and the query has vectorizable aggregates.  Pure scan queries pay
	 * zero overhead.
	 */
	if (parse_for_pass2 != NULL)
	{
		PlannedStmt *stmt_serial;

		savedParallelPlanTree = stmt->planTree;
		savedParallelSubplan = stmt->subplans;
		saved_parallel_context = CurrentMemoryContext;

		if (engine_enable_vectorization)
		{
			PG_TRY();
			{
				MutatePlannedStmt(stmt);
				pass1Vectorized = true;
			}
			PG_CATCH();
			{
				ErrorData  *edata;

				MemoryContextSwitchTo(saved_parallel_context);
				edata = CopyErrorData();
				FlushErrorState();
				ereport(DEBUG1,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Parallel plan can't be vectorized. Falling back to original parallel execution."),
						 errdetail("%s", edata->message)));
				stmt->planTree = savedParallelPlanTree;
				stmt->subplans = savedParallelSubplan;
			}
			PG_END_TRY();
		}

		max_parallel_workers_per_gather = 0;

		PG_TRY();
		{
			if (lowCardinalityGroupBy)
				enable_sort = false;

			if (PreviousPlannerHook)
#if PG_VERSION_NUM >= PG_VERSION_19
				stmt_serial = PreviousPlannerHook(parse_for_pass2, query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
				stmt_serial = PreviousPlannerHook(parse_for_pass2, query_string, cursorOptions, boundParams);
#else
				stmt_serial = PreviousPlannerHook(parse_for_pass2, cursorOptions, boundParams);
#endif
			else
#if PG_VERSION_NUM >= PG_VERSION_19
				stmt_serial = standard_planner(parse_for_pass2, query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
				stmt_serial = standard_planner(parse_for_pass2, query_string, cursorOptions, boundParams);
#else
				stmt_serial = standard_planner(parse_for_pass2, cursorOptions, boundParams);
#endif

			enable_sort = saved_enable_sort;
			max_parallel_workers_per_gather = saved_max_parallel_workers_per_gather;

			savedPlanTree = stmt_serial->planTree;
			savedSubplan  = stmt_serial->subplans;
			saved_context = CurrentMemoryContext;

			if (engine_enable_vectorization)
				pass2Vectorized = TryVectorizeSerialPlan(stmt_serial, saved_context,
														 &savedPlanTree, &savedSubplan);

			/*
			 * If Pass 1 was vectorized successfully, keep that plan whenever it
			 * is cheaper than the serial vectorized fallback.  Otherwise prefer
			 * the serial vectorized plan.  If Pass 1 could not be vectorized,
			 * preserve the original fallback behavior.
			 *
			 * If Pass 1 did not produce a parallel plan, keep the first
			 * vectorized plan we managed to build.
			 */
			/*
			 * Automatic planning is enabled in this path: evaluate both axes.
			 * Axis 1: vectorized vs native (for pass1/parallel and pass2/serial).
			 * Axis 2: parallel(best) vs serial(best).
			 */
			if (pass1Vectorized &&
				savedParallelPlanTree->total_cost < stmt->planTree->total_cost)
			{
				stmt->planTree = savedParallelPlanTree;
				stmt->subplans = savedParallelSubplan;
				pass1Vectorized = false;
			}

			if (pass2Vectorized &&
				savedPlanTree->total_cost < stmt_serial->planTree->total_cost)
			{
				stmt_serial->planTree = savedPlanTree;
				stmt_serial->subplans = savedSubplan;
				pass2Vectorized = false;
			}

			if (stmt_serial->planTree->total_cost < stmt->planTree->total_cost)
				return stmt_serial;

			return stmt;
		}
		PG_CATCH();
		{
			ErrorData  *edata;
			MemoryContextSwitchTo(saved_context);
			enable_sort = saved_enable_sort;
			max_parallel_workers_per_gather = saved_max_parallel_workers_per_gather;

			edata = CopyErrorData();
			FlushErrorState();
			ereport(DEBUG1,
					(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Query can't be vectorized. Falling back to parallel execution."),
						errdetail("%s", edata->message)));

			/*
			 * Vectorization failed (mixed/unsupported aggregates).
			 * Fall through to return the original parallel plan from Pass 1.
			 */
		}
		PG_END_TRY();

		return stmt;	/* Pass 1 parallel plan */
	}

	/* No aggregates or vectorization disabled: attempt index scan mutation only */
	savedPlanTree = stmt->planTree;
	savedSubplan  = stmt->subplans;
	saved_context = CurrentMemoryContext;

	PG_TRY();
	{
		MutatePlannedStmt(stmt);
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContextSwitchTo(saved_context);

		edata = CopyErrorData();
		FlushErrorState();
		ereport(DEBUG1,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Query can't be vectorized. Falling back to original execution."),
					errdetail("%s", edata->message)));
		stmt->planTree = savedPlanTree;
		stmt->subplans = savedSubplan;
	}

	PG_END_TRY();

	return stmt;
}

/*
 * IsCreateTableAs
 *
 * Searches a lower case copy of the query string using strstr to check
 * for the keywords CREATE, TABLE, and AS, in that order.  There can be
 * false positives, but we try to minimize them.
 */
static bool
IsCreateTableAs(const char *query)
{
	char *c, *t, *a;
	size_t query_len;

	if (query == NULL)
		return false;

	query_len = strlen(query);
	char *haystack = (char *) palloc(query_len + 1);
	int32 i;

	/* Create a lower case copy of the string. */
	for (i = 0; i < query_len; i++)
	{
		haystack[i] = tolower(query[i]);
	}

	haystack[i] = '\0';

	c = strstr(haystack, "create");
	if (c == NULL)
	{
		pfree(haystack);
		return false;
	}

	t = strstr(c + 6, "table");
	if (t == NULL)
	{
		pfree(haystack);
		return false;
	}

	a = strstr(t + 5, "as");
	if (a == NULL)
	{
		pfree(haystack);
		return false;
	}

	pfree(haystack);

	return true;
}

#if PG_VERSION_NUM >= PG_VERSION_14
static bool
QueryHasSingleLowCardinalityColumnarGroupBy(Query *parse)
{
	SortGroupClause *groupClause;
	TargetEntry *groupTle;
	Var *groupVar;
	RangeTblEntry *rte;
	HeapTuple statTuple;
	bool lowCardinality = false;

	if (parse == NULL || parse->groupClause == NIL || list_length(parse->groupClause) != 1)
		return false;

	groupClause = linitial_node(SortGroupClause, parse->groupClause);
	groupTle = get_sortgroupclause_tle(groupClause, parse->targetList);
	if (groupTle == NULL || !IsA(groupTle->expr, Var))
		return false;

	groupVar = (Var *) groupTle->expr;
	if (groupVar->varno <= 0 || groupVar->varlevelsup != 0)
		return false;

	rte = rt_fetch(groupVar->varno, parse->rtable);
	if (rte == NULL || rte->rtekind != RTE_RELATION)
		return false;

	if (engine_tableam_oid == InvalidOid)
		engine_tableam_oid = get_table_am_oid("columnar", true);

	if (!OidIsValid(engine_tableam_oid) || GetRelationTableAmOid(rte->relid) != engine_tableam_oid)
		return false;

	statTuple = SearchSysCache3(STATRELATTINH,
							   ObjectIdGetDatum(rte->relid),
							   Int16GetDatum(groupVar->varattno),
							   BoolGetDatum(false));
	if (!HeapTupleIsValid(statTuple))
		return false;

	{
		Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(statTuple);

		if (stats->stadistinct > 0.0 && stats->stadistinct <= 1024.0)
			lowCardinality = true;
		else if (stats->stadistinct <= 0.0)
		{
			AttStatsSlot mcvSlot;

			if (get_attstatsslot(&mcvSlot, statTuple,
							   STATISTIC_KIND_MCV, InvalidOid,
							   ATTSTATSSLOT_VALUES))
			{
				if (mcvSlot.nvalues > 0 && mcvSlot.nvalues <= 64)
					lowCardinality = true;
				free_attstatsslot(&mcvSlot);
			}
			else
			{
				/*
				 * PG19 can report pathological stadistinct values for colcompress
				 * before ANALYZE captures usable MCVs. Let the follow-up plan check
				 * decide whether sort avoidance is worthwhile.
				 */
				lowCardinality = true;
			}
		}
	}

	ReleaseSysCache(statTuple);
	return lowCardinality;
}

static Oid
GetRelationTableAmOid(Oid relid)
{
	HeapTuple classTuple;
	Form_pg_class classForm;
	Oid relam = InvalidOid;

	classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(classTuple))
		return InvalidOid;

	classForm = (Form_pg_class) GETSTRUCT(classTuple);
	relam = classForm->relam;
	ReleaseSysCache(classTuple);

	return relam;
}

static bool
PlanHasColumnarCustomScan(Plan *plan)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, CustomScan))
	{
		CustomScan *cscan = (CustomScan *) plan;
		ListCell *lc;

		if (cscan->methods == engine_customscan_methods() ||
			(cscan->methods != NULL && cscan->methods->CustomName != NULL &&
			 strcmp(cscan->methods->CustomName, "ColcompressScan") == 0))
			return true;

		foreach(lc, cscan->custom_plans)
		{
			if (PlanHasColumnarCustomScan((Plan *) lfirst(lc)))
				return true;
		}
	}

	return PlanHasColumnarCustomScan(plan->lefttree) ||
		PlanHasColumnarCustomScan(plan->righttree);
}

static bool
PlanHasPathologicalSortedColumnarAgg(Plan *plan)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, Agg))
	{
		Agg *agg = (Agg *) plan;
		Plan *child = agg->plan.lefttree;

		if (agg->numCols == 1 &&
			agg->aggstrategy == AGG_SORTED &&
			child != NULL &&
			agg->plan.plan_rows >= child->plan_rows * 0.50 &&
			PlanHasColumnarCustomScan(child))
			return true;
	}

	return PlanHasPathologicalSortedColumnarAgg(plan->lefttree) ||
		PlanHasPathologicalSortedColumnarAgg(plan->righttree);
}

static bool
PlanHasHashColumnarAgg(Plan *plan)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, Agg))
	{
		Agg *agg = (Agg *) plan;

		if (agg->aggstrategy == AGG_HASHED &&
			PlanHasColumnarCustomScan(agg->plan.lefttree))
			return true;
	}

	return PlanHasHashColumnarAgg(plan->lefttree) ||
		PlanHasHashColumnarAgg(plan->righttree);
}

/*
 * QueryHasVectorizableAggregate
 *
 * Returns true if the query contains aggregate functions that may be
 * vectorized by the planner hook.  We rely on the parser-populated
 * parse->hasAggs flag — it is set for any query with aggregates and is
 * available at zero cost before planning begins.
 *
 * This is used to gate the temporary zeroing of
 * max_parallel_workers_per_gather: we only force a serial aggregate plan
 * when the query actually has aggregates.  Pure scan queries (SELECT *,
 * WHERE clauses, ORDER BY, etc.) keep their parallel workers unaffected.
 */
static bool
QueryHasVectorizableAggregate(Query *parse)
{
	if (parse == NULL)
		return false;

	/*
	 * parse->hasAggs is set by the parser for any query that contains
	 * aggregate function calls in the target list or HAVING clause.
	 * It is a reliable, cheap indicator that a T_Agg node will appear
	 * in the plan tree and may be eligible for vectorization.
	 */
	return parse->hasAggs;
}
#endif /* PG_VERSION_NUM >= PG_VERSION_14 */

/*
 * QueryStringHasPlainExplain
 *
 * Returns true if the original command string contains a plain EXPLAIN
 * statement, i.e. EXPLAIN without ANALYZE=true.
 *
 * The planner hook is invoked for the explained SELECT, not the outer
 * ExplainStmt utility wrapper, so inspecting only the current Query node is
 * insufficient for multi-statement commands such as `SET ...; EXPLAIN ...`.
 * Parse the full command string and inspect the raw ExplainStmt options.
 *
 * NOTE: debug_query_string may contain PL/pgSQL assignment syntax
 * (e.g. "mymode := 'eager'") which is not valid SQL and would cause
 * raw_parser() to throw a parse error.  We wrap the call in a PG_TRY
 * block so that any parse error is silently swallowed — this function
 * is a heuristic check only and returning false on parse failure is safe.
 */
static bool
QueryStringHasPlainExplain(const char *query)
{
	List       *raw_parsetree_list;
	ListCell   *lc;

	if (query == NULL)
		return false;

	/*
	 * raw_parser() throws an error for non-SQL text (e.g. PL/pgSQL
	 * assignment "var := expr" stored in debug_query_string).  Catch and
	 * discard any such error: a false negative here is always safe.
	 */
	PG_TRY();
	{
#if PG_VERSION_NUM >= PG_VERSION_14
		raw_parsetree_list = raw_parser(query, RAW_PARSE_DEFAULT);
#else
		raw_parsetree_list = raw_parser(query);
#endif
	}
	PG_CATCH();
	{
		/* Not valid SQL — cannot be a plain EXPLAIN */
		FlushErrorState();
		return false;
	}
	PG_END_TRY();

	foreach(lc, raw_parsetree_list)
	{
		RawStmt *rawStmt = lfirst_node(RawStmt, lc);

		if (rawStmt != NULL && rawStmt->stmt != NULL && IsA(rawStmt->stmt, ExplainStmt))
		{
			ExplainStmt *explainStmt = castNode(ExplainStmt, rawStmt->stmt);
			ListCell *optionCell;
			bool analyze = false;

			foreach(optionCell, explainStmt->options)
			{
				DefElem *option = lfirst_node(DefElem, optionCell);

				if (option != NULL && strcmp(option->defname, "analyze") == 0)
				{
					analyze = defGetBoolean(option);
					break;
				}
			}

			if (!analyze)
				return true;
		}
	}

	return false;
}

void engine_planner_init(void)
{
	PreviousPlannerHook = planner_hook;
	planner_hook = ColumnarPlannerHook;
#if  PG_VERSION_NUM >= PG_VERSION_14
	engine_register_aggregator_node();
	engine_register_groupagg_node();
#endif
	engine_register_indexscan_node();
}
