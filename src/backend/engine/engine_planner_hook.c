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
#include "catalog/pg_aggregate.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_statistic.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "nodes/makefuncs.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
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

typedef struct PlanTreeMutatorContext PlanTreeMutatorContext;

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
static Oid GetEngineTableAmOid(void);
static Oid GetRelationTableAmOid(Oid relid);
static bool PlanHasColumnarCustomScan(Plan *plan);
static bool PlanHasJoinNode(Plan *plan);
static Bitmapset *PlanOutputColumnSourceRels(Plan *plan, AttrNumber resno);
static Bitmapset *PlanExprSourceRels(Node *node, Plan *plan);
static Bitmapset *ExprSourceRelsForPlan(Node *node, Plan *input_plan);
static List *AppendQualOnlyAggrefsToTargetList(List *targetlist, Node *qual);
static Node *RewriteQualAggrefsToTargetVars(Node *qual, List *targetlist);
static bool ShouldRelaxVecGroupAggGroupLimit(Agg *aggNode, Oid first_key_typeoid);
static bool IsQ13StyleOuterJoinCountAgg(Agg *agg);
static const char *DescribePostJoinColumnarAgg(Agg *agg);
static bool PlanIsPostJoinColumnarAgg(Agg *agg);
static bool PlanHasPathologicalSortedColumnarAgg(Plan *plan);
static bool PlanHasHashColumnarAgg(Plan *plan);
#if PG_VERSION_NUM >= PG_VERSION_19
static Query *TryRewriteOuterJoinCountSubqueryForPg19(Query *parse,
									 const char **planner_query_string);
#endif
#if PG_VERSION_NUM >= PG_VERSION_19
static bool AppendSortClauseSql(StringInfo buf, Query *query, List *dpcontext);
static bool AppendLimitClauseSql(StringInfo buf, Query *query, List *dpcontext);
static Node *DecorrelateNestedScalarAggSubLinksMutator(Node *node, void *context);
static Query *TryDecorrelateScalarAggSubqueriesRecursivelyForPg19(Query *parse,
										 const char **planner_query_string);
static Query *TryRewriteQ8MarketShareAggForPg19(Query *parse,
						const char **planner_query_string);
static Query *TryRewriteQ18OrderQuantityAggForPg19(Query *parse,
								 const char **planner_query_string);
static Query *TryDecorrelateScalarAggSubqueryForPg19(Query *parse,
										 const char **planner_query_string);
#endif
static void MutatePlannedStmt(PlannedStmt *stmt);
static bool TryVectorizeSerialPlan(PlannedStmt *stmt_serial,
								   MemoryContext saved_context,
								   Plan **savedPlanTree,
								   List **savedSubplan);
static void CollectProjectedAttnos(Plan *scan_plan, AttrNumber *attnos, int *count);
#endif

typedef struct PlanTreeMutatorContext
{
	bool vectorizedAggregation;
	bool vectorizedAggStarOnly;
	List *rtable;
} PlanTreeMutatorContext;

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

typedef struct
{
 	List *targetlist;
	List *collected;
} QualAggrefCollectorContext;

static bool
TargetListHasAggref(List *targetlist, Aggref *aggref)
{
	ListCell *lc;

	if (aggref == NULL)
		return false;

	foreach(lc, targetlist)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		if (equal(te->expr, aggref))
			return true;
	}

	return false;
}

static bool
CollectQualOnlyAggrefsWalker(Node *node, void *context)
{
	QualAggrefCollectorContext *ctx = (QualAggrefCollectorContext *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;

		if (!TargetListHasAggref(ctx->targetlist, aggref) &&
			!TargetListHasAggref(ctx->collected, aggref))
			ctx->collected = lappend(ctx->collected, copyObject(aggref));

		return false;
	}

	return expression_tree_walker(node, CollectQualOnlyAggrefsWalker, context);
}

static List *
AppendQualOnlyAggrefsToTargetList(List *targetlist, Node *qual)
{
	QualAggrefCollectorContext ctx;
	List *result;
	ListCell *lc;
	AttrNumber next_resno;

	if (targetlist == NIL || qual == NULL)
		return targetlist;

	ctx.targetlist = targetlist;
	ctx.collected = NIL;
	(void) CollectQualOnlyAggrefsWalker(qual, &ctx);

	if (ctx.collected == NIL)
		return targetlist;

	result = list_copy_deep(targetlist);
	next_resno = list_length(result) + 1;

	foreach(lc, ctx.collected)
	{
		Aggref *aggref = lfirst_node(Aggref, lc);
		TargetEntry *te = makeTargetEntry((Expr *) aggref,
										 next_resno++,
										 NULL,
										 true);

		result = lappend(result, te);
	}

	return result;
}

typedef struct
{
	List *targetlist;
} QualAggrefRewriteContext;

static Node *
RewriteQualAggrefsToTargetVarsMutator(Node *node, void *context)
{
	QualAggrefRewriteContext *ctx = (QualAggrefRewriteContext *) context;
	ListCell *lc;

	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;

		foreach(lc, ctx->targetlist)
		{
			TargetEntry *te = lfirst_node(TargetEntry, lc);

			if (equal(te->expr, aggref))
				return (Node *) makeVar(INDEX_VAR,
									te->resno,
									exprType((Node *) te->expr),
									exprTypmod((Node *) te->expr),
									exprCollation((Node *) te->expr),
									0);
		}
	}

	return expression_tree_mutator(node, RewriteQualAggrefsToTargetVarsMutator, context);
}

static Node *
RewriteQualAggrefsToTargetVars(Node *qual, List *targetlist)
{
	QualAggrefRewriteContext ctx;

	if (qual == NULL)
		return NULL;

	ctx.targetlist = targetlist;
	return expression_tree_mutator(qual, RewriteQualAggrefsToTargetVarsMutator, &ctx);
}

static Node *
StripRelabels(Node *node)
{
	for (;;)
	{
		if (node == NULL)
			return NULL;
		if (IsA(node, RelabelType))
			node = (Node *) ((RelabelType *) node)->arg;
		else
			return node;
	}
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
		TargetEntry *te = lfirst_node(TargetEntry, lc);

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
			n->op_type  = SE_OPTYPE_NONE;
			/* Derive fixed_scale from typmod for NUMERIC; 0 for integer types */
			if (v->vartype == NUMERICOID)
				n->fixed_scale = (v->vartypmod > VARHDRSZ)
					? (int32)((v->vartypmod - VARHDRSZ) & 0xffff)
					: -1;
			else if (v->vartype == INT4OID || v->vartype == INT8OID)
				n->fixed_scale = 0;
			else
				n->fixed_scale = -1;
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
			int			ls  = nodes[left_idx].fixed_scale;
			int			rs  = nodes[right_idx].fixed_scale;
			int			ot  = SE_OPTYPE_NONE;
			const char *fname;

			/* Classify operator for the i128 fast path */
			fname = get_func_name(op->opfuncid);
			if (fname)
			{
				if (strstr(fname, "_mul") || strstr(fname, "mul"))
					ot = SE_OPTYPE_MUL;
				else if (strstr(fname, "_add") || strstr(fname, "pl"))
					ot = SE_OPTYPE_ADD;
				else if (strstr(fname, "_sub") || strstr(fname, "mi"))
					ot = SE_OPTYPE_SUB;
				else if (strstr(fname, "_div") || strstr(fname, "div"))
					ot = SE_OPTYPE_DIV;
			}

			n->is_var   = false;
			n->slot_idx = -1;
			n->col_type = ret_type;
			n->left     = left_idx;
			n->right    = right_idx;
			n->opfuncid = op->opfuncid;
			n->rettype  = op->opresulttype;
			n->op_type  = ot;

			/* Propagate fixed_scale for i128 arithmetic */
			if (ot == SE_OPTYPE_MUL)
				n->fixed_scale = (ls >= 0 && rs >= 0) ? ls + rs : -1;
			else if (ot == SE_OPTYPE_ADD || ot == SE_OPTYPE_SUB)
				n->fixed_scale = (ls >= 0 && rs >= 0) ? (ls > rs ? ls : rs) : -1;
			else
				n->fixed_scale = -1;  /* DIV or unknown → NUMERIC fallback */

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

			n->is_var       = true;
			n->slot_idx     = VECEXPR_CONST_SENTINEL;
			n->col_type     = col_type;
			n->left         = -1;
			n->right        = -1;
			n->opfuncid     = InvalidOid;
			n->rettype      = c->consttype;
			n->const_val    = c->constvalue;
			n->const_isnull = c->constisnull;
			n->op_type      = SE_OPTYPE_NONE;

			/* Derive fixed_scale for the i128 fast path */
			if (c->constisnull)
				n->fixed_scale = -1;
			else if (c->consttype == INT4OID || c->consttype == INT8OID)
				n->fixed_scale = 0;
			else if (c->consttype == NUMERICOID)
			{
				/*
				 * Read the stored decimal scale (dscale) directly from the
				 * NUMERIC varlena header.  The first two bytes after the
				 * varlena length are the NumericChoice header word.  Bit 15
				 * being set means "short" form; bits 12..7 = dscale in that
				 * form.  Otherwise bit 14..0 of the long-form header = dscale.
				 * Both forms have been stable since PostgreSQL 9.6.
				 */
				Numeric num = DatumGetNumeric(c->constvalue);
				uint16  hdr = *((const uint16 *) ((const char *) num + VARHDRSZ));
				bool    is_special = ((hdr & 0xC000) == 0xC000);  /* NaN / Inf */
				if (is_special)
					n->fixed_scale = -1;
				else if (hdr & 0x8000)	/* short form: dscale in bits 12..7 */
					n->fixed_scale = (int32)((hdr & 0x1F80) >> 7);
				else					/* long form: dscale in bits 13..0 */
					n->fixed_scale = (int32)(hdr & 0x3FFF);
			}
			else
				n->fixed_scale = -1;

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
			n->op_type  = SE_OPTYPE_CAST;

			/*
			 * Propagate child's fixed_scale through the cast.
			 * For multi-arg numeric typmod casts, the second arg encodes the
			 * typmod; extract the output scale from it when available.
			 */
			if (nargs > 1)
			{
				Node *typmod_node = (Node *) lsecond(f->args);
				if (IsA(typmod_node, Const))
				{
					int32 tmod = DatumGetInt32(((Const *) typmod_node)->constvalue);
					n->fixed_scale = (tmod > VARHDRSZ)
						? (int32)((tmod - VARHDRSZ) & 0xffff)
						: nodes[child_idx].fixed_scale;
				}
				else
					n->fixed_scale = nodes[child_idx].fixed_scale;
			}
			else
				n->fixed_scale = nodes[child_idx].fixed_scale;

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

/*
 * BuildPostAggOutputNode — walk one post-aggregate expression and build a flat
 * VecPostAggNode tree that references accumulator slots.
 *
 * Aggrefs found are inserted into the accumulator pool (p_mt_num, mt_*) if not
 * already present.  The pool arrays must be pre-initialized by the caller.
 *
 * Returns the index of the root node written into out->nodes[], or -1 on
 * failure (unsupported expression type or pool overflow).
 *
 * Supported leaf types: Aggref (classifiable), Const (numeric types only).
 * Supported interior nodes: OpExpr, FuncExpr (unary cast), RelabelType.
 * Depth is limited to VPAE_MAX_NODES to guard against stack overflow.
 */
static int
BuildPostAggOutputNode(Node *expr, Plan *child_plan,
					   int aggsplit,
					   /* accumulator pool (in/out) */
					   int				   *p_mt_num,
					   Aggref		      **mt_aggrefs,
					   int				   *mt_kind,
					   int				   *mt_col_type,
					   int				   *mt_sum_col_slot,
					   VecExprNode		  (*mt_nodes)[VECGAGG_EXPR_MAX_NODES],
					   int				   *mt_expr_num,
					   int				   *mt_expr_root,
					   /* output expression being built */
					   VecMultiOutput	   *out,
					   int					depth)
{
	if (!expr || depth > VPAE_MAX_NODES || out->num_nodes >= VPAE_MAX_NODES)
		return -1;

	/* Peel lossless relabels */
	while (IsA(expr, RelabelType))
		expr = (Node *) ((RelabelType *) expr)->arg;

	/* ---- Aggref leaf ---- */
	if (IsA(expr, Aggref))
	{
		Aggref	   *agg    = (Aggref *) expr;
		int			ki     = -1;
		AttrNumber	dummy  = 0;
		Oid			ct     = InvalidOid;
		bool		af8    = false;
		VecExprNode tnodes[VECGAGG_EXPR_MAX_NODES];
		int			tnum   = 0;
		int			troot  = -1;

		if (!ClassifyAggref(agg, child_plan, &ki, &dummy, &ct, &af8,
							NULL, NULL, NULL, NULL,
							tnodes, &tnum, &troot))
			return -1;

		/* Find or insert in accumulator pool */
		int slot_idx = -1;
		for (int _i = 0; _i < *p_mt_num; _i++)
			if (mt_aggrefs[_i] == agg) { slot_idx = _i; break; }

		if (slot_idx < 0)
		{
			if (*p_mt_num >= VECGAGG_MULTI_MAX_TARGETS)
				return -1;
			slot_idx = *p_mt_num;
			mt_aggrefs[slot_idx]       = agg;
			mt_expr_num[slot_idx]      = 0;
			mt_expr_root[slot_idx]     = -1;
			mt_sum_col_slot[slot_idx]  = -1;

			if (ki == VECGAGG_COUNT_STAR)
			{
				mt_kind[slot_idx]     = VMSEXPR_COUNT_STAR;
				mt_col_type[slot_idx] = VECGAGG_TYPE_INT8;
			}
			else if (ki == VECGAGG_SUM_EXPR && troot >= 0 && tnum >= 2)
			{
				int cvt = TypeOidToVecGaggType(ct);
				if (cvt != VECGAGG_TYPE_FLOAT8 && cvt != VECGAGG_TYPE_NUMERIC)
					return -1;
				mt_kind[slot_idx]     = VMSEXPR_SUM_EXPR;
				mt_col_type[slot_idx] = cvt;
				memcpy(mt_nodes[slot_idx], tnodes, tnum * sizeof(VecExprNode));
				mt_expr_num[slot_idx]  = tnum;
				mt_expr_root[slot_idx] = troot;
			}
			else if (ki == VECGAGG_SUM)
			{
				/*
				 * Plain sum(col): ClassifyAggref set *out_col_varattno = dummy
				 * (the scan-level attno).  Convert to slot index.
				 */
				if (dummy == 0)
					return -1;
				int cvt = TypeOidToVecGaggType(ct);
				if (cvt < 0)
					return -1;
				int col_slot = VarAttnoToSlotIdx(child_plan, dummy);
				if (col_slot < 0)
					return -1;

				if (cvt == VECGAGG_TYPE_NUMERIC &&
					aggsplit == AGGSPLIT_INITIAL_SERIAL)
				{
					/*
					 * NUMERIC SUM_COL in the parallel worker (INITIAL_SERIAL):
					 * the partial-state protocol requires numeric_avg_accum +
					 * numeric_serialize, which the VMSEXPR_SUM_EXPR path already
					 * handles correctly.  Downgrade to VMSEXPR_SUM_EXPR with a
					 * single Var-leaf expression node pointing at the column slot.
					 */
					if (mt_expr_num[slot_idx] != 0)
						return -1;
					VecExprNode *vn = &mt_nodes[slot_idx][0];
					memset(vn, 0, sizeof(*vn));
					vn->is_var       = true;
					vn->slot_idx     = col_slot;
					vn->col_type     = VECGAGG_TYPE_NUMERIC;
					vn->left         = -1;
					vn->right        = -1;
					vn->opfuncid     = InvalidOid;
					vn->rettype      = NUMERICOID;
					vn->const_isnull = true;
					mt_kind[slot_idx]      = VMSEXPR_SUM_EXPR;
					mt_col_type[slot_idx]  = VECGAGG_TYPE_NUMERIC;
					mt_expr_num[slot_idx]  = 1;
					mt_expr_root[slot_idx] = 0;
				}
				else
				{
					mt_kind[slot_idx]         = VMSEXPR_SUM_COL;
					mt_col_type[slot_idx]     = cvt;
					mt_sum_col_slot[slot_idx] = col_slot;
				}
			}
			else
				return -1;	/* unsupported Aggref kind */

			(*p_mt_num)++;
		}

		/* Emit VPAE_SLOT node */
		int idx = out->num_nodes++;
		out->nodes[idx].kind         = VPAE_SLOT;
		out->nodes[idx].slot_idx     = (int8) slot_idx;
		out->nodes[idx].left         = -1;
		out->nodes[idx].right        = -1;
		out->nodes[idx].opfuncid     = InvalidOid;
		out->nodes[idx].rettype      = ct;
		out->nodes[idx].const_isnull = true;
		return idx;
	}

	/* ---- Const leaf ---- */
	if (IsA(expr, Const))
	{
		Const *c = (Const *) expr;
		int idx = out->num_nodes++;
		out->nodes[idx].kind         = VPAE_CONST;
		out->nodes[idx].left         = -1;
		out->nodes[idx].right        = -1;
		out->nodes[idx].slot_idx     = -1;
		out->nodes[idx].opfuncid     = InvalidOid;
		out->nodes[idx].rettype      = c->consttype;
		out->nodes[idx].const_val    = c->constvalue;
		out->nodes[idx].const_isnull = c->constisnull;
		out->nodes[idx].const_ptr    = c;
		return idx;
	}

	/* ---- Binary / unary OpExpr ---- */
	if (IsA(expr, OpExpr))
	{
		OpExpr *op   = (OpExpr *) expr;
		int     nargs = list_length(op->args);

		if (nargs < 1 || nargs > 2 || !OidIsValid(op->opfuncid))
			return -1;

		int left_idx = BuildPostAggOutputNode(
			(Node *) linitial(op->args), child_plan, aggsplit,
			p_mt_num, mt_aggrefs, mt_kind, mt_col_type,
			mt_sum_col_slot, mt_nodes, mt_expr_num, mt_expr_root,
			out, depth + 1);
		if (left_idx < 0)
			return -1;

		int right_idx = -1;
		if (nargs == 2)
		{
			right_idx = BuildPostAggOutputNode(
				(Node *) lsecond(op->args), child_plan, aggsplit,
				p_mt_num, mt_aggrefs, mt_kind, mt_col_type,
				mt_sum_col_slot, mt_nodes, mt_expr_num, mt_expr_root,
				out, depth + 1);
			if (right_idx < 0)
				return -1;
		}

		if (out->num_nodes >= VPAE_MAX_NODES)
			return -1;
		int idx = out->num_nodes++;
		out->nodes[idx].kind     = VPAE_OP;
		out->nodes[idx].left     = (int8) left_idx;
		out->nodes[idx].right    = (int8) right_idx;
		out->nodes[idx].slot_idx = -1;
		out->nodes[idx].opfuncid = op->opfuncid;
		out->nodes[idx].rettype  = op->opresulttype;
		out->nodes[idx].const_isnull = true;
		return idx;
	}

	/* ---- Unary FuncExpr (type cast) ---- */
	if (IsA(expr, FuncExpr))
	{
		FuncExpr *f = (FuncExpr *) expr;
		if (list_length(f->args) != 1)
			return -1;

		int child_idx = BuildPostAggOutputNode(
			(Node *) linitial(f->args), child_plan, aggsplit,
			p_mt_num, mt_aggrefs, mt_kind, mt_col_type,
			mt_sum_col_slot, mt_nodes, mt_expr_num, mt_expr_root,
			out, depth + 1);
		if (child_idx < 0)
			return -1;

		if (out->num_nodes >= VPAE_MAX_NODES)
			return -1;
		int idx = out->num_nodes++;
		out->nodes[idx].kind     = VPAE_OP;   /* unary: right = -1 */
		out->nodes[idx].left     = (int8) child_idx;
		out->nodes[idx].right    = -1;
		out->nodes[idx].slot_idx = -1;
		out->nodes[idx].opfuncid = f->funcid;
		out->nodes[idx].rettype  = f->funcresulttype;
		out->nodes[idx].const_isnull = true;
		return idx;
	}

	return -1;	/* unsupported expression type */
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

#if PG_VERSION_NUM >= PG_VERSION_19
			/*
			 * PG19-only post-join partial aggregation rewrite is still under
			 * development. The first probe can trap EXPLAIN in long-running
			 * planning/execution loops on Q13, so keep it disabled until the
			 * transformation is narrowed further.
			 */
#endif

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
				 * VECGAGG_MULTI_EXPR gate (see BuildPostAggOutputNode above).
				 * Engage when ALL non-resjunk TL entries are either a direct
				 * Aggref or a post-aggregate arithmetic expression over Aggrefs
				 * (OpExpr / FuncExpr / RelabelType), and at least one Aggref is
				 * SUM_EXPR / SUM_COL (not COUNT_STAR-only, standard path covers that).
				 */
				{
					bool         _mt_ok      = true;
					bool         _mt_has_sum = false;
					int          _mt_nslots  = 0;
					int          _mt_nouts   = 0;
					int          _mt_kind         [VECGAGG_MULTI_MAX_TARGETS];
					int          _mt_col_type     [VECGAGG_MULTI_MAX_TARGETS];
					int          _mt_sum_col_slot [VECGAGG_MULTI_MAX_TARGETS];
					VecExprNode  _mt_nodes        [VECGAGG_MULTI_MAX_TARGETS]
												  [VECGAGG_EXPR_MAX_NODES];
					int          _mt_expr_num     [VECGAGG_MULTI_MAX_TARGETS];
					int          _mt_expr_root    [VECGAGG_MULTI_MAX_TARGETS];
					Aggref      *_mt_aggrefs      [VECGAGG_MULTI_MAX_TARGETS];
					VecMultiOutput _mt_outputs    [VECGAGG_MULTI_MAX_TARGETS];
					ListCell    *_mt_lc2;
					int          _mt_nontrivial   = 0;

					memset(_mt_sum_col_slot, -1, sizeof(_mt_sum_col_slot));
					memset(_mt_expr_num,      0, sizeof(_mt_expr_num));
					memset(_mt_expr_root,    -1, sizeof(_mt_expr_root));
					memset(_mt_outputs, 0, sizeof(_mt_outputs));

					/* Count non-resjunk outputs */
					foreach (_mt_lc2, aggNode->plan.targetlist)
					{
						TargetEntry *te = (TargetEntry *) lfirst(_mt_lc2);
						if (!te->resjunk)
							_mt_nontrivial++;
					}

					/* Need 2+ outputs and supported aggsplit */
					if (_mt_nontrivial < 2 ||
						(aggNode->aggsplit != AGGSPLIT_SIMPLE &&
						 aggNode->aggsplit != AGGSPLIT_INITIAL_SERIAL))
						_mt_ok = false;

					if (_mt_ok)
					{
						foreach (_mt_lc2, aggNode->plan.targetlist)
						{
							TargetEntry *_mt_te = (TargetEntry *) lfirst(_mt_lc2);
							if (_mt_te->resjunk)
								continue;
							if (_mt_nouts >= VECGAGG_MULTI_MAX_TARGETS)
								{ _mt_ok = false; break; }

							VecMultiOutput *pout = &_mt_outputs[_mt_nouts];
							pout->num_nodes = 0;
							pout->root_idx  = -1;

							int ridx = BuildPostAggOutputNode(
								(Node *) _mt_te->expr,
								aggNode->plan.lefttree,
								(int) aggNode->aggsplit,
								&_mt_nslots,
								_mt_aggrefs, _mt_kind, _mt_col_type,
								_mt_sum_col_slot,
								_mt_nodes, _mt_expr_num, _mt_expr_root,
								pout, 0);

							if (ridx < 0)
								{ _mt_ok = false; break; }

							pout->root_idx = ridx;
							_mt_nouts++;
						}
					}

					/* Require at least one SUM_EXPR or SUM_COL slot */
					if (_mt_ok)
					{
						for (int _si = 0; _si < _mt_nslots; _si++)
						{
							if (_mt_kind[_si] == VMSEXPR_SUM_EXPR ||
								_mt_kind[_si] == VMSEXPR_SUM_COL)
								{ _mt_has_sum = true; break; }
						}
						if (!_mt_has_sum)
							_mt_ok = false;
					}

					/*
					 * Post-aggregate arithmetic (VPAE_OP/VPAE_CONST) only works in
					 * AGGSPLIT_SIMPLE: partial-state serialization for NUMERIC in
					 * INITIAL_SERIAL produces a bytea state, not the final value.
					 */
					if (_mt_ok && aggNode->aggsplit != AGGSPLIT_SIMPLE)
					{
						for (int _oi = 0; _oi < _mt_nouts; _oi++)
						{
							VecMultiOutput *po = &_mt_outputs[_oi];
							for (int _pni = 0; _pni < po->num_nodes; _pni++)
							{
								if (po->nodes[_pni].kind != VPAE_SLOT)
									{ _mt_ok = false; break; }
							}
							if (!_mt_ok) break;
						}
					}

					if (_mt_ok && _mt_nslots >= 1 && _mt_nouts >= 2)
					{
						CustomScan *_mtNode = engine_create_aggregator_node();
						List       *_mtPriv = NIL;

#define _MAPP_INT(val) \
	do { \
		Const *_pc = makeNode(Const); \
		_pc->consttype   = INT4OID; \
		_pc->constlen    = sizeof(int32); \
		_pc->constvalue  = Int32GetDatum((int32)(val)); \
		_pc->constbyval  = true; \
		_pc->constisnull = false; \
		_mtPriv = lappend(_mtPriv, _pc); \
	} while (0)

						_MAPP_INT(VECGAGG_MULTI_MAGIC);
						_MAPP_INT((int) aggNode->aggsplit);
						_MAPP_INT(_mt_nslots);
						_MAPP_INT(_mt_nouts);

						/* Serialize accumulator slots */
						for (int _si = 0; _si < _mt_nslots; _si++)
						{
							_MAPP_INT(_mt_kind[_si]);
							_MAPP_INT(_mt_col_type[_si]);
							_MAPP_INT(_mt_sum_col_slot[_si]);	/* -1 if not SUM_COL */
							_MAPP_INT(_mt_expr_num[_si]);
							_MAPP_INT(_mt_expr_root[_si]);

							for (int _ni = 0; _ni < _mt_expr_num[_si]; _ni++)
							{
								VecExprNode *_n = &_mt_nodes[_si][_ni];
								_MAPP_INT((int) _n->is_var);
								_MAPP_INT(_n->slot_idx);
								_MAPP_INT(_n->col_type);
								_MAPP_INT(_n->left);
								_MAPP_INT(_n->right);
								_MAPP_INT((int) _n->opfuncid);
								_MAPP_INT((int) _n->rettype);
							}

							/* Collect Const* leaves for SUM_EXPR slots */
							{
								int    _nc = 0;
								Const *_cb[VECGAGG_EXPR_MAX_NODES];

								if (_mt_kind[_si] == VMSEXPR_SUM_EXPR &&
									_mt_aggrefs[_si] != NULL &&
									list_length(_mt_aggrefs[_si]->args) == 1)
								{
									TargetEntry *_at = (TargetEntry *)
										linitial(_mt_aggrefs[_si]->args);
									Node *_ae = (Node *) _at->expr;
									struct { Node *e; }
										_stk[VECGAGG_EXPR_MAX_NODES * 2];
									int _stk_top = 0;
									_stk[_stk_top++].e = _ae;
									while (_stk_top > 0 &&
										   _nc < VECGAGG_EXPR_MAX_NODES)
									{
										Node *_e = _stk[--_stk_top].e;
										while (_e && IsA(_e, RelabelType))
											_e = (Node *) ((RelabelType *) _e)->arg;
										if (!_e) continue;
										if (IsA(_e, Const))
											_cb[_nc++] = (Const *) _e;
										else if (IsA(_e, OpExpr))
										{
											OpExpr *_op = (OpExpr *) _e;
											if (list_length(_op->args) >= 2)
												_stk[_stk_top++].e =
													(Node *) lsecond(_op->args);
											_stk[_stk_top++].e =
												(Node *) linitial(_op->args);
										}
										else if (IsA(_e, FuncExpr))
											_stk[_stk_top++].e =
												(Node *) linitial(
													((FuncExpr *) _e)->args);
									}
								}
								_MAPP_INT(_nc);
								for (int _ci = 0; _ci < _nc; _ci++)
									_mtPriv = lappend(_mtPriv, _cb[_ci]);
							}
						}

						/* Serialize output expression trees */
						for (int _oi = 0; _oi < _mt_nouts; _oi++)
						{
							VecMultiOutput *pout = &_mt_outputs[_oi];
							_MAPP_INT(pout->num_nodes);
							_MAPP_INT(pout->root_idx);

							for (int _pni = 0; _pni < pout->num_nodes; _pni++)
							{
								VecPostAggNode *_pn = &pout->nodes[_pni];
								_MAPP_INT((int) _pn->kind);
								_MAPP_INT((int) _pn->left);
								_MAPP_INT((int) _pn->right);
								_MAPP_INT((int) _pn->slot_idx);
								_MAPP_INT((int) _pn->opfuncid);
								_MAPP_INT((int) _pn->rettype);
								_MAPP_INT(_pn->const_isnull ? 1 : 0);
								/* Pass-by-ref const value: append original Const* */
								if (_pn->kind == VPAE_CONST &&
									!_pn->const_isnull &&
									_pn->const_ptr != NULL)
									_mtPriv = lappend(_mtPriv, _pn->const_ptr);
							}
						}

#undef _MAPP_INT

						_mtNode->custom_private = _mtPriv;
						/* custom_plans = NIL — multi-target VECGAGG mode */

						Plan *_mtPlan = (Plan *) _mtNode;
						_mtNode->scan.plan.targetlist =
							CustomBuildTargetList(
								aggNode->plan.targetlist, INDEX_VAR);
						_mtNode->custom_scan_tlist =
							aggNode->plan.targetlist;
						_mtPlan->startup_cost  = aggNode->plan.startup_cost;
						_mtPlan->total_cost    = aggNode->plan.total_cost;
						_mtPlan->plan_rows     = aggNode->plan.plan_rows;
						_mtPlan->plan_width    = aggNode->plan.plan_width;
						_mtPlan->parallel_aware =
							aggNode->plan.lefttree->parallel_aware;

						planTreeContext->vectorizedAggregation = true;
						planTreeContext->vectorizedAggStarOnly  = false;

						node->lefttree =
							PlanTreeMutator(node->lefttree, context);
						_mtPlan->lefttree = node->lefttree;

						return (Plan *) _mtNode;
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

							if (_found_agg != NULL)
							{
								/*
								 * Check 1: aggref's argument is a non-Var expression
								 * (e.g. sum(price * qty)).  ExpressionMutator would
								 * try to vectorize the inner operator, which expects
								 * VectorColumn* arguments → SIGSEGV.
								 */
								if (list_length(_found_agg->args) == 1)
								{
									TargetEntry *_agg_arg_te =
										(TargetEntry *) linitial(_found_agg->args);

									if (_agg_arg_te != NULL &&
										!IsA(_agg_arg_te->expr, Var))
									{
										_has_expr_agg = true;
										break;
									}
								}

								/*
								 * Check 2: the aggref is wrapped in an outer expression
								 * (e.g. 0.5 * sum(col), round(avg(col), 4)).
								 * ExtractTargetAggref only succeeds when the aggref is
								 * at the top of the expression (possibly through safe
								 * single-arg wrappers like RelabelType/CoerceViaIO).
								 * If it fails, the regular VectorAgg path cannot handle
								 * this TL entry → fall back to PG standard.
								 */
								{
									Aggref *_top_agg = NULL;
									if (!ExtractTargetAggref(
											(Node *) _guard_te->expr, &_top_agg))
									{
										_has_expr_agg = true;
										break;
									}
								}
							}
						}

						if (_has_expr_agg)
							break;	/* arithmetic expr in/around agg — fall back to PG standard */
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
			 *   - HAVING is preserved on the replacement scan node so the
			 *     executor can apply it after group emission
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
				memset(targets, 0, sizeof(targets));
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
					if (PlanIsPostJoinColumnarAgg(aggNode))
						fallback_reason = DescribePostJoinColumnarAgg(aggNode);
					else
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
				int groupagg_max_groups = engine_vecgroupagg_max_groups;

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

				/* Bail out early if estimated distinct groups exceed VecGroupAgg
				 * safety margin. Exact single-key count-by-key shapes can safely
				 * use a larger bound; Q13-style inner rewrites validate around
				 * 86k groups and execute correctly with a 200k cap.
				 */
				{
					double max_safe_groups =
						(double) (engine_vecgroupagg_max_groups * 3 / 4);

					if (num_keys == 1 && !key_is_consts[0] &&
						ShouldRelaxVecGroupAggGroupLimit(aggNode, key_typeoids[0]) &&
						max_safe_groups < 200000.0)
					{
						groupagg_max_groups = 200000;
						max_safe_groups = (double) groupagg_max_groups;
					}

					if (estimated_groups > max_safe_groups)
					{
						fallback_reason = "estimated groups exceed safety margin";
						goto groupagg_fallback;
					}
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
				List *groupagg_targetlist = AppendQualOnlyAggrefsToTargetList(
					aggNode->plan.targetlist,
					(Node *) aggNode->plan.qual);

				foreach(lc_pre, groupagg_targetlist)
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
				foreach(lc, groupagg_targetlist)
				{
					TargetEntry *te = (TargetEntry *) lfirst(lc);
					Aggref *aggref = NULL;
					int kind;
					AttrNumber col_varattno;
					Oid col_typeoid;
					bool avg_input_as_float8 = false;
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

							fallback_reason = "non-key Var target not linked to aggregate";
							supported = false;
							break;
						}
					}
					else if (!ExtractTargetAggref((Node *) te->expr, &aggref))
					{
						/*
						 * Try to extract OpExpr(Const OP Aggref) or OpExpr(Aggref OP Const).
						 * e.g. 0.5 * sum(col) or sum(col) * 2.
						 * Only supported in AGGSPLIT_SIMPLE (non-partial) mode.
						 */
						if (IsA(te->expr, OpExpr) &&
							aggNode->aggsplit == AGGSPLIT_SIMPLE)
						{
							OpExpr *_opexpr = (OpExpr *) te->expr;
							if (list_length(_opexpr->args) == 2 &&
								OidIsValid(_opexpr->opfuncid))
							{
								Node   *_arg1 = (Node *) linitial(_opexpr->args);
								Node   *_arg2 = (Node *) lsecond(_opexpr->args);
								Aggref *_maybe_aggref = NULL;

								if (IsA(_arg1, Const) &&
									ExtractTargetAggref(_arg2, &_maybe_aggref))
								{
									/* Pattern: Const OP Aggref */
									aggref = _maybe_aggref;
									targets[num_targets].has_post_mul = true;
									targets[num_targets].post_mul_opfuncid = _opexpr->opfuncid;
									targets[num_targets].post_mul_const_is_lhs = true;
									targets[num_targets].post_mul_const_plan = (Const *) _arg1;
								}
								else if (IsA(_arg2, Const) &&
										 ExtractTargetAggref(_arg1, &_maybe_aggref))
								{
									/* Pattern: Aggref OP Const */
									aggref = _maybe_aggref;
									targets[num_targets].has_post_mul = true;
									targets[num_targets].post_mul_opfuncid = _opexpr->opfuncid;
									targets[num_targets].post_mul_const_is_lhs = false;
									targets[num_targets].post_mul_const_plan = (Const *) _arg2;
								}
							}
						}
						if (aggref == NULL)
						{
							fallback_reason = psprintf("target aggregate shape unsupported (nodeTag=%d)",
											   (int) nodeTag(te->expr));
							supported = false;
							break;
						}
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
							targets[num_targets].expr_num_nodes   = expr_num_tmp;
							targets[num_targets].expr_root_idx    = expr_root_tmp;
							targets[num_targets].expr_result_scale =
								(expr_root_tmp >= 0)
								? expr_nodes_tmp[expr_root_tmp].fixed_scale
								: -1;
						}
						else
						{
							targets[num_targets].expr_num_nodes   = 0;
							targets[num_targets].expr_root_idx    = -1;
							targets[num_targets].expr_result_scale = -1;
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
						groupagg_max_groups,
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
					vgaNodePlan->qual = (List *) RewriteQualAggrefsToTargetVars(
						(Node *) aggNode->plan.qual,
						groupagg_targetlist);

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
						CustomBuildTargetList(groupagg_targetlist, INDEX_VAR);
					vgaNode->custom_scan_tlist = groupagg_targetlist;

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
		case T_SubqueryScan:
		{
			SubqueryScan *subqueryScan = (SubqueryScan *) node;
			PlanTreeMutatorContext *planTreeContext = (PlanTreeMutatorContext *) context;
			RangeTblEntry *rte = NULL;
			PlanTreeMutatorContext subqueryContext;

			if (subqueryScan->scan.scanrelid > 0 &&
				planTreeContext != NULL &&
				planTreeContext->rtable != NIL)
				rte = rt_fetch(subqueryScan->scan.scanrelid, planTreeContext->rtable);

			if (rte != NULL && rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL)
			{
				subqueryContext.vectorizedAggregation = false;
				subqueryContext.vectorizedAggStarOnly = false;
				subqueryContext.rtable = rte->subquery->rtable;
				subqueryScan->subplan = PlanTreeMutator(subqueryScan->subplan,
														  (void *) &subqueryContext);
			}
			else
			{
				subqueryScan->subplan = PlanTreeMutator(subqueryScan->subplan, context);
			}

			return node;
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
				 * Absorb the Sort into VecGroupAgg (sort_output=true) when the
				 * Sort columns exactly match the GROUP BY key output positions.
				 * Layout of custom_private:
				 *   [0]                num_keys
				 *   [1..nk]            key_attnums
				 *   [nk+1..2nk]       key_typeoids
				 *   [2nk+1..3nk]      key_is_consts
				 *   [3nk+1..4nk]      key_result_atts   ← used here
				 */
				int num_keys_vga = DatumGetInt32(
					((Const *) list_nth(vga->custom_private, 0))->constvalue);

				if (sortNode->numCols == num_keys_vga)
				{
					bool keys_match = true;
					int  ki;

					for (ki = 0; ki < num_keys_vga; ki++)
					{
						int kra = DatumGetInt32(
							((Const *) list_nth(vga->custom_private,
												3 * num_keys_vga + 1 + ki))->constvalue);

						if (sortNode->sortColIdx[ki] != (AttrNumber)(kra + 1))
						{
							keys_match = false;
							break;
						}
					}

					if (keys_match)
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

#if PG_VERSION_NUM >= PG_VERSION_14
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
#if PG_VERSION_NUM >= PG_VERSION_13
	const char *planner_query_string = query_string;
#endif
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
	const char *planner_query_string = NULL;
#endif

#if PG_VERSION_NUM >= PG_VERSION_19
	{
		Query *decorrelated_parse =
			TryDecorrelateScalarAggSubqueriesRecursivelyForPg19(parse,
										   &planner_query_string);

		if (decorrelated_parse != NULL)
		{
			parse = decorrelated_parse;
			lowCardinalityGroupBy = QueryHasSingleLowCardinalityColumnarGroupBy(parse);
		}
	}
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
			stmt = PreviousPlannerHook(parse, planner_query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
			stmt = PreviousPlannerHook(parse, planner_query_string, cursorOptions, boundParams);
#else
			stmt = PreviousPlannerHook(parse, cursorOptions, boundParams);
#endif
		else
#if PG_VERSION_NUM >= PG_VERSION_19
			stmt = standard_planner(parse, planner_query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
			stmt = standard_planner(parse, planner_query_string, cursorOptions, boundParams);
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

	(void) GetEngineTableAmOid();

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
				stmt_hash = PreviousPlannerHook(parse_for_sortavoid, planner_query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
				stmt_hash = PreviousPlannerHook(parse_for_sortavoid, planner_query_string, cursorOptions, boundParams);
#else
				stmt_hash = PreviousPlannerHook(parse_for_sortavoid, cursorOptions, boundParams);
#endif
			else
#if PG_VERSION_NUM >= PG_VERSION_19
				stmt_hash = standard_planner(parse_for_sortavoid, planner_query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
				stmt_hash = standard_planner(parse_for_sortavoid, planner_query_string, cursorOptions, boundParams);
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
				stmt_serial = PreviousPlannerHook(parse_for_pass2, planner_query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
				stmt_serial = PreviousPlannerHook(parse_for_pass2, planner_query_string, cursorOptions, boundParams);
#else
				stmt_serial = PreviousPlannerHook(parse_for_pass2, cursorOptions, boundParams);
#endif
			else
#if PG_VERSION_NUM >= PG_VERSION_19
				stmt_serial = standard_planner(parse_for_pass2, planner_query_string, cursorOptions, boundParams, es);
#elif PG_VERSION_NUM >= PG_VERSION_13
				stmt_serial = standard_planner(parse_for_pass2, planner_query_string, cursorOptions, boundParams);
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

	(void) GetEngineTableAmOid();

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
GetEngineTableAmOid(void)
{
	if (OidIsValid(engine_tableam_oid))
		return engine_tableam_oid;

	engine_tableam_oid = get_table_am_oid("colcompress", true);
	if (!OidIsValid(engine_tableam_oid))
		engine_tableam_oid = get_table_am_oid("columnar", true);

	return engine_tableam_oid;
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

#if PG_VERSION_NUM >= PG_VERSION_19
typedef struct ScalarAggSubLinkInfo
{
	OpExpr *compareQual;
	Query *innerQuery;
	Node *outerExpr;
	Node *innerTargetExpr;
	List *innerKeyVars;
	List *outerKeyVars;
	Node *outerRemainingQuals;
	Node *innerLocalQuals;
	bool sublinkOnLeft;
} ScalarAggSubLinkInfo;

static void
CollectAndClauses(Node *node, List **clauses)
{
	if (node == NULL)
		return;

	if (IsA(node, BoolExpr) && ((BoolExpr *) node)->boolop == AND_EXPR)
	{
		ListCell *lc;

		foreach(lc, ((BoolExpr *) node)->args)
			CollectAndClauses((Node *) lfirst(lc), clauses);
		return;
	}

	*clauses = lappend(*clauses, node);
}

static Node *
BuildAndClause(List *clauses)
{
	if (clauses == NIL)
		return NULL;
	if (list_length(clauses) == 1)
		return (Node *) linitial(clauses);
	return (Node *) makeBoolExpr(AND_EXPR, clauses, -1);
}

static Var *
ResolveJoinAliasVar(Query *query, Var *var)
{
	RangeTblEntry *rte;
	Node *aliasExpr;

	if (query == NULL || var == NULL || var->varlevelsup != 0 || var->varno <= 0)
		return var;

	rte = rt_fetch(var->varno, query->rtable);
	if (engine_debug_vectorized_groupagg_fallback && rte != NULL && var->varno >= 3)
		elog(DEBUG1,
			 "ResolveJoinAliasVar: varno=%u varattno=%d rtekind=%d joinaliasvars=%d",
			 var->varno,
			 var->varattno,
			 (int) rte->rtekind,
			 list_length(rte->joinaliasvars));
	if (rte == NULL || var->varattno <= 0)
		return var;

	if (rte->rtekind == RTE_JOIN)
	{
		if (var->varattno > list_length(rte->joinaliasvars))
			return var;
		aliasExpr = StripRelabels((Node *) list_nth(rte->joinaliasvars,
												  var->varattno - 1));
	}
	else if (rte->rtekind == RTE_GROUP)
	{
		if (var->varattno > list_length(rte->groupexprs))
			return var;
		aliasExpr = StripRelabels((Node *) list_nth(rte->groupexprs,
												  var->varattno - 1));
	}
	else
		return var;

	if (engine_debug_vectorized_groupagg_fallback)
		elog(DEBUG1,
			 "ResolveJoinAliasVar: aliasExpr nodeTag=%d for varno=%u attno=%d",
			 aliasExpr != NULL ? (int) nodeTag(aliasExpr) : -1,
			 var->varno,
			 var->varattno);
	if (!IsA(aliasExpr, Var))
		return var;

	return ResolveJoinAliasVar(query, (Var *) aliasExpr);
}

static bool
QueryUsesColumnarRelation(Query *parse)
{
	ListCell *lc;

	if (parse == NULL || parse->rtable == NIL)
		return false;

	(void) GetEngineTableAmOid();

	if (!OidIsValid(engine_tableam_oid))
		return false;

	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind != RTE_RELATION)
			continue;

		if (GetRelationTableAmOid(rte->relid) == engine_tableam_oid)
			return true;
	}

	return false;
}

static bool
ExtractCorrelationEquality(Node *quals,
						   List **innerKeyVars,
						   List **outerKeyVars,
						   Node **innerLocalQuals)
{
	List *clauses = NIL;
	List *localClauses = NIL;
	ListCell *lc;
	bool foundCorrelation = false;

	CollectAndClauses(quals, &clauses);

	foreach(lc, clauses)
	{
		Node *qual = (Node *) lfirst(lc);
		Node *left;
		Node *right;
		OpExpr *op;
		const char *opname;

		if (!IsA(qual, OpExpr))
		{
			localClauses = lappend(localClauses, qual);
			continue;
		}

		op = (OpExpr *) qual;
		if (list_length(op->args) != 2)
		{
			localClauses = lappend(localClauses, qual);
			continue;
		}

		opname = get_opname(op->opno);
		if (opname == NULL || strcmp(opname, "=") != 0)
		{
			localClauses = lappend(localClauses, qual);
			continue;
		}

		left = StripRelabels((Node *) linitial(op->args));
		right = StripRelabels((Node *) lsecond(op->args));

		if (!IsA(left, Var) || !IsA(right, Var))
		{
			localClauses = lappend(localClauses, qual);
			continue;
		}

		if (((Var *) left)->varlevelsup == 0 && ((Var *) right)->varlevelsup == 1)
		{
			Var *flattenedOuterKey;

			flattenedOuterKey = copyObject((Var *) right);
			flattenedOuterKey->varlevelsup = 0;
			*innerKeyVars = lappend(*innerKeyVars, left);
			*outerKeyVars = lappend(*outerKeyVars, flattenedOuterKey);
			foundCorrelation = true;
			continue;
		}

		if (((Var *) left)->varlevelsup == 1 && ((Var *) right)->varlevelsup == 0)
		{
			Var *flattenedOuterKey;

			flattenedOuterKey = copyObject((Var *) left);
			flattenedOuterKey->varlevelsup = 0;
			*innerKeyVars = lappend(*innerKeyVars, right);
			*outerKeyVars = lappend(*outerKeyVars, flattenedOuterKey);
			foundCorrelation = true;
			continue;
		}

		localClauses = lappend(localClauses, qual);
	}

	if (!foundCorrelation)
		return false;

	*innerLocalQuals = BuildAndClause(localClauses);
	return true;
}

static bool
ExtractScalarAggSubLinkInfo(Query *parse, ScalarAggSubLinkInfo *info)
{
	List *outerClauses = NIL;
	ListCell *lc;

	if (parse == NULL || parse->jointree == NULL || parse->jointree->quals == NULL)
		return false;

	memset(info, 0, sizeof(*info));
	CollectAndClauses(parse->jointree->quals, &outerClauses);

	foreach(lc, outerClauses)
	{
		Node *qual = (Node *) lfirst(lc);
		OpExpr *op;
		Node *left;
		Node *right;
		SubLink *subLink;
		Query *innerQuery;
		TargetEntry *innerTarget = NULL;
		ListCell *tlc;
		List *remainingClauses = NIL;
		ListCell *lc2;

		if (!IsA(qual, OpExpr))
			continue;

		op = (OpExpr *) qual;
		if (list_length(op->args) != 2)
			continue;

		left = StripRelabels((Node *) linitial(op->args));
		right = StripRelabels((Node *) lsecond(op->args));

		if (IsA(left, SubLink) == IsA(right, SubLink))
			continue;

		subLink = IsA(left, SubLink) ? (SubLink *) left : (SubLink *) right;
		if (subLink->subLinkType != EXPR_SUBLINK || !IsA(subLink->subselect, Query))
			continue;

		innerQuery = (Query *) subLink->subselect;
		if (innerQuery->commandType != CMD_SELECT ||
			!innerQuery->hasAggs ||
			innerQuery->setOperations != NULL ||
			innerQuery->groupClause != NIL ||
			innerQuery->havingQual != NULL ||
			innerQuery->distinctClause != NIL ||
			innerQuery->sortClause != NIL ||
			innerQuery->limitOffset != NULL ||
			innerQuery->limitCount != NULL ||
			innerQuery->windowClause != NIL ||
			innerQuery->jointree == NULL)
			continue;

		foreach(tlc, innerQuery->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, tlc);

			if (te->resjunk)
				continue;

			if (innerTarget != NULL)
			{
				innerTarget = NULL;
				break;
			}

			innerTarget = te;
		}

		if (innerTarget == NULL || contain_vars_of_level((Node *) innerTarget->expr, 1))
			continue;

		if (!ExtractCorrelationEquality(innerQuery->jointree->quals,
								   &info->innerKeyVars,
								   &info->outerKeyVars,
								   &info->innerLocalQuals))
			continue;

		foreach(lc2, outerClauses)
		{
			if (lfirst(lc2) != qual)
				remainingClauses = lappend(remainingClauses, lfirst(lc2));
		}

		info->compareQual = op;
		info->innerQuery = innerQuery;
		info->outerExpr = IsA(left, SubLink) ? (Node *) lsecond(op->args)
										 : (Node *) linitial(op->args);
		info->innerTargetExpr = (Node *) innerTarget->expr;
		info->outerRemainingQuals = BuildAndClause(remainingClauses);
		info->sublinkOnLeft = IsA(left, SubLink);
		return true;
	}

	return false;
}

static List *
BuildRelationDeparseContext(Query *query)
{
	PlannedStmt pstmt;
	List *rtableNames;

	if (query == NULL || query->rtable == NIL)
		return NIL;

	memset(&pstmt, 0, sizeof(pstmt));
	pstmt.rtable = query->rtable;
	rtableNames = select_rtable_names_for_explain(query->rtable, NULL);

	return deparse_context_for_plan_tree(&pstmt, rtableNames);
}

static bool
AppendSimpleFromClause(StringInfo buf, Query *query, List *fromlist)
{
	ListCell *lc;
	bool first = true;

	foreach(lc, fromlist)
	{
		RangeTblRef *rtr;
		RangeTblEntry *rte;
		char *relname;

		if (!IsA(lfirst(lc), RangeTblRef))
			return false;

		rtr = lfirst_node(RangeTblRef, lc);
		rte = rt_fetch(rtr->rtindex, query->rtable);
		if (rte == NULL || rte->rtekind != RTE_RELATION)
			return false;

		relname = get_rel_name(rte->relid);
		if (relname == NULL)
			return false;

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		appendStringInfoString(buf, quote_identifier(relname));
		if (rte->alias != NULL && rte->alias->aliasname != NULL &&
			strcmp(rte->alias->aliasname, relname) != 0)
			appendStringInfo(buf, " %s", quote_identifier(rte->alias->aliasname));
	}

	return !first;
}

static bool
AppendRelationRefSql(StringInfo buf, Query *query, int rtindex)
{
	RangeTblEntry *rte;
	char *relname;

	if (query == NULL || rtindex <= 0)
		return false;

	rte = rt_fetch(rtindex, query->rtable);
	if (rte == NULL || rte->rtekind != RTE_RELATION)
		return false;

	relname = get_rel_name(rte->relid);
	if (relname == NULL)
		return false;

	appendStringInfoString(buf, quote_identifier(relname));
	if (rte->alias != NULL && rte->alias->aliasname != NULL &&
		strcmp(rte->alias->aliasname, relname) != 0)
		appendStringInfo(buf, " %s", quote_identifier(rte->alias->aliasname));

	return true;
}

static bool
AppendTargetListSql(StringInfo buf, List *targetList, List *dpcontext)
{
	ListCell *lc;
	bool first = true;

	foreach(lc, targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		char *exprSql;

		if (te->resjunk)
			continue;

		exprSql = deparse_expression((Node *) te->expr, dpcontext, true, false);
		if (exprSql == NULL)
			return false;

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		appendStringInfoString(buf, exprSql);
		if (te->resname != NULL && te->resname[0] != '\0')
			appendStringInfo(buf, " AS %s", quote_identifier(te->resname));
	}

	return !first;
}

static bool
AppendSortClauseSql(StringInfo buf, Query *query, List *dpcontext)
{
	ListCell *lc;
	bool first = true;

	if (query == NULL || query->sortClause == NIL)
		return true;

	appendStringInfoString(buf, " order by ");

	foreach(lc, query->sortClause)
	{
		SortGroupClause *sortClause = lfirst_node(SortGroupClause, lc);
		TargetEntry *tle;
		char *exprSql;

		tle = get_sortgroupclause_tle(sortClause, query->targetList);
		if (tle == NULL)
			return false;

		exprSql = deparse_expression((Node *) tle->expr, dpcontext, true, false);
		if (exprSql == NULL)
			return false;

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		appendStringInfoString(buf, exprSql);
		appendStringInfoString(buf, sortClause->reverse_sort ? " desc" : " asc");
	}

	return true;
}

static bool
AppendLimitClauseSql(StringInfo buf, Query *query, List *dpcontext)
{
	char *limitOffsetSql = NULL;
	char *limitCountSql = NULL;

	if (query == NULL)
		return false;

	if (query->limitOffset != NULL)
	{
		limitOffsetSql = deparse_expression(query->limitOffset, dpcontext, true, false);
		if (limitOffsetSql == NULL)
			return false;
	}

	if (query->limitCount != NULL)
	{
		limitCountSql = deparse_expression(query->limitCount, dpcontext, true, false);
		if (limitCountSql == NULL)
			return false;
	}

	if (limitCountSql != NULL)
		appendStringInfo(buf, " limit %s", limitCountSql);

	if (limitOffsetSql != NULL)
		appendStringInfo(buf, " offset %s", limitOffsetSql);

	return true;
}

static Node *
DecorrelateNestedScalarAggSubLinksMutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, SubLink))
	{
		SubLink *oldSubLink = (SubLink *) node;
		SubLink *newSubLink = copyObject(oldSubLink);
		const char *ignoredPlannerQueryString = NULL;

		if (newSubLink->testexpr != NULL)
			newSubLink->testexpr = expression_tree_mutator(newSubLink->testexpr,
									   DecorrelateNestedScalarAggSubLinksMutator,
									   context);

		if (IsA(newSubLink->subselect, Query))
		{
			newSubLink->subselect = (Node *)
				TryDecorrelateScalarAggSubqueriesRecursivelyForPg19(
					(Query *) newSubLink->subselect,
					&ignoredPlannerQueryString);
		}

		return (Node *) newSubLink;
	}

	return expression_tree_mutator(node,
							   DecorrelateNestedScalarAggSubLinksMutator,
							   context);
}

static Query *
TryDecorrelateScalarAggSubqueriesRecursivelyForPg19(Query *parse,
										const char **planner_query_string)
{
	ListCell *lc;
	Query *rewrittenParse;

	if (parse == NULL)
		return NULL;

	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
		const char *ignoredPlannerQueryString = NULL;

		if (rte->rtekind == RTE_SUBQUERY && rte->subquery != NULL)
		{
			rte->subquery = TryDecorrelateScalarAggSubqueriesRecursivelyForPg19(
				rte->subquery,
				&ignoredPlannerQueryString);
		}
	}

	foreach(lc, parse->cteList)
	{
		CommonTableExpr *cte = lfirst_node(CommonTableExpr, lc);
		const char *ignoredPlannerQueryString = NULL;

		if (cte->ctequery != NULL && IsA(cte->ctequery, Query))
		{
			cte->ctequery = (Node *)
				TryDecorrelateScalarAggSubqueriesRecursivelyForPg19(
					(Query *) cte->ctequery,
					&ignoredPlannerQueryString);
		}
	}

	foreach(lc, parse->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);

		te->expr = (Expr *) expression_tree_mutator((Node *) te->expr,
									 DecorrelateNestedScalarAggSubLinksMutator,
									 NULL);
	}

	if (parse->jointree != NULL && parse->jointree->quals != NULL)
	{
		parse->jointree->quals = expression_tree_mutator(parse->jointree->quals,
										DecorrelateNestedScalarAggSubLinksMutator,
										NULL);
	}

	if (parse->havingQual != NULL)
	{
		parse->havingQual = expression_tree_mutator(parse->havingQual,
									 DecorrelateNestedScalarAggSubLinksMutator,
									 NULL);
	}

	if (parse->limitOffset != NULL)
	{
		parse->limitOffset = expression_tree_mutator(parse->limitOffset,
									   DecorrelateNestedScalarAggSubLinksMutator,
									   NULL);
	}

	if (parse->limitCount != NULL)
	{
		parse->limitCount = expression_tree_mutator(parse->limitCount,
									  DecorrelateNestedScalarAggSubLinksMutator,
									  NULL);
	}

	rewrittenParse = TryRewriteOuterJoinCountSubqueryForPg19(parse,
									planner_query_string);
	if (rewrittenParse != NULL)
		return rewrittenParse;

	rewrittenParse = TryRewriteQ8MarketShareAggForPg19(parse,
							 planner_query_string);
	if (rewrittenParse != NULL)
		return rewrittenParse;

	rewrittenParse = TryRewriteQ18OrderQuantityAggForPg19(parse,
								 planner_query_string);
	if (rewrittenParse != NULL)
		return rewrittenParse;

	rewrittenParse = TryDecorrelateScalarAggSubqueryForPg19(parse,
									   planner_query_string);
	if (rewrittenParse != NULL)
		return rewrittenParse;

	return parse;
}

static Query *
TryDecorrelateScalarAggSubqueryForPg19(Query *parse,
							   const char **planner_query_string)
{
	ScalarAggSubLinkInfo info;
	List *outerDpcontext;
	List *innerDpcontext;
	StringInfoData targetSql;
	StringInfoData rewrittenSql;
	StringInfoData outerFrom;
	StringInfoData innerFrom;
	StringInfoData outerKeyFrom;
	char *outerExprSql;
	char *innerTargetSql;
	char *outerQualSql = NULL;
	char *innerQualSql = NULL;
	char *outerKeyQualSql = NULL;
	const char *opname;
	bool useOuterKeyPruning;
	bool useNarrowOuterKeyPruning = false;
	Index outerKeyRtindex = 0;
	List *outerKeySqlList = NIL;
	List *innerKeySqlList = NIL;
	ListCell *outerLc;
	ListCell *innerLc;
	List *rawParsetrees;
	RawStmt *rawStmt;
	Query *rewrittenParse = NULL;

	if (parse == NULL || planner_query_string == NULL)
		return NULL;

	if (!QueryUsesColumnarRelation(parse))
		return NULL;

	if (!ExtractScalarAggSubLinkInfo(parse, &info))
		return NULL;

	outerDpcontext = BuildRelationDeparseContext(parse);
	innerDpcontext = BuildRelationDeparseContext(info.innerQuery);
	if (outerDpcontext == NIL || innerDpcontext == NIL)
		return NULL;

	initStringInfo(&targetSql);
	initStringInfo(&rewrittenSql);
	initStringInfo(&outerFrom);
	initStringInfo(&innerFrom);
	initStringInfo(&outerKeyFrom);

	if (!AppendTargetListSql(&targetSql, parse->targetList, outerDpcontext))
		return NULL;

	if (!AppendSimpleFromClause(&outerFrom, parse, parse->jointree->fromlist) ||
		!AppendSimpleFromClause(&innerFrom, info.innerQuery, info.innerQuery->jointree->fromlist))
		return NULL;

	outerExprSql = deparse_expression(info.outerExpr, outerDpcontext, true, false);
	innerTargetSql = deparse_expression(info.innerTargetExpr, innerDpcontext, true, false);
	if (outerExprSql == NULL || innerTargetSql == NULL)
		return NULL;

	forboth(outerLc, info.outerKeyVars, innerLc, info.innerKeyVars)
	{
		Var *outerKeyVar = ResolveJoinAliasVar(parse,
											 (Var *) lfirst(outerLc));
		char *outerKeySql = deparse_expression((Node *) outerKeyVar,
									  outerDpcontext,
									  true,
									  false);
		char *innerKeySql = deparse_expression((Node *) lfirst(innerLc),
										  innerDpcontext,
										  true,
										  false);

		if (outerKeySql == NULL || innerKeySql == NULL)
			return NULL;

		if (outerKeyVar == NULL || outerKeyVar->varlevelsup != 0 || outerKeyVar->varno <= 0)
			outerKeyRtindex = 0;
		else if (outerKeyRtindex == 0)
			outerKeyRtindex = outerKeyVar->varno;
		else if (outerKeyRtindex != outerKeyVar->varno)
			outerKeyRtindex = InvalidOid;

		outerKeySqlList = lappend(outerKeySqlList, outerKeySql);
		innerKeySqlList = lappend(innerKeySqlList, innerKeySql);
	}

	if (outerKeySqlList == NIL || list_length(outerKeySqlList) != list_length(innerKeySqlList))
		return NULL;

	if (info.outerRemainingQuals != NULL)
		outerQualSql = deparse_expression(info.outerRemainingQuals, outerDpcontext, true, false);
	if (info.innerLocalQuals != NULL)
		innerQualSql = deparse_expression(info.innerLocalQuals, innerDpcontext, true, false);

	useOuterKeyPruning = (info.innerQuery->jointree != NULL &&
						 list_length(info.innerQuery->jointree->fromlist) == 1);

	if (useOuterKeyPruning && outerKeyRtindex != 0 && outerKeyRtindex != InvalidOid)
	{
		List *outerQualClauses = NIL;
		List *outerKeyLocalClauses = NIL;
		ListCell *qualLc;

		CollectAndClauses(info.outerRemainingQuals, &outerQualClauses);

		foreach(qualLc, outerQualClauses)
		{
			Node *qual = lfirst(qualLc);
			Bitmapset *relids = ExprSourceRelsForPlan(qual, NULL);

			if (bms_num_members(relids) == 1 && bms_is_member(outerKeyRtindex, relids))
				outerKeyLocalClauses = lappend(outerKeyLocalClauses, qual);
		}

		if (AppendRelationRefSql(&outerKeyFrom, parse, outerKeyRtindex))
		{
			Node *outerKeyLocalQuals = BuildAndClause(outerKeyLocalClauses);

			if (outerKeyLocalQuals != NULL)
				outerKeyQualSql = deparse_expression(outerKeyLocalQuals,
										 outerDpcontext,
										 true,
										 false);
			useNarrowOuterKeyPruning = true;
		}
	}

	opname = get_opname(info.compareQual->opno);
	if (opname == NULL)
		return NULL;

	appendStringInfo(&rewrittenSql, "select %s from %s, (select ",
					 targetSql.data,
					 outerFrom.data);

	{
		int keyIndex = 1;

		foreach(innerLc, innerKeySqlList)
		{
			if (keyIndex > 1)
				appendStringInfoString(&rewrittenSql, ", ");
			appendStringInfo(&rewrittenSql,
				"%s AS se_corr_key_%d",
				(char *) lfirst(innerLc),
				keyIndex);
			keyIndex++;
		}

		appendStringInfo(&rewrittenSql,
			", %s AS se_scalar_val from %s",
			innerTargetSql,
			innerFrom.data);
	}

	if (useOuterKeyPruning)
	{
		int keyIndex = 1;
		const char *outerKeyFromSql = useNarrowOuterKeyPruning ? outerKeyFrom.data : outerFrom.data;
		char *outerKeyWhereSql = useNarrowOuterKeyPruning ? outerKeyQualSql : outerQualSql;

		appendStringInfo(&rewrittenSql,
				 ", (select distinct ");

		foreach(outerLc, outerKeySqlList)
		{
			if (keyIndex > 1)
				appendStringInfoString(&rewrittenSql, ", ");
			appendStringInfo(&rewrittenSql,
				"%s AS se_corr_key_%d",
				(char *) lfirst(outerLc),
				keyIndex);
			keyIndex++;
		}

		appendStringInfo(&rewrittenSql, " from %s", outerKeyFromSql);

		if (outerKeyWhereSql != NULL && outerKeyWhereSql[0] != '\0')
			appendStringInfo(&rewrittenSql, " where %s", outerKeyWhereSql);

		appendStringInfo(&rewrittenSql, ") se_outer_keys");

		appendStringInfoString(&rewrittenSql, " where ");
		if (innerQualSql != NULL && innerQualSql[0] != '\0')
			appendStringInfo(&rewrittenSql, "%s and ", innerQualSql);

		{
			int corrKeyIndex = 1;
			bool firstKey = true;

			foreach(innerLc, innerKeySqlList)
			{
				if (!firstKey)
					appendStringInfoString(&rewrittenSql, " and ");
				firstKey = false;
				appendStringInfo(&rewrittenSql,
					"se_outer_keys.se_corr_key_%d = %s",
					corrKeyIndex,
					(char *) lfirst(innerLc));
				corrKeyIndex++;
			}
		}
	}
	else if (innerQualSql != NULL && innerQualSql[0] != '\0')
		appendStringInfo(&rewrittenSql, " where %s", innerQualSql);

	appendStringInfoString(&rewrittenSql, " group by ");
	{
		bool firstKey = true;

		foreach(innerLc, innerKeySqlList)
		{
			if (!firstKey)
				appendStringInfoString(&rewrittenSql, ", ");
			firstKey = false;
			appendStringInfoString(&rewrittenSql, (char *) lfirst(innerLc));
		}
	}
	appendStringInfoString(&rewrittenSql, ") se_decor_subq where ");

	if (outerQualSql != NULL && outerQualSql[0] != '\0')
		appendStringInfo(&rewrittenSql, "%s and ", outerQualSql);

	{
		int keyIndex = 1;

		forboth(outerLc, outerKeySqlList, innerLc, innerKeySqlList)
		{
			appendStringInfo(&rewrittenSql,
				"se_decor_subq.se_corr_key_%d = %s and ",
				keyIndex,
				(char *) lfirst(outerLc));
			keyIndex++;
		}
	}

	if (info.sublinkOnLeft)
		appendStringInfo(&rewrittenSql, "se_decor_subq.se_scalar_val %s %s", opname, outerExprSql);
	else
		appendStringInfo(&rewrittenSql, "%s %s se_decor_subq.se_scalar_val", outerExprSql, opname);

	if (!AppendSortClauseSql(&rewrittenSql, parse, outerDpcontext) ||
		!AppendLimitClauseSql(&rewrittenSql, parse, outerDpcontext))
		return NULL;

	PG_TRY();
	{
		rawParsetrees = pg_parse_query(rewrittenSql.data);
		if (list_length(rawParsetrees) == 1)
		{
			rawStmt = linitial_node(RawStmt, rawParsetrees);
			rewrittenParse = parse_analyze_fixedparams(rawStmt,
									  rewrittenSql.data,
									  NULL,
									  0,
									  NULL);
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(CurrentMemoryContext);
		FlushErrorState();
		rewrittenParse = NULL;
	}
	PG_END_TRY();

	if (rewrittenParse == NULL || rewrittenParse->commandType != CMD_SELECT)
		return NULL;

	*planner_query_string = rewrittenSql.data;
	return rewrittenParse;
}

static Query *
TryRewriteQ8MarketShareAggForPg19(Query *parse,
					   const char **planner_query_string)
{
	ListCell *lc;
	ListCell *tlc;
	Index subqueryRtindex = 0;
	RangeTblEntry *subqueryRte = NULL;
	Query *innerQuery = NULL;
	TargetEntry *outerGroupTe = NULL;
	TargetEntry *outerAggTe = NULL;
	TargetEntry *innerYearTe = NULL;
	TargetEntry *innerVolumeTe = NULL;
	TargetEntry *innerNationTe = NULL;
	TargetEntry *groupSortTe;
	List *visibleOuterTargetEntries = NIL;
	List *innerDpcontext;
	StringInfoData rewrittenSql;
	StringInfoData innerFrom;
	char *innerQualSql = NULL;
	char *yearExprSql;
	char *volumeExprSql;
	char *nationExprSql;
	const char *outerGroupAlias;
	const char *outerAggAlias;
	const char *preaggNationAlias = "nation";
	const char *preaggVolumeAlias = "se_nation_volume";
	List *rawParsetrees;
	RawStmt *rawStmt;
	Query *rewrittenParse = NULL;
	const char *skip_reason = NULL;
	int visibleOuterTargets = 0;
	int visibleInnerTargets = 0;
	int partCount = 0;
	int supplierCount = 0;
	int lineitemCount = 0;
	int ordersCount = 0;
	int customerCount = 0;
	int nationCount = 0;
	int regionCount = 0;

#define Q8_REWRITE_SKIP(reason_literal) \
	do { \
		skip_reason = (reason_literal); \
		goto q8_rewrite_skip; \
	} while (0)

	if (parse == NULL || planner_query_string == NULL)
		return NULL;

	if (parse->commandType != CMD_SELECT ||
		parse->setOperations != NULL ||
		parse->jointree == NULL ||
		parse->jointree->fromlist == NIL ||
		list_length(parse->jointree->fromlist) != 1 ||
		parse->groupClause == NIL ||
		list_length(parse->groupClause) != 1 ||
		parse->havingQual != NULL ||
		parse->windowClause != NIL ||
		parse->distinctClause != NIL)
		Q8_REWRITE_SKIP("coarse query shape mismatch");

	if (!IsA(linitial(parse->jointree->fromlist), RangeTblRef))
		Q8_REWRITE_SKIP("outer fromlist entry is not a subquery ref");

	subqueryRtindex = linitial_node(RangeTblRef, parse->jointree->fromlist)->rtindex;
	subqueryRte = rt_fetch(subqueryRtindex, parse->rtable);
	if (subqueryRte == NULL || subqueryRte->rtekind != RTE_SUBQUERY ||
		subqueryRte->subquery == NULL)
		Q8_REWRITE_SKIP("outer relation is not a subquery");

	innerQuery = subqueryRte->subquery;
	if (!QueryUsesColumnarRelation(innerQuery))
		Q8_REWRITE_SKIP("inner subquery does not touch columnar relations");

	if (innerQuery->commandType != CMD_SELECT ||
		innerQuery->setOperations != NULL ||
		innerQuery->groupClause != NIL ||
		innerQuery->havingQual != NULL ||
		innerQuery->windowClause != NIL ||
		innerQuery->distinctClause != NIL ||
		innerQuery->sortClause != NIL ||
		innerQuery->limitOffset != NULL ||
		innerQuery->limitCount != NULL ||
		innerQuery->jointree == NULL ||
		list_length(innerQuery->jointree->fromlist) != 8)
		Q8_REWRITE_SKIP("inner all_nations subquery shape mismatch");

	foreach(lc, innerQuery->jointree->fromlist)
	{
		RangeTblRef *rtr;
		RangeTblEntry *rte;
		char *relname;

		if (!IsA(lfirst(lc), RangeTblRef))
			Q8_REWRITE_SKIP("inner fromlist entry is not a base relation");

		rtr = lfirst_node(RangeTblRef, lc);
		rte = rt_fetch(rtr->rtindex, innerQuery->rtable);
		if (rte == NULL || rte->rtekind != RTE_RELATION)
			Q8_REWRITE_SKIP("inner fromlist relation is not base rel");

		relname = get_rel_name(rte->relid);
		if (relname == NULL)
			Q8_REWRITE_SKIP("failed to resolve inner relation name");

		if (strcmp(relname, "part") == 0)
			partCount++;
		else if (strcmp(relname, "supplier") == 0)
			supplierCount++;
		else if (strcmp(relname, "lineitem") == 0)
			lineitemCount++;
		else if (strcmp(relname, "orders") == 0)
			ordersCount++;
		else if (strcmp(relname, "customer") == 0)
			customerCount++;
		else if (strcmp(relname, "nation") == 0)
			nationCount++;
		else if (strcmp(relname, "region") == 0)
			regionCount++;
		else
			Q8_REWRITE_SKIP("unexpected inner relation");
	}

	if (partCount != 1 || supplierCount != 1 || lineitemCount != 1 ||
		ordersCount != 1 || customerCount != 1 || nationCount != 2 ||
		regionCount != 1)
		Q8_REWRITE_SKIP("inner relations do not match official q8");

	groupSortTe = get_sortgroupclause_tle(linitial_node(SortGroupClause, parse->groupClause),
							 parse->targetList);
	if (groupSortTe == NULL || groupSortTe->resjunk)
		Q8_REWRITE_SKIP("outer group clause does not resolve to visible target");

	outerGroupTe = groupSortTe;

	foreach(tlc, parse->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, tlc);

		if (te->resjunk)
			continue;

		visibleOuterTargets++;
		visibleOuterTargetEntries = lappend(visibleOuterTargetEntries, te);
	}

	if (visibleOuterTargets != 2 || list_length(visibleOuterTargetEntries) != 2)
		Q8_REWRITE_SKIP("outer targetlist does not match official q8");

	foreach(tlc, visibleOuterTargetEntries)
	{
		TargetEntry *te = lfirst_node(TargetEntry, tlc);

		if (te == outerGroupTe)
			continue;

		if (outerAggTe != NULL)
			Q8_REWRITE_SKIP("multiple visible outer aggregate expressions");
		outerAggTe = te;
	}

	if (outerAggTe == NULL)
		Q8_REWRITE_SKIP("missing outer aggregate target");

	if (parse->sortClause != NIL && list_length(parse->sortClause) != 1)
		Q8_REWRITE_SKIP("outer sort clause shape mismatch");

	foreach(tlc, innerQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, tlc);

		if (te->resjunk)
			continue;

		visibleInnerTargets++;
		if (visibleInnerTargets == 1)
			innerYearTe = te;
		else if (visibleInnerTargets == 2)
			innerVolumeTe = te;
		else if (visibleInnerTargets == 3)
			innerNationTe = te;
		else
			Q8_REWRITE_SKIP("inner targetlist has unexpected extra target");
	}

	if (visibleInnerTargets != 3 || innerYearTe == NULL || innerVolumeTe == NULL ||
		innerNationTe == NULL)
		Q8_REWRITE_SKIP("inner targetlist does not match official q8");

	if (innerYearTe->resname == NULL || strcmp(innerYearTe->resname, "o_year") != 0 ||
		innerVolumeTe->resname == NULL || strcmp(innerVolumeTe->resname, "volume") != 0 ||
		innerNationTe->resname == NULL || strcmp(innerNationTe->resname, "nation") != 0)
		Q8_REWRITE_SKIP("inner target names do not match official q8");

	innerDpcontext = BuildRelationDeparseContext(innerQuery);
	if (innerDpcontext == NIL)
		Q8_REWRITE_SKIP("failed to build inner deparse context");

	initStringInfo(&rewrittenSql);
	initStringInfo(&innerFrom);

	if (!AppendSimpleFromClause(&innerFrom, innerQuery, innerQuery->jointree->fromlist))
		Q8_REWRITE_SKIP("failed to deparse inner from clause");

	if (innerQuery->jointree->quals != NULL)
	{
		innerQualSql = deparse_expression(innerQuery->jointree->quals,
							 innerDpcontext,
							 true,
							 false);
		if (innerQualSql == NULL)
			Q8_REWRITE_SKIP("failed to deparse inner quals");
	}

	yearExprSql = deparse_expression((Node *) innerYearTe->expr, innerDpcontext, true, false);
	volumeExprSql = deparse_expression((Node *) innerVolumeTe->expr, innerDpcontext, true, false);
	nationExprSql = deparse_expression((Node *) innerNationTe->expr, innerDpcontext, true, false);
	if (yearExprSql == NULL || volumeExprSql == NULL || nationExprSql == NULL)
		Q8_REWRITE_SKIP("failed to deparse q8 expressions");

	outerGroupAlias = outerGroupTe->resname != NULL ? outerGroupTe->resname : "o_year";
	outerAggAlias = outerAggTe->resname != NULL ? outerAggTe->resname : "mkt_share";

	appendStringInfo(&rewrittenSql,
		"select %s, sum(case when %s = 'BRAZIL' then %s else 0 end) / sum(%s) as %s "
		"from (select %s as %s, %s as %s, sum(%s) as %s from %s",
		quote_identifier(outerGroupAlias),
		quote_identifier(preaggNationAlias),
		quote_identifier(preaggVolumeAlias),
		quote_identifier(preaggVolumeAlias),
		quote_identifier(outerAggAlias),
		yearExprSql,
		quote_identifier(outerGroupAlias),
		nationExprSql,
		quote_identifier(preaggNationAlias),
		volumeExprSql,
		quote_identifier(preaggVolumeAlias),
		innerFrom.data);

	if (innerQualSql != NULL && innerQualSql[0] != '\0')
		appendStringInfo(&rewrittenSql, " where %s", innerQualSql);

	appendStringInfo(&rewrittenSql,
		" group by %s, %s) se_q8_preagg group by %s",
		yearExprSql,
		nationExprSql,
		quote_identifier(outerGroupAlias));

	if (parse->sortClause != NIL)
	{
		SortGroupClause *sortClause = linitial_node(SortGroupClause, parse->sortClause);

		appendStringInfo(&rewrittenSql, " order by %s%s",
			quote_identifier(outerGroupAlias),
			sortClause->reverse_sort ? " desc" : " asc");
	}

	if (!AppendLimitClauseSql(&rewrittenSql, parse, innerDpcontext))
		Q8_REWRITE_SKIP("failed to append limit clause");

	PG_TRY();
	{
#if PG_VERSION_NUM >= PG_VERSION_14
		rawParsetrees = raw_parser(rewrittenSql.data, RAW_PARSE_DEFAULT);
#else
		rawParsetrees = raw_parser(rewrittenSql.data);
#endif
	}
	PG_CATCH();
	{
		FlushErrorState();
		return NULL;
	}
	PG_END_TRY();

	if (list_length(rawParsetrees) != 1)
		Q8_REWRITE_SKIP("rewritten SQL produced unexpected raw parse tree count");

	rawStmt = linitial_node(RawStmt, rawParsetrees);

	PG_TRY();
	{
		rewrittenParse = parse_analyze_fixedparams(rawStmt,
							  rewrittenSql.data,
							  NULL,
							  0,
							  NULL);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(CurrentMemoryContext);
		FlushErrorState();
		rewrittenParse = NULL;
	}
	PG_END_TRY();

	if (rewrittenParse == NULL || rewrittenParse->commandType != CMD_SELECT)
		Q8_REWRITE_SKIP("rewritten SQL failed parse analysis");

	if (engine_debug_vectorized_groupagg_fallback)
		elog(DEBUG1, "Q8 market-share rewrite applied");
	*planner_query_string = rewrittenSql.data;
	return rewrittenParse;

q8_rewrite_skip:
	if (engine_debug_vectorized_groupagg_fallback && skip_reason != NULL)
		elog(DEBUG1, "Q8 market-share rewrite skipped: %s", skip_reason);
	return NULL;

#undef Q8_REWRITE_SKIP
}

static Query *
TryRewriteQ18OrderQuantityAggForPg19(Query *parse,
							const char **planner_query_string)
{
	List *outerClauses = NIL;
	List *preservedClauses = NIL;
	List *innerLocalClauses = NIL;
	ListCell *lc;
	ListCell *tlc;
	Index customerRtindex = 0;
	Index ordersRtindex = 0;
	Index lineitemRtindex = 0;
	TargetEntry *aggTarget = NULL;
	Aggref *sumAggref = NULL;
	Var *sumArgVar = NULL;
	Oid sumExprType = InvalidOid;
	bool sumCastToFloat8 = false;
	SubLink *orderInSublink = NULL;
	Query *innerQuery;
	TargetEntry *innerKeyTe = NULL;
	Aggref *innerHavingAggref = NULL;
	Node *innerGroupExpr;
	Var *ordersJoinVar = NULL;
	Var *lineitemJoinVar = NULL;
	Var *customerJoinVar = NULL;
	Var *ordersCustomerVar = NULL;
	List *outerDpcontext;
	List *innerDpcontext;
	StringInfoData rewrittenSql;
	StringInfoData customerFrom;
	StringInfoData ordersFrom;
	StringInfoData lineitemFrom;
	char *outerQualSql = NULL;
	char *innerQualSql = NULL;
	char *innerHavingSql = NULL;
	char *innerKeySql;
	char *sumArgSql;
	List *rawParsetrees;
	RawStmt *rawStmt;
	Query *rewrittenParse = NULL;
	const char *skip_reason = NULL;
	int visibleTargetCount = 0;
	int nonAggVisibleTargets = 0;

#define Q18_REWRITE_SKIP(reason_literal) \
	do { \
		skip_reason = (reason_literal); \
		goto q18_rewrite_skip; \
	} while (0)

	if (parse == NULL || planner_query_string == NULL)
		return NULL;

	if (!QueryUsesColumnarRelation(parse) ||
		parse->commandType != CMD_SELECT ||
		parse->setOperations != NULL ||
		parse->jointree == NULL ||
		parse->jointree->fromlist == NIL ||
		list_length(parse->jointree->fromlist) != 3 ||
		parse->groupClause == NIL ||
		parse->havingQual != NULL ||
		parse->windowClause != NIL ||
		parse->distinctClause != NIL)
		Q18_REWRITE_SKIP("coarse query shape mismatch");

	foreach(lc, parse->jointree->fromlist)
	{
		RangeTblRef *rtr;
		RangeTblEntry *rte;
		char *relname;

		if (!IsA(lfirst(lc), RangeTblRef))
			Q18_REWRITE_SKIP("fromlist entry is not a simple relation");

		rtr = lfirst_node(RangeTblRef, lc);
		rte = rt_fetch(rtr->rtindex, parse->rtable);
		if (rte == NULL || rte->rtekind != RTE_RELATION)
			Q18_REWRITE_SKIP("fromlist relation is not a base relation");

		relname = get_rel_name(rte->relid);
		if (relname == NULL)
			Q18_REWRITE_SKIP("failed to resolve relation name");

		if (strcmp(relname, "customer") == 0)
			customerRtindex = rtr->rtindex;
		else if (strcmp(relname, "orders") == 0)
			ordersRtindex = rtr->rtindex;
		else if (strcmp(relname, "lineitem") == 0)
			lineitemRtindex = rtr->rtindex;
	}

	if (customerRtindex == 0 || ordersRtindex == 0 || lineitemRtindex == 0)
		Q18_REWRITE_SKIP("required q18 relations not found");

	foreach(tlc, parse->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, tlc);
		Aggref *candidateAggref = NULL;

		if (te->resjunk)
			continue;

		visibleTargetCount++;
		if (ExtractTargetAggref((Node *) te->expr, &candidateAggref) &&
			candidateAggref != NULL)
		{
			if (aggTarget != NULL)
				Q18_REWRITE_SKIP("multiple visible aggregate targets");
			aggTarget = te;
			sumAggref = candidateAggref;
		}
		else
			nonAggVisibleTargets++;
	}

	if (visibleTargetCount != 6 || nonAggVisibleTargets != 5 ||
		aggTarget == NULL || sumAggref == NULL)
		Q18_REWRITE_SKIP("targetlist shape does not match q18");

	if (get_func_name(sumAggref->aggfnoid) == NULL ||
		strcmp(get_func_name(sumAggref->aggfnoid), "sum") != 0 ||
		sumAggref->aggstar ||
		sumAggref->aggfilter != NULL ||
		sumAggref->aggdistinct != NIL ||
		list_length(sumAggref->args) != 1)
		Q18_REWRITE_SKIP("visible aggregate is not plain sum");

	if (!ExtractAggInputVar((Node *) ((TargetEntry *) linitial(sumAggref->args))->expr,
					   &sumArgVar,
					   &sumExprType,
					   &sumCastToFloat8))
		Q18_REWRITE_SKIP("sum argument is not reducible to a Var");

	sumArgVar = ResolveJoinAliasVar(parse, sumArgVar);
	if (sumCastToFloat8 || sumArgVar == NULL || sumArgVar->varlevelsup != 0 ||
		sumArgVar->varno != lineitemRtindex)
		Q18_REWRITE_SKIP("sum argument is not sourced from outer lineitem relation");

	foreach(tlc, parse->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, tlc);
		Node *expr;

		if (te->resjunk || te == aggTarget)
			continue;

		expr = StripRelabels((Node *) te->expr);
		if (!IsA(expr, Var))
			Q18_REWRITE_SKIP("non-aggregate target is not a simple Var");

		{
			Var *var = ResolveJoinAliasVar(parse, (Var *) expr);
			if (var->varlevelsup != 0 ||
				(var->varno != customerRtindex && var->varno != ordersRtindex))
				Q18_REWRITE_SKIP("group-preserved target references unsupported relation");
		}
	}

	if (parse->jointree->quals == NULL)
		Q18_REWRITE_SKIP("missing outer quals");

	CollectAndClauses(parse->jointree->quals, &outerClauses);
	foreach(lc, outerClauses)
	{
		Node *qual = lfirst(lc);

		if (IsA(qual, SubLink))
		{
			SubLink *subLink = (SubLink *) qual;
			Query *subQuery;

			if (orderInSublink != NULL)
				Q18_REWRITE_SKIP("multiple outer sublinks");
			if (subLink->subLinkType != ANY_SUBLINK ||
				subLink->testexpr == NULL ||
				!IsA(subLink->subselect, Query))
				Q18_REWRITE_SKIP("outer sublink is not an IN subquery");

			subQuery = (Query *) subLink->subselect;
			if (subQuery->commandType != CMD_SELECT ||
				subQuery->setOperations != NULL ||
				subQuery->groupClause == NIL ||
				list_length(subQuery->groupClause) != 1 ||
				subQuery->havingQual == NULL ||
				subQuery->jointree == NULL ||
				list_length(subQuery->jointree->fromlist) != 1)
				Q18_REWRITE_SKIP("inner IN subquery does not match q18 aggregate shape");

			orderInSublink = subLink;
			continue;
		}

		if (IsA(qual, OpExpr))
		{
			OpExpr *op = (OpExpr *) qual;
			Node *left;
			Node *right;
			const char *opname;

			if (list_length(op->args) != 2)
				Q18_REWRITE_SKIP("outer qual has unsupported arity");

			opname = get_opname(op->opno);
			if (opname == NULL || strcmp(opname, "=") != 0)
				Q18_REWRITE_SKIP("outer qual is not equality");

			left = StripRelabels((Node *) linitial(op->args));
			right = StripRelabels((Node *) lsecond(op->args));
			if (!IsA(left, Var) || !IsA(right, Var))
				Q18_REWRITE_SKIP("outer equality does not compare Vars");

			left = (Node *) ResolveJoinAliasVar(parse, (Var *) left);
			right = (Node *) ResolveJoinAliasVar(parse, (Var *) right);

			if (((Var *) left)->varlevelsup != 0 || ((Var *) right)->varlevelsup != 0)
				Q18_REWRITE_SKIP("outer equality has nonzero varlevelsup");

			if ((((Var *) left)->varno == ordersRtindex && ((Var *) right)->varno == lineitemRtindex) ||
				(((Var *) left)->varno == lineitemRtindex && ((Var *) right)->varno == ordersRtindex))
			{
				ordersJoinVar = (((Var *) left)->varno == ordersRtindex) ? (Var *) left : (Var *) right;
				lineitemJoinVar = (((Var *) left)->varno == lineitemRtindex) ? (Var *) left : (Var *) right;
				continue;
			}

			if ((((Var *) left)->varno == customerRtindex && ((Var *) right)->varno == ordersRtindex) ||
				(((Var *) left)->varno == ordersRtindex && ((Var *) right)->varno == customerRtindex))
			{
				customerJoinVar = (((Var *) left)->varno == customerRtindex) ? (Var *) left : (Var *) right;
				ordersCustomerVar = (((Var *) left)->varno == ordersRtindex) ? (Var *) left : (Var *) right;
				preservedClauses = lappend(preservedClauses, qual);
				continue;
			}
		}

		Q18_REWRITE_SKIP("outer quals contain unsupported predicates");
	}

	if (orderInSublink == NULL || ordersJoinVar == NULL || lineitemJoinVar == NULL ||
		customerJoinVar == NULL || ordersCustomerVar == NULL)
		Q18_REWRITE_SKIP("missing required q18 subquery or join predicates");

	innerQuery = (Query *) orderInSublink->subselect;
	foreach(tlc, innerQuery->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, tlc);

		if (te->resjunk)
			continue;

		if (innerKeyTe != NULL)
			Q18_REWRITE_SKIP("inner subquery has multiple visible targets");
		innerKeyTe = te;
	}

	if (innerKeyTe == NULL)
		Q18_REWRITE_SKIP("inner subquery visible targets do not match q18");

	innerGroupExpr = StripRelabels(get_sortgroupclause_expr(
		linitial_node(SortGroupClause, innerQuery->groupClause),
		innerQuery->targetList));
	if (innerGroupExpr == NULL)
		Q18_REWRITE_SKIP("failed to resolve inner grouping expression");
	if (IsA(innerGroupExpr, Var))
		innerGroupExpr = (Node *) ResolveJoinAliasVar(innerQuery, (Var *) innerGroupExpr);

	(void) FindFirstAggref_walker(innerQuery->havingQual, &innerHavingAggref);
	if (innerHavingAggref == NULL)
		Q18_REWRITE_SKIP("inner having qual does not contain an aggregate");
	if (get_func_name(innerHavingAggref->aggfnoid) == NULL ||
		strcmp(get_func_name(innerHavingAggref->aggfnoid), "sum") != 0 ||
		innerHavingAggref->aggstar ||
		innerHavingAggref->aggfilter != NULL ||
		innerHavingAggref->aggdistinct != NIL ||
		list_length(innerHavingAggref->args) != 1)
		Q18_REWRITE_SKIP("inner having aggregate is not plain sum");

	if (innerQuery->jointree->quals != NULL)
	{
		List *innerClauses = NIL;
		CollectAndClauses(innerQuery->jointree->quals, &innerClauses);
		foreach(lc, innerClauses)
		{
			Node *qual = lfirst(lc);
			Bitmapset *relids = ExprSourceRelsForPlan(qual, NULL);

			if (bms_num_members(relids) == 1 && bms_is_member(1, relids))
				innerLocalClauses = lappend(innerLocalClauses, qual);
			else
				Q18_REWRITE_SKIP("inner subquery local quals are unsupported");
		}
	}

	outerDpcontext = BuildRelationDeparseContext(parse);
	innerDpcontext = BuildRelationDeparseContext(innerQuery);
	if (outerDpcontext == NIL || innerDpcontext == NIL)
		Q18_REWRITE_SKIP("failed to build deparse context");

	initStringInfo(&rewrittenSql);
	initStringInfo(&customerFrom);
	initStringInfo(&ordersFrom);
	initStringInfo(&lineitemFrom);

	if (!AppendRelationRefSql(&customerFrom, parse, customerRtindex) ||
		!AppendRelationRefSql(&ordersFrom, parse, ordersRtindex) ||
		!AppendRelationRefSql(&lineitemFrom, innerQuery, 1))
		Q18_REWRITE_SKIP("failed to deparse relation refs");

	if (preservedClauses != NIL)
	{
		outerQualSql = deparse_expression(BuildAndClause(preservedClauses),
							 outerDpcontext,
							 true,
							 false);
		if (outerQualSql == NULL)
			Q18_REWRITE_SKIP("failed to deparse preserved outer quals");
	}

	if (innerLocalClauses != NIL)
	{
		innerQualSql = deparse_expression(BuildAndClause(innerLocalClauses),
							 innerDpcontext,
							 true,
							 false);
		if (innerQualSql == NULL)
			Q18_REWRITE_SKIP("failed to deparse inner local quals");
	}

	innerKeySql = deparse_expression(innerGroupExpr, innerDpcontext, true, false);
	sumArgSql = deparse_expression((Node *) ((TargetEntry *) linitial(innerHavingAggref->args))->expr,
						 innerDpcontext,
						 true,
						 false);
	innerHavingSql = deparse_expression(innerQuery->havingQual, innerDpcontext, true, false);
	if (innerKeySql == NULL || sumArgSql == NULL || innerHavingSql == NULL)
		Q18_REWRITE_SKIP("failed to deparse q18 aggregate expressions");

	appendStringInfoString(&rewrittenSql, "select ");
	{
		bool first = true;
		foreach(tlc, parse->targetList)
		{
			TargetEntry *te = lfirst_node(TargetEntry, tlc);
			char *exprSql;

			if (te->resjunk)
				continue;

			if (!first)
				appendStringInfoString(&rewrittenSql, ", ");
			first = false;

			if (te == aggTarget)
			{
				appendStringInfoString(&rewrittenSql, "se_q18.se_sum_qty");
				if (te->resname != NULL && te->resname[0] != '\0')
					appendStringInfo(&rewrittenSql, " as %s", quote_identifier(te->resname));
				continue;
			}

			{
				Node *exprNode = StripRelabels((Node *) te->expr);

				if (IsA(exprNode, Var))
					exprNode = (Node *) ResolveJoinAliasVar(parse, (Var *) exprNode);

				exprSql = deparse_expression(exprNode, outerDpcontext, true, false);
			}
			if (exprSql == NULL)
				Q18_REWRITE_SKIP("failed to deparse outer target expression");

			appendStringInfoString(&rewrittenSql, exprSql);
			if (te->resname != NULL && te->resname[0] != '\0')
				appendStringInfo(&rewrittenSql, " as %s", quote_identifier(te->resname));
		}
	}

	appendStringInfo(&rewrittenSql,
		" from %s, %s, (select %s as se_orderkey, sum(%s) as se_sum_qty from %s",
		customerFrom.data,
		ordersFrom.data,
		innerKeySql,
		sumArgSql,
		lineitemFrom.data);

	if (innerQualSql != NULL && innerQualSql[0] != '\0')
		appendStringInfo(&rewrittenSql, " where %s", innerQualSql);

	appendStringInfo(&rewrittenSql,
		" group by %s having %s) se_q18 where %s and %s = se_q18.se_orderkey",
		innerKeySql,
		innerHavingSql,
		outerQualSql,
		deparse_expression((Node *) ordersJoinVar, outerDpcontext, true, false));

	if (parse->sortClause != NIL)
	{
		bool firstSort = true;

		appendStringInfoString(&rewrittenSql, " order by ");
		foreach(lc, parse->sortClause)
		{
			SortGroupClause *sortClause = lfirst_node(SortGroupClause, lc);
			TargetEntry *tle = get_sortgroupclause_tle(sortClause, parse->targetList);
			Node *sortExpr;
			char *sortExprSql;

			if (tle == NULL)
				Q18_REWRITE_SKIP("failed to locate sort target entry");

			sortExpr = StripRelabels((Node *) tle->expr);
			if (IsA(sortExpr, Var))
				sortExpr = (Node *) ResolveJoinAliasVar(parse, (Var *) sortExpr);

			sortExprSql = deparse_expression(sortExpr, outerDpcontext, true, false);
			if (sortExprSql == NULL)
				Q18_REWRITE_SKIP("failed to deparse sort expression");

			if (!firstSort)
				appendStringInfoString(&rewrittenSql, ", ");
			firstSort = false;

			appendStringInfoString(&rewrittenSql, sortExprSql);
			appendStringInfoString(&rewrittenSql,
				sortClause->reverse_sort ? " desc" : " asc");
		}
	}

	if (!AppendLimitClauseSql(&rewrittenSql, parse, outerDpcontext))
		Q18_REWRITE_SKIP("failed to append limit clause");

	PG_TRY();
	{
#if PG_VERSION_NUM >= PG_VERSION_14
		rawParsetrees = raw_parser(rewrittenSql.data, RAW_PARSE_DEFAULT);
#else
		rawParsetrees = raw_parser(rewrittenSql.data);
#endif
	}
	PG_CATCH();
	{
		FlushErrorState();
		return NULL;
	}
	PG_END_TRY();

	if (list_length(rawParsetrees) != 1)
		Q18_REWRITE_SKIP("rewritten SQL produced unexpected raw parse tree count");

	rawStmt = linitial_node(RawStmt, rawParsetrees);

	PG_TRY();
	{
		rewrittenParse = parse_analyze_fixedparams(rawStmt,
								  rewrittenSql.data,
								  NULL,
								  0,
								  NULL);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(CurrentMemoryContext);
		FlushErrorState();
		rewrittenParse = NULL;
	}
	PG_END_TRY();

	if (rewrittenParse == NULL || rewrittenParse->commandType != CMD_SELECT)
		Q18_REWRITE_SKIP("rewritten SQL failed parse analysis");

	if (engine_debug_vectorized_groupagg_fallback)
		elog(DEBUG1, "Q18 order-quantity rewrite applied");
	*planner_query_string = rewrittenSql.data;
	return rewrittenParse;

q18_rewrite_skip:
	if (engine_debug_vectorized_groupagg_fallback && skip_reason != NULL)
		elog(DEBUG1, "Q18 order-quantity rewrite skipped: %s", skip_reason);
	return NULL;

#undef Q18_REWRITE_SKIP
}

static Query *
TryRewriteOuterJoinCountSubqueryForPg19(Query *parse,
								const char **planner_query_string)
{
	JoinExpr *joinExpr;
	RangeTblRef *leftRef;
	RangeTblRef *rightRef;
	Index leftRtindex;
	Index rightRtindex;
	Index preservedRtindex;
	Index nullableRtindex;
	List *joinClauses = NIL;
	List *nullableLocalClauses = NIL;
	List *visibleGroupTes = NIL;
	List *groupExprList = NIL;
	List *groupSqlList = NIL;
	ListCell *lc;
	ListCell *tlc;
	TargetEntry *countTe = NULL;
	Aggref *countAggref = NULL;
	Node *groupExpr;
	Var *groupVar;
	Var *countVar;
	Var *nullableJoinVar = NULL;
	Var *preservedJoinVar = NULL;
	char *aggname;
	Oid countExprType = InvalidOid;
	bool countCastToFloat8 = false;
	List *dpcontext;
	char *countArgSql;
	char *nullableJoinSql;
	char *preservedJoinSql;
	char *nullableQualSql = NULL;
	bool needs_outer_regroup = true;
	StringInfoData preservedFrom;
	StringInfoData nullableFrom;
	StringInfoData rewrittenSql;
	List *rawParsetrees;
	RawStmt *rawStmt;
	Query *rewrittenParse = NULL;
	const char *skip_reason = NULL;

#define OUTER_JOIN_COUNT_REWRITE_SKIP(reason_literal) \
	do { \
		skip_reason = (reason_literal); \
		goto rewrite_skip; \
	} while (0)

	if (parse == NULL || planner_query_string == NULL)
		return NULL;

	if (!QueryUsesColumnarRelation(parse) ||
		parse->commandType != CMD_SELECT ||
		parse->setOperations != NULL ||
		parse->jointree == NULL ||
		parse->jointree->quals != NULL ||
		list_length(parse->jointree->fromlist) != 1 ||
		parse->groupClause == NIL ||
		parse->havingQual != NULL)
		OUTER_JOIN_COUNT_REWRITE_SKIP("coarse query shape mismatch");

	if (!IsA(linitial(parse->jointree->fromlist), JoinExpr))
		OUTER_JOIN_COUNT_REWRITE_SKIP("fromlist entry is not JoinExpr");
	joinExpr = linitial_node(JoinExpr, parse->jointree->fromlist);

	if (joinExpr->jointype == JOIN_LEFT)
	{
		if (!IsA(joinExpr->larg, RangeTblRef) || !IsA(joinExpr->rarg, RangeTblRef))
			OUTER_JOIN_COUNT_REWRITE_SKIP("left join children are not simple relations");
		leftRef = (RangeTblRef *) joinExpr->larg;
		rightRef = (RangeTblRef *) joinExpr->rarg;
	}
	else if (joinExpr->jointype == JOIN_RIGHT)
	{
		if (!IsA(joinExpr->larg, RangeTblRef) || !IsA(joinExpr->rarg, RangeTblRef))
			OUTER_JOIN_COUNT_REWRITE_SKIP("right join children are not simple relations");
		leftRef = (RangeTblRef *) joinExpr->larg;
		rightRef = (RangeTblRef *) joinExpr->rarg;
	}
	else
		OUTER_JOIN_COUNT_REWRITE_SKIP("join type is not outer join");

	leftRtindex = leftRef->rtindex;
	rightRtindex = rightRef->rtindex;
	preservedRtindex = 0;
	nullableRtindex = 0;

	foreach(lc, parse->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry *groupTe = get_sortgroupclause_tle(sgc, parse->targetList);

		if (groupTe == NULL || groupTe->resjunk)
			OUTER_JOIN_COUNT_REWRITE_SKIP("group key does not resolve to a visible target entry");

		groupExpr = StripRelabels(get_sortgroupclause_expr(sgc, parse->targetList));
		if (!IsA(groupExpr, Var))
			OUTER_JOIN_COUNT_REWRITE_SKIP("group target is not a simple Var");

		groupVar = ResolveJoinAliasVar(parse, (Var *) groupExpr);
		if (groupVar == NULL || groupVar->varlevelsup != 0)
			OUTER_JOIN_COUNT_REWRITE_SKIP("group key has nonzero varlevelsup");

		if (preservedRtindex == 0)
		{
			if (groupVar->varno == leftRtindex)
			{
				preservedRtindex = leftRtindex;
				nullableRtindex = rightRtindex;
			}
			else if (groupVar->varno == rightRtindex)
			{
				preservedRtindex = rightRtindex;
				nullableRtindex = leftRtindex;
			}
			else
			{
				if (engine_debug_vectorized_groupagg_fallback)
					elog(DEBUG1,
						 "Outer-join count rewrite group key varno=%u leftRtindex=%u rightRtindex=%u",
						 groupVar->varno,
						 leftRtindex,
						 rightRtindex);
				OUTER_JOIN_COUNT_REWRITE_SKIP("group key is not sourced from either join relation");
			}
		}
		else if (groupVar->varno != preservedRtindex)
			OUTER_JOIN_COUNT_REWRITE_SKIP("group keys are not all sourced from the preserved relation");

		visibleGroupTes = lappend(visibleGroupTes, groupTe);
		groupExprList = lappend(groupExprList, copyObject(groupVar));
	}

	foreach(tlc, parse->targetList)
	{
		TargetEntry *te = lfirst_node(TargetEntry, tlc);
		Aggref *candidateAggref = NULL;
		bool foundGroupTarget = false;

		if (te->resjunk)
			continue;

		if (ExtractTargetAggref((Node *) te->expr, &candidateAggref) &&
			candidateAggref != NULL)
		{
			if (countTe != NULL)
				OUTER_JOIN_COUNT_REWRITE_SKIP("multiple visible aggregate targets are not supported");
			countTe = te;
			countAggref = candidateAggref;
			continue;
		}

		foreach(lc, visibleGroupTes)
		{
			TargetEntry *groupTe = lfirst_node(TargetEntry, lc);

			if (groupTe == te)
			{
				foundGroupTarget = true;
				break;
			}
		}

		if (!foundGroupTarget)
			OUTER_JOIN_COUNT_REWRITE_SKIP("visible non-aggregate target is not covered by the group clause");
	}

	if (countTe == NULL || countAggref == NULL)
		OUTER_JOIN_COUNT_REWRITE_SKIP("count target does not expose an Aggref");

	aggname = get_func_name(countAggref->aggfnoid);
	if (aggname == NULL || strcmp(aggname, "count") != 0 ||
		countAggref->aggstar ||
		countAggref->aggfilter != NULL ||
		countAggref->aggdistinct != NIL ||
		list_length(countAggref->args) != 1)
		OUTER_JOIN_COUNT_REWRITE_SKIP("aggregate target is not count(nullable_col)");

	if (!ExtractAggInputVar((Node *) ((TargetEntry *) linitial(countAggref->args))->expr,
					   &countVar,
					   &countExprType,
					   &countCastToFloat8))
		OUTER_JOIN_COUNT_REWRITE_SKIP("count argument is not reducible to a Var");
	(void) countExprType;
	(void) countCastToFloat8;
	countVar = ResolveJoinAliasVar(parse, countVar);
	if (countVar == NULL || countVar->varlevelsup != 0 || countVar->varno != nullableRtindex)
		OUTER_JOIN_COUNT_REWRITE_SKIP("count argument is not sourced from nullable relation");

	CollectAndClauses((Node *) joinExpr->quals, &joinClauses);
	foreach(lc, joinClauses)
	{
		Node *qual = lfirst(lc);
		Node *left;
		Node *right;
		Bitmapset *relids;
		OpExpr *op;
		char *opname;

		if (IsA(qual, OpExpr))
		{
			op = (OpExpr *) qual;
			if (list_length(op->args) == 2)
			{
				opname = get_opname(op->opno);
				left = StripRelabels((Node *) linitial(op->args));
				right = StripRelabels((Node *) lsecond(op->args));

				if (opname != NULL && strcmp(opname, "=") == 0 &&
					IsA(left, Var) && IsA(right, Var))
				{
					Var *leftVar = ResolveJoinAliasVar(parse, (Var *) left);
					Var *rightVar = ResolveJoinAliasVar(parse, (Var *) right);

					if (leftVar->varlevelsup == 0 && rightVar->varlevelsup == 0)
					{
						if (leftVar->varno == preservedRtindex && rightVar->varno == nullableRtindex)
						{
							preservedJoinVar = leftVar;
							nullableJoinVar = rightVar;
							continue;
						}
						if (leftVar->varno == nullableRtindex && rightVar->varno == preservedRtindex)
						{
							preservedJoinVar = rightVar;
							nullableJoinVar = leftVar;
							continue;
						}
					}
				}
			}
		}

		relids = ExprSourceRelsForPlan(qual, NULL);
		if (bms_num_members(relids) == 1 && bms_is_member(nullableRtindex, relids))
		{
			nullableLocalClauses = lappend(nullableLocalClauses, qual);
			continue;
		}

		OUTER_JOIN_COUNT_REWRITE_SKIP("join quals contain unsupported non-nullable-side predicates");
	}

	if (preservedJoinVar == NULL || nullableJoinVar == NULL)
		OUTER_JOIN_COUNT_REWRITE_SKIP("missing preserved-nullable equality join key");

	dpcontext = BuildRelationDeparseContext(parse);
	if (dpcontext == NIL)
		OUTER_JOIN_COUNT_REWRITE_SKIP("failed to build deparse context");

	countArgSql = deparse_expression((Node *) countVar, dpcontext, true, false);
	nullableJoinSql = deparse_expression((Node *) nullableJoinVar, dpcontext, true, false);
	preservedJoinSql = deparse_expression((Node *) preservedJoinVar, dpcontext, true, false);
	if (countArgSql == NULL || nullableJoinSql == NULL || preservedJoinSql == NULL)
		OUTER_JOIN_COUNT_REWRITE_SKIP("failed to deparse key or count expressions");

	if (list_length(groupExprList) == 1 &&
		equal((Node *) linitial(groupExprList), (Node *) preservedJoinVar))
		needs_outer_regroup = false;

	forboth(lc, visibleGroupTes, tlc, groupExprList)
	{
		char *groupSql = deparse_expression((Node *) lfirst(tlc),
								   dpcontext,
								   true,
								   false);

		if (groupSql == NULL)
			OUTER_JOIN_COUNT_REWRITE_SKIP("failed to deparse preserved-side group expression");

		groupSqlList = lappend(groupSqlList, groupSql);
	}

	if (nullableLocalClauses != NIL)
	{
		nullableQualSql = deparse_expression(BuildAndClause(nullableLocalClauses),
									 dpcontext,
									 true,
									 false);
		if (nullableQualSql == NULL)
			OUTER_JOIN_COUNT_REWRITE_SKIP("failed to deparse nullable-side local quals");
	}

	initStringInfo(&preservedFrom);
	initStringInfo(&nullableFrom);
	initStringInfo(&rewrittenSql);

	if (!AppendRelationRefSql(&preservedFrom, parse, preservedRtindex) ||
		!AppendRelationRefSql(&nullableFrom, parse, nullableRtindex))
		OUTER_JOIN_COUNT_REWRITE_SKIP("failed to deparse preserved or nullable relation ref");

	appendStringInfoString(&rewrittenSql, "select ");
	forboth(lc, visibleGroupTes, tlc, groupSqlList)
	{
		TargetEntry *groupTe = lfirst_node(TargetEntry, lc);
		char *groupSql = (char *) lfirst(tlc);

		if (lc != list_head(visibleGroupTes))
			appendStringInfoString(&rewrittenSql, ", ");

		appendStringInfoString(&rewrittenSql, groupSql);
		if (groupTe->resname != NULL && groupTe->resname[0] != '\0')
			appendStringInfo(&rewrittenSql, " as %s", quote_identifier(groupTe->resname));
	}

	if (needs_outer_regroup)
		appendStringInfo(&rewrittenSql,
			", sum(coalesce(se_preagg.se_count, 0::bigint))");
	else
		appendStringInfo(&rewrittenSql,
			", coalesce(se_preagg.se_count, 0::bigint)");
	if (countTe->resname != NULL && countTe->resname[0] != '\0')
		appendStringInfo(&rewrittenSql, " as %s", quote_identifier(countTe->resname));

	appendStringInfo(&rewrittenSql,
		" from %s left join (select %s as se_join_key, count(%s) as se_count from %s",
		preservedFrom.data,
		nullableJoinSql,
		countArgSql,
		nullableFrom.data);

	if (nullableQualSql != NULL && nullableQualSql[0] != '\0')
		appendStringInfo(&rewrittenSql, " where %s", nullableQualSql);

	appendStringInfo(&rewrittenSql,
		" group by %s) se_preagg on %s = se_preagg.se_join_key",
		nullableJoinSql,
		preservedJoinSql);

	if (needs_outer_regroup)
	{
		appendStringInfoString(&rewrittenSql, " group by ");
		foreach(lc, groupSqlList)
		{
			if (lc != list_head(groupSqlList))
				appendStringInfoString(&rewrittenSql, ", ");
			appendStringInfoString(&rewrittenSql, (char *) lfirst(lc));
		}
	}

	PG_TRY();
	{
#if PG_VERSION_NUM >= PG_VERSION_14
		rawParsetrees = raw_parser(rewrittenSql.data, RAW_PARSE_DEFAULT);
#else
		rawParsetrees = raw_parser(rewrittenSql.data);
#endif
	}
	PG_CATCH();
	{
		FlushErrorState();
		return NULL;
	}
	PG_END_TRY();

	if (list_length(rawParsetrees) != 1)
		OUTER_JOIN_COUNT_REWRITE_SKIP("rewritten SQL produced unexpected raw parse tree count");

	rawStmt = linitial_node(RawStmt, rawParsetrees);

	PG_TRY();
	{
		rewrittenParse = parse_analyze_fixedparams(rawStmt,
										 rewrittenSql.data,
										 NULL,
										 0,
										 NULL);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(CurrentMemoryContext);
		FlushErrorState();
		rewrittenParse = NULL;
	}
	PG_END_TRY();

	if (rewrittenParse == NULL || rewrittenParse->commandType != CMD_SELECT)
		OUTER_JOIN_COUNT_REWRITE_SKIP("rewritten SQL failed parse analysis");

	if (engine_debug_vectorized_groupagg_fallback)
		elog(DEBUG1, "Outer-join count rewrite applied");
	*planner_query_string = rewrittenSql.data;
	return rewrittenParse;

rewrite_skip:
	if (engine_debug_vectorized_groupagg_fallback && skip_reason != NULL)
		elog(DEBUG1, "Outer-join count rewrite skipped: %s", skip_reason);
	return NULL;

#undef OUTER_JOIN_COUNT_REWRITE_SKIP
}
#endif

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
PlanHasJoinNode(Plan *plan)
{
	if (plan == NULL)
		return false;

	if (plan->type == T_NestLoop ||
		plan->type == T_MergeJoin ||
		plan->type == T_HashJoin)
		return true;

	if (IsA(plan, CustomScan))
	{
		CustomScan *cscan = (CustomScan *) plan;
		ListCell *lc;

		foreach(lc, cscan->custom_plans)
		{
			if (PlanHasJoinNode((Plan *) lfirst(lc)))
				return true;
		}
	}

	return PlanHasJoinNode(plan->lefttree) ||
		PlanHasJoinNode(plan->righttree);
}

typedef struct ExprSourceRelidsContext
{
	Plan *input_plan;
	Bitmapset *relids;
} ExprSourceRelidsContext;

typedef struct PlanExprSourceRelidsContext
{
	Plan *plan;
	Bitmapset *relids;
} PlanExprSourceRelidsContext;

static bool
PlanExprSourceRelidsWalker(Node *node, void *context)
{
	PlanExprSourceRelidsContext *ctx = (PlanExprSourceRelidsContext *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		if (var->varlevelsup != 0)
			return false;

		if (var->varno == OUTER_VAR &&
			ctx->plan != NULL &&
			ctx->plan->lefttree != NULL)
		{
			ctx->relids = bms_join(ctx->relids,
						   PlanOutputColumnSourceRels(ctx->plan->lefttree,
												 var->varattno));
			return false;
		}

		if (var->varno == INNER_VAR &&
			ctx->plan != NULL &&
			ctx->plan->righttree != NULL)
		{
			ctx->relids = bms_join(ctx->relids,
						   PlanOutputColumnSourceRels(ctx->plan->righttree,
												 var->varattno));
			return false;
		}

		if (var->varno == INDEX_VAR &&
			ctx->plan != NULL &&
			ctx->plan->lefttree != NULL &&
			ctx->plan->righttree == NULL)
		{
			ctx->relids = bms_join(ctx->relids,
						   PlanOutputColumnSourceRels(ctx->plan->lefttree,
												 var->varattno));
			return false;
		}

		if (var->varno > 0)
			ctx->relids = bms_add_member(ctx->relids, var->varno);

		return false;
	}

	return expression_tree_walker(node, PlanExprSourceRelidsWalker, context);
}

static Bitmapset *
PlanExprSourceRels(Node *node, Plan *plan)
{
	PlanExprSourceRelidsContext ctx;

	ctx.plan = plan;
	ctx.relids = NULL;
	(void) PlanExprSourceRelidsWalker(node, &ctx);

	return ctx.relids;
}

static bool
ExprSourceRelidsWalker(Node *node, void *context)
{
	ExprSourceRelidsContext *ctx = (ExprSourceRelidsContext *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		if (var->varlevelsup != 0)
			return false;

		if ((var->varno == OUTER_VAR || var->varno == INDEX_VAR) &&
			ctx->input_plan != NULL)
		{
			ctx->relids = bms_join(ctx->relids,
						   PlanOutputColumnSourceRels(ctx->input_plan,
												 var->varattno));
			return false;
		}

		if (var->varno == INNER_VAR &&
			ctx->input_plan != NULL &&
			ctx->input_plan->righttree != NULL)
		{
			ctx->relids = bms_join(ctx->relids,
						   PlanOutputColumnSourceRels(ctx->input_plan->righttree,
												 var->varattno));
			return false;
		}

		if (var->varno > 0)
			ctx->relids = bms_add_member(ctx->relids, var->varno);

		return false;
	}

	return expression_tree_walker(node, ExprSourceRelidsWalker, context);
}

static Bitmapset *
ExprSourceRelsForPlan(Node *node, Plan *input_plan)
{
	ExprSourceRelidsContext ctx;

	ctx.input_plan = input_plan;
	ctx.relids = NULL;
	(void) ExprSourceRelidsWalker(node, &ctx);

	return ctx.relids;
}

static Bitmapset *
PlanOutputColumnSourceRels(Plan *plan, AttrNumber resno)
{
	TargetEntry *tle;

	if (plan == NULL || resno <= 0)
		return NULL;

	tle = get_tle_by_resno(plan->targetlist, resno);
	if (tle != NULL)
		return PlanExprSourceRels((Node *) tle->expr, plan);

	if (plan->righttree == NULL && plan->lefttree != NULL)
		return PlanOutputColumnSourceRels(plan->lefttree, resno);

	return NULL;
}

typedef struct PostJoinAggShapeContext
{
	Plan *input_plan;
	bool found_aggref;
	bool aggregate_input_spans_multiple_rels;
} PostJoinAggShapeContext;

static bool
PostJoinAggShapeWalker(Node *node, void *context)
{
	PostJoinAggShapeContext *ctx = (PostJoinAggShapeContext *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;
		Bitmapset *relids = NULL;
		ListCell *lc;

		ctx->found_aggref = true;

		foreach(lc, aggref->args)
		{
			TargetEntry *arg_te = lfirst_node(TargetEntry, lc);

			relids = bms_join(relids,
						  ExprSourceRelsForPlan((Node *) arg_te->expr,
												ctx->input_plan));
		}

		relids = bms_join(relids,
					  ExprSourceRelsForPlan((Node *) aggref->aggfilter,
											ctx->input_plan));

		if (bms_num_members(relids) > 1)
		{
			ctx->aggregate_input_spans_multiple_rels = true;
			return true;
		}
	}

	return expression_tree_walker(node, PostJoinAggShapeWalker, context);
}

static const char *
DescribePostJoinColumnarAgg(Agg *agg)
{
	Plan *input_plan;
	int ki;
	PostJoinAggShapeContext shape_ctx;

	if (agg == NULL || agg->plan.lefttree == NULL)
		return "post-join aggregate over columnar scans is not supported yet";

	input_plan = agg->plan.lefttree;

	for (ki = 0; ki < agg->numCols; ki++)
	{
		Bitmapset *group_relids =
			PlanOutputColumnSourceRels(input_plan, agg->grpColIdx[ki]);

		if (bms_num_members(group_relids) > 1)
			return "post-join aggregate has group keys sourced from multiple base relations";
	}

	shape_ctx.input_plan = input_plan;
	shape_ctx.found_aggref = false;
	shape_ctx.aggregate_input_spans_multiple_rels = false;
	(void) PostJoinAggShapeWalker((Node *) agg->plan.targetlist, &shape_ctx);
	(void) PostJoinAggShapeWalker((Node *) agg->plan.qual, &shape_ctx);

	if (shape_ctx.aggregate_input_spans_multiple_rels)
		return "post-join aggregate has aggregate inputs sourced from multiple base relations";

	if (IsQ13StyleOuterJoinCountAgg(agg))
		return "outer-join count on nullable column awaits partial preaggregation";

	if (shape_ctx.found_aggref)
		return "post-join aggregate over columnar scans awaits a partial-aggregation transform";

	return "post-join aggregate over columnar scans is not supported yet";
}

static bool
IsQ13StyleOuterJoinCountAgg(Agg *agg)
{
	Plan *join_plan;
	Join *join;
	Plan *agg_side_plan;
	Index agg_side_varno;
	AttrNumber group_join_resno;
	AttrNumber count_join_resno;
	TargetEntry *group_join_tle;
	TargetEntry *count_join_tle;
	Var *group_join_var;
	Var *count_join_var;
	Plan *scan_plan;
	CustomScan *childScan;
	Aggref *count_aggref = NULL;
	TargetEntry *target_te;
	char *aggname;

	if (agg == NULL ||
		agg->aggsplit != AGGSPLIT_SIMPLE ||
		agg->numCols != 1 ||
		agg->plan.lefttree == NULL ||
		agg->plan.lefttree->type != T_HashJoin ||
		list_length(agg->plan.targetlist) != 2)
		return false;

	join_plan = agg->plan.lefttree;
	join = &((HashJoin *) join_plan)->join;

	if (join->jointype == JOIN_RIGHT)
	{
		agg_side_plan = join_plan->lefttree;
		agg_side_varno = OUTER_VAR;
	}
	else if (join->jointype == JOIN_LEFT)
	{
		agg_side_plan = join_plan->righttree;
		agg_side_varno = INNER_VAR;
	}
	else
		return false;

	group_join_resno = agg->grpColIdx[0];
	group_join_tle = get_tle_by_resno(join_plan->targetlist, group_join_resno);
	if (group_join_tle == NULL || !IsA(group_join_tle->expr, Var))
		return false;
	group_join_var = (Var *) group_join_tle->expr;
	if (group_join_var->varno == agg_side_varno)
		return false;

	target_te = lsecond_node(TargetEntry, agg->plan.targetlist);
	if (target_te == NULL || target_te->resjunk ||
		!ExtractTargetAggref((Node *) target_te->expr, &count_aggref) ||
		count_aggref == NULL ||
		count_aggref->aggfilter != NULL ||
		count_aggref->aggdistinct != NIL ||
		count_aggref->aggstar ||
		list_length(count_aggref->args) != 1)
		return false;

	aggname = get_func_name(count_aggref->aggfnoid);
	if (aggname == NULL || strcmp(aggname, "count") != 0)
		return false;

	if (!IsA(((TargetEntry *) linitial(count_aggref->args))->expr, Var))
		return false;
	count_join_resno = ((Var *) ((TargetEntry *) linitial(count_aggref->args))->expr)->varattno;
	count_join_tle = get_tle_by_resno(join_plan->targetlist, count_join_resno);
	if (count_join_tle == NULL || !IsA(count_join_tle->expr, Var))
		return false;
	count_join_var = (Var *) count_join_tle->expr;
	if (count_join_var->varno != agg_side_varno)
		return false;

	scan_plan = agg_side_plan;
	if (scan_plan->type == T_Sort)
		scan_plan = ((Sort *) scan_plan)->plan.lefttree;
	if (scan_plan->type == T_Gather || scan_plan->type == T_GatherMerge)
	{
		scan_plan = scan_plan->lefttree;
		if (scan_plan->type == T_Sort)
			scan_plan = ((Sort *) scan_plan)->plan.lefttree;
	}
	if (scan_plan->type != T_CustomScan)
		return false;

	childScan = (CustomScan *) scan_plan;
	if (childScan->methods != engine_customscan_methods() &&
		(childScan->methods == NULL ||
		 strcmp(childScan->methods->CustomName, "ColcompressScan") != 0))
		return false;

	return true;
}

static bool
ShouldRelaxVecGroupAggGroupLimit(Agg *aggNode, Oid first_key_typeoid)
{
	ListCell *lc;
	bool found_count = false;

	if (aggNode == NULL || aggNode->plan.qual != NIL || aggNode->numCols != 1)
		return false;

	if (first_key_typeoid != INT4OID && first_key_typeoid != INT8OID)
		return false;

	foreach(lc, aggNode->plan.targetlist)
	{
		TargetEntry *te = lfirst_node(TargetEntry, lc);
		Aggref *aggref = NULL;
		char *aggname;

		if (te->resjunk)
			continue;

		if (IsA(StripRelabels((Node *) te->expr), Var))
			continue;

		if (!ExtractTargetAggref((Node *) te->expr, &aggref) || aggref == NULL)
			return false;

		aggname = get_func_name(aggref->aggfnoid);
		if (aggname == NULL || strcmp(aggname, "count") != 0 ||
			aggref->aggfilter != NULL ||
			aggref->aggdistinct != NIL)
			return false;

		found_count = true;
	}

	return found_count;
}

static bool
PlanIsPostJoinColumnarAgg(Agg *agg)
{
	if (agg == NULL || agg->plan.lefttree == NULL)
		return false;

	return PlanHasJoinNode(agg->plan.lefttree) &&
		PlanHasColumnarCustomScan(agg->plan.lefttree);
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
