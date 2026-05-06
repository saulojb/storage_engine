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
		case BPCHAROID:	return VECGAGG_TYPE_BPCHAR;
		case TEXTOID:	return VECGAGG_TYPE_TEXT;
		default:		return -1;
	}
}

/*
 * Classify an Aggref into VECGAGG_COUNT_STAR / SUM / MIN / MAX / AVG.
 * Fills *out_kind, *out_col_varattno, *out_col_typeoid.
 * Returns false if the aggregate is not supported.
 */
static bool
ClassifyAggref(Aggref *aggref, Plan *child_plan,
			   int *out_kind, AttrNumber *out_col_varattno, Oid *out_col_typeoid)
{
	const char *fname;

	/* count(*): aggstar = true, no args */
	if (aggref->aggstar)
	{
		*out_kind         = VECGAGG_COUNT_STAR;
		*out_col_varattno = 0;
		*out_col_typeoid  = INT8OID;
		return true;
	}

	/* We only handle single-argument aggregates */
	if (list_length(aggref->args) != 1)
		return false;

	TargetEntry *arg_te = (TargetEntry *) linitial(aggref->args);
	if (!IsA(arg_te->expr, Var))
		return false;

	Var *arg_var = (Var *) arg_te->expr;

	/* Map scan-output position to table varattno */
	AttrNumber scan_pos  = arg_var->varattno;
	AttrNumber col_attno = ScanOutputPosToVarAttno(child_plan, scan_pos);
	if (col_attno == 0)
		return false;

	/* Check type is supported */
	Oid arg_typeoid = arg_var->vartype;
	if (TypeOidToVecGaggType(arg_typeoid) < 0)
		return false;

	fname = get_func_name(aggref->aggfnoid);
	if (fname == NULL)
		return false;

	if (strcmp(fname, "sum") == 0 || strcmp(fname, "int4_sum") == 0 ||
		strcmp(fname, "int8_sum") == 0 || strcmp(fname, "float8pl") == 0)
		*out_kind = VECGAGG_SUM;
	else if (strcmp(fname, "avg") == 0)
	{
		if (arg_typeoid != FLOAT4OID && arg_typeoid != FLOAT8OID)
			return false;
		*out_kind = VECGAGG_AVG;
	}
	else if (strcmp(fname, "min") == 0 || strcmp(fname, "int4smaller") == 0 ||
			 strcmp(fname, "int8smaller") == 0 || strcmp(fname, "float8smaller") == 0)
		*out_kind = VECGAGG_MIN;
	else if (strcmp(fname, "max") == 0 || strcmp(fname, "int4larger") == 0 ||
			 strcmp(fname, "int8larger") == 0 || strcmp(fname, "float8larger") == 0)
		*out_kind = VECGAGG_MAX;
	else
		return false;

	*out_col_varattno = col_attno;
	*out_col_typeoid  = arg_typeoid;
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

					PlanTreeMutator(node->lefttree, context);
					PlanTreeMutator(node->righttree, context);
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
			 *   - Single GROUP BY key
			 *   - Key type: int4/int8/float8
			 *   - Aggregates: count(*), sum, min, max over int4/int8/float8
			 *   - Direct ColcompressScan child (no intermediate nodes)
			 */
			if (engine_enable_vectorized_groupagg &&
				(aggNode->aggstrategy == AGG_HASHED ||
				 aggNode->aggstrategy == AGG_SORTED) &&
				(aggNode->aggsplit == AGGSPLIT_SIMPLE ||
				 aggNode->aggsplit == AGGSPLIT_INITIAL_SERIAL) &&
				(aggNode->numCols == 1 || aggNode->numCols == 0) &&
				engine_enable_vectorization)
			{
				Plan		   *child_plan = node->lefttree;
				Plan		   *scan_plan;  /* the actual ColcompressScan */
				CustomScan	   *childScan;
				const char	   *fallback_reason = "unspecified";
				AttrNumber		scan_key_pos;
				AttrNumber		key_table_attno;
				int				key_slot_idx;
				TargetEntry	   *key_scan_te;
				Oid				key_typeoid;
				bool			key_is_const = false;
				Const		   *key_const = NULL;
				VecGroupAggTarget targets[VECGROUPAGG_MAX_TARGETS];
				int				num_targets = 0;
				bool			supported = true;
				ListCell	   *lc;
				int				result_att;
				int				key_result_att = -1;
				double			estimated_groups = 0.0;
				/*
				 * For AGG_SORTED the planner inserts a Sort node between
				 * the Agg and the scan.  Strip it — VecGroupAgg uses a
				 * hash table and does not need sorted input.
				 */
				scan_plan = child_plan;
				if (scan_plan->type == T_Sort)
					scan_plan = ((Sort *) scan_plan)->plan.lefttree;

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

				if (aggNode->numCols == 1)
				{
					/* Get GROUP BY key info from the scan plan's targetlist */
					scan_key_pos = aggNode->grpColIdx[0];
					key_table_attno = ScanOutputPosToVarAttno(scan_plan, scan_key_pos);
					if (key_table_attno == 0)
					{
						fallback_reason = "failed to map GROUP BY key to table attno";
						goto groupagg_fallback;
					}
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
									key_const = c;
									key_typeoid = v->vartype;
								}
								else if (const_key_attno != v->varattno)
								{
									fallback_reason = "multiple Var=Const quals with different keys";
									goto groupagg_fallback;
								}
							}
						}
					}

					if (const_key_attno == 0 || key_const == NULL)
					{
						fallback_reason = "numCols=0 without inferable Var=Const key";
						goto groupagg_fallback;
					}

					key_table_attno = const_key_attno;
					key_is_const = true;
					scan_key_pos = 0;
				}

				estimated_groups = (aggNode->numGroups > 0.0)
					? aggNode->numGroups
					: aggNode->plan.plan_rows;

				estimated_groups = EstimateGroupByDistinct(planTreeContext,
											   scan_plan,
											   key_table_attno,
											   estimated_groups);

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
				 * Compute the 0-based slot output position for the GROUP BY key.
				 * ColumnarReadNextVector stores columns at the rank of their
				 * varattno in the sorted projected-attno list, NOT at varattno-1.
				 */
				if (!key_is_const)
				{
					key_slot_idx = VarAttnoToSlotIdx(scan_plan, key_table_attno);
					if (key_slot_idx < 0)
					{
						fallback_reason = "failed to map GROUP BY key to vector slot";
						goto groupagg_fallback;
					}
				}
				else
				{
					key_slot_idx = -1;
				}

				if (!key_is_const)
				{
					key_scan_te = get_tle_by_resno(scan_plan->targetlist, scan_key_pos);
					if (key_scan_te == NULL || !IsA(key_scan_te->expr, Var))
					{
						fallback_reason = "GROUP BY key target is not Var";
						goto groupagg_fallback;
					}
					key_typeoid = ((Var *) key_scan_te->expr)->vartype;
				}
				if (KeyTypeOidToVecGaggType(key_typeoid) < 0)
				{
					fallback_reason = "GROUP BY key type unsupported";
					goto groupagg_fallback;
				}

				/* Walk the result targetlist to find aggregate columns */
				result_att = 0;
				foreach(lc, aggNode->plan.targetlist)
				{
					TargetEntry *te = (TargetEntry *) lfirst(lc);

					if (te->resjunk)
						continue;

					if (IsA(te->expr, Var))
					{
						/*
						 * GROUP BY key column in output.  We already handle this
						 * via VecGroupEntry.key; no extra target needed.
						 * But we must account for result_att ordering.
						 */
						key_result_att = result_att;  /* record key's 0-based output position */
						result_att++;
						continue;
					}

					if (!IsA(te->expr, Aggref))
					{
						supported = false;
						break;
					}

					if (num_targets >= VECGROUPAGG_MAX_TARGETS)
					{
						supported = false;
						break;
					}

					{
						Aggref	   *aggref = (Aggref *) te->expr;
						int			kind;
						AttrNumber	col_varattno;
						Oid			col_typeoid;

					if (!ClassifyAggref(aggref, scan_plan,
										&kind, &col_varattno, &col_typeoid))
					{
						fallback_reason = "aggregate target unsupported";
						supported = false;
						break;
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
						if (kind == VECGAGG_COUNT_STAR)
							col_slot_idx = -1;
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
					targets[num_targets].col_type       = TypeOidToVecGaggType(col_typeoid);
					targets[num_targets].result_attnum  = result_att;
					targets[num_targets].result_typeoid = aggref->aggtype;
						num_targets++;
						result_att++;
					}
				}

				if (!supported || num_targets == 0 || key_result_att < 0)
				{
					if (num_targets == 0)
						fallback_reason = "no vectorizable aggregate targets";
					else if (key_result_att < 0)
						fallback_reason = "group key not present in output targetlist";
					else if (supported == false && strcmp(fallback_reason, "unspecified") == 0)
						fallback_reason = "targetlist shape incompatible with VecGroupAgg";
					goto groupagg_fallback;
				}

				{
					/* Build VectorGroupAgg plan node */
					CustomScan *vgaNode = engine_create_groupagg_node(
						key_slot_idx,
						key_typeoid,
						key_is_const,
						key_const,
						key_result_att,
						(aggNode->aggstrategy == AGG_SORTED), /* sort_output */
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
			{
				PG_TRY();
				{
					MutatePlannedStmt(stmt_serial);
					pass2Vectorized = true;
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
					stmt_serial->planTree = savedPlanTree;
					stmt_serial->subplans = savedSubplan;
					pass2Vectorized = false;
				}
				PG_END_TRY();
			}

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
 */
static bool
QueryStringHasPlainExplain(const char *query)
{
	List       *raw_parsetree_list;
	ListCell   *lc;

	if (query == NULL)
		return false;

	/* raw_parser is already used elsewhere in the extension for syntax checks */
#if PG_VERSION_NUM >= PG_VERSION_14
	raw_parsetree_list = raw_parser(query, RAW_PARSE_DEFAULT);
#else
	raw_parsetree_list = raw_parser(query);
#endif

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
