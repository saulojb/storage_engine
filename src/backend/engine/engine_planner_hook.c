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
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "tcop/utility.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"
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

#include "engine/utils/listutils.h"

static planner_hook_type PreviousPlannerHook = NULL;
static Oid engine_tableam_oid = InvalidOid;

static PlannedStmt * ColumnarPlannerHook(Query *parse,  const char *query_string,
									 int cursorOptions, ParamListInfo boundParams
#if PG_VERSION_NUM >= PG_VERSION_19
									 , ExplainState *es
#endif
									 );
static bool IsCreateTableAs(const char *query);
static bool IsExplainQuery(const char *query);

typedef struct PlanTreeMutatorContext
{
	bool vectorizedAggregation;
} PlanTreeMutatorContext;

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

		newAggRefNode->aggfnoid = vectorizedProcedureOid;

		return (Node *) newAggRefNode;
	}

	return expression_tree_mutator(node, ExpressionMutator, (void *) context);
}

#if PG_VERSION_NUM >= PG_VERSION_14

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

				vectorizedAggregateExecution->constbyval = true;
				vectorizedAggregateExecution->consttype = CUSTOM_SCAN_VECTORIZED_AGGREGATE;
				vectorizedAggregateExecution->constvalue =  planTreeContext->vectorizedAggregation;
				vectorizedAggregateExecution->constlen = sizeof(bool);

				customScan->custom_private = lappend(customScan->custom_private, vectorizedAggregateExecution);
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

			if (!engine_enable_vectorization)
				return node;

			if (aggNode->plan.lefttree->type == T_CustomScan)
			{
				/*
				 * Only vectorize simple (non-split) aggregation.  Partial or
				 * final split nodes (parallel query) require serialfn/combinefn
				 * which our vectorized aggregates do not provide.
				 */
				if (aggNode->aggstrategy == AGG_PLAIN &&
					aggNode->aggsplit == AGGSPLIT_SIMPLE)
				{
					vectorizedAggNode = engine_create_aggregator_node();

					FLATCOPY(newAgg, aggNode, Agg);

					newAgg->plan.targetlist = 
						(List *) expression_tree_mutator((Node *) newAgg->plan.targetlist, ExpressionMutator, NULL);


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


					PlanTreeMutatorContext *planTreeContext = (PlanTreeMutatorContext *) context;
					planTreeContext->vectorizedAggregation = true;

					PlanTreeMutator(node->lefttree, context);
					PlanTreeMutator(node->righttree, context);

					vectorizedAggNode->scan.plan.lefttree = node->lefttree;
					vectorizedAggNode->scan.plan.righttree = node->righttree;

					return (Plan *) vectorizedAggNode;
				}

				return node;
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
	}

	node->lefttree = PlanTreeMutator(node->lefttree, context);
	node->righttree = PlanTreeMutator(node->righttree, context);

	return node;
}
#endif

static PlannedStmt *
ColumnarPlannerHook(Query *parse,
					const char *query_string,
					int cursorOptions,
					ParamListInfo boundParams
#if PG_VERSION_NUM >= PG_VERSION_19
					, ExplainState *es
#endif
					)
{
	PlannedStmt	*stmt;
#if PG_VERSION_NUM >= PG_VERSION_14
	Plan *savedPlanTree;
	List *savedSubplan;
	MemoryContext saved_context;
#endif

	if (PreviousPlannerHook)
#if PG_VERSION_NUM >= PG_VERSION_19
			stmt = PreviousPlannerHook(parse, query_string, cursorOptions, boundParams, es);
#else
			stmt = PreviousPlannerHook(parse, query_string, cursorOptions, boundParams);
#endif
		else
#if PG_VERSION_NUM >= PG_VERSION_19
			stmt = standard_planner(parse, query_string, cursorOptions, boundParams, es);
#else
			stmt = standard_planner(parse, query_string, cursorOptions, boundParams);
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

#if PG_VERSION_NUM >= PG_VERSION_14
	/*
	 * Skip plan tree mutation for EXPLAIN queries.  When citus is loaded
	 * alongside storage_engine, citus calls ExecutorStart internally during
	 * EXPLAIN processing.  Mutating the plan tree in that context causes a
	 * segfault because citus does not expect the plan to be modified after
	 * its own planner hook has run.  Plain EXPLAIN (without ANALYZE) does
	 * not execute the plan, so vectorization is irrelevant anyway.
	 */
	if (IsExplainQuery(query_string))
		return stmt;

	if (!(engine_enable_vectorization			/* Vectorization should be enabled */
			|| engine_index_scan)				/* or Engine Index Scan */
		|| stmt->commandType != CMD_SELECT		/* only SELECTS are supported  */
		|| list_length(stmt->rtable) != 1)		/* JOINs are not yet supported */
		return stmt;

	if (engine_tableam_oid == InvalidOid)
		engine_tableam_oid = get_table_am_oid("columnar", true);

	savedPlanTree = stmt->planTree;
	savedSubplan = stmt->subplans;

	saved_context = CurrentMemoryContext;

	PG_TRY();
	{
		List		*subplans = NULL;
		ListCell	*cell;

		PlanTreeMutatorContext plainTreeContext;
		plainTreeContext.vectorizedAggregation = 0;

		stmt->planTree = (Plan *) PlanTreeMutator(stmt->planTree, (void *) &plainTreeContext);

		foreach(cell, stmt->subplans)
		{
			PlanTreeMutatorContext subPlainTreeContext;
			plainTreeContext.vectorizedAggregation = 0;
			Plan *subplan = (Plan *) PlanTreeMutator(lfirst(cell), (void *) &subPlainTreeContext);
			subplans = lappend(subplans, subplan);
		}

		stmt->subplans = subplans;
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
#endif

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

/*
 * IsExplainQuery
 *
 * Returns true if the query string is a plain EXPLAIN (without ANALYZE),
 * case-insensitive, with optional leading whitespace.
 *
 * We skip plan-tree mutation only for plain EXPLAIN because citus calls
 * ExecutorStart internally during plain EXPLAIN processing and does not
 * expect the plan to be modified after its own planner hook has run.
 * EXPLAIN ANALYZE actually executes the plan, so we allow mutation there
 * so that the vectorized plan tree is visible and active.
 */
static bool
IsExplainQuery(const char *query)
{
	const char *p;

	if (query == NULL)
		return false;

	/* Skip leading whitespace */
	while (*query == ' ' || *query == '\t' || *query == '\n' || *query == '\r')
		query++;

	/* Must start with EXPLAIN */
	if (pg_strncasecmp(query, "explain", 7) != 0)
		return false;

	/* Advance past "explain" and skip whitespace */
	p = query + 7;
	while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')
		p++;

	/* If the next keyword is ANALYZE, allow mutation (return false) */
	if (pg_strncasecmp(p, "analyze", 7) == 0)
		return false;

	return true;
}

void engine_planner_init(void)
{
	PreviousPlannerHook = planner_hook;
	planner_hook = ColumnarPlannerHook;
#if  PG_VERSION_NUM >= PG_VERSION_14
	engine_register_aggregator_node();
#endif
	engine_register_indexscan_node();
}
