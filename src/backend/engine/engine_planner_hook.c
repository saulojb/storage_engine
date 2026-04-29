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
static bool IsExplainQuery(const char *query);
static bool QueryHasVectorizableAggregate(Query *parse);

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
		case FLOAT8OID:	return VECGAGG_TYPE_FLOAT8;
		default:		return -1;
	}
}

/*
 * Classify an Aggref into VECGAGG_COUNT_STAR / SUM / MIN / MAX.
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
			if ((aggNode->aggstrategy == AGG_HASHED ||
				 aggNode->aggstrategy == AGG_SORTED) &&
				aggNode->aggsplit == AGGSPLIT_SIMPLE &&
				aggNode->numCols == 1 &&
				engine_enable_vectorization)
			{
				Plan		   *child_plan = node->lefttree;
				Plan		   *scan_plan;  /* the actual ColcompressScan */
				CustomScan	   *childScan;
				AttrNumber		scan_key_pos;
				AttrNumber		key_varattno;
				TargetEntry	   *key_scan_te;
				Oid				key_typeoid;
				VecGroupAggTarget targets[VECGROUPAGG_MAX_TARGETS];
				int				num_targets = 0;
				bool			supported = true;
				ListCell	   *lc;
				int				result_att;  /* 0-based position in result tuple */				int				key_result_att = 0; /* 0-based position of GROUP BY key in result */
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
					goto groupagg_fallback;
				childScan = (CustomScan *) scan_plan;
				if (childScan->methods != engine_customscan_methods())
					goto groupagg_fallback;

				/* Get GROUP BY key info from the scan plan's targetlist */
				scan_key_pos = aggNode->grpColIdx[0];  /* 1-based in scan output */
				key_varattno = ScanOutputPosToVarAttno(scan_plan, scan_key_pos);
				if (key_varattno == 0)
					goto groupagg_fallback;

				key_scan_te = get_tle_by_resno(scan_plan->targetlist, scan_key_pos);
				if (key_scan_te == NULL || !IsA(key_scan_te->expr, Var))
					goto groupagg_fallback;
				key_typeoid = ((Var *) key_scan_te->expr)->vartype;
				if (TypeOidToVecGaggType(key_typeoid) < 0)
					goto groupagg_fallback;

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
						supported = false;
						break;
					}

					targets[num_targets].agg_kind       = kind;
					targets[num_targets].col_type       = TypeOidToVecGaggType(col_typeoid);
					targets[num_targets].col_attnum     = (int) col_varattno;
					targets[num_targets].result_attnum  = result_att;
					targets[num_targets].result_typeoid = aggref->aggtype;
						num_targets++;
						result_att++;
					}
				}

				if (!supported || num_targets == 0)
					goto groupagg_fallback;

				{
					/* Build VectorGroupAgg plan node */
					CustomScan *vgaNode = engine_create_groupagg_node(
						(int) key_varattno,
						key_typeoid,
						key_result_att,
						(aggNode->aggstrategy == AGG_SORTED), /* sort_output */
						num_targets,
						targets);

					/*
					 * The child ColcompressScan must be signaled to return
					 * VectorTupleTableSlot batches.
					 */
					PlanTreeMutatorContext *planTreeContext =
						(PlanTreeMutatorContext *) context;
					planTreeContext->vectorizedAggregation = true;

					/* This recurse marks the ColcompressScan child */
					PlanTreeMutator(scan_plan, context);

					/* Set up node cost/row estimates from original agg */
					Plan *vgaNodePlan = (Plan *) vgaNode;
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

					/* Child plan goes in custom_plans for ExecInitNode */
					vgaNode->custom_plans = lappend(vgaNode->custom_plans, scan_plan);

					return (Plan *) vgaNode;
				}

groupagg_fallback:
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
				 * output; sort_output is at custom_private[3] as key_result_att.
				 */
				if (sortNode->numCols == 1)
				{
					int key_result_att_vga = DatumGetInt32(
						((Const *) list_nth(vga->custom_private, 3))->constvalue);

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
	Plan *savedPlanTree;
	List *savedSubplan;
	MemoryContext saved_context;
	int saved_max_parallel_workers_per_gather = max_parallel_workers_per_gather;
	Query *parse_for_pass2 = NULL;
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
	if (engine_enable_vectorization && QueryHasVectorizableAggregate(parse))
		parse_for_pass2 = copyObject(parse);
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
		|| stmt->commandType != CMD_SELECT)		/* only SELECTS are supported  */
		return stmt;

	if (engine_tableam_oid == InvalidOid)
		engine_tableam_oid = get_table_am_oid("columnar", true);

	/*
	 * Vectorized aggregation strategy (two-pass planning):
	 *
	 * Vectorized aggregates only work with AGGSPLIT_SIMPLE (serial T_Agg).
	 * When parallel workers are available the planner generates
	 * AGGSPLIT_INITIAL_SERIAL / AGGSPLIT_FINAL_DESERIAL split nodes instead,
	 * making vectorization impossible.
	 *
	 * To handle mixed queries correctly (some aggregates vectorizable, some
	 * not — e.g. SELECT min(a), my_custom_agg(b) FROM foo), we use a
	 * two-pass approach:
	 *
	 *   Pass 1 (done above): plan with original parallelism → parallel plan
	 *                         stored in stmt.  This is the fallback.
	 *   Pass 2 (below, only when hasAggs=true): re-plan with parallelism=0
	 *                         → serial plan → attempt PlanTreeMutator.
	 *         Success → return vectorized serial plan (faster than parallel
	 *                   for pure min/max/sum/count via chunk metadata).
	 *         Failure → PG_CATCH discards serial plan, returns stmt from
	 *                   Pass 1 (the original parallel plan).
	 *
	 * Cost: one extra planner call only when the query has aggregates AND
	 * vectorization is enabled.  Pure scan queries pay zero overhead.
	 */
	if (engine_enable_vectorization && parse_for_pass2 != NULL)
	{
		PlannedStmt *stmt_serial;

		max_parallel_workers_per_gather = 0;

		PG_TRY();
		{
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

			max_parallel_workers_per_gather = saved_max_parallel_workers_per_gather;

			savedPlanTree = stmt_serial->planTree;
			savedSubplan  = stmt_serial->subplans;
			saved_context = CurrentMemoryContext;

			{
				List		*subplans = NULL;
				ListCell	*cell;
				PlanTreeMutatorContext plainTreeContext;
				plainTreeContext.vectorizedAggregation = 0;

				stmt_serial->planTree = (Plan *) PlanTreeMutator(stmt_serial->planTree,
																 (void *) &plainTreeContext);

				foreach(cell, stmt_serial->subplans)
				{
					PlanTreeMutatorContext subPlainTreeContext;
					subPlainTreeContext.vectorizedAggregation = 0;
					Plan *subplan = (Plan *) PlanTreeMutator(lfirst(cell),
															 (void *) &subPlainTreeContext);
					subplans = lappend(subplans, subplan);
				}

				stmt_serial->subplans = subplans;
			}

			/* Vectorization succeeded — use the serial vectorized plan */
			return stmt_serial;
		}
		PG_CATCH();
		{
			ErrorData  *edata;
			MemoryContextSwitchTo(saved_context);
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
	engine_register_groupagg_node();
#endif
	engine_register_indexscan_node();
}
