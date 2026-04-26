/*-------------------------------------------------------------------------
 *
 * engine_customscan.c
 *
 * This file contains the implementation of a postgres custom scan that
 * we use to push down the projections into the table access methods.
 *
 * Copyright (c) Citus Data, Inc.
 * Copyright (c) Hydra, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "pg_version_compat.h"

#include "postgres.h"

#include <math.h>

#include "access/amapi.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_statistic.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= PG_VERSION_18
#include "commands/explain_format.h"
#endif
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/restrictinfo.h"
#if PG_VERSION_NUM >= PG_VERSION_16
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#endif
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"

#include "engine/engine.h"
#include "engine/engine_customscan.h"
#include "engine/engine_metadata.h"
#include "engine/engine_tableam.h"
#include "engine/engine_version_compat.h"
#include "engine/utils/listutils.h"
#include "engine/rowcompress.h"

#include "engine/vectorization/engine_vector_execution.h"
#include "engine/vectorization/engine_vector_types.h"

#ifndef Abs
#define Abs(x)			((x) >= 0 ? (x) : -(x))
#endif

/*
 * ColumnarScanState represents the state for a columnar scan. It's a
 * CustomScanState with additional fields specific to columnar scans.
 */
typedef struct ColumnarScanState
{
	CustomScanState custom_scanstate; /* must be first field */
	Bitmapset *attrNeeded;

	ExprContext *css_RuntimeContext;
	List *qual;

	/* Parallel execution */
	ParallelColumnarScan parallelColumnarScan;

	/* Vectorization */
	struct
	{
		bool vectorizationEnabled;
		bool vectorizationAggregate;
		TupleTableSlot *scanVectorSlot;
		TupleTableSlot *resultVectorSlot;
		uint32 vectorPendingRowNumber;
		uint32 vectorRowIndex;
		List *vectorizedQualList;
		List *constructedVectorizedQualList;
		List *attrNeededList;
	} vectorization;

	/* Scan snapshot*/
	Snapshot snapshot;
	bool snapshotRegisteredByUs;
} ColumnarScanState;

/*
 * RowcompressScanState — per-scan state for the RowcompressScan custom node.
 * This is a thin wrapper over the standard rowcompress sequential scan,
 * adding batch-level min/max pruning and async read-ahead.
 */
typedef struct RowcompressScanState
{
	CustomScanState css;           /* must be first */
	TableScanDesc   scanDesc;      /* rowcompress sequential scan */
	List           *pushdownClauses; /* plain clause exprs for batch pruning */
	Snapshot        snapshot;
	bool            snapshotRegisteredByUs;
} RowcompressScanState;

typedef bool (*PathPredicate)(Path *path);


/* functions to cost paths in-place */
static void CostColumnarPaths(PlannerInfo *root, RelOptInfo *rel, Oid relationId);
static void CostColumnarIndexPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
								  IndexPath *indexPath);
static void CostColumnarSeqPath(RelOptInfo *rel, Oid relationId, Path *path);
static void AdjustColumnarParallelScanCost(Path *path);
static void CostColumnarScan(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
							 CustomPath *cpath, int numberOfColumnsRead,
							 int nClauses);

/* functions to add new paths */
static void AddColumnarScanPaths(PlannerInfo *root, RelOptInfo *rel,
								 RangeTblEntry *rte);
static Path * AddColumnarScanPath(PlannerInfo *root, RelOptInfo *rel,
								  RangeTblEntry *rte, Relids required_relids);

/* helper functions to be used when costing paths or altering them */
static void RemovePathsByPredicate(RelOptInfo *rel, PathPredicate removePathPredicate);
static bool IsNotIndexPath(Path *path);
static bool IsRowCompressRangeIndexPath(Path *path);
static Cost ColumnarIndexScanAdditionalCost(PlannerInfo *root, RelOptInfo *rel,
											Oid relationId, IndexPath *indexPath);
static int RelationIdGetNumberOfAttributes(Oid relationId);
static Cost ColumnarPerStripeScanCost(RelOptInfo *rel, Oid relationId,
									  int numberOfColumnsRead);
static uint64 ColumnarTableStripeCount(Oid relationId);
static Path * CreateColumnarSeqScanPath(PlannerInfo *root, RelOptInfo *rel,
										Oid relationId);
static void AddColumnarScanPathsRec(PlannerInfo *root, RelOptInfo *rel,
									RangeTblEntry *rte, Relids paramRelids,
									Relids candidateRelids,
									int depthLimit);

/* hooks and callbacks */
static void ColumnarSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
									   RangeTblEntry *rte);
#if PG_VERSION_NUM < PG_VERSION_19
static void ColumnarGetRelationInfoHook(PlannerInfo *root, Oid relationObjectId,
										bool inhparent, RelOptInfo *rel);
#endif

static Plan * ColumnarScanPath_PlanCustomPath(PlannerInfo *root,
											  RelOptInfo *rel,
											  struct CustomPath *best_path,
											  List *tlist,
											  List *clauses,
											  List *custom_plans);
static List * ColumnarScanPath_ReparameterizeCustomPathByChild(PlannerInfo *root,
															   List *custom_private,
															   RelOptInfo *child_rel);
static Node * ColumnarScan_CreateCustomScanState(CustomScan *cscan);

static void ColumnarScan_BeginCustomScan(CustomScanState *node, EState *estate,
										 int eflags);

static TupleTableSlot * CustomExecScan(ColumnarScanState *node,
									   ExecScanAccessMtd accessMtd,
									   ExecScanRecheckMtd recheckMtd);
static TupleTableSlot * ColumnarScan_ExecCustomScan(CustomScanState *node);
static void ColumnarScan_EndCustomScan(CustomScanState *node);
static void ColumnarScan_ReScanCustomScan(CustomScanState *node);
static void ColumnarScan_ExplainCustomScan(CustomScanState *node, List *ancestors,
										   ExplainState *es);
static Size Columnar_EstimateDSMCustomScan(CustomScanState *node,
										   ParallelContext *pcxt);
static void Columnar_InitializeDSMCustomScan(CustomScanState *node,
											ParallelContext *pcxt,
											void *coordinate);
static void Columnar_ReinitializeDSMCustomScan(CustomScanState *node,
											   ParallelContext *pcxt,
											   void *coordinate);
static void Columnar_InitializeWorkerCustomScan(CustomScanState *node,
												shm_toc *toc,
												void *coordinate);

/* helper functions to build strings for EXPLAIN */
static const char * ColumnarPushdownClausesStr(List *context, List *clauses);
static const char * ColumnarProjectedColumnsStr(List *context,
												List *projectedColumns);
#if PG_VERSION_NUM >= 130000
static List * set_deparse_context_planstate(List *dpcontext, Node *node,
											List *ancestors);
#endif

/* other helpers */
static List * ColumnarVarNeeded(ColumnarScanState *columnarScanState);

/* RowcompressScan forward declarations */
static void RCAddScanPath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
static Plan * RCScan_PlanCustomPath(PlannerInfo *root, RelOptInfo *rel,
									struct CustomPath *best_path,
									List *tlist, List *clauses,
									List *custom_plans);
static Node * RCScan_CreateCustomScanState(CustomScan *cscan);
static void RCScan_BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot * RCScan_Next(ScanState *node);
static bool RCScan_Recheck(ScanState *node, TupleTableSlot *slot);
static TupleTableSlot * RCScan_ExecCustomScan(CustomScanState *node);
static void RCScan_EndCustomScan(CustomScanState *node);
static void RCScan_ReScanCustomScan(CustomScanState *node);
static void RCScan_ExplainCustomScan(CustomScanState *node, List *ancestors,
									 ExplainState *es);

/* saved hook value in case of unload */
static set_rel_pathlist_hook_type PreviousSetRelPathlistHook = NULL;
#if PG_VERSION_NUM < PG_VERSION_19
static get_relation_info_hook_type PreviousGetRelationInfoHook = NULL;
#endif

static bool EnableColumnarCustomScan = true;
static bool EnableColumnarQualPushdown = true;
static double ColumnarQualPushdownCorrelationThreshold = 0.4;
static int ColumnarMaxCustomScanPaths = 64;
static int ColumnarPlannerDebugLevel = DEBUG3;


const struct CustomPathMethods ColumnarScanPathMethods = {
	.CustomName = "ColcompressScan",
	.PlanCustomPath = ColumnarScanPath_PlanCustomPath,
	.ReparameterizeCustomPathByChild = ColumnarScanPath_ReparameterizeCustomPathByChild,
};

const struct CustomScanMethods ColumnarScanScanMethods = {
	.CustomName = "ColcompressScan",
	.CreateCustomScanState = ColumnarScan_CreateCustomScanState,
};

const struct CustomExecMethods ColumnarScanExecuteMethods = {
	.CustomName = "ColcompressScan",

	.BeginCustomScan = ColumnarScan_BeginCustomScan,
	.ExecCustomScan = ColumnarScan_ExecCustomScan,
	.EndCustomScan = ColumnarScan_EndCustomScan,
	.ReScanCustomScan = ColumnarScan_ReScanCustomScan,

	.ExplainCustomScan = ColumnarScan_ExplainCustomScan,

	.EstimateDSMCustomScan = Columnar_EstimateDSMCustomScan,
	.InitializeDSMCustomScan = Columnar_InitializeDSMCustomScan,
	.ReInitializeDSMCustomScan = Columnar_ReinitializeDSMCustomScan,
	.InitializeWorkerCustomScan = Columnar_InitializeWorkerCustomScan
};

/* ----------------------------------------------------------------
 * RowcompressScan method tables
 * ---------------------------------------------------------------- */
static const struct CustomPathMethods RowcompressScanPathMethods = {
	.CustomName = "RowcompressScan",
	.PlanCustomPath = RCScan_PlanCustomPath,
};

static const struct CustomScanMethods RowcompressScanScanMethods = {
	.CustomName = "RowcompressScan",
	.CreateCustomScanState = RCScan_CreateCustomScanState,
};

static const struct CustomExecMethods RowcompressScanExecuteMethods = {
	.CustomName = "RowcompressScan",
	.BeginCustomScan = RCScan_BeginCustomScan,
	.ExecCustomScan = RCScan_ExecCustomScan,
	.EndCustomScan = RCScan_EndCustomScan,
	.ReScanCustomScan = RCScan_ReScanCustomScan,
	.ExplainCustomScan = RCScan_ExplainCustomScan,
};

static const struct config_enum_entry debug_level_options[] = {
	{ "debug5", DEBUG5, false },
	{ "debug4", DEBUG4, false },
	{ "debug3", DEBUG3, false },
	{ "debug2", DEBUG2, false },
	{ "debug1", DEBUG1, false },
	{ "debug", DEBUG2, true },
	{ "info", INFO, false },
	{ "notice", NOTICE, false },
	{ "warning", WARNING, false },
	{ "log", LOG, false },
	{ NULL, 0, false }
};


const CustomScanMethods *
engine_customscan_methods(void)
{
	return &ColumnarScanScanMethods;
}


/*
 * engine_customscan_init installs the hook required to intercept the postgres planner and
 * provide extra paths for columnar tables
 */
void
engine_customscan_init()
{
	PreviousSetRelPathlistHook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = ColumnarSetRelPathlistHook;

#if PG_VERSION_NUM < PG_VERSION_19
	PreviousGetRelationInfoHook = get_relation_info_hook;
	get_relation_info_hook = ColumnarGetRelationInfoHook;
#endif

	/* register customscan specific GUC's — skip if already defined */
	if (GetConfigOption("storage_engine.enable_custom_scan", true, false) == NULL)
	{
	DefineCustomBoolVariable(
		"storage_engine.enable_custom_scan",
		"Enables the use of a custom scan to push projections and quals "
					 "into the storage layer.",
		NULL,
		&EnableColumnarCustomScan,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
	DefineCustomBoolVariable(
		"storage_engine.enable_qual_pushdown",
		"Enables qual pushdown into columnar. This has no effect unless "
					 "storage_engine.enable_custom_scan is true.",
		NULL,
		&EnableColumnarQualPushdown,
		true,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
	DefineCustomRealVariable(
		"storage_engine.qual_pushdown_correlation_threshold",
		"Correlation threshold to attempt to push a qual "
					 "referencing the given column. A value of 0 means "
					 "attempt to push down all quals, even if the column "
					 "is uncorrelated.",
		NULL,
		&ColumnarQualPushdownCorrelationThreshold,
		0.4,
		0.0,
		1.0,
		PGC_USERSET,
		GUC_NO_SHOW_ALL |GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
	DefineCustomIntVariable(
		"storage_engine.max_custom_scan_paths",
		"Maximum number of custom scan paths to generate "
					 "for a columnar table when planning.",
		NULL,
		&ColumnarMaxCustomScanPaths,
		64,
		1,
		1024,
		PGC_USERSET,
		GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
		NULL, NULL, NULL);
	DefineCustomEnumVariable(
		"storage_engine.planner_debug_level",
		"Message level for columnar planning information.",
		NULL,
		&ColumnarPlannerDebugLevel,
		DEBUG3,
		debug_level_options,
		PGC_USERSET,
		0,
		NULL,
		NULL,
		NULL);
	} /* end GUC guard */

	/*
	 * Guard against double-registration when another extension (e.g. Citus
	 * columnar or TimescaleDB) has already registered a custom scan node
	 * with the same name.  GetCustomScanMethods with missing_ok=true returns
	 * NULL instead of erroring out, so we only register when the slot is free.
	 */
	if (GetCustomScanMethods(ColumnarScanScanMethods.CustomName, true) == NULL)
		RegisterCustomScanMethods(&ColumnarScanScanMethods);

	if (GetCustomScanMethods(RowcompressScanScanMethods.CustomName, true) == NULL)
		RegisterCustomScanMethods(&RowcompressScanScanMethods);
}

static void
ColumnarSetRelPathlistHook(PlannerInfo *root, RelOptInfo *rel, Index rti,
						   RangeTblEntry *rte)
{
	/* call into previous hook if assigned */
	if (PreviousSetRelPathlistHook)
	{
		PreviousSetRelPathlistHook(root, rel, rti, rte);
	}

	if (!OidIsValid(rte->relid) || rte->rtekind != RTE_RELATION || rte->inh)
	{
		/* some calls to the pathlist hook don't have a valid relation set. Do nothing */
		return;
	}

	/*
	 * Here we want to inspect if this relation pathlist hook is accessing a columnar table.
	 * If that is the case we want to insert an extra path that pushes down the projection
	 * into the scan of the table to minimize the data read.
	 */
	Relation relation = RelationIdGetRelation(rte->relid);
	if (relation->rd_tableam == GetColumnarTableAmRoutine())
	{
		if (rte->tablesample != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("sample scans not supported on columnar tables")));
		}

		/*
		 * There are cases where IndexPath is normally more preferrable over
		 * SeqPath for heapAM but not for columnarAM. In such cases, an
		 * IndexPath could wrongly dominate a SeqPath based on the costs
		 * estimated by postgres earlier. For this reason, here we manually
		 * create a SeqPath, estimate the cost based on columnarAM and append
		 * to pathlist.
		 *
		 * Before doing that, we first re-cost all the existing paths so that
		 * add_path makes correct cost comparisons when appending our SeqPath.
		 */
		CostColumnarPaths(root, rel, rte->relid);

		Path *seqPath = CreateColumnarSeqScanPath(root, rel, rte->relid);
		add_path(rel, seqPath);

		if (EnableColumnarCustomScan)
		{
			/*
			 * When columnar custom scan is enabled (columnar.enable_custom_scan),
			 * we only consider ColumnarScanPath's & IndexPath's. For this reason,
			 * we remove other paths and re-estimate IndexPath costs to make accurate
			 * comparisons between them.
			 *
			 * Even more, we might calculate an equal cost for a
			 * ColumnarCustomScan and a SeqPath if we are reading all columns
			 * of given table since we don't consider chunk group filtering
			 * when costing ColumnarCustomScan.
			 * In that case, if we don't remove SeqPath's, we might wrongly choose
			 * SeqPath thinking that its cost would be equal to ColumnarCustomScan.
			 */
			RemovePathsByPredicate(rel, IsNotIndexPath);
			AddColumnarScanPaths(root, rel, rte);
		}
	}
	else if (IsRowCompressTableAmTable(rte->relid))
	{
		/*
		 * For rowcompress, an Index Scan driven by a RANGE predicate
		 * (BETWEEN / < / <= / > / >=) returns TIDs in index-key order.
		 * Because rows with nearby key values are spread uniformly across
		 * all batches, the same batch can be decompressed once per
		 * distinct key value touched — e.g., a 31-day BETWEEN scan on
		 * randomly-distributed dates decompresses every batch 31 times
		 * instead of once.  A sequential scan is far more efficient in
		 * that case.
		 *
		 * EQUALITY predicates (=, IN) are fine: within a single key
		 * value the index returns TIDs in block/batch order, so each
		 * batch is decompressed at most once.
		 *
		 * Strategy:
		 *  1. Remove Index Scan paths that contain at least one range clause.
		 *  2. Ensure a Seq Scan path exists afterwards.  The planner's
		 *     add_path() may have pruned the Seq Scan before our hook ran
		 *     because the index path appeared cheaper by the standard cost
		 *     model (which does not account for rowcompress batch
		 *     decompression overhead).  We therefore append the Seq Scan
		 *     directly to rel->pathlist (bypassing dominance checks) only
		 *     when no Seq Scan is present.
		 */
		if (!RowCompressGetIndexScan(rte->relid))
			RemovePathsByPredicate(rel, IsRowCompressRangeIndexPath);

		/* Guarantee at least one Seq Scan path after the removal above */
		bool hasSeqScan = false;
		Path *chkPath   = NULL;
		foreach_ptr(chkPath, rel->pathlist)
		{
			if (chkPath->pathtype == T_SeqScan)
			{
				hasSeqScan = true;
				break;
			}
		}
		if (!hasSeqScan)
		{
			Path *seqPath = create_seqscan_path(root, rel,
												 rel->lateral_relids, 0);
			/* Direct lappend: skip add_path dominance checks since we just
			 * removed the path that would have dominated this Seq Scan. */
			rel->pathlist = lappend(rel->pathlist, seqPath);
		}

		/*
		 * When batch-level pruning is configured and custom scans are enabled,
		 * add a RowcompressScan path that pushes down WHERE-clause predicates
		 * for batch skipping.  The path replaces the SeqScan (same cost, but
		 * with actual run-time pruning benefit).
		 */
		if (EnableColumnarCustomScan &&
			RowCompressGetPruningAttnum(rte->relid) > 0)
		{
			RCAddScanPath(root, rel, rte);
		}
	}
	RelationClose(relation);
}


#if PG_VERSION_NUM < PG_VERSION_19
static void
ColumnarGetRelationInfoHook(PlannerInfo *root, Oid relationObjectId,
							bool inhparent, RelOptInfo *rel)
{
	if (PreviousGetRelationInfoHook)
	{
		PreviousGetRelationInfoHook(root, relationObjectId, inhparent, rel);
	}

	if (IsColumnarTableAmTable(relationObjectId))
	{
		/*
		 * Parallel columnar scan is handled by the custom scan parallel path
		 * via columnar.enable_parallel_execution GUC and the DSM callbacks
		 * (Columnar_EstimateDSMCustomScan/InitializeDSMCustomScan/InitializeWorkerCustomScan).
		 * The TableAM parallelscan_* callbacks are also implemented for the
		 * standard parallel seq-scan fallback path.
		 */

		/* disable index-only scan */
		IndexOptInfo *indexOptInfo = NULL;
		foreach_ptr(indexOptInfo, rel->indexlist)
		{
			memset(indexOptInfo->canreturn, false, indexOptInfo->ncolumns * sizeof(bool));
		}
	}

	/*
	 * rowcompress now supports parallel sequential scans via atomic batch
	 * claiming (RowCompressParallelScanDescData).  Let the planner pick the
	 * degree of parallelism based on the system's max_parallel_workers_per_gather
	 * and the table's own parallel_workers storage option.
	 */
}
#endif /* PG_VERSION_NUM < PG_VERSION_19 */


/*
 * RemovePathsByPredicate removes the paths that removePathPredicate
 * evaluates to true from pathlist of given rel.
 */
static void
RemovePathsByPredicate(RelOptInfo *rel, PathPredicate removePathPredicate)
{
	List *filteredPathList = NIL;
	List *filteredPartialPathList = NIL;

	Path *path = NULL;
	foreach_ptr(path, rel->pathlist)
	{
		if (!removePathPredicate(path))
		{
			filteredPathList = lappend(filteredPathList, path);
		}
	}

	rel->pathlist = filteredPathList;

	foreach_ptr(path, rel->partial_pathlist)
	{
		if (!removePathPredicate(path))
		{
			filteredPartialPathList = lappend(filteredPartialPathList, path);
		}
	}

	rel->partial_pathlist = filteredPartialPathList;
}


/*
 * IsNotIndexPath returns true if given path is not an IndexPath or a
 * BitmapHeapPath.  We keep both so that the planner can consider GIN/GiST
 * bitmap index scans alongside plain IndexScans and ColumnarScanPaths.
 *
 * We also preserve CustomPath nodes from pg_search (ParadeDB Base Scan) so
 * that BM25 full-text search via the @@@ operator works transparently on
 * colcompress tables without requiring enable_custom_scan = false.
 */
static bool
IsNotIndexPath(Path *path)
{
	if (IsA(path, CustomPath))
	{
		CustomPath *cp = (CustomPath *) path;

		if (strcmp(cp->methods->CustomName, "ParadeDB Base Scan") == 0)
			return false;	/* preserve — let pg_search win */
	}

#if PG_VERSION_NUM >= PG_VERSION_19
	/*
	 * PG19 removed get_relation_info_hook, so we can no longer zero
	 * canreturn early to suppress index-only scans.  Remove them here
	 * instead: in PG19 an index-only scan path has pathtype T_IndexOnlyScan.
	 */
	if (IsA(path, IndexPath) && path->pathtype == T_IndexOnlyScan)
		return true;
#endif

	return !IsA(path, IndexPath) && !IsA(path, BitmapHeapPath);
}


/*
 * IsRowCompressRangeIndexPath returns true if the path is a plain IndexPath
 * whose index clauses contain at least one non-equality (range) condition
 * such as <, <=, >, >= or BETWEEN.  Such scans return TIDs in index-key
 * order; for rowcompress this can cause repeated batch decompression when
 * the key column is uncorrelated with physical row order.
 *
 * Equality predicates (strategy == BTEqualStrategyNumber) return TIDs in
 * physical/batch order within each key value, so they are safe.
 */
static bool
IsRowCompressRangeIndexPath(Path *path)
{
	if (!IsA(path, IndexPath))
		return false;

	IndexPath *ipath  = (IndexPath *) path;
	ListCell  *lc;

	foreach(lc, ipath->indexclauses)
	{
		IndexClause  *iclause = (IndexClause *) lfirst(lc);
		RestrictInfo *rinfo   = iclause->rinfo;

		if (!IsA(rinfo->clause, OpExpr))
			continue;

		Oid opno = ((OpExpr *) rinfo->clause)->opno;

		/*
		 * Look up the strategy number of this operator in the index's
		 * opfamily for the relevant column.  A strategy other than
		 * BTEqualStrategyNumber (3) indicates a range clause.
		 */
		StrategyNumber stratnum =
			get_op_opfamily_strategy(opno,
				ipath->indexinfo->opfamily[iclause->indexcol]);

		if (stratnum != 0 && stratnum != BTEqualStrategyNumber)
			return true; /* range clause → remove this index path */
	}

	return false; /* all clauses are equalities → keep */
}


/*
 * CreateColumnarSeqScanPath returns Path for sequential scan on columnar
 * table with relationId.
 */
static Path *
CreateColumnarSeqScanPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId)
{
	/* columnar doesn't support parallel scan */
	int parallelWorkers = 0;

	Relids requiredOuter = rel->lateral_relids;
	Path *path = create_seqscan_path(root, rel, requiredOuter, parallelWorkers);
	CostColumnarSeqPath(rel, relationId, path);
	return path;
}


/*
 * CostColumnarPaths re-costs paths of given RelOptInfo for
 * columnar table with relationId.
 */
static void
CostColumnarPaths(PlannerInfo *root, RelOptInfo *rel, Oid relationId)
{
	Path *path = NULL;

	/* Re-cost paths in the main pathlist */
	foreach_ptr(path, rel->pathlist)
	{
		if (IsA(path, IndexPath))
		{
			/*
			 * Re-cost plain IndexScan paths to account for columnar stripe
			 * read overhead.
			 */
			CostColumnarIndexPath(root, rel, relationId, (IndexPath *) path);
		}
		else if (IsA(path, BitmapHeapPath))
		{
			/*
			 * BitmapHeapPath (e.g. GIN bitmap scan) costs are estimated by the
			 * index AM.  For tables with index_scan=false (analytics workloads),
			 * a GIN Bitmap Heap Scan forces random-access reads through
			 * engine_index_fetch_tuple, which reads one stripe per matched row.
			 * With 200k matches spread across many chunk groups this is far
			 * more expensive than a parallel ColcompressScan that decompresses
			 * once per stripe.  Add disable_cost so the planner always prefers
			 * the ColcompressScan (or parallel ColcompressScan) path instead.
			 *
			 * For tables with index_scan=true (OLTP / document repos), GIN
			 * bitmap scans are left alone — those tables have fewer rows per
			 * match and the bitmap path is genuinely cheaper.
			 */
			bool useIndexScan = engine_index_scan;
			if (!useIndexScan)
			{
				ColumnarOptions tableOptions = { 0 };
				if (ReadColumnarOptions(relationId, &tableOptions))
					useIndexScan = tableOptions.indexScan;
			}
			if (!useIndexScan)
			{
				((BitmapHeapPath *) path)->path.total_cost += disable_cost;
			}
		}
		else if (path->pathtype == T_SeqScan)
		{
			CostColumnarSeqPath(rel, relationId, path);
		}
	}

	/*
	 * Also re-cost paths in partial_pathlist (parallel workers).
	 * Without this, a Parallel Index Scan in partial_pathlist escapes the
	 * disable_cost penalty when index_scan=false, causing the planner to
	 * prefer it over the parallel ColcompressScan and bypassing stripe pruning.
	 */
	foreach_ptr(path, rel->partial_pathlist)
	{
		if (IsA(path, IndexPath))
		{
			CostColumnarIndexPath(root, rel, relationId, (IndexPath *) path);
		}
		else if (IsA(path, BitmapHeapPath))
		{
			bool useIndexScan = engine_index_scan;
			if (!useIndexScan)
			{
				ColumnarOptions tableOptions = { 0 };
				if (ReadColumnarOptions(relationId, &tableOptions))
					useIndexScan = tableOptions.indexScan;
			}
			if (!useIndexScan)
				((BitmapHeapPath *) path)->path.total_cost += disable_cost;
		}
		else if (path->pathtype == T_SeqScan)
		{
			CostColumnarSeqPath(rel, relationId, path);
		}
	}
}


/*
 * CostColumnarIndexPath re-costs given index path for columnar table with
 * relationId.
 */
static void
CostColumnarIndexPath(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
					  IndexPath *indexPath)
{
	if (!enable_indexscan)
	{
		/* costs are already set to disable_cost, don't adjust them */
		return;
	}

	ereport(DEBUG4, (errmsg("columnar table index scan costs estimated by "
							"indexAM: startup cost = %.10f, total cost = "
							"%.10f", indexPath->path.startup_cost,
							indexPath->path.total_cost)));

	/*
	 * We estimate the cost for columnar table read during index scan. Also,
	 * instead of overwriting total cost, we "add" ours to the cost estimated
	 * by indexAM since we should consider index traversal related costs too.
	 *
	 * Index scan is enabled if either the session GUC is on, OR the table
	 * has index_scan = true in its per-table options (document repositories).
	 */
	bool useIndexScan = engine_index_scan;
	if (!useIndexScan)
	{
		ColumnarOptions tableOptions = { 0 };
		if (ReadColumnarOptions(relationId, &tableOptions))
			useIndexScan = tableOptions.indexScan;
	}

	if (!useIndexScan)
	{
		/*
		 * index_scan is disabled for this table (both GUC and per-table flag
		 * are off).  Add disable_cost so the planner never picks this path,
		 * exactly like SET enable_indexscan = off does.  A proportional
		 * penalty is insufficient: for highly-selective queries (e.g. a
		 * 1-month date range over 2 years of data) the per-row penalty is
		 * still smaller than reading all stripes sequentially, so the planner
		 * would choose IndexScan and bypass stripe pruning.
		 */
		indexPath->path.total_cost += disable_cost;
	}
	else
	{
		/*
		 * index_scan is enabled: add the columnar-specific additional cost
		 * (chunk-decompression overhead) so the planner can still compare the
		 * index path against the seq scan path fairly.
		 */
		Cost columnarIndexScanCost = ColumnarIndexScanAdditionalCost(root, rel, relationId,
																	indexPath);
		indexPath->path.total_cost += columnarIndexScanCost;
	}

	ereport(DEBUG4, (errmsg("columnar table index scan costs re-estimated "
							"by columnarAM (including indexAM costs): "
							"startup cost = %.10f, total cost = %.10f",
							indexPath->path.startup_cost,
							indexPath->path.total_cost)));
}


/*
 * ColumnarIndexScanAdditionalCost returns additional cost estimated for
 * index scan described by IndexPath for columnar table with relationId.
 */
static Cost
ColumnarIndexScanAdditionalCost(PlannerInfo *root, RelOptInfo *rel,
								Oid relationId, IndexPath *indexPath)
{
	/*
	 * Use the number of columns the planner actually needs to read, not the
	 * total column count of the relation.  For wide tables with large columns
	 * (e.g. XML/JSON blobs), using RelationIdGetNumberOfAttributes inflates
	 * perStripeCost enormously even for queries that only touch a handful of
	 * narrow columns, making the planner always reject the IndexScan path.
	 *
	 * rel->reltarget->exprs holds the output expressions needed by the plan
	 * above this node — typically one Var per projected column.
	 */
	int numberOfColumnsRead = (rel->reltarget && rel->reltarget->exprs != NIL)
		? list_length(rel->reltarget->exprs)
		: RelationIdGetNumberOfAttributes(relationId);
	if (numberOfColumnsRead <= 0)
		numberOfColumnsRead = RelationIdGetNumberOfAttributes(relationId);
	Cost perStripeCost = ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);

	/*
	 * We don't need to pass correct loop count to amcostestimate since we
	 * will only use index correlation & index selectivity, and loop count
	 * doesn't have any effect on those two.
	 */
	double fakeLoopCount = 1;
	Cost fakeIndexStartupCost;
	Cost fakeIndexTotalCost;
	double fakeIndexPages;
	Selectivity indexSelectivity;
	double indexCorrelation;
	amcostestimate_function amcostestimate = indexPath->indexinfo->amcostestimate;
	amcostestimate(root, indexPath, fakeLoopCount, &fakeIndexStartupCost,
				   &fakeIndexTotalCost, &indexSelectivity,
				   &indexCorrelation, &fakeIndexPages);

	Relation relation = RelationIdGetRelation(relationId);
	uint64 rowCount = ColumnarTableRowCount(relation);
	RelationClose(relation);
	double estimatedRows = rowCount * indexSelectivity;

	uint64 stripeCount = ColumnarTableStripeCount(relationId);
	double avgStripeRowCount = (stripeCount > 0)
		? rowCount / (double) stripeCount
		: (double) rowCount;

	/*
	 * IndexScan fetches individual rows from compressed chunks, NOT entire
	 * stripes.  A stripe contains avgChunksPerStripe chunk groups; reading
	 * one matching row requires decompressing only the one chunk group that
	 * contains it.  Using full-stripe granularity inflates the cost so much
	 * (especially for wide tables with XML/JSON blob columns) that the
	 * planner always rejects IndexScan in favour of ColcompressScan, even
	 * when the index would be orders of magnitude faster at runtime.
	 *
	 * engine_chunk_group_row_limit is the per-chunk-group row count GUC
	 * (default 10 000).  perChunkCost = perStripeCost / avgChunksPerStripe.
	 */
	double avgChunksPerStripe =
		Max(avgStripeRowCount / engine_chunk_group_row_limit, 1.0);
	Cost perChunkCost = perStripeCost / avgChunksPerStripe;

	/*
	 * While being close to 0 means low correlation, being close to -1 or +1
	 * means high correlation. For index scans on columnar tables, it doesn't
	 * matter if the column and the index are "correlated" (+1) or
	 * "anti-correlated" (-1) since both help us avoiding from reading the
	 * same chunk group again and again.
	 */
	double absIndexCorrelation = Abs(indexCorrelation);
	double complementIndexCorrelation = 1 - absIndexCorrelation;

	/*
	 * Estimate the number of chunk groups we need to decompress.
	 * Best case (high correlation): rows cluster in the same chunk groups,
	 *   so we read estimatedRows / chunkRowCount chunks.
	 * Worst case (no correlation): every row is in a different chunk group,
	 *   so we read up to estimatedRows chunks.
	 */
	double minChunkReadCount = estimatedRows / engine_chunk_group_row_limit;
	double maxChunkReadCount = estimatedRows;
	double estimatedChunkReadCount =
		minChunkReadCount +
		complementIndexCorrelation * (maxChunkReadCount - minChunkReadCount);

	/* even in the best case, we decompress at least one chunk group */
	estimatedChunkReadCount = Max(estimatedChunkReadCount, 1.0);

	Cost scanCost = perChunkCost * estimatedChunkReadCount;

	ereport(DEBUG4, (errmsg("re-costing index scan for columnar table: "
							"selectivity = %.10f, complement abs correlation = %.10f, "
							"per stripe cost = %.10f, avg chunks/stripe = %.2f, "
							"per chunk cost = %.10f, "
							"estimated chunk read count = %.10f, "
							"total additional cost = %.10f",
							indexSelectivity, complementIndexCorrelation,
							perStripeCost, avgChunksPerStripe,
							perChunkCost, estimatedChunkReadCount,
							scanCost)));

	return scanCost;
}


/*
 * CostColumnarSeqPath sets costs given seq path for columnar table with
 * relationId.
 */
static void
CostColumnarSeqPath(RelOptInfo *rel, Oid relationId, Path *path)
{
	if (!enable_seqscan)
	{
		/* costs are already set to disable_cost, don't adjust them */
		return;
	}

	/*
	 * Seq scan doesn't support projection or qual pushdown, so we will read
	 * all the stripes and all the columns.
	 */
	double stripesToRead = ColumnarTableStripeCount(relationId);
	int numberOfColumnsRead = RelationIdGetNumberOfAttributes(relationId);

	path->rows = rel->tuples;
	path->startup_cost = 0;
	path->total_cost = stripesToRead *
					   ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);
}


/*
 * RelationIdGetNumberOfAttributes returns number of attributes that relation
 * with relationId has.
 */
static int
RelationIdGetNumberOfAttributes(Oid relationId)
{
	Relation relation = RelationIdGetRelation(relationId);
	int nattrs = relation->rd_att->natts;
	RelationClose(relation);
	return nattrs;
}


/*
 * CheckVarStats() checks whether a qual involving this Var is likely to be
 * useful based on the correlation stats. If so, or if stats are unavailable,
 * return true; otherwise return false and sets absVarCorrelation in case
 * caller wants to use for logging purposes.
 */
static bool
CheckVarStats(PlannerInfo *root, Var *var, Oid sortop, float4 *absVarCorrelation)
{
	/*
	 * Collect isunique, ndistinct, and varCorrelation.
	 */
	VariableStatData varStatData;
	examine_variable(root, (Node *) var, var->varno, &varStatData);
	if (varStatData.rel == NULL ||
		!HeapTupleIsValid(varStatData.statsTuple))
	{
		return true;
	}

	AttStatsSlot sslot;
	if (!get_attstatsslot(&sslot, varStatData.statsTuple,
						  STATISTIC_KIND_CORRELATION, sortop,
						  ATTSTATSSLOT_NUMBERS))
	{
		ReleaseVariableStats(varStatData);
		return true;
	}

	Assert(sslot.nnumbers == 1);

	float4 varCorrelation = sslot.numbers[0];

	ReleaseVariableStats(varStatData);

	/*
	 * If the Var is not highly correlated, then the chunk's min/max bounds
	 * will be nearly useless.
	 */
	if (Abs(varCorrelation) < ColumnarQualPushdownCorrelationThreshold)
	{
		if (absVarCorrelation)
		{
			/*
			 * Report absVarCorrelation if caller wants to know why given
			 * var is rejected.
			 */
			*absVarCorrelation = Abs(varCorrelation);
		}
		return false;
	}

	return true;
}


/*
 * ExprReferencesRelid returns true if any of the Expr's Vars refer to the
 * given relid; false otherwise.
 */
static bool
ExprReferencesRelid(Expr *expr, Index relid)
{
	List *exprVars = pull_var_clause(
		(Node *) expr, PVC_RECURSE_AGGREGATES |
		PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS);
	ListCell *lc;
	foreach(lc, exprVars)
	{
		Var *var = (Var *) lfirst(lc);
		if (var->varno == relid)
		{
			return true;
		}
	}

	return false;
}


/*
 * ExtractPushdownClause extracts an Expr node from given clause for pushing down
 * into the given rel (including join clauses). This test may not be exact in
 * all cases; it's used to reduce the search space for parameterization.
 *
 * Note that we don't try to handle cases like "Var + ExtParam = 3". That
 * would require going through eval_const_expression after parameter binding,
 * and that doesn't seem worth the effort. Here we just look for "Var op Expr"
 * or "Expr op Var", where Var references rel and Expr references other rels
 * (or no rels at all).
 *
 * Moreover, this function also looks into BoolExpr's to recursively extract
 * pushdownable OpExpr's of them:
 * i)   AND_EXPR:
 *      Take pushdownable args of AND expressions by ignoring the other args.
 * ii)  OR_EXPR:
 *      Ignore the whole OR expression if we cannot extract a pushdownable Expr
 *      from one of its args.
 * iii) NOT_EXPR:
 *      Simply ignore NOT expressions since we don't expect to see them before
 *      an expression that we can pushdown, see the comment in function.
 *
 * The reasoning for those three rules could also be summarized as such;
 * for any expression that we cannot push-down, we must assume that it
 * evaluates to true.
 *
 * For example, given following WHERE clause:
 * (
 *     (a > random() OR a < 30)
 *     AND
 *     a < 200
 * ) OR
 * (
 *     a = 300
 *     OR
 *     a > 400
 * );
 * Even if we can pushdown (a < 30), we cannot pushdown (a > random() OR a < 30)
 * due to (a > random()). However, we can pushdown (a < 200), so we extract
 * (a < 200) from the lhs of the top level OR expression.
 *
 * For the rhs of the top level OR expression, since we can pushdown both (a = 300)
 * and (a > 400), we take this part as is.
 *
 * Finally, since both sides of the top level OR expression yielded pushdownable
 * expressions, we will pushdown the following:
 *  (a < 200) OR ((a = 300) OR (a > 400))
 */
static Expr *
ExtractPushdownClause(PlannerInfo *root, RelOptInfo *rel, Node *node)
{
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, BoolExpr))
	{
		BoolExpr *boolExpr = castNode(BoolExpr, node);
		if (boolExpr->boolop == NOT_EXPR)
		{
			/*
			 * Standard planner should have already applied de-morgan rule to
			 * simple NOT expressions. If we encounter with such an expression
			 * here, then it can't be a pushdownable one, such as:
			 *   WHERE id NOT IN (SELECT id FROM something).
			 */
			ereport(ColumnarPlannerDebugLevel,
					(errmsg("columnar planner: cannot push down clause: "
							"must not contain a subplan")));
			return NULL;
		}

		List *pushdownableArgs = NIL;

		Node *boolExprArg = NULL;
		foreach_ptr(boolExprArg, boolExpr->args)
		{
			Expr *pushdownableArg = ExtractPushdownClause(root, rel,
														  (Node *) boolExprArg);
			if (pushdownableArg)
			{
				pushdownableArgs = lappend(pushdownableArgs, pushdownableArg);
			}
			else if (boolExpr->boolop == OR_EXPR)
			{
				ereport(ColumnarPlannerDebugLevel,
						(errmsg("columnar planner: cannot push down clause: "
								"all arguments of an OR expression must be "
								"pushdownable but one of them was not, due "
								"to the reason given above")));
				return NULL;
			}

			/* simply skip AND args that we cannot pushdown */
		}

		int npushdownableArgs = list_length(pushdownableArgs);
		if (npushdownableArgs == 0)
		{
			ereport(ColumnarPlannerDebugLevel,
					(errmsg("columnar planner: cannot push down clause: "
							"none of the arguments were pushdownable, "
							"due to the reason(s) given above")));
			return NULL;
		}
		else if (npushdownableArgs == 1)
		{
			return (Expr *) linitial(pushdownableArgs);
		}

		if (boolExpr->boolop == AND_EXPR)
		{
			return make_andclause(pushdownableArgs);
		}
		else if (boolExpr->boolop == OR_EXPR)
		{
			return make_orclause(pushdownableArgs);
		}
		else
		{
			/* already discarded NOT expr, so should not be reachable */
			return NULL;
		}
	}

	if (!IsA(node, OpExpr) || list_length(((OpExpr *) node)->args) != 2)
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"must be binary operator expression")));
		return NULL;
	}

	OpExpr *opExpr = castNode(OpExpr, node);
	Expr *lhs = list_nth(opExpr->args, 0);
	Expr *rhs = list_nth(opExpr->args, 1);

	/*
	 * Strip implicit coercions (RelabelType, CoerceViaIO with COERCE_IMPLICIT_CAST)
	 * so that VARCHAR(n)/CHAR(n) columns whose predicates are implicitly cast to
	 * text — e.g. (col_varchar)::text = 'value'::text — can still have their
	 * quals pushed down as chunk-group filters.  The coercion wrapper is retained
	 * in the actual pushed-down clause; we only strip it here for the purpose of
	 * locating the underlying Var and choosing the correct opfamily.
	 */
	Expr *lhsBase = (Expr *) strip_implicit_coercions((Node *) lhs);
	Expr *rhsBase = (Expr *) strip_implicit_coercions((Node *) rhs);

	Var *varSide;
	Expr *exprSide;
	Oid effectiveVarType; /* type as presented to the operator (may differ from Var type) */

	if (IsA(lhsBase, Var) && ((Var *) lhsBase)->varno == rel->relid &&
		!ExprReferencesRelid((Expr *) rhs, rel->relid))
	{
		varSide = castNode(Var, lhsBase);
		exprSide = rhs;
		effectiveVarType = exprType((Node *) lhs); /* e.g. TEXTOID after varchar::text */
	}
	else if (IsA(rhsBase, Var) && ((Var *) rhsBase)->varno == rel->relid &&
			 !ExprReferencesRelid((Expr *) lhs, rel->relid))
	{
		varSide = castNode(Var, rhsBase);
		exprSide = lhs;
		effectiveVarType = exprType((Node *) rhs);
	}
	else
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"must match 'Var <op> Expr' or 'Expr <op> Var'"),
				 errhint("Var must only reference this rel, "
						 "and Expr must not reference this rel")));
		return NULL;
	}

	if (varSide->varattno <= 0)
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"var is whole-row reference or system column")));
		return NULL;
	}

	if (contain_volatile_functions((Node *) exprSide))
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"expr contains volatile functions")));
		return NULL;
	}

	/*
	 * Use the effective (possibly coerced) type for opfamily lookup instead of
	 * the raw Var type.  This allows VARCHAR(n) → text implicit coercions:
	 * if the clause is (varchar_col)::text = 'value'::text, effectiveVarType is
	 * TEXTOID, whose default btree opclass (text_ops) contains the text = operator.
	 * Without this, the varchar opclass would be used but text's = operator is not
	 * a member of varchar_ops, causing the pushdown to be silently rejected.
	 */
	Oid varOpClass = GetDefaultOpClass(effectiveVarType, BTREE_AM_OID);
	Oid varOpFamily;
	Oid varOpcInType;

	if (!OidIsValid(varOpClass) ||
		!get_opclass_opfamily_and_input_type(varOpClass, &varOpFamily,
											 &varOpcInType))
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"cannot find default btree opclass and opfamily for type: %s",
						format_type_be(effectiveVarType))));
		return NULL;
	}

	if (!op_in_opfamily(opExpr->opno, varOpFamily))
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"operator %d not a member of opfamily %d",
						opExpr->opno, varOpFamily)));
		return NULL;
	}

	Oid sortop = get_opfamily_member(varOpFamily, varOpcInType,
									 varOpcInType, BTLessStrategyNumber);
	Assert(OidIsValid(sortop));

	/*
	 * Check that statistics on the Var support the utility of this
	 * clause.
	 */
	float4 absVarCorrelation = 0;
	if (!CheckVarStats(root, varSide, sortop, &absVarCorrelation))
	{
		ereport(ColumnarPlannerDebugLevel,
				(errmsg("columnar planner: cannot push down clause: "
						"absolute correlation (%.3f) of var attribute %d is "
						"smaller than the value configured in "
						"\"storage_engine.qual_pushdown_correlation_threshold\" "
						"(%.3f)", absVarCorrelation, varSide->varattno,
						ColumnarQualPushdownCorrelationThreshold)));
		return NULL;
	}

	/*
	 * If the Var was wrapped in an implicit coercion (e.g. varchar(n)::text,
	 * char(n)::text), the clause reaching us uses the coerced type's operator
	 * (e.g. text_eq) while the min/max values in engine.chunk are stored using
	 * the Var's native type (e.g. varchar).  predicate_refuted_by() cannot cross
	 * opfamily boundaries, so chunk-group pruning would silently return "0 pruned".
	 *
	 * When exprSide is a Const (the common case: col = 'literal'), we rebuild the
	 * clause using the Var's native type's operator and a recast constant.  For
	 * binary-compatible coercions (RelabelType — which includes all varchar/text
	 * and char/text pairs), the Datum bit-pattern is identical, so retyping the
	 * constant is safe and lossless.
	 */
	if (varSide->vartype != effectiveVarType && IsA(exprSide, Const))
	{
		Oid nativeOpClass = GetDefaultOpClass(varSide->vartype, BTREE_AM_OID);
		Oid nativeOpFamily, nativeOpcInType;

		if (OidIsValid(nativeOpClass) &&
			get_opclass_opfamily_and_input_type(nativeOpClass, &nativeOpFamily,
												&nativeOpcInType))
		{
			/* Which comparison strategy does the operator implement? */
			int16 strategy = get_op_opfamily_strategy(opExpr->opno, varOpFamily);
			if (strategy != 0)
			{
				Oid nativeOpno = get_opfamily_member(nativeOpFamily,
													 nativeOpcInType,
													 nativeOpcInType,
													 strategy);
				if (OidIsValid(nativeOpno))
				{
					Const    *origConst = castNode(Const, exprSide);
					int16     nativeTyplen;
					bool      nativeTypbyval;

					get_typlenbyval(nativeOpcInType, &nativeTyplen, &nativeTypbyval);

					Const *nativeConst = makeConst(nativeOpcInType,
												   -1,
												   origConst->constcollid,
												   nativeTyplen,
												   origConst->constvalue,
												   origConst->constisnull,
												   nativeTypbyval);

					OpExpr *nativeOpExpr = makeNode(OpExpr);
					nativeOpExpr->opno         = nativeOpno;
					nativeOpExpr->opfuncid     = get_opcode(nativeOpno);
					nativeOpExpr->opresulttype = BOOLOID;
					nativeOpExpr->opretset     = false;
					nativeOpExpr->opcollid     = InvalidOid;
					nativeOpExpr->inputcollid  = varSide->varcollid;
					nativeOpExpr->args         = list_make2(copyObject(varSide),
															nativeConst);
					nativeOpExpr->location     = -1;

					node = (Node *) nativeOpExpr;
				}
			}
		}
	}

	return (Expr *) node;
}


/*
 * FilterPushdownClauses filters for clauses that are candidates for pushing
 * down into rel.
 */
static List *
FilterPushdownClauses(PlannerInfo *root, RelOptInfo *rel, List *inputClauses)
{
	List *filteredClauses = NIL;
	ListCell *lc;
	foreach(lc, inputClauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * Ignore clauses that don't refer to this rel, and pseudoconstants.
		 *
		 * XXX: A pseudoconstant may be of use, but it doesn't make sense to
		 * push it down because it doesn't contain any Vars. Look into if
		 * there's something we should do with pseudoconstants here.
		 */
		if (rinfo->pseudoconstant ||
			!bms_is_member(rel->relid, rinfo->required_relids))
		{
			continue;
		}

		Expr *pushdownableExpr = ExtractPushdownClause(root, rel, (Node *) rinfo->clause);
		if (!pushdownableExpr)
		{
			continue;
		}

		rinfo = copyObject(rinfo);
		rinfo->clause = pushdownableExpr;
		filteredClauses = lappend(filteredClauses, rinfo);
	}

	return filteredClauses;
}


/*
 * PushdownJoinClauseMatches is a callback that returns true, indicating that
 * we want all of the clauses from generate_implied_equalities_for_column().
 */
static bool
PushdownJoinClauseMatches(PlannerInfo *root, RelOptInfo *rel,
						  EquivalenceClass *ec, EquivalenceMember *em,
						  void *arg)
{
	return true;
}


/*
 * FindPushdownJoinClauses finds join clauses, including those implied by ECs,
 * that may be pushed down.
 */
static List *
FindPushdownJoinClauses(PlannerInfo *root, RelOptInfo *rel)
{
	List *joinClauses = copyObject(rel->joininfo);

	/*
	 * Here we are generating the clauses just so we can later extract the
	 * interesting relids. This is somewhat wasteful, but it allows us to
	 * filter out joinclauses, reducing the number of relids we need to
	 * consider.
	 *
	 * XXX: also find additional clauses for joininfo that are implied by ECs?
	 */
	List *ecClauses = generate_implied_equalities_for_column(
		root, rel, PushdownJoinClauseMatches, NULL,
		rel->lateral_referencers);
	List *allClauses = list_concat(joinClauses, ecClauses);

	return FilterPushdownClauses(root, rel, allClauses);
}


/*
 * FindCandidateRelids identifies candidate rels for parameterization from the
 * list of join clauses.
 *
 * Some rels cannot be considered for parameterization, such as a partitioned
 * parent of the given rel. Other rels are just not useful because they don't
 * appear in a join clause that could be pushed down.
 */
static Relids
FindCandidateRelids(PlannerInfo *root, RelOptInfo *rel, List *joinClauses)
{
	Relids candidateRelids = NULL;
	ListCell *lc;
	foreach(lc, joinClauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		candidateRelids = bms_add_members(candidateRelids,
										  rinfo->required_relids);
	}

	candidateRelids = bms_del_members(candidateRelids, rel->relids);
	candidateRelids = bms_del_members(candidateRelids, rel->lateral_relids);
	return candidateRelids;
}


/*
 * Combinations() calculates the number of combinations of n things taken k at
 * a time. When the correct result is large, the calculation may produce a
 * non-integer result, or overflow to inf, which caller should handle
 * appropriately.
 *
 * Use the following two formulae from Knuth TAoCP, 1.2.6:
 *    (2) Combinations(n, k) = (n*(n-1)..(n-k+1)) / (k*(k-1)..1)
 *    (5) Combinations(n, k) = Combinations(n, n-k)
 */
static double
Combinations(int n, int k)
{
	double v = 1;

	/*
	 * If k is close to n, then both the numerator and the denominator are
	 * close to n!, and we may overflow even if the input is reasonable
	 * (e.g. Combinations(500, 500)). Use formula (5) to choose the smaller,
	 * but equivalent, k.
	 */
	k = Min(k, n - k);

	/* calculate numerator of formula (2) first */
	for (int i = n; i >= n - k + 1; i--)
	{
		v *= i;
	}

	/*
	 * Divide by each factor in the denominator of formula (2), skipping
	 * division by 1.
	 */
	for (int i = k; i >= 2; i--)
	{
		v /= i;
	}

	return v;
}


/*
 * ChooseDepthLimit() calculates the depth limit for the parameterization
 * search, given the number of candidate relations.
 *
 * The maximum number of paths generated for a given depthLimit is:
 *
 *    Combinations(nCandidates, 0) + Combinations(nCandidates, 1) + ... +
 *    Combinations(nCandidates, depthLimit)
 *
 * There's no closed formula for a partial sum of combinations, so just keep
 * increasing the depth until the number of combinations exceeds the limit.
 */
static int
ChooseDepthLimit(int nCandidates)
{
	if (!EnableColumnarQualPushdown)
	{
		return 0;
	}

	int depth = 0;
	double numPaths = 1;

	while (depth < nCandidates)
	{
		numPaths += Combinations(nCandidates, depth + 1);

		if (numPaths > (double) ColumnarMaxCustomScanPaths)
		{
			break;
		}

		depth++;
	}

	return depth;
}


/*
 * AddColumnarScanPaths is the entry point for recursively generating
 * parameterized paths. See AddColumnarScanPathsRec() for discussion.
 */
static void
AddColumnarScanPaths(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	List *joinClauses = FindPushdownJoinClauses(root, rel);
	Relids candidateRelids = FindCandidateRelids(root, rel, joinClauses);

	int depthLimit = ChooseDepthLimit(bms_num_members(candidateRelids));

	/* must always parameterize by lateral refs */
	Relids paramRelids = bms_copy(rel->lateral_relids);

	AddColumnarScanPathsRec(root, rel, rte, paramRelids, candidateRelids,
							depthLimit);
}


/*
 * AddColumnarScanPathsRec is a recursive function to search the
 * parameterization space and add CustomPaths for columnar scans.
 *
 * The set paramRelids is the parameterization at the current level, and
 * candidateRelids is the set from which we draw to generate paths with
 * greater parameterization.
 *
 * Engine tables resemble indexes because of the ability to push down
 * quals. Ordinary quals, such as x = 7, can be pushed down easily. But join
 * quals of the form "x = y" (where "y" comes from another rel) require the
 * proper parameterization.
 *
 * Paths that require more outer rels can push down more join clauses that
 * depend on those outer rels. But requiring more outer rels gives the planner
 * fewer options for the shape of the plan. That means there is a trade-off,
 * and we should generate plans of various parameterizations, then let the
 * planner choose. We always need to generate one minimally-parameterized path
 * (parameterized only by lateral refs, if present) to make sure that at least
 * one path can be chosen. Then, we generate as many parameterized paths as we
 * reasonably can.
 *
 * The set of all possible parameterizations is the power set of
 * candidateRelids. The power set has cardinality 2^N, where N is the
 * cardinality of candidateRelids. To avoid creating a huge number of paths,
 * limit the depth of the search; the depthLimit is equivalent to the maximum
 * number of required outer rels (beyond the minimal parameterization) for the
 * path. A depthLimit of zero means that only the minimally-parameterized path
 * will be generated.
 */
static void
AddColumnarScanPathsRec(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
						Relids paramRelids, Relids candidateRelids,
						int depthLimit)
{
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	Assert(!bms_overlap(paramRelids, candidateRelids));

	Path *columnarScanPath = AddColumnarScanPath(root, rel, rte, paramRelids);
	add_path(rel, columnarScanPath);
	if (engine_enable_parallel_execution)
		columnarScanPath->total_cost += columnarScanPath->rows * 0.1;

	/* For columnar custom scan we should also do planning for parallel exeuction
	 * if it is enabled with GUC.
	 */
	if (engine_enable_parallel_execution)
	{
		int engine_min_parallel_process_running = engine_min_parallel_processes;

		if (engine_min_parallel_processes > max_parallel_workers)
		{
			elog(DEBUG1, "storage_engine.min_parallel_proceses is set higher than max_parallel_workers.");
			elog(DEBUG1, "Using max_parallel_workers instead for parallel columnar scan.");
			engine_min_parallel_process_running = 
				Min(max_parallel_workers, engine_min_parallel_processes);
		}

		if (parallel_leader_participation)
			engine_min_parallel_process_running--;

		/*
		 * We know how many workers are we going to run. Since workers are using single stripe
		 * probably it doesn't have much sense to spawn more workers than stripes in table.
		 */
		engine_min_parallel_process_running = Min(engine_min_parallel_process_running,
													parallel_leader_participation ?
													ColumnarTableStripeCount(rte->relid) - 1 :
													ColumnarTableStripeCount(rte->relid));

		if (rel->consider_parallel && rel->lateral_relids == NULL && 
			engine_min_parallel_process_running > 0)
		{
			/*
			 * Passing NULL for JOIN quals in parallel execution.
			 */
			Path *parallelColumnarScanPath = 
				AddColumnarScanPath(root, rel, rte, NULL);

			parallelColumnarScanPath->parallel_workers = engine_min_parallel_process_running;
			parallelColumnarScanPath->parallel_aware = true;
			AdjustColumnarParallelScanCost(parallelColumnarScanPath);

			add_partial_path(rel, parallelColumnarScanPath);

			/*
			 * When the query has ORDER BY (root->query_pathkeys != NIL),
			 * also add a pre-sorted partial path so that
			 * generate_useful_gather_paths() can create a
			 * Gather Merge(Sort(ColcompressScan)) path that satisfies the
			 * required ordering.
			 *
			 * Without this, PG15 may not automatically add Sort inside
			 * partial paths for columnar scans (useful_pathkeys_list can be
			 * NIL when there are no matching indexes), causing the planner to
			 * choose Gather(ColcompressScan) without any Sort node above it.
			 * This silently drops ORDER BY, returning rows in an arbitrary
			 * worker-completion order.
			 */
			if (root->query_pathkeys != NIL)
			{
				Path *sortedParallelPath = (Path *)
					create_sort_path(root, rel,
									 parallelColumnarScanPath,
									 root->query_pathkeys,
									 -1.0 /* no tuple limit */);
				add_partial_path(rel, sortedParallelPath);
			}
		}
	}

	/* recurse for all candidateRelids, unless we hit the depth limit */
	Assert(depthLimit >= 0);
	if (depthLimit-- == 0)
	{
		return;
	}

	/*
	 * Iterate through parameter combinations depth-first. Deeper levels
	 * generate paths of greater parameterization (and hopefully lower
	 * cost).
	 */
	Relids tmpCandidateRelids = bms_copy(candidateRelids);
	int relid = -1;
	while ((relid = bms_next_member(candidateRelids, relid)) >= 0)
	{
		Relids tmpParamRelids = bms_add_member(
			bms_copy(paramRelids), relid);

		/*
		 * Because we are generating combinations (not permutations), remove
		 * the relid from the set of candidates at this level as we descend to
		 * the next.
		 */
		tmpCandidateRelids = bms_del_member(tmpCandidateRelids, relid);

		AddColumnarScanPathsRec(root, rel, rte, tmpParamRelids,
								tmpCandidateRelids, depthLimit);
	}

	bms_free(tmpCandidateRelids);
}


/*
 * ParameterizationAsString returns the string representation of the set of
 * rels given in paramRelids.
 *
 * Takes a StringInfo so that it doesn't return palloc'd memory. This makes it
 * easy to call this function as an argument to ereport(), such that it won't
 * be evaluated unless the message is going to be output somewhere.
 */
static char *
ParameterizationAsString(PlannerInfo *root, Relids paramRelids, StringInfo buf)
{
	bool firstTime = true;
	int relid = -1;

	if (bms_num_members(paramRelids) == 0)
	{
		return "unparameterized";
	}

	appendStringInfoString(buf, "parameterized by rels {");
	while ((relid = bms_next_member(paramRelids, relid)) >= 0)
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		const char *relname = quote_identifier(rte->eref->aliasname);

		appendStringInfo(buf, "%s%s", firstTime ? "" : ", ", relname);

		if (relname != rte->eref->aliasname)
		{
			pfree((void *) relname);
		}

		firstTime = false;
	}
	appendStringInfoString(buf, "}");
	return buf->data;
}


/*
 * ContainsExecParams tests whether the node contains any exec params. The
 * signature accepts an extra argument for use with expression_tree_walker.
 */
static bool
ContainsExecParams(Node *node, void *notUsed)
{
	if (node == NULL)
	{
		return false;
	}
	else if (IsA(node, Param))
	{
		Param *param = castNode(Param, node);
		if (param->paramkind == PARAM_EXEC)
		{
			return true;
		}
	}
	return expression_tree_walker(node, ContainsExecParams, NULL);
}

#if PG_VERSION_NUM >= PG_VERSION_16

/*
 * fixup_inherited_columns
 *
 * Exact function Copied from PG16 as it's static.
 *
 * When user is querying on a table with children, it implicitly accesses
 * child tables also. So, we also need to check security label of child
 * tables and columns, but there is no guarantee attribute numbers are
 * same between the parent and children.
 * It returns a bitmapset which contains attribute number of the child
 * table based on the given bitmapset of the parent.
 */
static Bitmapset *
fixup_inherited_columns(Oid parentId, Oid childId, Bitmapset *columns)
{
	Bitmapset *result = NULL;

	/*
	 * obviously, no need to do anything here
	 */
	if (parentId == childId)
	{
		return columns;
	}

	int index = -1;
	while ((index = bms_next_member(columns, index)) >= 0)
	{
		/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
		AttrNumber attno = index + FirstLowInvalidHeapAttributeNumber;

		/*
		 * whole-row-reference shall be fixed-up later
		 */
		if (attno == InvalidAttrNumber)
		{
			result = bms_add_member(result, index);
			continue;
		}

		char *attname = get_attname(parentId, attno, false);
		attno = get_attnum(childId, attname);
		if (attno == InvalidAttrNumber)
		{
			elog(ERROR, "cache lookup failed for attribute %s of relation %u",
				 attname, childId);
		}

		result = bms_add_member(result,
								attno - FirstLowInvalidHeapAttributeNumber);

		pfree(attname);
	}

	return result;
}
#endif


/*
 * Create and add a path with the given parameterization paramRelids.
 *
 * XXX: Consider refactoring to be more like postgresGetForeignPaths(). The
 * only differences are param_info and custom_private.
 */
static Path *
AddColumnarScanPath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
					Relids paramRelids)
{
	/*
	 * Must return a CustomPath, not a larger structure containing a
	 * CustomPath as the first field. Otherwise, nodeToString() will fail to
	 * output the additional fields.
	 */
	CustomPath *cpath = makeNode(CustomPath);

	cpath->methods = &ColumnarScanPathMethods;

	/*
	 * populate generic path information
	 */
	Path *path = &cpath->path;
	path->pathtype = T_CustomScan;
	path->parent = rel;
	path->pathtarget = rel->reltarget;

	/* columnar scans are not parallel-aware, but they are parallel-safe */
	path->parallel_safe = rel->consider_parallel;

	path->param_info = get_baserel_parampathinfo(root, rel, paramRelids);

	/*
	 * Usable clauses for this parameterization exist in baserestrictinfo and
	 * ppi_clauses.
	 */
	List *allClauses = copyObject(rel->baserestrictinfo);
	if (path->param_info != NULL)
	{
		allClauses = list_concat(allClauses, path->param_info->ppi_clauses);
	}

	allClauses = FilterPushdownClauses(root, rel, allClauses);

	/*
	 * Plain clauses may contain extern params, but not exec params, and can
	 * be evaluated at init time or rescan time. Track them in another list
	 * that is a subset of allClauses.
	 *
	 * Note: although typically baserestrictinfo contains plain clauses,
	 * that's not always true. It can also contain a qual referencing a Var at
	 * a higher query level, which can be turned into an exec param, and
	 * therefore it won't be a plain clause.
	 */
	List *plainClauses = NIL;
	ListCell *lc;
	foreach(lc, allClauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		if (bms_is_subset(rinfo->required_relids, rel->relids) &&
			!ContainsExecParams((Node *) rinfo->clause, NULL))
		{
			plainClauses = lappend(plainClauses, rinfo);
		}
	}

	/*
	 * We can't make our own CustomPath structure, so we need to put
	 * everything in the custom_private list. To keep the two lists separate,
	 * we make them sublists in a 2-element list.
	 */
	if (EnableColumnarQualPushdown)
	{
		cpath->custom_private = list_make2(copyObject(plainClauses),
										   copyObject(allClauses));
	}
	else
	{
		cpath->custom_private = list_make2(NIL, NIL);
	}

	int numberOfColumnsRead = 0;
#if PG_VERSION_NUM >= PG_VERSION_16
	if (rte->perminfoindex > 0)
	{
		/*
		 * If perminfoindex > 0, that means that this relation's permission info
		 * is directly found in the list of rteperminfos of the Query(root->parse)
		 * So, all we have to do here is retrieve that info.
		 */
		RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos,
														   rte);
		numberOfColumnsRead = bms_num_members(perminfo->selectedCols);
	}
	else
	{
		/*
		 * If perminfoindex = 0, that means we are skipping the check for permission info
		 * for this relation, which means that it's either a partition or an inheritance child.
		 * In these cases, we need to access the permission info of the top parent of this relation.
		 * After thorough checking, we found that the index of the top parent pointing to the correct
		 * range table entry in Query's range tables (root->parse->rtable) is found under
		 * RelOptInfo rel->top_parent->relid.
		 * For reference, check expand_partitioned_rtentry and expand_inherited_rtentry PG functions
		 */
		Assert(rel->top_parent);
		RangeTblEntry *parent_rte = rt_fetch(rel->top_parent->relid, root->parse->rtable);
		RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos,
														   parent_rte);
		numberOfColumnsRead = bms_num_members(fixup_inherited_columns(perminfo->relid,
																	  rte->relid,
																	  perminfo->
																	  selectedCols));
	}
#else
	numberOfColumnsRead = bms_num_members(rte->selectedCols);
#endif
	int numberOfClausesPushed = list_length(allClauses);

	/* Queries that contain only aggregate with STAR doesn't have any
	 * selectedCols so we should consider at least one column that needs
	 * read (for better cost calculation).
	 */
	if (numberOfColumnsRead == 0)
		numberOfColumnsRead = 1;

	CostColumnarScan(root, rel, rte->relid, cpath, numberOfColumnsRead,
					 numberOfClausesPushed);


	StringInfoData buf;
	initStringInfo(&buf);
	ereport(ColumnarPlannerDebugLevel,
			(errmsg("columnar planner: adding CustomScan path for %s",
					rte->eref->aliasname),
			 errdetail("%s; %d clauses pushed down",
					   ParameterizationAsString(root, paramRelids, &buf),
					   numberOfClausesPushed)));

	return path;
}


/*
 * AdjustColumnarParallelScanCost calculates path cost based on
 * number of parallel workers (based on postgres code).
 */
static void
AdjustColumnarParallelScanCost(Path *path) 
{
	/* Adjust costing for parallelism, if used. */
	if (path->parallel_workers > 0)
	{
		double parallel_divisor = path->parallel_workers;

		if (parallel_leader_participation)
		{
			double leader_contribution;

			leader_contribution = 1.0 - (0.3 * path->parallel_workers);
			if (leader_contribution > 0)
				parallel_divisor += leader_contribution;
		}

		/* The CPU cost is divided among all the workers. */
		path->total_cost /= parallel_divisor;

		path->rows = clamp_row_est(path->rows / parallel_divisor);
	}
}


/*
 * CostColumnarScan calculates the cost of scanning the columnar table. The
 * cost is estimated by using all stripe metadata to estimate based on the
 * columns to read how many pages need to be read.
 */
static void
CostColumnarScan(PlannerInfo *root, RelOptInfo *rel, Oid relationId,
				 CustomPath *cpath, int numberOfColumnsRead, int nClauses)
{
	Path *path = &cpath->path;

	List *allClauses = lsecond(cpath->custom_private);
	Selectivity clauseSel = clauselist_selectivity(
		root, allClauses, rel->relid, JOIN_INNER, NULL);

	/*
	 * We already filtered out clauses where the overall selectivity would be
	 * misleading, such as inequalities involving an uncorrelated column. So
	 * we can apply the selectivity directly to the number of stripes.
	 */
	double stripesToRead = clauseSel * ColumnarTableStripeCount(relationId);
	stripesToRead = Max(stripesToRead, 1.0);

	path->rows = rel->tuples;
	path->startup_cost = 0;
	path->total_cost = stripesToRead *
					   ColumnarPerStripeScanCost(rel, relationId, numberOfColumnsRead);
}


/*
 * ColumnarPerStripeScanCost calculates the cost to scan a single stripe
 * of given columnar table based on number of columns that needs to be
 * read during scan operation.
 */
static Cost
ColumnarPerStripeScanCost(RelOptInfo *rel, Oid relationId, int numberOfColumnsRead)
{
	Relation relation = RelationIdGetRelation(relationId);
	List *stripeList = StripesForRelfilenode(RelationPhysicalIdentifier_compat(relation), ForwardScanDirection);
	RelationClose(relation);

	uint32 maxColumnCount = 0;
	uint64 totalStripeSize = 0;
	StripeMetadata *stripeMetadata = NULL;
	foreach_ptr(stripeMetadata, stripeList)
	{
		totalStripeSize += stripeMetadata->dataLength;
		maxColumnCount = Max(maxColumnCount, stripeMetadata->columnCount);
	}

	/*
	 * When no stripes are in the table we don't have a count in maxColumnCount. To
	 * prevent a division by zero turning into a NaN we keep the ratio on zero.
	 * This will result in a cost of 0 for scanning the table which is a reasonable
	 * cost on an empty table.
	 */
	if (maxColumnCount == 0)
	{
		return 0;
	}

	double columnSelectionRatio = numberOfColumnsRead / (double) maxColumnCount;
	Cost tableScanCost = (double) totalStripeSize / BLCKSZ * columnSelectionRatio;
	Cost perStripeScanCost = tableScanCost / list_length(stripeList);

	/*
	 * Finally, multiply the cost of reading a single stripe by seq page read
	 * cost to make our estimation scale compatible with postgres.
	 * Since we are calculating the cost for a single stripe here, we use seq
	 * page cost instead of random page cost. This is because, random page
	 * access only happens when switching between columns, which is pretty
	 * much neglactable.
	 */
	double relSpaceSeqPageCost;
	get_tablespace_page_costs(rel->reltablespace,
							  NULL, &relSpaceSeqPageCost);
	perStripeScanCost = perStripeScanCost * relSpaceSeqPageCost;

	return perStripeScanCost;
}


/*
 * ColumnarTableStripeCount returns the number of stripes that columnar
 * table with relationId has by using stripe metadata.
 */
static uint64
ColumnarTableStripeCount(Oid relationId)
{
	Relation relation = RelationIdGetRelation(relationId);
	List *stripeList = StripesForRelfilenode(RelationPhysicalIdentifier_compat(relation), ForwardScanDirection);
	int stripeCount = list_length(stripeList);
	RelationClose(relation);

	return stripeCount;
}


static Plan *
ColumnarScanPath_PlanCustomPath(PlannerInfo *root,
								RelOptInfo *rel,
								struct CustomPath *best_path,
								List *tlist,
								List *clauses,
								List *custom_plans)
{
	/*
	 * Must return a CustomScan, not a larger structure containing a
	 * CustomScan as the first field. Otherwise, copyObject() will fail to
	 * copy the additional fields.
	 */
	CustomScan *cscan = makeNode(CustomScan);

	cscan->methods = &ColumnarScanScanMethods;

	/* XXX: also need to store projected column list for EXPLAIN */

	if (EnableColumnarQualPushdown)
	{
		/*
		 * Lists of pushed-down clauses. The Vars in custom_exprs referencing
		 * other relations will be changed into exec Params by
		 * create_customscan_plan().
		 *
		 * Like CustomPath->custom_private, keep a list of plain clauses
		 * separate from the list of all clauses by making them sublists of a
		 * 2-element list.
		 *
		 * XXX: custom_exprs are the quals that will be pushed into the
		 * columnar reader code; some of these may not be usable. We should
		 * fix this by processing the quals more completely and using
		 * ScanKeys.
		 */
		List *plainClauses = extract_actual_clauses(
			linitial(best_path->custom_private), false /* no pseudoconstants */);
		List *allClauses = extract_actual_clauses(
			lsecond(best_path->custom_private), false /* no pseudoconstants */);
		cscan->custom_exprs = copyObject(list_make2(plainClauses, allClauses));
	}
	else
	{
		cscan->custom_exprs = list_make2(NIL, NIL);
	}

	cscan->scan.plan.qual = extract_actual_clauses(
		clauses, false /* no pseudoconstants */);
	cscan->scan.plan.targetlist = list_copy(tlist);
	cscan->scan.scanrelid = best_path->path.parent->relid;

	/*
	 * Vectorized QUAL execution. Split QUAL list into vectorized list 
	 * stored in third position of custom_exprs list and rest of
	 * qual list (which can't be vectorized) in scan.plan.qual.
	 */
	if (engine_enable_vectorization)
	{
		List *candidateQualList = CreateVectorizedExprList(cscan->scan.plan.qual);
		List *listDifference = list_difference(candidateQualList, cscan->scan.plan.qual);

		cscan->custom_exprs = lappend(cscan->custom_exprs, listDifference);

		if (listDifference != NULL)
			cscan->scan.plan.qual = list_intersection(cscan->scan.plan.qual,
													  candidateQualList);
	}
	else
	{
		/* We still need to create third entry in custom_exprs */
		cscan->custom_exprs = lappend(cscan->custom_exprs, NIL);
	}

	return (Plan *) cscan;
}


/*
 * ReparameterizeMutator changes all varnos referencing the topmost parent of
 * child_rel to instead reference child_rel directly.
 */
static Node *
ReparameterizeMutator(Node *node, RelOptInfo *child_rel)
{
	if (node == NULL)
	{
		return NULL;
	}
	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);
		if (bms_is_member(var->varno, child_rel->top_parent_relids))
		{
			var = copyObject(var);
			var->varno = child_rel->relid;
		}
		return (Node *) var;
	}

	if (IsA(node, RestrictInfo))
	{
		RestrictInfo *rinfo = castNode(RestrictInfo, node);
		rinfo = copyObject(rinfo);
		rinfo->clause = (Expr *) expression_tree_mutator(
			(Node *) rinfo->clause, ReparameterizeMutator, (void *) child_rel);
		return (Node *) rinfo;
	}
	return expression_tree_mutator(node, ReparameterizeMutator,
								   (void *) child_rel);
}


/*
 * ColumnarScanPath_ReparameterizeCustomPathByChild is a method called when a
 * path is reparameterized directly to a child relation, rather than the
 * top-level parent.
 *
 * For instance, let there be a join of two partitioned columnar relations PX
 * and PY. A path for a ColumnarScan of PY3 might be parameterized by PX so
 * that the join qual "PY3.a = PX.a" (referencing the parent PX) can be pushed
 * down. But if the planner decides on a partition-wise join, then the path
 * will be reparameterized on the child table PX3 directly.
 *
 * When that happens, we need to update all Vars in the pushed-down quals to
 * reference PX3, not PX, to match the new parameterization. This method
 * notifies us that it needs to be done, and allows us to update the
 * information in custom_private.
 */
static List *
ColumnarScanPath_ReparameterizeCustomPathByChild(PlannerInfo *root,
												 List *custom_private,
												 RelOptInfo *child_rel)
{
	return (List *) ReparameterizeMutator((Node *) custom_private, child_rel);
}


static Node *
ColumnarScan_CreateCustomScanState(CustomScan *cscan)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) newNode(
		sizeof(ColumnarScanState), T_CustomScanState);

	CustomScanState *cscanstate = &columnarScanState->custom_scanstate;
	cscanstate->methods = &ColumnarScanExecuteMethods;

	return (Node *) cscanstate;
}


/*
 * EvalParamsMutator evaluates Params in the expression and replaces them with
 * Consts.
 */
static Node *
EvalParamsMutator(Node *node, ExprContext *econtext)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, Param))
	{
		Param *param = (Param *) node;
		int16 typLen;
		bool typByVal;
		bool isnull;

		get_typlenbyval(param->paramtype, &typLen, &typByVal);

		/* XXX: should save ExprState for efficiency */
		ExprState *exprState = ExecInitExprWithParams((Expr *) node,
													  econtext->ecxt_param_list_info);
		Datum pval = ExecEvalExpr(exprState, econtext, &isnull);

		return (Node *) makeConst(param->paramtype,
								  param->paramtypmod,
								  param->paramcollid,
								  (int) typLen,
								  pval,
								  isnull,
								  typByVal);
	}

	return expression_tree_mutator(node, EvalParamsMutator, (void *) econtext);
}


static void
ColumnarScan_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags)
{
	CustomScan *cscan = (CustomScan *) cscanstate->ss.ps.plan;
	ColumnarScanState *columnarScanState = (ColumnarScanState *) cscanstate;
	ExprContext *stdecontext = cscanstate->ss.ps.ps_ExprContext;

	/*
	 * Make a new ExprContext just like the existing one, except that we don't
	 * reset it every tuple.
	 */
	ExecAssignExprContext(estate, &cscanstate->ss.ps);
	columnarScanState->css_RuntimeContext = cscanstate->ss.ps.ps_ExprContext;
	cscanstate->ss.ps.ps_ExprContext = stdecontext;

	ResetExprContext(columnarScanState->css_RuntimeContext);
	List *plainClauses = linitial(cscan->custom_exprs);
	columnarScanState->qual = (List *) EvalParamsMutator(
		(Node *) plainClauses, columnarScanState->css_RuntimeContext);

	/*
	 * Third list in custom_exprs should contain vectorized qual list if not 
	 * NULL.
	 */
	if (lthird(cscan->custom_exprs) != NIL)
		columnarScanState->vectorization.vectorizedQualList =  lthird(cscan->custom_exprs);

	ListCell *lc;
	foreach(lc, cscan->custom_private)
	{
		Const *privateCustomData = lfirst(lc);
		if (privateCustomData->consttype == CUSTOM_SCAN_VECTORIZED_AGGREGATE)
		{
			columnarScanState->vectorization.vectorizationAggregate =
				privateCustomData->constvalue;
		}
	}

	/*
	 * Vectorization is enabled if global variable is set and there is at least one
	 * filter which can be vectorized.
	 */
	columnarScanState->vectorization.vectorizationEnabled =
		engine_enable_vectorization &&
		(columnarScanState->vectorization.vectorizedQualList != NULL ||
		 columnarScanState->vectorization.vectorizationAggregate);

	if (columnarScanState->vectorization.vectorizationAggregate)
	{
		ScanState *node = (ScanState *) &columnarScanState->custom_scanstate.ss;

		if (node->ps.ps_ProjInfo)
		{
			columnarScanState->vectorization.resultVectorSlot =
				CreateVectorTupleTableSlot(node->ps.ps_ProjInfo->pi_state.resultslot->tts_tupleDescriptor);
		}
		else
		{
			columnarScanState->vectorization.resultVectorSlot =
				CreateVectorTupleTableSlot(node->ps.ps_ResultTupleDesc);
		}
	}

	if (columnarScanState->vectorization.vectorizationEnabled)
	{
		columnarScanState->vectorization.scanVectorSlot =
			CreateVectorTupleTableSlot(cscanstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	}

	columnarScanState->attrNeeded = 
		ColumnarAttrNeeded(&cscanstate->ss, columnarScanState->vectorization.vectorizedQualList);

	int bmsMember = -1;
	while ((bmsMember = bms_next_member(columnarScanState->attrNeeded, bmsMember)) >= 0)
	{
		columnarScanState->vectorization.attrNeededList = 
			lappend_int(columnarScanState->vectorization.attrNeededList, bmsMember);
	}

	/*
	 * If we have pending changes that need to be flushed (row_mask after update/delete)
	 * or new stripe we need to to them here because sequential columnar scan 
	 * can have parallel execution and updated are not allowed in parallel mode.
	 */
	columnarScanState->snapshot = estate->es_snapshot;
	columnarScanState->snapshotRegisteredByUs = false;
#if PG_VERSION_NUM >= PG_VERSION_16
	Oid relationOid = cscanstate->ss.ss_currentRelation->rd_locator.relNumber;
#else
	Oid relationOid = cscanstate->ss.ss_currentRelation->rd_node.relNode;
#endif

	if(!IsInParallelMode())
	{
		RowMaskFlushWriteStateForRelfilenode(relationOid, GetCurrentSubTransactionId());

		FlushWriteStateWithNewSnapshot(relationOid, &columnarScanState->snapshot,
									   &columnarScanState->snapshotRegisteredByUs);
	}

	/* scan slot is already initialized */
}


/*
 * ColumnarAttrNeeded returns a list of AttrNumber's for the ones that are
 * needed during columnar custom scan.
 * Throws an error if finds a Var referencing to an attribute not supported
 * by ColumnarScan.
 */
Bitmapset *
ColumnarAttrNeeded(ScanState *ss, List *customList)
{
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;
	int natts = slot->tts_tupleDescriptor->natts;
	Bitmapset *attr_needed = NULL;
	Plan *plan = ss->ps.plan;
	int flags = PVC_RECURSE_AGGREGATES |
				PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS;
	List *vars = list_concat(pull_var_clause((Node *) plan->targetlist, flags),
							 pull_var_clause((Node *) plan->qual, flags));

	if (customList != NULL)
		vars = list_concat(vars, pull_var_clause((Node *)customList, flags));

	ListCell *lc;

	foreach(lc, vars)
	{
		Var *var = lfirst(lc);

		/* 
		 * We support following special variables with update / delete.
		 */
		if (var->varattno == SelfItemPointerAttributeNumber ||
			var->varattno == TableOidAttributeNumber)
		{
			continue;
		}
		else if (var->varattno < SelfItemPointerAttributeNumber)
		{
			ereport(ERROR, 
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("MIN / MAX TransactionID or CommandID not supported for ColcompressScan")));
		}

		if (var->varattno == 0 )
		{
			/* all attributes are required, we don't need to add more so break*/
			attr_needed = bms_add_range(attr_needed, 0, natts - 1);
			break;
		}

		attr_needed = bms_add_member(attr_needed, var->varattno - 1);
	}

	return attr_needed;
}


/*
 * ExecScanFetch -- check interrupts & fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.  If we aren't, just execute
 * the access method's next-tuple routine.
 */
static inline TupleTableSlot *
ExecScanFetch(ScanState *node,
			  ExecScanAccessMtd accessMtd,
			  ExecScanRecheckMtd recheckMtd)
{
	EState	   *estate = node->ps.state;

	CHECK_FOR_INTERRUPTS();

	if (estate->es_epq_active != NULL)
	{
		EPQState   *epqstate = estate->es_epq_active;

		/*
		 * We are inside an EvalPlanQual recheck.  Return the test tuple if
		 * one is available, after rechecking any access-method-specific
		 * conditions.
		 */
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid == 0)
		{
			/*
			 * This is a ForeignScan or CustomScan which has pushed down a
			 * join to the remote side.  The recheck method is responsible not
			 * only for rechecking the scan/join quals but also for storing
			 * the correct tuple in the slot.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			if (!(*recheckMtd) (node, slot))
				ExecClearTuple(slot);	/* would not be returned by scan */
			return slot;
		}
		else if (epqstate->relsubs_done[scanrelid - 1])
		{
			/*
			 * Return empty slot, as we already performed an EPQ substitution
			 * for this relation.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Return empty slot, as we already returned a tuple */
			return ExecClearTuple(slot);
		}
		else if (epqstate->relsubs_slot[scanrelid - 1] != NULL)
		{
			/*
			 * Return replacement tuple provided by the EPQ caller.
			 */

			TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];

			Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);

			/* Mark to remember that we shouldn't return more */
			epqstate->relsubs_done[scanrelid - 1] = true;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
		else if (epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
		{
			/*
			 * Fetch and return replacement tuple using a non-locking rowmark.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Mark to remember that we shouldn't return more */
			epqstate->relsubs_done[scanrelid - 1] = true;

			if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot))
				return NULL;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
	}

	/*
	 * Run the node-type-specific access method function to get the next tuple
	 */
	return (*accessMtd) (node);
}

static TupleTableSlot *
CustomExecScan(ColumnarScanState *columnarScanState,
			   ExecScanAccessMtd accessMtd,
			   ExecScanRecheckMtd recheckMtd)
{
	ExprContext *econtext;
	ExprState  *qual;
	ProjectionInfo *projInfo;

	ScanState *node = (ScanState *) &columnarScanState->custom_scanstate.ss;

	/*
	 * Fetch data from node
	 */
	qual = node->ps.qual;
	projInfo = node->ps.ps_ProjInfo;
	econtext = node->ps.ps_ExprContext;

	/* interrupt checks are in ExecScanFetch */

	/*
	 * No qual to check and no projection to do and vectorization is not enabled,
	 * just skip all the overhead and return the raw scan tuple.
	 */
	if (!qual && !projInfo && !columnarScanState->vectorization.vectorizationEnabled)
	{
		ResetExprContext(econtext);
		return ExecScanFetch(node, accessMtd, recheckMtd);
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);
	
	uint resultVectorTupleSlotIdx = 0;

	if (columnarScanState->vectorization.vectorizationAggregate)
	{
		ExecClearTuple(columnarScanState->vectorization.resultVectorSlot);
		CleanupVectorSlot((VectorTupleTableSlot *)
							columnarScanState->vectorization.resultVectorSlot);
	}

	/*
	 * get a tuple from the access method.  Loop until we obtain a tuple that
	 * passes the qualification.
	 */
	for (;;)
	{
		TupleTableSlot *slot = NULL;

		if (columnarScanState->vectorization.vectorizationEnabled && 
			columnarScanState->vectorization.vectorPendingRowNumber == 0 &&
			resultVectorTupleSlotIdx > 0)
		{
			((VectorTupleTableSlot *) columnarScanState->vectorization.resultVectorSlot)->dimension = resultVectorTupleSlotIdx;
			ExecStoreVirtualTuple(columnarScanState->vectorization.resultVectorSlot);
			resultVectorTupleSlotIdx = 0;
			return columnarScanState->vectorization.resultVectorSlot;
		}

		/*
		 * No vectorizaion in place so fetch slot by slot.
		 */
		if (!columnarScanState->vectorization.vectorizationEnabled)
		{
			slot = ExecScanFetch(node, accessMtd, recheckMtd);

			resultVectorTupleSlotIdx = 0;

			/*
			 * if the slot returned by the accessMtd contains NULL, then it means
			 * there is nothing more to scan so we just return an empty slot,
			 * being careful to use the projection result slot so it has correct
			 * tupleDesc.
			 */
			if (TupIsNull(slot))
			{
				if (projInfo)
					return ExecClearTuple(projInfo->pi_state.resultslot);
				else
					return slot;
			}
		}
		/*
		 * Get next tuple from vector tuple slot if vectorization exeuction is enabled
		 * and if we exhausted previous vector.
		 */
		else if (columnarScanState->vectorization.vectorizationEnabled &&
				 columnarScanState->vectorization.vectorPendingRowNumber == 0)
		{
			slot = ExecScanFetch(node, accessMtd, recheckMtd);

			/*
			 * if the slot returned by the accessMtd contains NULL, then it means
			 * there is nothing more to scan so we just return an empty slot,
			 * being careful to use the projection result slot so it has correct
			 * tupleDesc.
			 */
			if (TupIsNull(slot))
			{
				if (projInfo)
					return ExecClearTuple(projInfo->pi_state.resultslot);
				else
					return slot;
			}

			if (columnarScanState->vectorization.vectorizedQualList != NULL)
			{

				if (columnarScanState->vectorization.constructedVectorizedQualList == NULL)
				{
					columnarScanState->vectorization.constructedVectorizedQualList =
						ConstructVectorizedQualList(slot, columnarScanState->vectorization.vectorizedQualList);
				}

				VectorTupleTableSlot *vectorSlot = (VectorTupleTableSlot *) slot;

				bool *resultQual = 
					ExecuteVectorizedQual(slot,
										columnarScanState->vectorization.constructedVectorizedQualList,
										AND_EXPR, econtext);

				memcpy(vectorSlot->keep, resultQual, COLUMNAR_VECTOR_COLUMN_SIZE);
			}
			/*
			 * No qual, no vectorized qual, no projection but we need to return vector
			 * for vectorized aggregate
			 */
			else if (!qual && !projInfo && columnarScanState->vectorization.vectorizationAggregate)
			{
				columnarScanState->vectorization.vectorPendingRowNumber = 0;
				return slot;
			}
		}

		uint64 rowNumber = 0;

		/*
		 * Get next tuple from vector tuple slot if vectorization execution 
		 * is enabled.
		 */
		if (columnarScanState->vectorization.vectorizationEnabled)
		{
			VectorTupleTableSlot *vectorSlot = 
				(VectorTupleTableSlot *) columnarScanState->vectorization.scanVectorSlot;

			bool rowFound = false;

			while (columnarScanState->vectorization.vectorPendingRowNumber != 0 &&
				   rowFound == false)
			{
				if (vectorSlot->keep[columnarScanState->vectorization.vectorRowIndex])
				{
					slot = columnarScanState->custom_scanstate.ss.ss_ScanTupleSlot;
					ExecClearTuple(slot);
					ExtractTupleFromVectorSlot(slot,
											   vectorSlot,
											   columnarScanState->vectorization.vectorRowIndex,
											   columnarScanState->vectorization.attrNeededList);
					
					rowNumber = vectorSlot->rowNumber[columnarScanState->vectorization.vectorRowIndex];
					if (!columnarScanState->vectorization.vectorizationAggregate)
						slot->tts_tid = row_number_to_tid(rowNumber);
					rowFound = true;
				}
				columnarScanState->vectorization.vectorPendingRowNumber--;
				columnarScanState->vectorization.vectorRowIndex++;
			}

			if (rowFound == false)
				continue;
		}

		/*
		 * place the current tuple into the expr context
		 */
		econtext->ecxt_scantuple = slot;

		/*
		 * check that the current tuple satisfies the qual-clause
		 *
		 * check for non-null qual here to avoid a function call to ExecQual()
		 * when the qual is null ... saves only a few cycles, but they add up
		 * ...
		 */
		if (qual == NULL || ExecQual(qual, econtext))
		{
			/*
			 * Found a satisfactory scan tuple.
			 */
			if (projInfo)
			{
				/*
				 * Form a projection tuple, store it in the result tuple slot
				 * and return it.
				 */
				slot = ExecProject(projInfo);
			}

			if (columnarScanState->vectorization.vectorizationAggregate)
			{
				WriteTupleToVectorSlot(slot,
									  (VectorTupleTableSlot *) columnarScanState->vectorization.resultVectorSlot,
									  resultVectorTupleSlotIdx);
				((VectorTupleTableSlot *) 
					columnarScanState->vectorization.resultVectorSlot)->rowNumber[resultVectorTupleSlotIdx] = rowNumber;
				resultVectorTupleSlotIdx++;
				continue;
			}		
			else
			{
				return slot;
			}
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);
	}
}


static TupleTableSlot *
ColumnarScanNext(ColumnarScanState *columnarScanState)
{
	CustomScanState *node = (CustomScanState *) columnarScanState;

	/*
	 * get information from the estate and scan state
	 */
	TableScanDesc scandesc = node->ss.ss_currentScanDesc;
	EState *estate = node->ss.ps.state;
	ScanDirection direction = estate->es_direction;
	bool vectorizationEnabled = columnarScanState->vectorization.vectorizationEnabled;

	TupleTableSlot *slot = NULL;

	if (vectorizationEnabled)
		slot = columnarScanState->vectorization.scanVectorSlot;
	else
		slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/* the columnar access method does not use the flags, they are specific to heap */
		uint32 flags = 0;

		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = engine_beginscan_extended(node->ss.ss_currentRelation,
											   columnarScanState->snapshot,
											   0, NULL, NULL, flags,
											   columnarScanState->attrNeeded,
											   columnarScanState->qual,
											   columnarScanState->parallelColumnarScan,
											   vectorizationEnabled);

		node->ss.ss_currentScanDesc = scandesc;
	}

	/* 
	* Cleanup vector tuple table slot when reading next chunk
	 */
	if (vectorizationEnabled)
	{
		VectorTupleTableSlot *vectorSlot = (VectorTupleTableSlot *) slot;
		int attrIndex = -1;
		foreach_int(attrIndex, columnarScanState->vectorization.attrNeededList)
		{
			VectorColumn *column = (VectorColumn *) vectorSlot->tts.tts_values[attrIndex];
			memset(column->isnull, true, COLUMNAR_VECTOR_COLUMN_SIZE);
			column->dimension = 0;
		}
		vectorSlot->dimension = 0;
	}

	if (table_scan_getnextslot(scandesc, direction, slot))
	{
		/*
		* Setup custom scan state for reading tuples directly from stored
		* vector tuple table slot
		*/
		if (vectorizationEnabled)
		{
			columnarScanState->vectorization.vectorPendingRowNumber = 
				((VectorTupleTableSlot *) slot)->dimension;
			columnarScanState->vectorization.vectorRowIndex = 0;
		}
		return slot;
	}

	return NULL;
}


/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
ColumnarScanRecheck(ColumnarScanState *node, TupleTableSlot *slot)
{
	return true;
}


static TupleTableSlot *
ColumnarScan_ExecCustomScan(CustomScanState *node)
{
	return CustomExecScan((ColumnarScanState *) node,
						  (ExecScanAccessMtd) ColumnarScanNext,
						  (ExecScanRecheckMtd) ColumnarScanRecheck);
}


static void
ColumnarScan_EndCustomScan(CustomScanState *node)
{
	TableScanDesc scanDesc = node->ss.ss_currentScanDesc;
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;

	/*
	 * Cleanup BMS of selected scan attributes
	 */
	bms_free(((ColumnarScanState *)node)->attrNeeded);

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
	{
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	}
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
	{
		table_endscan(scanDesc);
	}

	/* Unregister snapshot */
	if (columnarScanState->snapshotRegisteredByUs)
	{
		UnregisterSnapshot(columnarScanState->snapshot);
	}
}


static void
ColumnarScan_ReScanCustomScan(CustomScanState *node)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;

	ResetExprContext(columnarScanState->css_RuntimeContext);
	List *allClauses = lsecond(cscan->custom_exprs);
	columnarScanState->qual = (List *) EvalParamsMutator(
		(Node *) allClauses, columnarScanState->css_RuntimeContext);

	TableScanDesc scanDesc = node->ss.ss_currentScanDesc;

	if (scanDesc != NULL)
	{
		/* XXX: hack to pass quals as scan keys */
		ScanKey scanKeys = (ScanKey) columnarScanState->qual;
		table_rescan(node->ss.ss_currentScanDesc,
					 scanKeys);
	}
}


static void
ColumnarScan_ExplainCustomScan(CustomScanState *node, List *ancestors,
							   ExplainState *es)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;

	List *context = set_deparse_context_planstate(
		es->deparse_cxt, (Node *) &node->ss.ps, ancestors);

	List *projectedColumns = ColumnarVarNeeded(columnarScanState);
	const char *projectedColumnsStr = ColumnarProjectedColumnsStr(
		context, projectedColumns);
	ExplainPropertyText("Engine Projected Columns",
						projectedColumnsStr, es);

	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	List *chunkGroupFilter = lsecond(cscan->custom_exprs);
	if (chunkGroupFilter != NULL)
	{
		const char *pushdownClausesStr = ColumnarPushdownClausesStr(
			context, chunkGroupFilter);
		ExplainPropertyText("Engine Chunk Group Filters",
							pushdownClausesStr, es);

		ColumnarScanDesc columnarScanDesc =
			(ColumnarScanDesc) node->ss.ss_currentScanDesc;
		if (columnarScanDesc != NULL)
		{
			ExplainPropertyInteger(
				"Engine Chunk Groups Removed by Filter",
				NULL, ColumnarScanChunkGroupsFiltered(columnarScanDesc), es);

			int64 stripped = ColumnarScanStripesSkipped(columnarScanDesc);
			if (stripped > 0)
				ExplainPropertyInteger(
					"Engine Stripes Removed by Pruning",
					NULL, stripped, es);
		}
	}

	if (columnarScanState->vectorization.vectorizationEnabled &&
		columnarScanState->vectorization.vectorizedQualList != NULL)
	{
		const char *vectorizedWhereClauses = ColumnarPushdownClausesStr(
					context, columnarScanState->vectorization.vectorizedQualList);
		ExplainPropertyText("Engine Vectorized Filter", 
							vectorizedWhereClauses, es);
	}

	if (columnarScanState->vectorization.vectorizationEnabled &&
		columnarScanState->vectorization.vectorizationAggregate)
	{
		ExplainPropertyText("Engine Vectorized Aggregate", "enabled", es);
	}

	if (engine_enable_page_cache)
	{
		ColumnarCacheStatistics *statistics = ColumnarGetCacheStatistics();

		ExplainPropertyUInteger(
			"Cache Hits",
			NULL,
			statistics->hits,
			es);

		ExplainPropertyUInteger(
			"Cache Misses",
			NULL,
			statistics->misses,
			es);

		ExplainPropertyUInteger(
			"Cache Evictions",
			NULL,
			statistics->evictions,
			es);

		ExplainPropertyUInteger(
			"Cache Writes",
			NULL,
			statistics->writes,
			es);

		ExplainPropertyUInteger(
			"Cache Maximum Size",
			NULL,
			statistics->maximumCacheSize,
			es);

		ExplainPropertyUInteger(
			"Cache Ending Size", 
			NULL, 
			statistics->endingCacheSize,
			es);

		ExplainPropertyUInteger(
			"Total Cache Entries",
			NULL,
			statistics->entries,
			es
		);
	}
}


/* Parallel Execution */

static Size 
Columnar_EstimateDSMCustomScan(CustomScanState *node,
							   ParallelContext *pcxt)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;
	Relation   rel = node->ss.ss_currentRelation;

	/* Count stripes so we can allocate exactly the right amount of DSM space */
	List	   *stripeList = StripesForRelfilenode(rel->rd_locator, ForwardScanDirection);
	int			stripeCount = list_length(stripeList);

	Size amSize = MAXALIGN(offsetof(ParallelColumnarScanData, stripeIds) +
						   (Size) stripeCount * sizeof(uint64));
	return MAXALIGN(add_size(amSize, EstimateSnapshotSpace(columnarScanState->snapshot)));
}


static void 
Columnar_InitializeDSMCustomScan(CustomScanState *node,
								 ParallelContext *pcxt,
								 void *coordinate)
{
	ParallelColumnarScan pscan = (ParallelColumnarScan) coordinate;
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;
	Relation   rel = node->ss.ss_currentRelation;

	/*
	 * Pre-fetch all stripe IDs for this table into the DSM so that workers
	 * can claim them atomically without catalog access in the hot path.
	 * This runs in the leader (single-threaded) before any workers start.
	 *
	 * Apply stripe-level pruning here so workers only scan stripes that can
	 * possibly satisfy the WHERE clauses.
	 */
	List	   *stripeList = StripesForRelfilenode(rel->rd_locator, ForwardScanDirection);

	/* Stripe-level pruning (parallel path) */
	if (columnarScanState->qual != NIL)
	{
		int64 skipped = 0;
		stripeList = ColumnarFilterStripes(rel, stripeList,
										  RelationGetDescr(rel),
										  columnarScanState->qual,
										  columnarScanState->snapshot,
										  &skipped);
		/*
		 * skipped is discarded here; the per-worker EXPLAIN counter will
		 * remain 0 for the parallel path (workers never call ColumnarBeginRead
		 * with stripe-level pruning — the pruning already happened here).
		 */
	}

	int			stripeCount = list_length(stripeList);
	int			i = 0;
	ListCell   *lc;

	foreach(lc, stripeList)
	{
		StripeMetadata *stripe = (StripeMetadata *) lfirst(lc);
		pscan->stripeIds[i++] = stripe->id;
	}
	pscan->stripeCount = stripeCount;
	pg_atomic_init_u64(&pscan->nextStripeIndex, 0);

	/*
	 * Serialize the snapshot right after the stripeIds array so workers
	 * can restore it with RestoreSnapshot() at the same offset.
	 */
	Size snapshotOff = MAXALIGN(offsetof(ParallelColumnarScanData, stripeIds) +
								(Size) stripeCount * sizeof(uint64));
	SerializeSnapshot(columnarScanState->snapshot, (char *) pscan + snapshotOff);

	if (parallel_leader_participation)
		columnarScanState->parallelColumnarScan = pscan;
	else
		columnarScanState->parallelColumnarScan = NULL;
}


static void 
Columnar_ReinitializeDSMCustomScan(CustomScanState *node,
								   ParallelContext *pcxt,
								   void *coordinate)
{
	ParallelColumnarScan pscan = (ParallelColumnarScan) coordinate;
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;

	/* Reset stripe index to restart from the first stripe */
	pg_atomic_write_u64(&pscan->nextStripeIndex, 0);

	if (parallel_leader_participation)
		columnarScanState->parallelColumnarScan = pscan;
	else
		columnarScanState->parallelColumnarScan = NULL;
}


static void 
Columnar_InitializeWorkerCustomScan(CustomScanState *node,
									shm_toc *toc,
									void *coordinate)
{
	ColumnarScanState *columnarScanState = (ColumnarScanState *) node;
	ParallelColumnarScan pscan = (ParallelColumnarScan) coordinate;

	/*
	 * Restore snapshot from the offset right after the stripeIds[] array.
	 * This matches where Columnar_InitializeDSMCustomScan serialised it.
	 */
	Size snapshotOff = MAXALIGN(offsetof(ParallelColumnarScanData, stripeIds) +
								(Size) pscan->stripeCount * sizeof(uint64));
	columnarScanState->snapshot = RestoreSnapshot((char *) pscan + snapshotOff);
	columnarScanState->parallelColumnarScan = pscan;
}


/*
 * ColumnarPushdownClausesStr represents the clauses to push down as a string.
 */
static const char *
ColumnarPushdownClausesStr(List *context, List *clauses)
{
	Expr *conjunction;

	Assert(list_length(clauses) > 0);

	if (list_length(clauses) == 1)
	{
		conjunction = (Expr *) linitial(clauses);
	}
	else
	{
		conjunction = make_andclause(clauses);
	}

	bool useTableNamePrefix = false;
	bool showImplicitCast = false;
	return deparse_expression((Node *) conjunction, context,
							  useTableNamePrefix, showImplicitCast);
}


/*
 * ColumnarProjectedColumnsStr generates projected column string for
 * explain output.
 */
static const char *
ColumnarProjectedColumnsStr(List *context, List *projectedColumns)
{
	if (list_length(projectedColumns) == 0)
	{
		return "<columnar optimized out all columns>";
	}

	bool useTableNamePrefix = false;
	bool showImplicitCast = false;
	return deparse_expression((Node *) projectedColumns, context,
							  useTableNamePrefix, showImplicitCast);
}


/*
 * ColumnarVarNeeded returns a list of Var objects for the ones that are
 * needed during columnar custom scan.
 * Throws an error if finds a Var referencing to an attribute not supported
 * by ColumnarScan.
 */
static List *
ColumnarVarNeeded(ColumnarScanState *columnarScanState)
{
	ScanState *scanState = &columnarScanState->custom_scanstate.ss;

	List *varList = NIL;

	Bitmapset *neededAttrSet =
		ColumnarAttrNeeded(scanState, columnarScanState->vectorization.vectorizedQualList);

	int bmsMember = -1;
	while ((bmsMember = bms_next_member(neededAttrSet, bmsMember)) >= 0)
	{
		Relation columnarRelation = scanState->ss_currentRelation;

		/* neededAttrSet already represents 0-indexed attribute numbers */
		Form_pg_attribute columnForm =
			TupleDescAttr(RelationGetDescr(columnarRelation), bmsMember);
		if (columnForm->attisdropped)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
							errmsg("cannot explain column with attrNum=%d "
								   "of columnar table %s since it is dropped",
								   bmsMember + 1,
								   RelationGetRelationName(columnarRelation))));
		}
		else if (columnForm->attnum <= 0)
		{
			/*
			 * ColumnarAttrNeeded should have already thrown an error for
			 * system columns. Similarly, it should have already expanded
			 * whole-row references to individual attributes.
			 */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot explain column with attrNum=%d "
								   "of columnar table %s since it is either "
								   "a system column or a whole-row "
								   "reference", columnForm->attnum,
								   RelationGetRelationName(columnarRelation))));
		}


		/*
		 * varlevelsup is used to figure out the (query) level of the Var
		 * that we are investigating. Since we are dealing with a particular
		 * relation, it is useless here.
		 */
		Index varlevelsup = 0;

		CustomScanState *customScanState = (CustomScanState *) columnarScanState;
		CustomScan *customScan = (CustomScan *) customScanState->ss.ps.plan;
		Index scanrelid = customScan->scan.scanrelid;
		Var *var = makeVar(scanrelid, columnForm->attnum, columnForm->atttypid,
						   columnForm->atttypmod, columnForm->attcollation,
						   varlevelsup);
		varList = lappend(varList, var);
	}

	return varList;
}


#if PG_VERSION_NUM >= 130000

/*
 * set_deparse_context_planstate is a compatibility wrapper for versions 13+.
 */
static List *
set_deparse_context_planstate(List *dpcontext, Node *node, List *ancestors)
{
	PlanState *ps = (PlanState *) node;
	return set_deparse_context_plan(dpcontext, ps->plan, ancestors);
}


#endif


/* ================================================================
 * RowcompressScan custom scan node
 *
 * A lightweight custom scan that wraps the standard rowcompress sequential
 * scan and adds two performance features:
 *
 *  1. Batch-level min/max pruning:  batches whose stored [min, max] range is
 *     refuted by the pushed-down WHERE clauses are skipped entirely without
 *     decompressing.
 *
 *  2. Async read-ahead:  before decompressing the current batch the scan
 *     issues a PrefetchBuffer() hint for the next batch, overlapping I/O
 *     with CPU-side decompression.
 *
 * Both features are implemented inside the rowcompress TAM
 * (rowcompress_getnextslot) once rowcompress_set_pushdown_clauses() has been
 * called on the TableScanDesc.  This custom node's only job is to (a) build
 * the path/plan with the right clauses, (b) call set_pushdown_clauses at
 * begin time, and (c) report pruning stats in EXPLAIN.
 * ================================================================ */

/*
 * RCAddScanPath — create and add a RowcompressScan CustomPath for a rowcompress
 * relation.  The path cost mirrors the plain SeqScan cost (we cannot predict
 * pruning at plan time).  The planner will still prefer this path over the
 * plain SeqScan because we call add_path() which respects dominance.
 */
static void
RCAddScanPath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Build a reference SeqScan path to copy cost estimates from */
	Path *seqPath = create_seqscan_path(root, rel, rel->lateral_relids, 0);

	/* Collect plain clause expressions from baserestrictinfo */
	List *plainClauses = NIL;
	ListCell *lc;
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		plainClauses = lappend(plainClauses, rinfo->clause);
	}

	CustomPath *cpath = makeNode(CustomPath);
	cpath->path.pathtype    = T_CustomScan;
	cpath->path.parent      = rel;
	cpath->path.pathtarget  = rel->reltarget;
	cpath->path.param_info  = NULL;
	cpath->path.parallel_aware    = false;
	cpath->path.parallel_safe     = rel->consider_parallel;
	cpath->path.parallel_workers  = 0;
	cpath->path.rows         = seqPath->rows;
	cpath->path.startup_cost = seqPath->startup_cost;
	cpath->path.total_cost   = seqPath->total_cost;
	cpath->flags             = 0;
	cpath->custom_paths      = NIL;
	cpath->custom_private    = list_make1(plainClauses);
	cpath->methods           = &RowcompressScanPathMethods;

	add_path(rel, (Path *) cpath);
}

static Plan *
RCScan_PlanCustomPath(PlannerInfo *root, RelOptInfo *rel,
					  struct CustomPath *best_path,
					  List *tlist, List *clauses,
					  List *custom_plans)
{
	List *plainClauses = (List *) linitial(best_path->custom_private);

	CustomScan *cscan = makeNode(CustomScan);
	cscan->scan.scanrelid = best_path->path.parent->relid;
	cscan->scan.plan.targetlist = tlist;
	/*
	 * Per-row qual: executor checks these after the TAM returns each row.
	 * Batch-level pruning (inside the TAM) may have already eliminated whole
	 * batches, so many of these checks will never execute in practice.
	 */
	cscan->scan.plan.qual = extract_actual_clauses(clauses, false);
	cscan->custom_exprs   = list_make1(plainClauses);
	cscan->custom_private = NIL;
	cscan->custom_scan_tlist = NIL;
	cscan->methods        = &RowcompressScanScanMethods;

	return (Plan *) cscan;
}

static Node *
RCScan_CreateCustomScanState(CustomScan *cscan)
{
	RowcompressScanState *state = (RowcompressScanState *)
		newNode(sizeof(RowcompressScanState), T_CustomScanState);
	state->css.methods = &RowcompressScanExecuteMethods;
	return (Node *) state;
}

static void
RCScan_BeginCustomScan(CustomScanState *node, EState *estate, int eflags)
{
	RowcompressScanState *state = (RowcompressScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Relation rel = node->ss.ss_currentRelation;

	/* Retrieve the plain clause list stored at plan time */
	state->pushdownClauses = (List *) linitial(cscan->custom_exprs);

	/* Handle snapshot: use estate snapshot, or register a fresh one */
	state->snapshot = estate->es_snapshot;
	state->snapshotRegisteredByUs = false;

	/* Open the sequential scan */
#if PG_VERSION_NUM >= PG_VERSION_19
	state->scanDesc = table_beginscan(rel, state->snapshot, 0, NULL, 0);
#else
	state->scanDesc = table_beginscan(rel, state->snapshot, 0, NULL);
#endif

	/* Push WHERE clauses into the TAM for batch-level min/max pruning */
	if (state->pushdownClauses != NIL)
	{
		rowcompress_set_pushdown_clauses(state->scanDesc,
										 state->pushdownClauses,
										 RelationGetDescr(rel));
	}
}

static TupleTableSlot *
RCScan_Next(ScanState *node)
{
	RowcompressScanState *state = (RowcompressScanState *) node;
	TupleTableSlot *slot = node->ss_ScanTupleSlot;

	if (!table_scan_getnextslot(state->scanDesc, ForwardScanDirection, slot))
		ExecClearTuple(slot);

	return slot;
}

static bool
RCScan_Recheck(ScanState *node, TupleTableSlot *slot)
{
	/* All row-level predicates are evaluated by ExecScan's qual loop */
	return true;
}

static TupleTableSlot *
RCScan_ExecCustomScan(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) RCScan_Next,
					(ExecScanRecheckMtd) RCScan_Recheck);
}

static void
RCScan_EndCustomScan(CustomScanState *node)
{
	RowcompressScanState *state = (RowcompressScanState *) node;

	if (state->scanDesc)
		table_endscan(state->scanDesc);

	if (state->snapshotRegisteredByUs)
		UnregisterSnapshot(state->snapshot);
}

static void
RCScan_ReScanCustomScan(CustomScanState *node)
{
	RowcompressScanState *state = (RowcompressScanState *) node;

	if (state->scanDesc)
	{
		table_rescan(state->scanDesc, NULL);

		/* Re-install the pushdown clauses after rescan */
		if (state->pushdownClauses != NIL)
		{
			Relation rel = node->ss.ss_currentRelation;
			rowcompress_set_pushdown_clauses(state->scanDesc,
											 state->pushdownClauses,
											 RelationGetDescr(rel));
		}
	}
}

static void
RCScan_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es)
{
	RowcompressScanState *state = (RowcompressScanState *) node;

	if (state->scanDesc != NULL)
	{
		int64 pruned = rowcompress_get_batches_pruned(state->scanDesc);
		ExplainPropertyInteger("Batches Pruned", NULL, pruned, es);
	}

#if PG_VERSION_NUM >= 130000
	if (state->pushdownClauses != NIL && es->verbose)
	{
		/* Show which clauses are being pushed down for batch pruning */
		List *context = set_deparse_context_planstate(
			es->deparse_cxt,
			(Node *) node,
			ancestors);
		const char *clsStr = ColumnarPushdownClausesStr(context,
														state->pushdownClauses);
		ExplainPropertyText("Batch Pruning Clauses", clsStr, es);
	}
#endif
}

