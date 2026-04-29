/*-------------------------------------------------------------------------
 *
 * engine_groupagg_node.h
 *      Vectorized GROUP BY aggregation node for colcompress tables.
 *
 *      Handles low-to-medium cardinality GROUP BY queries by pulling
 *      VectorColumn batches directly from ColcompressScan and accumulating
 *      per-group results in a HTAB, then emitting one result tuple per group.
 *
 *      Supported aggregates: count(*), sum, min, max for int4/int8/float8.
 *      Single GROUP BY key only (no composite keys).
 *
 *-------------------------------------------------------------------------
 */

#ifndef ENGINE_GROUPAGG_NODE_H
#define ENGINE_GROUPAGG_NODE_H

#include "postgres.h"
#include "nodes/execnodes.h"
#include "utils/hsearch.h"

/* Maximum groups before falling back to HashAggregate */
#define VECGROUPAGG_MAX_GROUPS 16384

/*
 * Aggregate type codes used in VecGroupAggTarget.
 */
#define VECGAGG_COUNT_STAR  1   /* count(*) */
#define VECGAGG_SUM         2   /* sum(col) */
#define VECGAGG_MIN         3   /* min(col) */
#define VECGAGG_MAX         4   /* max(col) */

/*
 * Column type codes used in VecGroupAggTarget.
 */
#define VECGAGG_TYPE_INT4   1
#define VECGAGG_TYPE_INT8   2
#define VECGAGG_TYPE_FLOAT8 3

/*
 * Describes one aggregate target in the GROUP BY query.
 */
typedef struct VecGroupAggTarget
{
	int		agg_kind;		/* VECGAGG_COUNT_STAR / SUM / MIN / MAX */
	int		col_type;		/* VECGAGG_TYPE_INT4/INT8/FLOAT8 */
	int		col_attnum;		/* 1-based attnum in scan tuple (0 = unused) */
	int		result_attnum;	/* 0-based position in result tuple */
	Oid		result_typeoid;	/* SQL return type OID (e.g. NUMERICOID for sum(int8)) */
} VecGroupAggTarget;

/*
 * Per-group accumulator entry stored in the HTAB.
 * Supports up to 8 aggregate targets per query.
 */
#define VECGROUPAGG_MAX_TARGETS 8

typedef struct VecGroupEntry
{
	Datum		key;					/* group key value (hash key) */
	bool		key_isnull;
	int64		int64_acc[VECGROUPAGG_MAX_TARGETS];
	float8		float8_acc[VECGROUPAGG_MAX_TARGETS];
	bool		acc_isnull[VECGROUPAGG_MAX_TARGETS]; /* NULL if no non-null input */
} VecGroupEntry;

/*
 * State for the VectorGroupAgg custom scan node.
 */
typedef struct VecGroupAggState
{
	CustomScanState		css;

	/* Scan info */
	int					key_attnum;		/* 1-based attnum of GROUP BY column */
	int					key_col_type;	/* VECGAGG_TYPE_* */
	Oid					key_typeoid;

	/* Aggregate targets */
	int					num_targets;
	VecGroupAggTarget	targets[VECGROUPAGG_MAX_TARGETS];

	/* Per-group hash table */
	HTAB			   *group_htab;
	int					num_groups;

	/* 0-based position of the GROUP BY key in the output result tuple */
	int					key_result_attnum;

	/*
	 * sort_output: when true (AGG_SORTED plans), emit groups in ascending
	 * key order instead of hash order to satisfy an ORDER BY clause.
	 */
	bool				sort_output;

	/* sorted emission: built after phase 1 when sort_output=true */
	VecGroupEntry	  **sorted_arr;		/* palloc'd array of entry pointers */
	int					sorted_idx;		/* next index to emit */

	/* Result emission state */
	bool				scan_done;
	HASH_SEQ_STATUS		hash_seq;
	bool				seq_started;

	/* Memory context for accumulator data */
	MemoryContext		agg_context;
} VecGroupAggState;

extern CustomScan *engine_create_groupagg_node(int key_attnum,
											   Oid key_typeoid,
											   int key_result_att,
											   bool sort_output,
											   int num_targets,
											   VecGroupAggTarget *targets);
extern bool engine_is_groupagg_node(Plan *plan);
extern void engine_groupagg_enable_sort_output(CustomScan *cscan);
extern void engine_register_groupagg_node(void);

#endif /* ENGINE_GROUPAGG_NODE_H */
