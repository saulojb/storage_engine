/*-------------------------------------------------------------------------
 *
 * engine_groupagg_node.h
 *      Vectorized GROUP BY aggregation node for colcompress tables.
 *
 *      Handles low-to-medium cardinality GROUP BY queries by pulling
 *      VectorColumn batches directly from ColcompressScan and accumulating
 *      per-group results in a HTAB, then emitting one result tuple per group.
 *
 *      Supported aggregates: count(*), sum, min, max, avg for int4/int8/float4/float8.
 *      Supported GROUP BY key types: int4/int8/float4/float8/bpchar/text.
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
#define VECGAGG_AVG         5   /* avg(col) */
#define VECGAGG_COUNT_COL   6   /* count(col) — NULL-aware */

/*
 * Column type codes used in VecGroupAggTarget.
 */
#define VECGAGG_TYPE_INT4   1
#define VECGAGG_TYPE_INT8   2
#define VECGAGG_TYPE_FLOAT4 3
#define VECGAGG_TYPE_FLOAT8 4
#define VECGAGG_TYPE_NUMERIC 5
#define VECGAGG_TYPE_BPCHAR 6
#define VECGAGG_TYPE_TEXT   7

/*
 * Describes one aggregate target in the GROUP BY query.
 */
typedef struct VecGroupAggTarget
{
	int		agg_kind;		/* VECGAGG_COUNT_STAR / SUM / MIN / MAX / AVG */
	int		col_type;		/* VECGAGG_TYPE_INT4/INT8/FLOAT4/FLOAT8 */
	int		col_attnum;		/* 0-based slot output position (-1 = unused, e.g. count(*)) */
	int		result_attnum;	/* 0-based position in result tuple */
	bool	avg_input_as_float8; /* avg(var::float8): accumulate as float8 transition */
	bool	use_int8_avg_path;  /* true for avg(int/bigint) partial: use aggtransfn/int8_avg_serialize */
	Oid		avg_transfn_oid;    /* pg_aggregate.aggtransfn OID when use_int8_avg_path */
	Oid		result_typeoid;	/* SQL return type OID (e.g. NUMERICOID for sum(int8)) */

	/*
	 * CASE WHEN col = const THEN val END conditional filter.
	 * When has_case_filter is true, a row is accumulated only when the
	 * filter column equals filter_eq_value.  Otherwise val is treated as NULL.
	 * Only supported for SUM / MIN / MAX.
	 */
	bool	has_case_filter;      /* true: SUM(CASE WHEN col=const THEN val END) */
	int		filter_col_attnum;    /* 0-based slot pos of filter column */
	int		filter_col_type;      /* VECGAGG_TYPE_* of filter column */
	Datum	filter_eq_value;      /* stable copy of the constant (runtime) */
	Oid		filter_eq_typeoid;    /* type OID of the filter column */
	int16	filter_typlen;        /* type length for datumCopy */
	bool	filter_typbyval;      /* by-value type? */
	/* Plan-time only: Const node used during serialization. NULL at runtime. */
	struct Const *filter_const_plan;
} VecGroupAggTarget;

/* Maximum composite GROUP BY keys */
#define VECGROUPAGG_MAX_KEYS    4

/*
 * HTAB lookup key for composite GROUP BY entries.
 * Used with HASH_FUNCTION + HASH_COMPARE for custom hashing.
 */
typedef struct VecGroupKey
{
	int		num_keys;
	int		key_type[VECGROUPAGG_MAX_KEYS];	/* VECGAGG_TYPE_* per key */
	bool	isnull[VECGROUPAGG_MAX_KEYS];
	Datum	key[VECGROUPAGG_MAX_KEYS];		/* numeric key value (by-value types) */
	int16	text_len[VECGROUPAGG_MAX_KEYS];	/* byte length for BPCHAR/TEXT keys */
	char	text_key[VECGROUPAGG_MAX_KEYS][16]; /* inline text storage */
} VecGroupKey;

/*
 * Per-group accumulator entry stored in the HTAB.
 * Supports up to 8 aggregate targets per query.
 */
#define VECGROUPAGG_MAX_TARGETS 8

typedef struct VecGroupEntry
{
	VecGroupKey k;					/* MUST BE FIRST: HTAB HASH_BLOBS compares
								 * first sizeof(VecGroupKey) bytes of entry */
	int64		int64_acc[VECGROUPAGG_MAX_TARGETS];
	int64		avg_count_acc[VECGROUPAGG_MAX_TARGETS];
	float8		float8_acc[VECGROUPAGG_MAX_TARGETS];
	Datum		numeric_acc[VECGROUPAGG_MAX_TARGETS];
	Datum		numeric_state_acc[VECGROUPAGG_MAX_TARGETS];
	bool		acc_isnull[VECGROUPAGG_MAX_TARGETS]; /* NULL if no non-null input */
} VecGroupEntry;

/*
 * State for the VectorGroupAgg custom scan node.
 */
typedef struct VecGroupAggState
{
	CustomScanState		css;

	/* Scan info — composite GROUP BY (up to VECGROUPAGG_MAX_KEYS) */
	int					num_keys;
	int					key_attnum[VECGROUPAGG_MAX_KEYS];  /* 0-based slot pos per key */
	bool				key_is_const[VECGROUPAGG_MAX_KEYS];
	Datum				key_const[VECGROUPAGG_MAX_KEYS];
	bool				key_const_isnull[VECGROUPAGG_MAX_KEYS];
	int					key_col_type[VECGROUPAGG_MAX_KEYS];	/* VECGAGG_TYPE_* */
	Oid					key_typeoid[VECGROUPAGG_MAX_KEYS];
	int16				key_typlen[VECGROUPAGG_MAX_KEYS];
	bool				key_typbyval[VECGROUPAGG_MAX_KEYS];

	/* Aggregate targets */
	int					num_targets;
	VecGroupAggTarget	targets[VECGROUPAGG_MAX_TARGETS];

	/* Per-group hash table */
	HTAB			   *group_htab;
	int					num_groups;

	/* 0-based position of each GROUP BY key in the output result tuple */
	int					key_result_attnum[VECGROUPAGG_MAX_KEYS];

	/*
	 * sort_output: when true (AGG_SORTED plans), emit groups in ascending
	 * key order instead of hash order to satisfy an ORDER BY clause.
	 */
	bool				sort_output;

	/* sorted emission: built after phase 1 when sort_output=true */
	VecGroupEntry	  **sorted_arr;		/* palloc'd array of entry pointers */
	int					sorted_idx;		/* next index to emit */
	bool				is_partial_serial; /* AGGSPLIT_INITIAL_SERIAL */

	/* Result emission state */
	bool				scan_done;
	HASH_SEQ_STATUS		hash_seq;
	bool				seq_started;

	/* Memory context for accumulator data */
	MemoryContext		agg_context;
	ExprContext			numeric_fake_aggexpr;
	AggState			numeric_fake_aggstate;

	/* Cached numeric transition/final functions for numeric aggregate paths */
	FmgrInfo		numeric_add_fmgr;
	FmgrInfo		numeric_cmp_fmgr;
	FmgrInfo		numeric_avg_accum_fmgr;
	FmgrInfo		numeric_avg_serialize_fmgr;
	FmgrInfo		numeric_avg_fmgr;
	FmgrInfo		numeric_sum_fmgr;
	bool			numeric_fmgr_ready;

	/* Per-target transition function fmgr for avg(int/bigint) partial path */
	FmgrInfo		avg_transfn_fmgr[VECGROUPAGG_MAX_TARGETS];
	FmgrInfo		int8_avg_serialize_fmgr;
	bool			int8_avg_serialize_ready;
} VecGroupAggState;

extern CustomScan *engine_create_groupagg_node(int num_keys,
											   int key_attnums[],
											   Oid key_typeoids[],
											   bool key_is_consts[],
											   Const *key_consts[],
											   int key_result_atts[],
											   bool sort_output,
											   int aggsplit_mode,
											   int num_targets,
											   VecGroupAggTarget *targets);
extern bool engine_is_groupagg_node(Plan *plan);
extern void engine_groupagg_enable_sort_output(CustomScan *cscan);
extern void engine_register_groupagg_node(void);

#endif /* ENGINE_GROUPAGG_NODE_H */
