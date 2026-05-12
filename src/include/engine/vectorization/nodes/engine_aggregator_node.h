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

	/* Accumulators for ExecVecSumExpr */
	float8			sumFloat8;
	Numeric			sumNumeric;
	bool			sumHasValue;
	bool			done;

	/* Memory context for accumulation (numeric datums) */
	MemoryContext	aggContext;
} VectorAggState;

extern CustomScan *engine_create_aggregator_node(void);
extern void engine_register_aggregator_node(void);

#endif
