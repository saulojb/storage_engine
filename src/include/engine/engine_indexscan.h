/*-------------------------------------------------------------------------
 *
 * engine_indexscan.h
 *	Custom scan method for index
 * 
 * IDENTIFICATION
 *	src/backend/columnar/engine_indexscan.c
 *
 *-------------------------------------------------------------------------
 */


#ifndef COLUMNAR_INDEXSCAN_H
#define COLUMNAR_INDEXSCAN_H

#include "postgres.h"

#include "nodes/execnodes.h"

typedef struct ColumnarIndexScanState
{
	CustomScanState css;
	IndexScanState *indexscan_state;
} ColumnarIndexScanState;

extern CustomScan * engine_create_indexscan_node(void);
extern void engine_register_indexscan_node(void);

#endif