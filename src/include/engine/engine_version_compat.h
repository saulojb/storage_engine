/*-------------------------------------------------------------------------
 *
 * engine_version_compat.h
 *
 *  Compatibility macros for writing code agnostic to PostgreSQL versions
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ENGINE_COMPAT_H
#define ENGINE_COMPAT_H

#include "pg_version_constants.h"

#if PG_VERSION_NUM >= PG_VERSION_14
#define ColumnarProcessUtility_compat(a, b, c, d, e, f, g, h) \
	ColumnarProcessUtility(a, b, c, d, e, f, g, h)
#define PrevProcessUtilityHook_compat(a, b, c, d, e, f, g, h) \
	PrevProcessUtilityHook(a, b, c, d, e, f, g, h)
#define GetOldestNonRemovableTransactionId_compat(a, b) \
	GetOldestNonRemovableTransactionId(a)
#define ExecSimpleRelationInsert_compat(a, b, c) \
	ExecSimpleRelationInsert(a, b, c)
#define index_insert_compat(a, b, c, d, e, f, g, h) \
	index_insert(a, b, c, d, e, f, g, h)
#else
#define ColumnarProcessUtility_compat(a, b, c, d, e, f, g, h) \
	ColumnarProcessUtility(a, b, d, e, f, g, h)
#define PrevProcessUtilityHook_compat(a, b, c, d, e, f, g, h) \
	PrevProcessUtilityHook(a, b, d, e, f, g, h)
#define GetOldestNonRemovableTransactionId_compat(a, b) GetOldestXmin(a, b)
#define ExecSimpleRelationInsert_compat(a, b, c) \
	ExecSimpleRelationInsert(b, c)
#define index_insert_compat(a, b, c, d, e, f, g, h) \
	index_insert(a, b, c, d, e, f, h)
#endif

#define ACLCHECK_OBJECT_TABLE OBJECT_TABLE

#define ExplainPropertyLong(qlabel, value, es) \
	ExplainPropertyInteger(qlabel, NULL, value, es)

#if PG_VERSION_NUM < 130000
#define detoast_attr(X) heap_tuple_untoast_attr(X)
#endif

/*
 * ExecFreeExprContext was removed in PG17. Use FreeExprContext directly
 * on the ps_ExprContext field.
 */
#if PG_VERSION_NUM >= PG_VERSION_17
#define ExecFreeExprContext(planstate) \
	FreeExprContext((planstate)->ps_ExprContext, true)
#endif

#endif /* ENGINE_COMPAT_H */
