/*-------------------------------------------------------------------------
 *
 * engine_writer.c
 *
 * This file contains function definitions for writing columnar tables. This
 * includes the logic for writing file level metadata, writing row stripes,
 * and calculating chunk skip nodes.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "pg_version_compat.h"

#include "postgres.h"

#include "safe_lib.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"

#if PG_VERSION_NUM >= PG_VERSION_16
#include "utils/relfilenumbermap.h"
#else
#include "utils/relfilenodemap.h"
#endif


#include "engine/engine.h"
#include "engine/engine_storage.h"
#include "engine/engine_version_compat.h"

#include "catalog/pg_collation.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"

/*
 * One sort key parsed from options.orderby ("col [ASC|DESC] [NULLS ...]").
 */
typedef struct ColSortKey
{
	AttrNumber	attno;		/* 1-based attribute number */
	int			colIdx;		/* 0-based column index (attno - 1) */
	bool		descending;
	bool		nullsFirst;
	Oid			collation;
} ColSortKey;

/*
 * One row buffered for sort-on-write (allocated in sortBufferContext).
 */
typedef struct SortBufferRow
{
	Datum	   *values;		/* deep copy of Datum array */
	bool	   *nulls;		/* copy of nulls array */
} SortBufferRow;

struct ColumnarWriteState
{
	TupleDesc tupleDescriptor;
	FmgrInfo **comparisonFunctionArray;
	RelFileLocator relfilelocator;

	MemoryContext stripeWriteContext;
	MemoryContext sortBufferContext;	/* dedicated context for sortRows; reset after each flush */
	MemoryContext perTupleContext;
	StripeBuffers *stripeBuffers;
	StripeSkipList *stripeSkipList;
	EmptyStripeReservation *emptyStripeReservation;
	ColumnarOptions options;
	ChunkData *chunkData;

	List *chunkGroupRowCounts;

	/*
	 * compressionBuffer buffer is used as temporary storage during
	 * data value compression operation. It is kept here to minimize
	 * memory allocations. It lives in stripeWriteContext and gets
	 * deallocated when memory context is reset.
	 */
	StringInfo compressionBuffer;

	/*
	 * Sort-on-write support (MergeTree-like write ordering).
	 * Rows are buffered in sortRows (allocated in sortBufferContext, NOT
	 * stripeWriteContext) and sorted at flush time using qsort +
	 * comparisonFunctionArray. sortNkeys == 0 means no sorting is active.
	 *
	 * IMPORTANT: sortBufferContext is reset AFTER drain completes, so
	 * arr[i]->values remain valid throughout the entire drain loop even
	 * when stripeWriteContext is reset between stripes.
	 */
	int			sortNkeys;
	ColSortKey *sortKeys;	/* palloc'd in WriteStateContext, never reset */
	List	   *sortRows;	/* list of SortBufferRow*, in sortBufferContext */
};

static StripeBuffers * CreateEmptyStripeBuffers(uint32 stripeMaxRowCount,
												uint32 chunkRowCount,
												uint32 columnCount);
static StripeSkipList * CreateEmptyStripeSkipList(uint32 stripeMaxRowCount,
												  uint32 chunkRowCount,
												  uint32 columnCount);
static void FlushStripe(ColumnarWriteState *writeState);
static uint64 ColumnarWriteRowInternal(ColumnarWriteState *writeState,
									   Datum *columnValues, bool *columnNulls);
static void ColumnarInitSortKeys(ColumnarWriteState *writeState);
static int  CompareSortBufferRows(const void *a, const void *b);

/* Module-level context pointer for qsort comparator (single-threaded PG) */
static ColumnarWriteState *CurrentSortWriteState = NULL;
static StringInfo SerializeBoolArray(bool *boolArray, uint32 boolArrayLength);
static void SerializeSingleDatum(StringInfo datumBuffer, Datum datum,
								 bool datumTypeByValue, int datumTypeLength,
								 char datumTypeAlign);
static void SerializeChunkData(ColumnarWriteState *writeState, uint32 chunkIndex,
							   uint32 rowCount);
static void UpdateChunkSkipNodeMinMax(ColumnChunkSkipNode *chunkSkipNode,
									  Datum columnValue, bool columnTypeByValue,
									  int columnTypeLength, Oid columnCollation,
									  FmgrInfo *comparisonFunction);
static Datum DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength);
static StringInfo CopyStringInfo(StringInfo sourceString);

/*
 * ColumnarBeginWrite initializes a columnar data load operation and returns a table
 * handle. This handle should be used for adding the row values and finishing the
 * data load operation.
 */
ColumnarWriteState *
ColumnarBeginWrite(RelFileLocator relfilelocator,
				   ColumnarOptions options,
				   TupleDesc tupleDescriptor)
{
	/* get comparison function pointers for each of the columns */
	uint32 columnCount = tupleDescriptor->natts;
	FmgrInfo **comparisonFunctionArray = palloc0(columnCount * sizeof(FmgrInfo *));
	for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		FmgrInfo *comparisonFunction = NULL;
		FormData_pg_attribute *attributeForm = TupleDescAttr(tupleDescriptor,
															 columnIndex);

		if (!attributeForm->attisdropped)
		{
			Oid typeId = attributeForm->atttypid;

			comparisonFunction = GetFunctionInfoOrNull(typeId, BTREE_AM_OID,
													   BTORDER_PROC);
		}

		comparisonFunctionArray[columnIndex] = comparisonFunction;
	}

	/*
	 * We allocate all stripe specific data in the stripeWriteContext, and
	 * reset this memory context once we have flushed the stripe to the file.
	 * This is to avoid memory leaks.
	 */
	MemoryContext stripeWriteContext = AllocSetContextCreate(CurrentMemoryContext,
															 "Stripe Write Memory Context",
															 ALLOCSET_DEFAULT_SIZES);

	bool *columnMaskArray = palloc(columnCount * sizeof(bool));
	memset(columnMaskArray, true, columnCount * sizeof(bool));

	ChunkData *chunkData = CreateEmptyChunkData(columnCount, columnMaskArray,
												options.chunkRowCount);

	ColumnarWriteState *writeState = palloc0(sizeof(ColumnarWriteState));
	writeState->relfilelocator = relfilelocator;
	writeState->options = options;
	writeState->tupleDescriptor = CreateTupleDescCopy(tupleDescriptor);
	writeState->comparisonFunctionArray = comparisonFunctionArray;
	writeState->stripeBuffers = NULL;
	writeState->stripeSkipList = NULL;
	writeState->emptyStripeReservation = NULL;
	writeState->stripeWriteContext = stripeWriteContext;
	writeState->chunkData = chunkData;
	writeState->compressionBuffer = NULL;
	writeState->perTupleContext = AllocSetContextCreate(CurrentMemoryContext,
														"Engine per tuple context",
														ALLOCSET_DEFAULT_SIZES);
	writeState->sortBufferContext = AllocSetContextCreate(CurrentMemoryContext,
														  "Engine Sort Buffer Context",
														  ALLOCSET_DEFAULT_SIZES);

	/* initialise sort-on-write if the table has an orderby option */
	writeState->sortNkeys = 0;
	writeState->sortKeys  = NULL;
	writeState->sortRows  = NIL;
	if (options.orderby != NULL && options.orderby[0] != '\0')
		ColumnarInitSortKeys(writeState);

	return writeState;
}

/*
 * ColumnarWriteRow adds a row to the columnar table. If the stripe is not initialized,
 * we create structures to hold stripe data and skip list. Then, we serialize and
 * append data to serialized value buffer for each of the columns and update
 * corresponding skip nodes. Then, whole chunk data is compressed at every
 * rowChunkCount insertion. Then, if row count exceeds stripeMaxRowCount, we flush
 * the stripe, and add its metadata to the table footer.
 *
 * Returns the "row number" assigned to written row.
 */
uint64
ColumnarWriteRow(ColumnarWriteState *writeState, Datum *columnValues, bool *columnNulls)
{
	/*
	 * When sort-on-write is active, buffer the row in stripeWriteContext so
	 * it survives the per-tuple context reset.  The rows are sorted and
	 * written to StripeBuffers during ColumnarFlushPendingWrites() at the
	 * end of the statement (or when the stripe fills up).
	 *
	 * We return ENGINE_FIRST_ROW_NUMBER as a placeholder — callers that
	 * require precise TIDs (e.g. RETURNING) are not compatible with sort
	 * ordering, but columnar tables have no secondary indexes so this is
	 * acceptable in practice.
	 */
	if (writeState->sortNkeys > 0)
	{
		uint32	colCount = writeState->tupleDescriptor->natts;

		/*
		 * Allocate the row in sortBufferContext (NOT stripeWriteContext).
		 * This context is NOT reset when a stripe is flushed, so pointers
		 * in arr[] remain valid throughout the entire drain loop.
		 */
		MemoryContext oldCtx =
			MemoryContextSwitchTo(writeState->sortBufferContext);

		SortBufferRow *row = palloc(sizeof(SortBufferRow));
		row->values = palloc(colCount * sizeof(Datum));
		row->nulls  = palloc(colCount * sizeof(bool));

		for (uint32 i = 0; i < colCount; i++)
		{
			if (columnNulls[i])
			{
				row->nulls[i]  = true;
				row->values[i] = (Datum) 0;
			}
			else
			{
				Form_pg_attribute attr =
					TupleDescAttr(writeState->tupleDescriptor, i);
				row->nulls[i]  = false;
				/* deep-copy pass-by-reference values */
				row->values[i] = datumCopy(columnValues[i],
										   attr->attbyval,
										   attr->attlen);
			}
		}

		writeState->sortRows = lappend(writeState->sortRows, row);
		MemoryContextSwitchTo(oldCtx);

		/*
		 * Flush sort buffer when it reaches stripe_row_limit to bound
		 * memory usage and produce incremental sorted stripes.
		 * Each flushed stripe is independently sorted; colcompress_merge
		 * can later produce a globally sorted result.
		 */
		if (list_length(writeState->sortRows) >=
				(int) writeState->options.stripeRowCount)
			ColumnarFlushPendingWrites(writeState);

		return ENGINE_FIRST_ROW_NUMBER; /* placeholder */
	}

	return ColumnarWriteRowInternal(writeState, columnValues, columnNulls);
}


/*
 * ColumnarWriteRowInternal is the core write path: serialises one row into the
 * current StripeBuffers, flushing when the stripe is full.
 */
static uint64
ColumnarWriteRowInternal(ColumnarWriteState *writeState, Datum *columnValues,
						  bool *columnNulls)
{
	uint32 columnIndex = 0;
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	uint32 columnCount = writeState->tupleDescriptor->natts;
	ColumnarOptions *options = &writeState->options;
	const uint32 chunkRowCount = options->chunkRowCount;
	ChunkData *chunkData = writeState->chunkData;
	MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

	uint32 chunkIndex;
	uint32 chunkRowIndex;

	if (stripeBuffers)
	{
		chunkIndex = stripeBuffers->rowCount / chunkRowCount;
		chunkRowIndex = stripeBuffers->rowCount % chunkRowCount;
		/*
		* For each column, we first need to check to see if the next row will fit
		* inside the chunk buffer.  If it does not fit, then we need to serialize
		* the stripe and make a new stripe for insertion.
		*/
		bool fits = true;

		for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
		{

			/* Check for nulls, skip if null. */
			if (columnNulls[columnIndex])
			{
				continue;
			}

			Form_pg_attribute attributeForm =
				TupleDescAttr(writeState->tupleDescriptor, columnIndex);

			int columnTypeLength = attributeForm->attlen;
			char columnTypeAlign = attributeForm->attalign;

			uint32 datumLength = att_addlength_datum(0, columnTypeLength, columnValues[columnIndex]);
			uint32 datumLengthAligned = att_align_nominal(datumLength, columnTypeAlign);

			/* Check to see if we are within the 1 gigabyte value. */
			if ((long) chunkData->valueBufferArray[columnIndex]->len + (long) datumLengthAligned > 1024000000)
			{
				fits = false;
				break;
			}
		}

		if (!fits)
		{
			/* Flush the stripe. */
			ColumnarFlushPendingWrites(writeState);

			/* Then set up for new stripeBuffers. */
			stripeBuffers = NULL;

			chunkData->rowCount = 0;
		}
	}

	if (stripeBuffers == NULL)
	{
		stripeBuffers = CreateEmptyStripeBuffers(options->stripeRowCount,
												 chunkRowCount, columnCount);
		stripeSkipList = CreateEmptyStripeSkipList(options->stripeRowCount,
												   chunkRowCount, columnCount);
		writeState->stripeBuffers = stripeBuffers;
		writeState->stripeSkipList = stripeSkipList;
		writeState->compressionBuffer = makeStringInfo();

#if PG_VERSION_NUM >= PG_VERSION_16
		Oid relationId = RelidByRelfilenumber(writeState->relfilelocator.spcOid,
											  writeState->relfilelocator.relNumber);
#else
		Oid relationId = RelidByRelfilenode(writeState->relfilelocator.spcNode,
											writeState->relfilelocator.relNode);
#endif
		Relation relation = relation_open(relationId, NoLock);
		writeState->emptyStripeReservation =
			ReserveEmptyStripe(relation, columnCount, chunkRowCount,
							   options->stripeRowCount);
		relation_close(relation, NoLock);

		/*
		 * serializedValueBuffer lives in stripe write memory context so it needs to be
		 * initialized when the stripe is created.
		 */
		for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
		{
			chunkData->valueBufferArray[columnIndex] = makeStringInfo();
		}
	}

	chunkIndex = stripeBuffers->rowCount / chunkRowCount;
	chunkRowIndex = stripeBuffers->rowCount % chunkRowCount;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnChunkSkipNode **chunkSkipNodeArray = stripeSkipList->chunkSkipNodeArray;
		ColumnChunkSkipNode *chunkSkipNode =
			&chunkSkipNodeArray[columnIndex][chunkIndex];

		if (columnNulls[columnIndex])
		{
			chunkData->existsArray[columnIndex][chunkRowIndex] = false;
		}
		else
		{
			FmgrInfo *comparisonFunction =
				writeState->comparisonFunctionArray[columnIndex];
			Form_pg_attribute attributeForm =
				TupleDescAttr(writeState->tupleDescriptor, columnIndex);
			bool columnTypeByValue = attributeForm->attbyval;
			int columnTypeLength = attributeForm->attlen;
			Oid columnCollation = attributeForm->attcollation;
			char columnTypeAlign = attributeForm->attalign;

			chunkData->existsArray[columnIndex][chunkRowIndex] = true;

			uint32 datumLength = att_addlength_datum(0, columnTypeLength, columnValues[columnIndex]);
			uint32 datumLengthAligned = att_align_nominal(datumLength, columnTypeAlign);

			if (datumLengthAligned >= (256UL << 20))
			{
				elog(ERROR, "Error with insert on column \'%s\'. Inserting %d bytes, exceeding 256MB",
							 attributeForm->attname.data, datumLength);
			}

			SerializeSingleDatum(chunkData->valueBufferArray[columnIndex],
								 columnValues[columnIndex], columnTypeByValue,
								 columnTypeLength, columnTypeAlign);

			UpdateChunkSkipNodeMinMax(chunkSkipNode, columnValues[columnIndex],
									  columnTypeByValue, columnTypeLength,
									  columnCollation, comparisonFunction);
		}

		chunkSkipNode->rowCount++;
	}

	stripeSkipList->chunkCount = chunkIndex + 1;

	/* last row of the chunk is inserted serialize the chunk */
	if (chunkRowIndex == chunkRowCount - 1)
	{
		SerializeChunkData(writeState, chunkIndex, chunkRowCount);
	}

	uint64 writtenRowNumber = writeState->emptyStripeReservation->stripeFirstRowNumber +
							  stripeBuffers->rowCount;
	stripeBuffers->rowCount++;
	if (stripeBuffers->rowCount >= options->stripeRowCount)
	{
		ColumnarFlushPendingWrites(writeState);
	}

	MemoryContextSwitchTo(oldContext);

	return writtenRowNumber;
}


/*
 * ColumnarEndWrite finishes a columnar data load operation. If we have an unflushed
 * stripe, we flush it.
 */
void
ColumnarEndWrite(ColumnarWriteState *writeState)
{
	ColumnarFlushPendingWrites(writeState);

	MemoryContextDelete(writeState->stripeWriteContext);
	MemoryContextDelete(writeState->sortBufferContext);
	pfree(writeState->comparisonFunctionArray);
	FreeChunkData(writeState->chunkData);
	pfree(writeState);
}


void
ColumnarFlushPendingWrites(ColumnarWriteState *writeState)
{
	/*
	 * When sort-on-write is active, the rows are buffered in sortRows.
	 * Sort and drain them into ColumnarWriteRowInternal before FlushStripe.
	 *
	 * CRITICAL ORDER:
	 * 1. Capture and clear sortRows FIRST (before any drain that might
	 *    recursively call ColumnarFlushPendingWrites when a stripe fills).
	 * 2. The SortBufferRow data lives in sortBufferContext (NOT
	 *    stripeWriteContext), so MemoryContextReset(stripeWriteContext)
	 *    during mid-drain FlushStripe does NOT invalidate arr[i]->values.
	 * 3. Reset sortBufferContext AFTER all draining is complete.
	 */
	if (writeState->sortNkeys > 0 && writeState->sortRows != NIL)
	{
		/* Step 1: grab the list and clear immediately to prevent re-entry */
		List *rowsToFlush = writeState->sortRows;
		writeState->sortRows = NIL;

		int n = list_length(rowsToFlush);
		SortBufferRow **arr = (SortBufferRow **) palloc(n * sizeof(SortBufferRow *));
		int i = 0;
		ListCell *lc;

		foreach (lc, rowsToFlush)
			arr[i++] = (SortBufferRow *) lfirst(lc);

		/* Step 2: sort */
		CurrentSortWriteState = writeState;
		qsort(arr, n, sizeof(SortBufferRow *), CompareSortBufferRows);
		CurrentSortWriteState = NULL;

		/* Step 3: drain — arr[i]->values are safe in sortBufferContext */
		for (i = 0; i < n; i++)
			ColumnarWriteRowInternal(writeState, arr[i]->values, arr[i]->nulls);

		/* Step 4: release sort buffer memory */
		pfree(arr);
		MemoryContextReset(writeState->sortBufferContext);
	}

	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	if (stripeBuffers != NULL)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

		FlushStripe(writeState);
		MemoryContextReset(writeState->stripeWriteContext);

		/* set stripe data and skip list to NULL so they are recreated next time */
		writeState->stripeBuffers = NULL;
		writeState->stripeSkipList = NULL;

		MemoryContextSwitchTo(oldContext);
	}
}


/*
 * ColumnarWritePerTupleContext
 *
 * Return per-tuple context for columnar write operation.
 */
MemoryContext
ColumnarWritePerTupleContext(ColumnarWriteState *state)
{
	return state->perTupleContext;
}


/*
 * CompareSortBufferRows — qsort comparator for SortBufferRow*.
 * Uses CurrentSortWriteState for sort-key descriptors and comparison functions.
 */
static int
CompareSortBufferRows(const void *a, const void *b)
{
	const SortBufferRow *rowA = *(SortBufferRow * const *) a;
	const SortBufferRow *rowB = *(SortBufferRow * const *) b;
	ColumnarWriteState  *ws   = CurrentSortWriteState;

	for (int k = 0; k < ws->sortNkeys; k++)
	{
		ColSortKey *key  = &ws->sortKeys[k];
		bool        nullA = rowA->nulls[key->colIdx];
		bool        nullB = rowB->nulls[key->colIdx];

		if (nullA || nullB)
		{
			if (nullA && nullB)
				continue;
			if (nullA)
				return key->nullsFirst ? -1 : 1;
			return key->nullsFirst ? 1 : -1;
		}

		FmgrInfo *cmpFn = ws->comparisonFunctionArray[key->colIdx];
		if (cmpFn == NULL)
			continue; /* type has no btree cmp — treat as equal */

		Datum d1  = rowA->values[key->colIdx];
		Datum d2  = rowB->values[key->colIdx];
		int   cmp = DatumGetInt32(FunctionCall2Coll(cmpFn, key->collation, d1, d2));
		if (cmp != 0)
			return key->descending ? -cmp : cmp;
	}
	return 0;
}


/*
 * ColumnarInitSortKeys — parse writeState->options.orderby and populate
 * writeState->sortNkeys / writeState->sortKeys.
 *
 * The orderby string has the form:
 *   "col1 [ASC|DESC] [NULLS FIRST|NULLS LAST], col2 ..."
 * Column names are resolved against the table's TupleDesc.
 */
static void
ColumnarInitSortKeys(ColumnarWriteState *writeState)
{
	TupleDesc	tupleDesc = writeState->tupleDescriptor;
	const char *orderby   = writeState->options.orderby;

	if (orderby == NULL || orderby[0] == '\0')
		return;

	/* mutable copy for strtok */
	char *copy = pstrdup(orderby);

	/* count commas to upper-bound the key array size */
	int maxKeys = 1;
	for (const char *p = copy; *p; p++)
		if (*p == ',') maxKeys++;

	ColSortKey *keys = (ColSortKey *) palloc(maxKeys * sizeof(ColSortKey));
	int keyIdx = 0;

	char *token = strtok(copy, ",");
	while (token != NULL && keyIdx < maxKeys)
	{
		/* trim leading whitespace */
		while (*token == ' ' || *token == '\t') token++;

		/* split at first space to separate column name from modifiers */
		char *space = strpbrk(token, " \t");
		char *colname;
		char *rest = NULL;
		if (space != NULL)
		{
			colname = pnstrdup(token, space - token);
			rest    = space + 1;
		}
		else
		{
			colname = pstrdup(token);
		}

		/* resolve column name to attnum */
		AttrNumber attno = InvalidAttrNumber;
		for (int i = 0; i < tupleDesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);
			if (!attr->attisdropped &&
				pg_strcasecmp(NameStr(attr->attname), colname) == 0)
			{
				attno = attr->attnum;
				break;
			}
		}
		if (attno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist in orderby specification",
							colname)));

		Form_pg_attribute sortAttr = TupleDescAttr(tupleDesc, attno - 1);

		/* parse optional ASC/DESC and NULLS FIRST/LAST */
		bool sortDescending = false;
		bool nfirst         = false;

		if (rest != NULL)
		{
			char *upper = pstrdup(rest);
			for (char *c = upper; *c; c++) *c = pg_toupper((unsigned char) *c);

			if (strstr(upper, "DESC"))
			{
				sortDescending = true;
				nfirst         = true; /* DESC default: NULLS FIRST */
			}
			if (strstr(upper, "NULLS FIRST"))
				nfirst = true;
			else if (strstr(upper, "NULLS LAST"))
				nfirst = false;

			pfree(upper);
		}

		keys[keyIdx].attno      = attno;
		keys[keyIdx].colIdx     = (int) (attno - 1);
		keys[keyIdx].descending = sortDescending;
		keys[keyIdx].nullsFirst = nfirst;
		keys[keyIdx].collation  = sortAttr->attcollation;

		keyIdx++;
		pfree(colname);
		token = strtok(NULL, ",");
	}

	pfree(copy);

	writeState->sortNkeys = keyIdx;
	writeState->sortKeys  = keys;
}


/*
 * CreateEmptyStripeBuffers allocates an empty StripeBuffers structure with the given
 * column count.
 */
static StripeBuffers *
CreateEmptyStripeBuffers(uint32 stripeMaxRowCount, uint32 chunkRowCount,
						 uint32 columnCount)
{
	uint32 columnIndex = 0;
	uint32 maxChunkCount = (stripeMaxRowCount / chunkRowCount) + 1;
	ColumnBuffers **columnBuffersArray = palloc0(columnCount * sizeof(ColumnBuffers *));

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 chunkIndex = 0;
		ColumnChunkBuffers **chunkBuffersArray =
			palloc0(maxChunkCount * sizeof(ColumnChunkBuffers *));

		for (chunkIndex = 0; chunkIndex < maxChunkCount; chunkIndex++)
		{
			chunkBuffersArray[chunkIndex] = palloc0(sizeof(ColumnChunkBuffers));
			chunkBuffersArray[chunkIndex]->existsBuffer = NULL;
			chunkBuffersArray[chunkIndex]->valueBuffer = NULL;
			chunkBuffersArray[chunkIndex]->valueCompressionType = COMPRESSION_NONE;
		}

		columnBuffersArray[columnIndex] = palloc0(sizeof(ColumnBuffers));
		columnBuffersArray[columnIndex]->chunkBuffersArray = chunkBuffersArray;
	}

	StripeBuffers *stripeBuffers = palloc0(sizeof(StripeBuffers));
	stripeBuffers->columnBuffersArray = columnBuffersArray;
	stripeBuffers->columnCount = columnCount;
	stripeBuffers->rowCount = 0;

	return stripeBuffers;
}


/*
 * CreateEmptyStripeSkipList allocates an empty StripeSkipList structure with
 * the given column count. This structure has enough chunks to hold statistics
 * for stripeMaxRowCount rows.
 */
static StripeSkipList *
CreateEmptyStripeSkipList(uint32 stripeMaxRowCount, uint32 chunkRowCount,
						  uint32 columnCount)
{
	uint32 columnIndex = 0;
	uint32 maxChunkCount = (stripeMaxRowCount / chunkRowCount) + 1;

	ColumnChunkSkipNode **chunkSkipNodeArray =
		palloc0(columnCount * sizeof(ColumnChunkSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		chunkSkipNodeArray[columnIndex] =
			palloc0(maxChunkCount * sizeof(ColumnChunkSkipNode));
	}

	StripeSkipList *stripeSkipList = palloc0(sizeof(StripeSkipList));
	stripeSkipList->columnCount = columnCount;
	stripeSkipList->chunkCount = 0;
	stripeSkipList->chunkSkipNodeArray = chunkSkipNodeArray;

	return stripeSkipList;
}


/*
 * FlushStripe flushes current stripe data into the file. The function first ensures
 * the last data chunk for each column is properly serialized and compressed. Then,
 * the function creates the skip list and footer buffers. Finally, the function
 * flushes the skip list, data, and footer buffers to the file.
 */
static void
FlushStripe(ColumnarWriteState *writeState)
{
	uint32 columnIndex = 0;
	uint32 chunkIndex = 0;
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	ColumnChunkSkipNode **columnSkipNodeArray = stripeSkipList->chunkSkipNodeArray;
	TupleDesc tupleDescriptor = writeState->tupleDescriptor;
	uint32 columnCount = tupleDescriptor->natts;
	uint32 chunkCount = stripeSkipList->chunkCount;
	uint32 chunkRowCount = writeState->options.chunkRowCount;
	uint32 lastChunkIndex = stripeBuffers->rowCount / chunkRowCount;
	uint32 lastChunkRowCount = stripeBuffers->rowCount % chunkRowCount;
	uint64 stripeSize = 0;
	uint64 stripeRowCount = stripeBuffers->rowCount;

	elog(DEBUG1, "Flushing Stripe of size %d", stripeBuffers->rowCount);

#if PG_VERSION_NUM >= PG_VERSION_16
	Oid relationId = RelidByRelfilenumber(writeState->relfilelocator.spcOid,
										  writeState->relfilelocator.relNumber);
#else
	Oid relationId = RelidByRelfilenode(writeState->relfilelocator.spcNode,
										writeState->relfilelocator.relNode);
#endif

	Relation relation = relation_open(relationId, NoLock);

	/*
	 * check if the last chunk needs serialization , the last chunk was not serialized
	 * if it was not full yet, e.g.  (rowCount > 0)
	 */
	if (lastChunkRowCount > 0)
	{
		SerializeChunkData(writeState, lastChunkIndex, lastChunkRowCount);
	}

	/* update buffer sizes in stripe skip list */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnChunkSkipNode *chunkSkipNodeArray = columnSkipNodeArray[columnIndex];
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];

		for (chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
		{
			ColumnChunkBuffers *chunkBuffers =
				columnBuffers->chunkBuffersArray[chunkIndex];
			uint64 existsBufferSize = chunkBuffers->existsBuffer->len;
			ColumnChunkSkipNode *chunkSkipNode = &chunkSkipNodeArray[chunkIndex];

			chunkSkipNode->existsChunkOffset = stripeSize;
			chunkSkipNode->existsLength = existsBufferSize;
			stripeSize += existsBufferSize;
		}

		for (chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
		{
			ColumnChunkBuffers *chunkBuffers =
				columnBuffers->chunkBuffersArray[chunkIndex];
			uint64 valueBufferSize = chunkBuffers->valueBuffer->len;
			CompressionType valueCompressionType = chunkBuffers->valueCompressionType;
			ColumnChunkSkipNode *chunkSkipNode = &chunkSkipNodeArray[chunkIndex];

			chunkSkipNode->valueChunkOffset = stripeSize;
			chunkSkipNode->valueLength = valueBufferSize;
			chunkSkipNode->valueCompressionType = valueCompressionType;
			chunkSkipNode->valueCompressionLevel = writeState->options.compressionLevel;
			chunkSkipNode->decompressedValueSize = chunkBuffers->decompressedValueSize;

			stripeSize += valueBufferSize;
		}
	}

	StripeMetadata *stripeMetadata =
		CompleteStripeReservation(relation, writeState->emptyStripeReservation->stripeId,
								  stripeSize, stripeRowCount, chunkCount);

	uint64 currentFileOffset = stripeMetadata->fileOffset;

	/*
	 * Each stripe has only one section:
	 * Data section, in which we store data for each column continuously.
	 * We store data for each for each column in chunks. For each chunk, we
	 * store two buffers: "exists" buffer, and "value" buffer. "exists" buffer
	 * tells which values are not NULL. "value" buffer contains values for
	 * present values. For each column, we first store all "exists" buffers,
	 * and then all "value" buffers.
	 */

	/* flush the data buffers */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];

		for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkCount; chunkIndex++)
		{
			ColumnChunkBuffers *chunkBuffers =
				columnBuffers->chunkBuffersArray[chunkIndex];
			StringInfo existsBuffer = chunkBuffers->existsBuffer;

			ColumnarStorageWrite(relation, currentFileOffset,
								 existsBuffer->data, existsBuffer->len);
			currentFileOffset += existsBuffer->len;
		}

		for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkCount; chunkIndex++)
		{
			ColumnChunkBuffers *chunkBuffers =
				columnBuffers->chunkBuffersArray[chunkIndex];
			StringInfo valueBuffer = chunkBuffers->valueBuffer;

			ColumnarStorageWrite(relation, currentFileOffset,
								 valueBuffer->data, valueBuffer->len);
			currentFileOffset += valueBuffer->len;
		}
	}

	SaveChunkGroups(writeState->relfilelocator,
					stripeMetadata->id,
					writeState->chunkGroupRowCounts);
	SaveStripeSkipList(writeState->relfilelocator,
					   stripeMetadata->id,
					   stripeSkipList, tupleDescriptor);
	SaveEmptyRowMask(LookupStorageId(writeState->relfilelocator),
					 stripeMetadata->id,
					 stripeMetadata->firstRowNumber,
					 writeState->chunkGroupRowCounts);

	writeState->chunkGroupRowCounts = NIL;

	relation_close(relation, NoLock);
}


/*
 * SerializeBoolArray serializes the given boolean array and returns the result
 * as a StringInfo. This function packs every 8 boolean values into one byte.
 */
static StringInfo
SerializeBoolArray(bool *boolArray, uint32 boolArrayLength)
{
	uint32 boolArrayIndex = 0;
	uint32 byteCount = ((boolArrayLength * sizeof(bool)) + (8 - sizeof(bool))) / 8;

	StringInfo boolArrayBuffer = makeStringInfo();
	enlargeStringInfo(boolArrayBuffer, byteCount);
	boolArrayBuffer->len = byteCount;
	memset(boolArrayBuffer->data, 0, byteCount);

	for (boolArrayIndex = 0; boolArrayIndex < boolArrayLength; boolArrayIndex++)
	{
		if (boolArray[boolArrayIndex])
		{
			uint32 byteIndex = boolArrayIndex / 8;
			uint32 bitIndex = boolArrayIndex % 8;
			boolArrayBuffer->data[byteIndex] |= (1 << bitIndex);
		}
	}

	return boolArrayBuffer;
}


/*
 * SerializeSingleDatum serializes the given datum value and appends it to the
 * provided string info buffer.
 */
static void
SerializeSingleDatum(StringInfo datumBuffer, Datum datum, bool datumTypeByValue,
					 int datumTypeLength, char datumTypeAlign)
{
	uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
	uint32 datumLengthAligned = att_align_nominal(datumLength, datumTypeAlign);

	enlargeStringInfo(datumBuffer, datumLengthAligned);

	char *currentDatumDataPointer = datumBuffer->data + datumBuffer->len;
	memset(currentDatumDataPointer, 0, datumLengthAligned);

	if (datumTypeLength > 0)
	{
		if (datumTypeByValue)
		{
			store_att_byval(currentDatumDataPointer, datum, datumTypeLength);
		}
		else
		{
			memcpy(currentDatumDataPointer, DatumGetPointer(datum), datumTypeLength);
		}
	}
	else
	{
		Assert(!datumTypeByValue);
		memcpy(currentDatumDataPointer, DatumGetPointer(datum), datumLength);
	}

	datumBuffer->len += datumLengthAligned;
}


/*
 * SerializeChunkData serializes and compresses chunk data at given chunk index with given
 * compression type for every column.
 */
static void
SerializeChunkData(ColumnarWriteState *writeState, uint32 chunkIndex, uint32 rowCount)
{
	uint32 columnIndex = 0;
	StripeBuffers *stripeBuffers = writeState->stripeBuffers;
	ChunkData *chunkData = writeState->chunkData;
	CompressionType requestedCompressionType = writeState->options.compressionType;
	int compressionLevel = writeState->options.compressionLevel;
	const uint32 columnCount = stripeBuffers->columnCount;
	StringInfo compressionBuffer = writeState->compressionBuffer;

	writeState->chunkGroupRowCounts =
		lappend_int(writeState->chunkGroupRowCounts, rowCount);

	/* serialize exist values, data values are already serialized */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];
		ColumnChunkBuffers *chunkBuffers = columnBuffers->chunkBuffersArray[chunkIndex];

		chunkBuffers->existsBuffer =
			SerializeBoolArray(chunkData->existsArray[columnIndex], rowCount);
	}

	/*
	 * check and compress value buffers, if a value buffer is not compressable
	 * then keep it as uncompressed, store compression information.
	 */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];
		ColumnChunkBuffers *chunkBuffers = columnBuffers->chunkBuffersArray[chunkIndex];
		CompressionType actualCompressionType = COMPRESSION_NONE;

		StringInfo serializedValueBuffer = chunkData->valueBufferArray[columnIndex];

		Assert(requestedCompressionType >= 0 &&
			   requestedCompressionType < COMPRESSION_COUNT);

		chunkBuffers->decompressedValueSize =
			chunkData->valueBufferArray[columnIndex]->len;

		/*
		 * if serializedValueBuffer is be compressed, update serializedValueBuffer
		 * with compressed data and store compression type.
		 */
		bool compressed = CompressBuffer(serializedValueBuffer, compressionBuffer,
										 requestedCompressionType,
										 compressionLevel);

		if (compressed)
		{
			serializedValueBuffer = compressionBuffer;
			actualCompressionType = requestedCompressionType;
		}

		/* store (compressed) value buffer */
		chunkBuffers->valueCompressionType = actualCompressionType;
		chunkBuffers->valueBuffer = CopyStringInfo(serializedValueBuffer);

		/* valueBuffer needs to be reset for next chunk's data */
		resetStringInfo(chunkData->valueBufferArray[columnIndex]);
	}
}


/*
 * UpdateChunkSkipNodeMinMax takes the given column value, and checks if this
 * value falls outside the range of minimum/maximum values of the given column
 * chunk skip node. If it does, the function updates the column chunk skip node
 * accordingly.
 */
static void
UpdateChunkSkipNodeMinMax(ColumnChunkSkipNode *chunkSkipNode, Datum columnValue,
						  bool columnTypeByValue, int columnTypeLength,
						  Oid columnCollation, FmgrInfo *comparisonFunction)
{
	bool hasMinMax = chunkSkipNode->hasMinMax;
	Datum previousMinimum = chunkSkipNode->minimumValue;
	Datum previousMaximum = chunkSkipNode->maximumValue;
	Datum currentMinimum = 0;
	Datum currentMaximum = 0;

	/* if type doesn't have a comparison function, skip min/max values */
	if (comparisonFunction == NULL)
	{
		return;
	}

	if (!hasMinMax)
	{
		currentMinimum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		currentMaximum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
	}
	else
	{
		Datum minimumComparisonDatum = FunctionCall2Coll(comparisonFunction,
														 columnCollation, columnValue,
														 previousMinimum);
		Datum maximumComparisonDatum = FunctionCall2Coll(comparisonFunction,
														 columnCollation, columnValue,
														 previousMaximum);
		int minimumComparison = DatumGetInt32(minimumComparisonDatum);
		int maximumComparison = DatumGetInt32(maximumComparisonDatum);

		if (minimumComparison < 0)
		{
			if (!columnTypeByValue)
				pfree(DatumGetPointer(previousMinimum));

			currentMinimum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		}
		else
		{
			currentMinimum = previousMinimum;
		}

		if (maximumComparison > 0)
		{
			if (!columnTypeByValue)
				pfree(DatumGetPointer(previousMaximum));

			currentMaximum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		}
		else
		{
			currentMaximum = previousMaximum;
		}
	}

	chunkSkipNode->hasMinMax = true;
	chunkSkipNode->minimumValue = currentMinimum;
	chunkSkipNode->maximumValue = currentMaximum;
}


/* Creates a copy of the given datum. */
static Datum
DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength)
{
	Datum datumCopy = 0;

	if (datumTypeByValue)
	{
		datumCopy = datum;
	}
	else
	{
		uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
		char *datumData = palloc0(datumLength);
		memcpy(datumData, DatumGetPointer(datum), datumLength);

		datumCopy = PointerGetDatum(datumData);
	}

	return datumCopy;
}


/*
 * CopyStringInfo creates a deep copy of given source string allocating only needed
 * amount of memory.
 */
static StringInfo
CopyStringInfo(StringInfo sourceString)
{
	StringInfo targetString = palloc0(sizeof(StringInfoData));

	if (sourceString->len > 0)
	{
		targetString->data = palloc0(sourceString->len);
		targetString->len = sourceString->len;
		targetString->maxlen = sourceString->len;
		memcpy(targetString->data, sourceString->data, sourceString->len);
	}

	return targetString;
}


bool
ContainsPendingWrites(ColumnarWriteState *state)
{
	return state->stripeBuffers != NULL && state->stripeBuffers->rowCount != 0;
}
