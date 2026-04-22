/*-------------------------------------------------------------------------
 *
 * rowcompress_tableam.c
 *
 * Row-Based Compressed Table Access Method.
 *
 * Stores rows in fixed-size batches; each batch is compressed as a unit
 * using the same compression infrastructure as the columnar AM. The on-disk
 * file layout (managed via ColumnarStorage) is:
 *
 *   [ColumnarStorage metapage (managed by ColumnarStorageInit)]
 *   [Batch 1 header + row offsets + compressed row data]
 *   [Batch 2 header + row offsets + compressed row data]
 *   ...
 *
 * Batch metadata (location, size, row range) is kept in engine.row_batch.
 *
 * This first version supports INSERT and sequential/index SELECT.
 * UPDATE and DELETE are not yet supported.
 *
 * Copyright (c) Saulo José Benvenutti.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/read_stream.h"
#include "storage/smgr.h"
#include "access/detoast.h"
#include "catalog/storage.h"
#include "optimizer/plancat.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/datum.h"
#include "utils/typcache.h"
#include "catalog/objectaccess.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"

#if PG_VERSION_NUM >= PG_VERSION_16
#include "storage/relfilelocator.h"
#include "utils/relfilenumbermap.h"
#else
#include "utils/relfilenodemap.h"
#endif
#if PG_VERSION_NUM >= PG_VERSION_17
#include "storage/read_stream.h"
#endif
#if PG_VERSION_NUM >= PG_VERSION_18
#include "utils/timestamp.h"
#endif

#include "engine/engine.h"
#include "engine/engine_storage.h"
#include "engine/engine_tableam.h"
#include "engine/engine_version_compat.h"
#include "engine/rowcompress.h"


/* ================================================================
 * TYPES
 * ================================================================ */

/*
 * RowCompressBatchHeader is written at the start of each on-disk batch.
 * It is followed immediately by:
 *   uint32  rowOffsets[rowCount]  -- byte offsets into the *uncompressed* data
 *   char    data[]                -- compressed (or plain) row data
 *
 * Each row in the uncompressed data is stored as:
 *   uint64  t_len                 -- HeapTuple t_len (8-byte val for alignment)
 *   char    t_data[t_len]         -- HeapTupleHeader + attribute data
 *   char    _pad[...]             -- zero-fill to the next MAXALIGN boundary
 *
 * rowOffsets[i] always points to an MAXALIGN-aligned byte in the
 * uncompressed data, which is also the start of a uint64 t_len prefix.
 * After the prefix (8 bytes), HeapTupleHeader begins at a MAXALIGN'd
 * address as long as palloc() is used for the decompressed buffer.
 */
typedef struct RowCompressBatchHeader
{
	uint32 rowCount;             /* number of rows in this batch */
	uint32 natts;                /* number of attributes (sanity check) */
	uint32 uncompressedDataSize; /* total size of row data before compression */
	int32  compressionType;      /* CompressionType enum value */
	int32  compressionLevel;     /* compression level used */
	int32  _reserved;            /* padding to 24-byte total */
} RowCompressBatchHeader;

/* An entry in the pending-write list */
typedef struct RowDataEntry
{
	char   *tupleData; /* copy of HeapTuple t_data (exactly t_len bytes) */
	uint32  tupleLen;  /* HeapTuple t_len */
} RowDataEntry;

/* Per-relation write state */
typedef struct RowCompressWriteState
{
	RelFileLocator  relfilelocator;
	uint64          storageId;
	TupleDesc       tupdesc;         /* copy of attribute descriptor */
	RowCompressOptions options;

	MemoryContext   batchContext;    /* context for accumulated row data */
	List           *rowList;         /* List of RowDataEntry */
	uint32          rowCount;        /* rows buffered so far in current batch */
	uint64          firstRowNumber;  /* row number of first buffered row */
} RowCompressWriteState;

typedef struct IndexFetchRowCompressData IndexFetchRowCompressData;

/* Sequential scan state */
typedef struct RowCompressScanDesc
{
	TableScanDescData rc_base;

	uint64         storageId;
	RowCompressOptions options;

	/* All batches for this table, ordered by batch_num */
	List          *batchList;
	ListCell      *currentBatchCell;

	/* Currently loaded decompressed batch */
	char          *batchData;          /* palloc'd decompressed buffer */
	uint32        *rowOffsets;          /* palloc'd offsets into batchData */
	uint32         batchRowCount;
	uint32         currentRowIndex;
	uint64         currentFirstRowNumber;
	IndexFetchRowCompressData *bitmapFetch;

	/* Deleted-row bitmask for the batch currently loaded into this scan state */
	uint8         *currentDeletedMask;    /* NULL = no deletions; lives in batchList meta */
	uint32         currentDeletedMaskLen;

	/* ANALYZE state: physical pages per batch (used in scan_analyze_next_block) */
	uint32         analyzeBlocksPerBatch;

	/* Batch-level pruning: set by rowcompress_set_pushdown_clauses() */
	RCPruningCtx  *pruningCtx;     /* NULL = pruning disabled for this scan */
	int64          batchesPruned;  /* number of batches skipped via pruning */

#if PG_VERSION_NUM < PG_VERSION_18
	struct TBMIterateResult *tbmres;
#else
	TBMIterateResult tbmres;
	bool          bmHavePage;
	int           bmNoffsets;
	OffsetNumber *bmOffsets;
#endif
	int            bmOffidx;
	uint64         bmRownum;
	uint64         bmRownumEnd;

	MemoryContext  scanContext;
} RowCompressScanDesc;

/*
 * RowCompressParallelScanDescData — shared DSM state for parallel rowcompress
 * sequential scans.  Each worker atomically claims the next batch index from
 * rc_next_batch_idx and processes all rows in that batch before claiming again.
 */
typedef struct RowCompressParallelScanDescData
{
	ParallelTableScanDescData base;           /* must be first */
	pg_atomic_uint64          rc_next_batch_idx; /* next batch index to claim */
} RowCompressParallelScanDescData;
typedef RowCompressParallelScanDescData *RowCompressParallelScanDesc;

/*
 * RC_DECOMP_CACHE_MAX — number of decompressed batches held simultaneously
 * in the index-fetch LRU cache.  Each slot holds one decompressed batch;
 * when all slots are occupied the least-recently-used one is evicted.
 */
#define RC_DECOMP_CACHE_MAX  32

/* One slot in the decompressed-batch LRU cache */
typedef struct RCDecompCacheEntry
{
	uint64   batchNum;            /* UINT64_MAX = empty slot */
	char    *batchData;           /* palloc'd decompressed row data */
	uint32  *rowOffsets;          /* palloc'd per-row byte offsets */
	uint32   batchRowCount;       /* number of rows in this batch */
	uint64   batchFirstRowNumber; /* first logical row number */
	int      lastUsed;            /* LRU clock value; higher = more recent */
} RCDecompCacheEntry;

/* Index fetch state (for index scans) */
typedef struct IndexFetchRowCompressData
{
	IndexFetchTableData rc_base;
	uint64              storageId;
	RowCompressOptions  options;

	/*
	 * Batch metadata cache — loaded once from engine.row_batch on first
	 * access, then binary-searched for every subsequent TID lookup.
	 * This eliminates one full-catalog scan per fetched row.
	 */
	RowCompressBatchMetadata *batchMetaArray; /* sorted by firstRowNumber */
	int                       batchMetaCount;

	/*
	 * LRU cache of decompressed batches.  Keeps the last
	 * RC_DECOMP_CACHE_MAX batches in memory so that an index scan
	 * revisiting the same batch (or the same batch shortly after an
	 * eviction) does not need to re-read and re-decompress from disk.
	 */
	RCDecompCacheEntry  decompCache[RC_DECOMP_CACHE_MAX];
	int                 lruClock;   /* incremented on every cache hit/fill */

	MemoryContext       scanContext;
} IndexFetchRowCompressData;


/* Column numbers for engine.row_batch */
#define Anum_rowcompress_batch_storage_id        1
#define Anum_rowcompress_batch_batch_num         2
#define Anum_rowcompress_batch_file_offset       3
#define Anum_rowcompress_batch_data_length       4
#define Anum_rowcompress_batch_first_row_number  5
#define Anum_rowcompress_batch_row_count         6
#define Anum_rowcompress_batch_deleted_mask      7
#define Anum_rowcompress_batch_min_value         8
#define Anum_rowcompress_batch_max_value         9

#define Natts_rowcompress_batch                  9

/*
 * RC_IS_DELETED — returns true if the bit for rowOffset is set in the
 * deletion bitmask (NULL mask = no deletions).
 */
#define RC_IS_DELETED(mask, maskLen, rowOffset) \
	((mask) != NULL && \
	 (uint32)(rowOffset) / 8 < (maskLen) && \
	 (((mask)[(rowOffset) / 8]) & (1u << ((rowOffset) % 8))) != 0)

/* Column numbers for engine.row_options */
#define Anum_rowcompress_options_regclass          1
#define Anum_rowcompress_options_batch_size        2
#define Anum_rowcompress_options_compression       3
#define Anum_rowcompress_options_compression_level 4
#define Anum_rowcompress_options_pruning_attnum    5
#define Natts_rowcompress_options                  5


/* ================================================================
 * GLOBAL STATE
 * ================================================================ */

static List          *RCWriteStateList    = NIL;
static MemoryContext  RCWriteStateContext = NULL;

/* Object access hook for DROP TABLE cleanup */
static object_access_hook_type PrevRCObjectAccessHook = NULL;
static void RCObjectAccessHook(ObjectAccessType access, Oid classId,
								Oid objectId, int subId, void *arg);


/* ================================================================
 * FORWARD DECLARATIONS
 * ================================================================ */

static Oid RCNamespaceId(void);
static Oid RCBatchRelationId(void);
static Oid RCBatchPkeyIndexId(void);
static Oid RCOptionsRelationId(void);

static void RCInsertBatchMetadata(uint64 storageId, uint64 batchNum,
								  uint64 firstRowNumber, uint32 rowCount,
								  uint64 fileOffset, uint64 dataLength,
								  bytea *minValue, bytea *maxValue);
static List *RCGetBatches(uint64 storageId);
static RowCompressBatchMetadata *RCFindBatchForRowNumber(uint64 storageId,
														  uint64 rowNumber);
static RowCompressBatchMetadata *RCFindBatchInList(List *batches, uint64 rowNumber);
static void RCMarkRowDeleted(uint64 storageId, uint64 batchNum, uint32 rowCount, uint32 rowOffset);
static void RCDeleteBatches(uint64 storageId);

static void RCInitOptions(Oid relationId);
static bool RCReadOptions(Oid relationId, RowCompressOptions *options);
static void RCSetOptions(Oid relationId, const RowCompressOptions *options);
static void RCDeleteOptions(Oid relationId, bool missingOk);

static Datum *RCDetoastValues(TupleDesc tupdesc, Datum *orig_values, bool *isnull);
static RowCompressWriteState *RCFindWriteState(RelFileLocator relfilelocator);
static RowCompressWriteState *RCGetOrCreateWriteState(Relation rel);
static void RCFlushBatch(RowCompressWriteState *ws, Relation rel);
static void RCFlushAllWriteStates(void);
static void RCDiscardAllWriteStates(void);

static bool RCLoadBatch(RowCompressScanDesc *scan, RowCompressBatchMetadata *batch);
static void RCEnsureBatchMetaCache(IndexFetchRowCompressData *fetch, Relation rel);
static RowCompressBatchMetadata *RCBinaryFindBatch(IndexFetchRowCompressData *fetch,
												   uint64 rowNumber);
static RCDecompCacheEntry *RCGetOrLoadBatch(IndexFetchRowCompressData *fetch,
											 Relation rel,
											 const RowCompressBatchMetadata *meta);
static void RCExtractRow(TupleDesc tupdesc, char *batchData,
						  uint32 *rowOffsets, uint32 rowIndex,
						  Datum *values, bool *isnull);

static void RowCompressXactCallback(XactEvent event, void *arg);
static void RowCompressSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
									   SubTransactionId parentSubid, void *arg);

static uint64 RCStorageId(Relation rel);

static const TupleTableSlotOps *rowcompress_slot_callbacks(Relation rel);
static TableScanDesc rowcompress_beginscan(Relation rel, Snapshot snapshot,
										   int nkeys, ScanKey key,
										   ParallelTableScanDesc parallel_scan,
										   uint32 flags);
static void rowcompress_endscan(TableScanDesc sscan);
static void rowcompress_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
							   bool allow_strat, bool allow_sync, bool allow_pagemode);
static bool rowcompress_getnextslot(TableScanDesc sscan, ScanDirection direction,
									TupleTableSlot *slot);
static Size rowcompress_parallelscan_estimate(Relation rel);
static Size rowcompress_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);
static void rowcompress_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);
static IndexFetchTableData *rowcompress_index_fetch_begin(Relation rel);
static void rowcompress_index_fetch_reset(IndexFetchTableData *scan);
static void rowcompress_index_fetch_end(IndexFetchTableData *scan);
static bool rowcompress_index_fetch_tuple(struct IndexFetchTableData *scan,
										  ItemPointer tid, Snapshot snapshot,
										  TupleTableSlot *slot,
										  bool *call_again, bool *all_dead);
static bool rowcompress_fetch_row_version(Relation relation, ItemPointer tid,
										  Snapshot snapshot, TupleTableSlot *slot);
static void rowcompress_get_latest_tid(TableScanDesc sscan, ItemPointer tid);
static bool rowcompress_tuple_tid_valid(TableScanDesc scan, ItemPointer tid);
static bool rowcompress_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
												  Snapshot snapshot);
#if PG_VERSION_NUM >= PG_VERSION_14
static TransactionId rowcompress_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate);
#else
static TransactionId rowcompress_compute_xid_horizon_for_tuples(Relation rel,
																 ItemPointerData *tids,
																 int nitems);
#endif
static void rowcompress_tuple_insert(Relation relation, TupleTableSlot *slot,
									 CommandId cid, int options, BulkInsertState bistate);
static void rowcompress_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
												  CommandId cid, int options,
												  BulkInsertState bistate, uint32 specToken);
static void rowcompress_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
												   uint32 specToken, bool succeeded);
static void rowcompress_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
									 CommandId cid, int options, BulkInsertState bistate);
static TM_Result rowcompress_tuple_delete(Relation relation, ItemPointer tid,
										  CommandId cid, Snapshot snapshot,
										  Snapshot crosscheck, bool wait,
										  TM_FailureData *tmfd, bool changingPart);
#if PG_VERSION_NUM >= PG_VERSION_16
static TM_Result rowcompress_tuple_update(Relation relation, ItemPointer otid,
										  TupleTableSlot *slot, CommandId cid,
										  Snapshot snapshot, Snapshot crosscheck,
										  bool wait, TM_FailureData *tmfd,
										  LockTupleMode *lockmode,
										  TU_UpdateIndexes *update_indexes);
#else
static TM_Result rowcompress_tuple_update(Relation relation, ItemPointer otid,
										  TupleTableSlot *slot, CommandId cid,
										  Snapshot snapshot, Snapshot crosscheck,
										  bool wait, TM_FailureData *tmfd,
										  LockTupleMode *lockmode, bool *update_indexes);
#endif
static TM_Result rowcompress_tuple_lock(Relation relation, ItemPointer tid,
										Snapshot snapshot, TupleTableSlot *slot,
										CommandId cid, LockTupleMode mode,
										LockWaitPolicy wait_policy, uint8 flags,
										TM_FailureData *tmfd);
static void rowcompress_finish_bulk_insert(Relation relation, int options);
static void rowcompress_relation_set_new_filenode(Relation rel,
												  const RelFileLocator *newrnode,
												  char persistence,
												  TransactionId *freezeXid,
												  MultiXactId *minmulti);
static void rowcompress_relation_nontransactional_truncate(Relation rel);
static void rowcompress_relation_copy_data(Relation rel, const RelFileLocator *newrnode);
static void rowcompress_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
												  Relation OldIndex, bool use_sort,
												  TransactionId OldestXmin,
												  TransactionId *xid_cutoff,
												  MultiXactId *multi_cutoff,
												  double *num_tuples,
												  double *tups_vacuumed,
												  double *tups_recently_dead);
static void rowcompress_vacuum_rel(Relation rel, VacuumParams *params,
								   BufferAccessStrategy bstrategy);
#if PG_VERSION_NUM >= PG_VERSION_17
static bool rowcompress_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream);
#else
static bool rowcompress_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
												BufferAccessStrategy bstrategy);
#endif
static bool rowcompress_scan_analyze_next_tuple(TableScanDesc scan,
												TransactionId OldestXmin,
												double *liverows, double *deadrows,
												TupleTableSlot *slot);
static double rowcompress_index_build_range_scan(Relation relation, Relation indexRelation,
												  IndexInfo *indexInfo, bool allow_sync,
												  bool anyvisible, bool progress,
												  BlockNumber start_blockno,
												  BlockNumber numblocks,
												  IndexBuildCallback callback,
												  void *callback_state,
												  TableScanDesc scan);
static void rowcompress_index_validate_scan(Relation relation, Relation indexRelation,
											IndexInfo *indexInfo, Snapshot snapshot,
											ValidateIndexState *validateIndexState);
static bool rowcompress_relation_needs_toast_table(Relation rel);
static bool rowcompress_scan_sample_next_block(TableScanDesc scan,
											   SampleScanState *scanstate);
static bool rowcompress_scan_sample_next_tuple(TableScanDesc scan,
											   SampleScanState *scanstate,
											   TupleTableSlot *slot);
#if PG_VERSION_NUM < PG_VERSION_18
static bool rowcompress_scan_bitmap_next_block(TableScanDesc sscan,
										   struct TBMIterateResult *tbmres);
static bool rowcompress_scan_bitmap_next_tuple(TableScanDesc sscan,
										   struct TBMIterateResult *tbmres,
										   TupleTableSlot *slot);
#else
static bool rowcompress_scan_bitmap_next_tuple(TableScanDesc sscan,
										   TupleTableSlot *slot,
										   bool *recheck,
										   uint64 *lossy_pages,
										   uint64 *exact_pages);
#endif


/* ================================================================
 * CATALOG HELPER: OID lookups
 * ================================================================ */

static Oid
RCNamespaceId(void)
{
	return get_namespace_oid(ROWCOMPRESS_NAMESPACE_NAME, true);
}

static Oid
RCBatchRelationId(void)
{
	return get_relname_relid(ROWCOMPRESS_BATCH_TABLE_NAME, RCNamespaceId());
}

static Oid
RCBatchPkeyIndexId(void)
{
	return get_relname_relid("row_batch_pkey", RCNamespaceId());
}

static Oid
RCOptionsRelationId(void)
{
	return get_relname_relid(ROWCOMPRESS_OPTIONS_TABLE_NAME, RCNamespaceId());
}


/* ================================================================
 * METADATA: batch table access
 * ================================================================ */

/*
 * RCInsertBatchMetadata inserts a row into engine.row_batch.
 */
static void
RCInsertBatchMetadata(uint64 storageId, uint64 batchNum,
					  uint64 firstRowNumber, uint32 rowCount,
					  uint64 fileOffset, uint64 dataLength,
					  bytea *minValue, bytea *maxValue)
{
	Datum values[Natts_rowcompress_batch];
	bool  nulls[Natts_rowcompress_batch];

	memset(nulls, false, sizeof(nulls));

	values[Anum_rowcompress_batch_storage_id - 1]       = UInt64GetDatum(storageId);
	values[Anum_rowcompress_batch_batch_num - 1]         = UInt64GetDatum(batchNum);
	values[Anum_rowcompress_batch_file_offset - 1]       = UInt64GetDatum(fileOffset);
	values[Anum_rowcompress_batch_data_length - 1]       = UInt64GetDatum(dataLength);
	values[Anum_rowcompress_batch_first_row_number - 1]  = UInt64GetDatum(firstRowNumber);
	values[Anum_rowcompress_batch_row_count - 1]         = UInt32GetDatum(rowCount);
	nulls[Anum_rowcompress_batch_deleted_mask - 1]       = true;  /* no deletions yet */

	if (minValue != NULL)
		values[Anum_rowcompress_batch_min_value - 1] = PointerGetDatum(minValue);
	else
		nulls[Anum_rowcompress_batch_min_value - 1] = true;

	if (maxValue != NULL)
		values[Anum_rowcompress_batch_max_value - 1] = PointerGetDatum(maxValue);
	else
		nulls[Anum_rowcompress_batch_max_value - 1] = true;

	Relation batchRel = table_open(RCBatchRelationId(), RowExclusiveLock);
	TupleDesc tupdesc = RelationGetDescr(batchRel);
	HeapTuple tuple   = heap_form_tuple(tupdesc, values, nulls);

	CatalogTupleInsert(batchRel, tuple);

	heap_freetuple(tuple);
	table_close(batchRel, RowExclusiveLock);
}

/*
 * RCGetBatches returns all RowCompressBatchMetadata for the given storageId,
 * ordered by batch_num ascending.
 */
static List *
RCGetBatches(uint64 storageId)
{
	Oid batchOid    = RCBatchRelationId();
	Oid pkeyIdxOid  = RCBatchPkeyIndexId();


	Relation batchRel = table_open(batchOid, AccessShareLock);

	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0],
				Anum_rowcompress_batch_storage_id,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum((int64) storageId));

	bool indexOk = (pkeyIdxOid != InvalidOid);
	SysScanDesc scan = systable_beginscan(batchRel, pkeyIdxOid, indexOk,
										  GetTransactionSnapshot(), 1, scanKey);

	List *batchList = NIL;
	HeapTuple tup;

	while ((tup = systable_getnext(scan)) != NULL)
	{
		bool isNull;
		TupleDesc tupdesc = RelationGetDescr(batchRel);

		uint64 sid = DatumGetUInt64(
			heap_getattr(tup, Anum_rowcompress_batch_storage_id, tupdesc, &isNull));

		if (sid != storageId)
			break; /* scanned past our storage_id */

		RowCompressBatchMetadata *meta = palloc(sizeof(RowCompressBatchMetadata));
		meta->batchNum       = DatumGetUInt64(
			heap_getattr(tup, Anum_rowcompress_batch_batch_num, tupdesc, &isNull));
		meta->fileOffset     = DatumGetUInt64(
			heap_getattr(tup, Anum_rowcompress_batch_file_offset, tupdesc, &isNull));
		meta->dataLength     = DatumGetUInt64(
			heap_getattr(tup, Anum_rowcompress_batch_data_length, tupdesc, &isNull));
		meta->firstRowNumber = DatumGetUInt64(
			heap_getattr(tup, Anum_rowcompress_batch_first_row_number, tupdesc, &isNull));
		meta->rowCount       = DatumGetUInt32(
			heap_getattr(tup, Anum_rowcompress_batch_row_count, tupdesc, &isNull));

		/* Read deleted_mask (nullable bytea) */
		Datum maskDatum = heap_getattr(tup, Anum_rowcompress_batch_deleted_mask,
									   tupdesc, &isNull);
		if (!isNull)
		{
			bytea *maskBytea = DatumGetByteaP(maskDatum);
			meta->deletedMaskLen = VARSIZE_ANY_EXHDR(maskBytea);
			meta->deletedMask    = palloc(meta->deletedMaskLen);
			memcpy(meta->deletedMask, VARDATA_ANY(maskBytea), meta->deletedMaskLen);
		}
		else
		{
			meta->deletedMask    = NULL;
			meta->deletedMaskLen = 0;
		}

		/* Read batch_min_value / batch_max_value (nullable bytea, v1.1+) */
		meta->hasMinMax = false;
		meta->rawMinValue = NULL;
		meta->rawMaxValue = NULL;
		if (tupdesc->natts >= Anum_rowcompress_batch_min_value)
		{
			Datum minDatum = heap_getattr(tup, Anum_rowcompress_batch_min_value,
										  tupdesc, &isNull);
			bool minIsNull = isNull;
			Datum maxDatum = heap_getattr(tup, Anum_rowcompress_batch_max_value,
										  tupdesc, &isNull);
			bool maxIsNull = isNull;

			if (!minIsNull && !maxIsNull)
			{
				bytea *minB = DatumGetByteaP(minDatum);
				bytea *maxB = DatumGetByteaP(maxDatum);
				meta->rawMinValue = palloc(VARSIZE_ANY(minB));
				meta->rawMaxValue = palloc(VARSIZE_ANY(maxB));
				memcpy(meta->rawMinValue, minB, VARSIZE_ANY(minB));
				memcpy(meta->rawMaxValue, maxB, VARSIZE_ANY(maxB));
				meta->hasMinMax = true;
			}
		}

		batchList = lappend(batchList, meta);
	}

	systable_endscan(scan);
	table_close(batchRel, AccessShareLock);

	return batchList;
}

/*
 * RCEnsureBatchMetaCache loads all batch metadata for the relation into
 * fetch->batchMetaArray (sorted by firstRowNumber) exactly once per index
 * scan.  Subsequent calls are no-ops.
 */
static void
RCEnsureBatchMetaCache(IndexFetchRowCompressData *fetch, Relation rel)
{
	if (fetch->batchMetaArray != NULL)
		return;   /* already loaded */

	uint64 storageId = RCStorageId(rel);
	List  *batches   = RCGetBatches(storageId);
	int    n         = list_length(batches);

	if (n == 0)
	{
		/* Empty table: allocate a dummy array so the pointer is non-NULL */
		fetch->batchMetaArray = palloc0(sizeof(RowCompressBatchMetadata));
		fetch->batchMetaCount = 0;
		list_free_deep(batches);
		return;
	}

	fetch->batchMetaArray = palloc(n * sizeof(RowCompressBatchMetadata));
	fetch->batchMetaCount = n;

	int       i  = 0;
	ListCell *lc;
	foreach(lc, batches)
	{
		RowCompressBatchMetadata *m = (RowCompressBatchMetadata *) lfirst(lc);
		fetch->batchMetaArray[i] = *m;
		/* Deep-copy deletedMask so it survives after list_free_deep */
		if (m->deletedMask != NULL && m->deletedMaskLen > 0)
		{
			fetch->batchMetaArray[i].deletedMask = palloc(m->deletedMaskLen);
			memcpy(fetch->batchMetaArray[i].deletedMask,
				   m->deletedMask, m->deletedMaskLen);
		}
		i++;
	}
	list_free_deep(batches);
}

/*
 * RCBinaryFindBatch does a binary search in fetch->batchMetaArray for the
 * batch that contains rowNumber.  Returns a pointer into the array (not a
 * copy), or NULL if not found.  The array must be sorted by firstRowNumber
 * (guaranteed because RCGetBatches uses the pkey index on (storageId, batchNum)
 * and batchNum increases monotonically with firstRowNumber).
 */
static RowCompressBatchMetadata *
RCBinaryFindBatch(IndexFetchRowCompressData *fetch, uint64 rowNumber)
{
	int lo = 0,
		hi = fetch->batchMetaCount - 1;

	while (lo <= hi)
	{
		int mid = lo + (hi - lo) / 2;
		RowCompressBatchMetadata *m = &fetch->batchMetaArray[mid];

		if (rowNumber < m->firstRowNumber)
			hi = mid - 1;
		else if (rowNumber >= m->firstRowNumber + m->rowCount)
			lo = mid + 1;
		else
			return m;
	}
	return NULL;
}

/*
 * RCGetOrLoadBatch returns a pointer to the RCDecompCacheEntry that holds
 * the decompressed data for *meta.  If the batch is already in the LRU
 * cache, the cached entry is returned (O(RC_DECOMP_CACHE_MAX) scan).
 * Otherwise the least-recently-used slot is evicted, the batch is read from
 * disk, decompressed, and the slot is returned.
 */
static RCDecompCacheEntry *
RCGetOrLoadBatch(IndexFetchRowCompressData *fetch, Relation rel,
				 const RowCompressBatchMetadata *meta)
{
	int i;

	/* Fast path: batch already in cache */
	for (i = 0; i < RC_DECOMP_CACHE_MAX; i++)
	{
		if (fetch->decompCache[i].batchNum == meta->batchNum)
		{
			fetch->decompCache[i].lastUsed = ++fetch->lruClock;
			return &fetch->decompCache[i];
		}
	}

	/* Find the LRU (or first empty) slot */
	int evict = 0;
	for (i = 1; i < RC_DECOMP_CACHE_MAX; i++)
	{
		RCDecompCacheEntry *e = &fetch->decompCache[i];
		if (e->batchNum == UINT64_MAX)
		{
			evict = i;
			break;   /* prefer empty slot immediately */
		}
		if (e->lastUsed < fetch->decompCache[evict].lastUsed)
			evict = i;
	}

	RCDecompCacheEntry *entry = &fetch->decompCache[evict];

	/* Free old data */
	if (entry->batchData)  { pfree(entry->batchData);  entry->batchData  = NULL; }
	if (entry->rowOffsets) { pfree(entry->rowOffsets); entry->rowOffsets = NULL; }

	/* Read compressed batch from disk */
	char *rawData = palloc(meta->dataLength);
	ColumnarStorageRead(rel, meta->fileOffset, rawData, meta->dataLength);

	RowCompressBatchHeader *hdr = (RowCompressBatchHeader *) rawData;

	uint32  offsetsBytes    = hdr->rowCount * sizeof(uint32);
	uint32 *storedOffsets   = (uint32 *)(rawData + sizeof(RowCompressBatchHeader));
	char   *storedData      = rawData + sizeof(RowCompressBatchHeader) + offsetsBytes;
	uint32  storedDataSize  = (uint32)(meta->dataLength -
							  sizeof(RowCompressBatchHeader) - offsetsBytes);

	/* Save scalar fields before pfree(rawData) corrupts the header */
	uint32 rowCount             = hdr->rowCount;
	uint32 compressionType      = hdr->compressionType;
	uint32 uncompressedDataSize = hdr->uncompressedDataSize;

	/* Decompress */
	char *batchData;
	if (compressionType != COMPRESSION_NONE)
	{
		StringInfoData compBuf = {storedData, storedDataSize, storedDataSize, 0};
		StringInfo decompBuf = DecompressBuffer(&compBuf,
												(CompressionType) compressionType,
												uncompressedDataSize);
		batchData = palloc(decompBuf->len);
		memcpy(batchData, decompBuf->data, decompBuf->len);
		pfree(decompBuf->data);
		pfree(decompBuf);
	}
	else
	{
		batchData = palloc(storedDataSize > 0 ? storedDataSize : 1);
		memcpy(batchData, storedData, storedDataSize);
	}

	uint32 *offsets = palloc(offsetsBytes > 0 ? offsetsBytes : 1);
	memcpy(offsets, storedOffsets, offsetsBytes);

	pfree(rawData);

	/* Populate cache entry */
	entry->batchNum            = meta->batchNum;
	entry->batchData           = batchData;
	entry->rowOffsets          = offsets;
	entry->batchRowCount       = rowCount;
	entry->batchFirstRowNumber = meta->firstRowNumber;
	entry->lastUsed            = ++fetch->lruClock;

	return entry;
}

/*
 * RCDeleteBatches removes all batch metadata rows for the given storageId.
 */
static void
RCDeleteBatches(uint64 storageId)
{
	Oid batchOid   = RCBatchRelationId();
	Oid pkeyIdxOid = RCBatchPkeyIndexId();

	if (!OidIsValid(batchOid))
		return;  /* schema/table not yet created — nothing to clean up */

	Relation batchRel = table_open(batchOid, RowExclusiveLock);

	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0],
				Anum_rowcompress_batch_storage_id,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum((int64) storageId));

	bool indexOk = (pkeyIdxOid != InvalidOid);
	SysScanDesc scan = systable_beginscan(batchRel, pkeyIdxOid, indexOk,
										  SnapshotSelf, 1, scanKey);

	HeapTuple tup;
	while ((tup = systable_getnext(scan)) != NULL)
	{
		bool isNull;
		uint64 sid = DatumGetUInt64(
			heap_getattr(tup, Anum_rowcompress_batch_storage_id,
						 RelationGetDescr(batchRel), &isNull));
		if (sid != storageId)
			break;
		CatalogTupleDelete(batchRel, &tup->t_self);
	}

	systable_endscan(scan);
	table_close(batchRel, RowExclusiveLock);
}


/* ================================================================
 * METADATA: options table access
 * ================================================================ */

/*
 * RCInitOptions inserts default options for a newly created rowcompress table.
 */
static void
RCInitOptions(Oid relationId)
{
	Datum values[Natts_rowcompress_options];
	bool  nulls[Natts_rowcompress_options];

	memset(nulls, false, sizeof(nulls));

	/* Default batch size */
	values[Anum_rowcompress_options_regclass - 1]           = ObjectIdGetDatum(relationId);
	values[Anum_rowcompress_options_batch_size - 1]         = Int32GetDatum(ROWCOMPRESS_DEFAULT_BATCH_SIZE);
	values[Anum_rowcompress_options_compression - 1]        = CStringGetDatum(
		CompressionTypeStr(ROWCOMPRESS_DEFAULT_COMPRESSION));
	values[Anum_rowcompress_options_compression_level - 1]  = Int32GetDatum(ROWCOMPRESS_DEFAULT_COMPRESSION_LEVEL);

	Relation optRel  = table_open(RCOptionsRelationId(), RowExclusiveLock);
	TupleDesc tupdesc = RelationGetDescr(optRel);
	HeapTuple tuple   = heap_form_tuple(tupdesc, values, nulls);

	CatalogTupleInsert(optRel, tuple);

	heap_freetuple(tuple);
	table_close(optRel, RowExclusiveLock);
}

/*
 * RCReadOptions reads the options for a rowcompress table.
 * Returns false (and fills defaults) if no row found.
 */
static bool
RCReadOptions(Oid relationId, RowCompressOptions *options)
{
	/* Fill defaults first */
	options->batchSize        = ROWCOMPRESS_DEFAULT_BATCH_SIZE;
	options->compression      = ROWCOMPRESS_DEFAULT_COMPRESSION;
	options->compressionLevel = ROWCOMPRESS_DEFAULT_COMPRESSION_LEVEL;

	Oid optOid = RCOptionsRelationId();
	if (!OidIsValid(optOid))
		return false;

	Relation optRel = table_open(optOid, AccessShareLock);

	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0],
				Anum_rowcompress_options_regclass,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	Oid pkeyIdx = get_relname_relid("row_options_pkey", RCNamespaceId());
	bool indexOk = OidIsValid(pkeyIdx);
	SysScanDesc scan = systable_beginscan(optRel, pkeyIdx, indexOk,
										  GetTransactionSnapshot(), 1, scanKey);

	HeapTuple tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
	{
		bool isNull;
		TupleDesc tupdesc = RelationGetDescr(optRel);

		options->batchSize = DatumGetInt32(
			heap_getattr(tup, Anum_rowcompress_options_batch_size, tupdesc, &isNull));

		Datum compDatum = heap_getattr(tup, Anum_rowcompress_options_compression, tupdesc, &isNull);
		const char *compStr = NameStr(*(Name) DatumGetPointer(compDatum));
		/* Parse compression type name */
		if (strcmp(compStr, "none") == 0)
			options->compression = COMPRESSION_NONE;
		else if (strcmp(compStr, "pglz") == 0)
			options->compression = COMPRESSION_PG_LZ;
		else if (strcmp(compStr, "lz4") == 0)
			options->compression = COMPRESSION_LZ4;
		else if (strcmp(compStr, "zstd") == 0)
			options->compression = COMPRESSION_ZSTD;
		else
			options->compression = ROWCOMPRESS_DEFAULT_COMPRESSION;

		options->compressionLevel = DatumGetInt32(
			heap_getattr(tup, Anum_rowcompress_options_compression_level, tupdesc, &isNull));

		/* Read pruning_attnum (nullable int2, added in v1.1) */
		options->pruningAttnum = 0;
		if (tupdesc->natts >= Anum_rowcompress_options_pruning_attnum)
		{
			Datum paDatum = heap_getattr(tup, Anum_rowcompress_options_pruning_attnum,
										 tupdesc, &isNull);
			if (!isNull)
				options->pruningAttnum = DatumGetInt16(paDatum);
		}
	}

	systable_endscan(scan);
	table_close(optRel, AccessShareLock);

	return true;
}

/*
 * RCDeleteOptions removes the options row for a rowcompress table.
 */
static void
RCDeleteOptions(Oid relationId, bool missingOk)
{
	Oid optOid = RCOptionsRelationId();
	if (!OidIsValid(optOid))
	{
		if (!missingOk)
			elog(ERROR, "engine.row_options table not found");
		return;
	}

	Relation optRel = table_open(optOid, RowExclusiveLock);

	ScanKeyData scanKey[1];
	ScanKeyInit(&scanKey[0],
				Anum_rowcompress_options_regclass,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	Oid pkeyIdx = get_relname_relid("row_options_pkey", RCNamespaceId());
	bool indexOk = OidIsValid(pkeyIdx);
	SysScanDesc scan = systable_beginscan(optRel, pkeyIdx, indexOk,
										  SnapshotSelf, 1, scanKey);

	HeapTuple tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
		CatalogTupleDelete(optRel, &tup->t_self);
	else if (!missingOk)
		elog(ERROR, "rowcompress options not found for relation %u", relationId);

	systable_endscan(scan);
	table_close(optRel, RowExclusiveLock);
}

/*
 * RCSetOptions updates (or inserts) the options row for a rowcompress table.
 * Performs a delete + re-insert so we don't need a separate UPDATE path.
 */
static void
RCSetOptions(Oid relationId, const RowCompressOptions *options)
{
	/* Remove the existing row if present */
	RCDeleteOptions(relationId, true /* missingOk */);

	/* Insert the new row with the provided options */
	Datum values[Natts_rowcompress_options];
	bool  nulls[Natts_rowcompress_options];

	memset(nulls, false, sizeof(nulls));

	values[Anum_rowcompress_options_regclass - 1]           = ObjectIdGetDatum(relationId);
	values[Anum_rowcompress_options_batch_size - 1]         = Int32GetDatum(options->batchSize);
	values[Anum_rowcompress_options_compression - 1]        = CStringGetDatum(
		CompressionTypeStr(options->compression));
	values[Anum_rowcompress_options_compression_level - 1]  = Int32GetDatum(options->compressionLevel);

	if (options->pruningAttnum > 0)
		values[Anum_rowcompress_options_pruning_attnum - 1] = Int16GetDatum(options->pruningAttnum);
	else
		nulls[Anum_rowcompress_options_pruning_attnum - 1]  = true;

	Relation  optRel  = table_open(RCOptionsRelationId(), RowExclusiveLock);
	TupleDesc tupdesc = RelationGetDescr(optRel);
	HeapTuple tuple   = heap_form_tuple(tupdesc, values, nulls);

	CatalogTupleInsert(optRel, tuple);

	heap_freetuple(tuple);
	table_close(optRel, RowExclusiveLock);
}


/* ================================================================
 * WRITE STATE MANAGEMENT
 * ================================================================ */

/*
 * RCStorageId returns the storage ID for a rowcompress relation
 * by reading it from the smgr file header.
 */
static uint64
RCStorageId(Relation rel)
{
	return ColumnarStorageGetStorageId(rel, false);
}

/*
 * RCDetoastValues detoasts any extended (TOAST) values in the datum array.
 * Returns the original array if no detoasting is needed.
 */
static Datum *
RCDetoastValues(TupleDesc tupdesc, Datum *orig_values, bool *isnull)
{
	int    natts  = tupdesc->natts;
	Datum *values = orig_values;

	for (int i = 0; i < natts; i++)
	{
		if (!isnull[i] &&
			TupleDescAttr(tupdesc, i)->attlen == -1 &&
			VARATT_IS_EXTENDED(values[i]))
		{
			if (values == orig_values)
			{
				values = palloc(sizeof(Datum) * natts);
				memcpy(values, orig_values, sizeof(Datum) * natts);
			}

			struct varlena *new_val = (struct varlena *) DatumGetPointer(values[i]);
			new_val = detoast_attr(new_val);
			values[i] = PointerGetDatum(new_val);
		}
	}

	return values;
}

/*
 * RCFindWriteState looks up the write state for a given relfilelocator.
 */
static RowCompressWriteState *
RCFindWriteState(RelFileLocator relfilelocator)
{
	ListCell *lc;
	foreach(lc, RCWriteStateList)
	{
		RowCompressWriteState *ws = (RowCompressWriteState *) lfirst(lc);
		if (RelFileLocatorEquals(ws->relfilelocator, relfilelocator))
			return ws;
	}
	return NULL;
}

/*
 * RCGetOrCreateWriteState returns the write state for rel, creating it
 * if needed.
 */
static RowCompressWriteState *
RCGetOrCreateWriteState(Relation rel)
{
#if PG_VERSION_NUM >= PG_VERSION_16
	RelFileLocator rfl = rel->rd_locator;
#else
	RelFileLocator rfl = rel->rd_node;
#endif

	RowCompressWriteState *ws = RCFindWriteState(rfl);
	if (ws != NULL)
		return ws;

	/* Lazy-create the write state context under TopTransactionContext */
	if (RCWriteStateContext == NULL)
	{
		RCWriteStateContext = AllocSetContextCreate(TopTransactionContext,
													"RowCompress WriteState",
													ALLOCSET_DEFAULT_SIZES);
	}

	MemoryContext oldCtx = MemoryContextSwitchTo(RCWriteStateContext);

	ws = palloc0(sizeof(RowCompressWriteState));
	ws->relfilelocator = rfl;
	ws->storageId      = RCStorageId(rel);
	ws->tupdesc        = CreateTupleDescCopyConstr(RelationGetDescr(rel));

	RCReadOptions(RelationGetRelid(rel), &ws->options);

	ws->batchContext = AllocSetContextCreate(RCWriteStateContext,
											 "RowCompress Batch Data",
											 ALLOCSET_DEFAULT_SIZES);
	ws->rowList      = NIL;
	ws->rowCount     = 0;
	ws->firstRowNumber = 0; /* will be set when first row is received */

	RCWriteStateList = lappend(RCWriteStateList, ws);

	MemoryContextSwitchTo(oldCtx);

	return ws;
}

/* ================================================================
 * PRUNING HELPERS
 * ================================================================ */

/*
 * RCDatumToBytea — serialise a Datum to bytea so it can be stored in the
 * catalog.  For pass-by-value types we store the raw 8-byte Datum value;
 * for varlena types we store the detoasted content.
 */
static bytea *
RCDatumToBytea(Datum val, Form_pg_attribute attr)
{
	Size dataSize;
	bytea *result;

	if (attr->attbyval)
	{
		/* Store the 8-byte Datum directly */
		dataSize = sizeof(Datum);
		result   = (bytea *) palloc(VARHDRSZ + dataSize);
		SET_VARSIZE(result, VARHDRSZ + dataSize);
		memcpy(VARDATA(result), &val, dataSize);
	}
	else if (attr->attlen > 0)
	{
		/* Fixed-length pass-by-reference (e.g. interval, point) */
		dataSize = (Size) attr->attlen;
		result   = (bytea *) palloc(VARHDRSZ + dataSize);
		SET_VARSIZE(result, VARHDRSZ + dataSize);
		memcpy(VARDATA(result), DatumGetPointer(val), dataSize);
	}
	else
	{
		/* Variable-length varlena: detoast first */
		struct varlena *detoasted = PG_DETOAST_DATUM(val);
		dataSize = VARSIZE_ANY(detoasted);
		result   = (bytea *) palloc(dataSize);
		memcpy(result, detoasted, dataSize);
		if ((Pointer) detoasted != DatumGetPointer(val))
			pfree(detoasted);
	}

	return result;
}

/*
 * RCByteaToDatum — deserialise a bytea produced by RCDatumToBytea back to
 * a Datum.  The returned Datum is valid for the lifetime of the palloc.
 */
static Datum
RCByteaToDatum(bytea *b, Form_pg_attribute attr)
{
	if (attr->attbyval)
	{
		Datum val;
		memcpy(&val, VARDATA(b), sizeof(Datum));
		return val;
	}
	else if (attr->attlen > 0)
	{
		/* Return pointer into the bytea payload */
		return PointerGetDatum(VARDATA(b));
	}
	else
	{
		/* varlena: the whole bytea IS the value */
		return PointerGetDatum(b);
	}
}

/*
 * RCBuildBaseConstraint — build a reusable clause list of the form
 *   (var >= $min) AND (var <= $max)
 * The Const nodes are placeholders; RCUpdateConstraint fills in real values.
 *
 * Mirrors BuildBaseConstraint in engine_reader.c.
 */
static Node *
RCBuildBaseConstraint(Var *var)
{
	/*
	 * We build:
	 *   BoolExpr AND [
	 *     OpExpr (var >= minConst),
	 *     OpExpr (var <= maxConst)
	 *   ]
	 * where minConst / maxConst are placeholder Int4Const(0) nodes that
	 * RCUpdateConstraint replaces with real values before each pruning test.
	 */
	Oid typeOid = var->vartype;

	/* Look up >= and <= operators for this type's btree opclass */
	TypeCacheEntry *typeCache = lookup_type_cache(typeOid,
												  TYPECACHE_BTREE_OPFAMILY);
	if (!OidIsValid(typeCache->btree_opf))
		return NULL; /* no btree support; pruning impossible */

	Oid geOp = get_opfamily_member(typeCache->btree_opf, typeOid, typeOid,
								   BTGreaterEqualStrategyNumber);
	Oid leOp = get_opfamily_member(typeCache->btree_opf, typeOid, typeOid,
								   BTLessEqualStrategyNumber);

	if (!OidIsValid(geOp) || !OidIsValid(leOp))
		return NULL;

	Const *minConst = makeConst(typeOid, -1, var->varcollid, -1,
								(Datum) 0, true /* isnull */, var->vartype < FLOAT4OID);
	Const *maxConst = makeConst(typeOid, -1, var->varcollid, -1,
								(Datum) 0, true /* isnull */, var->vartype < FLOAT4OID);

	OpExpr *geClause = (OpExpr *) make_opclause(geOp, BOOLOID, false,
												 (Expr *) copyObject(var),
												 (Expr *) minConst,
												 InvalidOid, var->varcollid);
	set_opfuncid(geClause);

	OpExpr *leClause = (OpExpr *) make_opclause(leOp, BOOLOID, false,
												 (Expr *) copyObject(var),
												 (Expr *) maxConst,
												 InvalidOid, var->varcollid);
	set_opfuncid(leClause);

	return (Node *) make_andclause(list_make2(geClause, leClause));
}

/*
 * RCUpdateConstraint — overwrite the placeholder Const nodes inside the
 * baseConstraint built by RCBuildBaseConstraint with the real batch min/max.
 */
static void
RCUpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue)
{
	BoolExpr *andNode = (BoolExpr *) baseConstraint;
	Assert(IsA(andNode, BoolExpr) && andNode->boolop == AND_EXPR);

	OpExpr *geClause = (OpExpr *) linitial(andNode->args); /* var >= min */
	OpExpr *leClause = (OpExpr *) lsecond(andNode->args);  /* var <= max */

	Const *minConst = (Const *) lsecond(geClause->args);
	Const *maxConst = (Const *) lsecond(leClause->args);

	minConst->constvalue = minValue;
	minConst->constisnull = false;
	maxConst->constvalue = maxValue;
	maxConst->constisnull = false;
}

/*
 * RCBatchCanBePruned — returns true if the batch's min/max stats guarantee
 * that no row in the batch can satisfy the pushed-down WHERE clauses.
 */
static bool
RCBatchCanBePruned(RowCompressScanDesc *scan,
				   RowCompressBatchMetadata *meta,
				   TupleDesc tupdesc)
{
	RCPruningCtx *ctx = scan->pruningCtx;
	if (ctx == NULL || !meta->hasMinMax)
		return false;

	Form_pg_attribute attrForm =
		TupleDescAttr(tupdesc, ctx->pruningAttnum - 1);

	Datum batchMin = RCByteaToDatum(meta->rawMinValue, attrForm);
	Datum batchMax = RCByteaToDatum(meta->rawMaxValue, attrForm);

	RCUpdateConstraint(ctx->baseConstraint, batchMin, batchMax);

	return predicate_refuted_by(list_make1(ctx->baseConstraint),
								ctx->clauses, false);
}

/*
 * RCFlushBatch compresses and writes all buffered rows to disk,
 * then inserts a batch metadata row.
 */
static void
RCFlushBatch(RowCompressWriteState *ws, Relation rel)
{
	if (ws->rowCount == 0)
		return;

	/*
	 * Build the uncompressed data buffer.  Each row is stored as:
	 *   [uint64 t_len][char t_data[t_len]][alignment padding]
	 * and rowOffsets[i] = byte offset of row i's uint64 prefix in this buffer.
	 * Each offset is MAXALIGN'd so that the HeapTupleHeader (at +8) is aligned.
	 */
	StringInfoData uncompBuf;
	initStringInfo(&uncompBuf);

	uint32 *rowOffsets = palloc(ws->rowCount * sizeof(uint32));

	/*
	 * Set up min/max tracking for the pruning column, if configured.
	 * typeCache is NULL when pruning is disabled or the type has no btree cmp.
	 */
	bool hasMinMax = false;
	Datum batchMin = (Datum) 0;
	Datum batchMax = (Datum) 0;
	TypeCacheEntry *mmTypeCache  = NULL;
	Form_pg_attribute mmAttrForm = NULL;

	if (ws->options.pruningAttnum > 0 &&
		ws->tupdesc != NULL &&
		ws->options.pruningAttnum <= ws->tupdesc->natts)
	{
		mmAttrForm  = TupleDescAttr(ws->tupdesc, ws->options.pruningAttnum - 1);
		mmTypeCache = lookup_type_cache(mmAttrForm->atttypid,
										TYPECACHE_CMP_PROC_FINFO);
		if (!OidIsValid(mmTypeCache->cmp_proc))
			mmTypeCache = NULL; /* no comparator available */
	}

	ListCell *lc;
	int idx = 0;
	foreach(lc, ws->rowList)
	{
		RowDataEntry *entry = (RowDataEntry *) lfirst(lc);

		/* Track min/max for the pruning column */
		if (mmTypeCache != NULL)
		{
			HeapTupleData htup;
			htup.t_len  = entry->tupleLen;
			htup.t_data = (HeapTupleHeader) entry->tupleData;
			bool isnull;
			Datum val = heap_getattr(&htup, ws->options.pruningAttnum,
									 ws->tupdesc, &isnull);
			if (!isnull)
			{
				if (!hasMinMax)
				{
					batchMin = datumCopy(val, mmAttrForm->attbyval,
										 mmAttrForm->attlen);
					batchMax = datumCopy(val, mmAttrForm->attbyval,
										 mmAttrForm->attlen);
					hasMinMax = true;
				}
				else
				{
					int cmpMin = DatumGetInt32(
						FunctionCall2Coll(&mmTypeCache->cmp_proc_finfo,
										  mmAttrForm->attcollation,
										  val, batchMin));
					int cmpMax = DatumGetInt32(
						FunctionCall2Coll(&mmTypeCache->cmp_proc_finfo,
										  mmAttrForm->attcollation,
										  val, batchMax));
					if (cmpMin < 0)
						batchMin = datumCopy(val, mmAttrForm->attbyval,
											 mmAttrForm->attlen);
					if (cmpMax > 0)
						batchMax = datumCopy(val, mmAttrForm->attbyval,
											 mmAttrForm->attlen);
				}
			}
		}

		/* MAXALIGN the current write position */
		while (uncompBuf.len % MAXIMUM_ALIGNOF != 0)
			appendStringInfoChar(&uncompBuf, '\0');

		rowOffsets[idx++] = (uint32) uncompBuf.len;

		/* Write uint64 length prefix (8 bytes ensures t_data is MAXALIGN'd) */
		uint64 tlen64 = (uint64) entry->tupleLen;
		appendBinaryStringInfo(&uncompBuf, (char *) &tlen64, sizeof(uint64));

		/* Write tuple data */
		appendBinaryStringInfo(&uncompBuf, entry->tupleData, entry->tupleLen);
	}

	/* Compress the row data */
	StringInfoData compBuf;
	initStringInfo(&compBuf);

	bool compressed = CompressBuffer(&uncompBuf, &compBuf,
									 ws->options.compression,
									 ws->options.compressionLevel);

	StringInfo dataToStore     = compressed ? &compBuf    : &uncompBuf;
	int32      actualCompType  = compressed ? (int32) ws->options.compression
										   : (int32) COMPRESSION_NONE;

	/* Build on-disk header */
	RowCompressBatchHeader header;
	header.rowCount              = ws->rowCount;
	header.natts                 = ws->tupdesc->natts;
	header.uncompressedDataSize  = (uint32) uncompBuf.len;
	header.compressionType       = actualCompType;
	header.compressionLevel      = ws->options.compressionLevel;
	header._reserved             = 0;

	uint32 offsetsBytes = ws->rowCount * sizeof(uint32);
	uint64 totalSize    = sizeof(header) + offsetsBytes + (uint64) dataToStore->len;

	/* Reserve file space and a batch ID */
	uint64 batchId    = ColumnarStorageReserveStripeId(rel);
	uint64 fileOffset = ColumnarStorageReserveData(rel, totalSize);


/* Write header, row offsets, then data */
ColumnarStorageWrite(rel, fileOffset,
 (char *) &header, sizeof(header));
ColumnarStorageWrite(rel, fileOffset + sizeof(header),
 (char *) rowOffsets, offsetsBytes);
ColumnarStorageWrite(rel, fileOffset + sizeof(header) + offsetsBytes,
 dataToStore->data, dataToStore->len);

/* Insert metadata row (transactional; rolled back if transaction aborts) */
	bytea *minBytea = NULL;
	bytea *maxBytea = NULL;
	if (hasMinMax && mmAttrForm != NULL)
	{
		minBytea = RCDatumToBytea(batchMin, mmAttrForm);
		maxBytea = RCDatumToBytea(batchMax, mmAttrForm);
	}
	RCInsertBatchMetadata(ws->storageId, batchId,
						  ws->firstRowNumber, ws->rowCount,
						  fileOffset, totalSize,
						  minBytea, maxBytea);

pfree(rowOffsets);
pfree(uncompBuf.data);
if (compressed)
pfree(compBuf.data);

/* Reset the batch accumulator */
MemoryContextReset(ws->batchContext);
ws->rowList    = NIL;
ws->rowCount   = 0;
ws->firstRowNumber = 0;
}

/*
 * RCFlushAllWriteStates flushes all pending write states.
 */
static void
RCFlushAllWriteStates(void)
{
if (RCWriteStateList == NIL)
return;

ListCell *lc;
foreach(lc, RCWriteStateList)
{
RowCompressWriteState *ws = (RowCompressWriteState *) lfirst(lc);

if (ws->rowCount > 0)
{
#if PG_VERSION_NUM >= PG_VERSION_16
Oid relid = RelidByRelfilenumber(ws->relfilelocator.spcOid,
  ws->relfilelocator.relNumber);
#else
Oid relid = RelidByRelfilenode(ws->relfilelocator.spcNode,
   ws->relfilelocator.relNode);
(void)relid;
#endif
if (!OidIsValid(relid))
continue;

Relation rel = table_open(relid, RowExclusiveLock);
RCFlushBatch(ws, rel);
table_close(rel, RowExclusiveLock);
}
}

if (RCWriteStateContext)
{
MemoryContextDelete(RCWriteStateContext);
RCWriteStateContext = NULL;
}
RCWriteStateList = NIL;
}

/*
 * RCDiscardAllWriteStates discards unflushed write states on abort.
 */
static void
RCDiscardAllWriteStates(void)
{
if (RCWriteStateContext)
{
MemoryContextDelete(RCWriteStateContext);
RCWriteStateContext = NULL;
}
RCWriteStateList = NIL;
}


/* ================================================================
 * BATCH READING
 * ================================================================ */

/*
 * RCLoadBatch loads and decompresses a batch into the scan state.
 * Returns true on success.
 */
static bool
RCLoadBatch(RowCompressScanDesc *scan, RowCompressBatchMetadata *batch)
{
Relation rel = scan->rc_base.rs_rd;

/*
 * Read raw file data into a temporary buffer (current memory context).
 * Persistent buffers (batchData, offsets) are allocated via
 * MemoryContextAlloc(scan->scanContext) so they survive between
 * successive getnextslot() calls without context switching issues.
 *
 * IMPORTANT: save hdr->rowCount BEFORE pfree(rawData); PG's allocator
 * writes a freelist link to rawData[0..7] on pfree, corrupting hdr->rowCount
 * (at offset 0) if used afterwards.
 */
char *rawData = palloc(batch->dataLength);
ColumnarStorageRead(rel, batch->fileOffset, rawData, batch->dataLength);

RowCompressBatchHeader *hdr = (RowCompressBatchHeader *) rawData;

uint32 offsetsBytes   = hdr->rowCount * sizeof(uint32);
uint32 *storedOffsets = (uint32 *)(rawData + sizeof(RowCompressBatchHeader));
char   *storedData    = rawData + sizeof(RowCompressBatchHeader) + offsetsBytes;
uint32  storedDataSize = (uint32)(batch->dataLength -
   sizeof(RowCompressBatchHeader) - offsetsBytes);

/* Save before pfree(rawData) corrupts the header */
uint32 rowCount       = hdr->rowCount;
uint32 compressionType = hdr->compressionType;
uint32 uncompressedDataSize = hdr->uncompressedDataSize;
uint64 firstRowNumber = batch->firstRowNumber;

/* Decompress to temporary, then copy into scan->scanContext */
char  *batchData;
uint32 batchDataLen;

if (compressionType != COMPRESSION_NONE)
{
StringInfoData compBuf = {storedData, storedDataSize, storedDataSize, 0};

StringInfo decompBuf = DecompressBuffer(&compBuf,
(CompressionType) compressionType,
uncompressedDataSize);
batchDataLen = decompBuf->len;
batchData = MemoryContextAlloc(scan->scanContext, batchDataLen);
memcpy(batchData, decompBuf->data, batchDataLen);
pfree(decompBuf->data);
pfree(decompBuf);
}
else
{
batchDataLen = storedDataSize;
batchData = MemoryContextAlloc(scan->scanContext,
   batchDataLen > 0 ? batchDataLen : 1);
memcpy(batchData, storedData, batchDataLen);
}

/* Copy row offsets into scan context */
uint32 *offsets = MemoryContextAlloc(scan->scanContext,
  offsetsBytes > 0 ? offsetsBytes : 1);
memcpy(offsets, storedOffsets, offsetsBytes);

pfree(rawData);  /* safe now; all needed fields saved above */

/* Replace old buffers */
if (scan->batchData)   pfree(scan->batchData);
if (scan->rowOffsets)  pfree(scan->rowOffsets);

scan->batchData             = batchData;
scan->rowOffsets            = offsets;
scan->batchRowCount         = rowCount;
scan->currentRowIndex       = 0;
scan->currentFirstRowNumber = firstRowNumber;
scan->currentDeletedMask    = batch->deletedMask;
scan->currentDeletedMaskLen = batch->deletedMaskLen;

return true;
}

/*
 * RCExtractRow extracts the rowIndex-th row from a loaded batch into
 * values/isnull arrays.
 */
static void
RCExtractRow(TupleDesc tupdesc, char *batchData,
  uint32 *rowOffsets, uint32 rowIndex,
  Datum *values, bool *isnull)
{
uint32 offset = rowOffsets[rowIndex];

/* Read the uint64 length prefix */
uint64 tlen64;
memcpy(&tlen64, batchData + offset, sizeof(uint64));
uint32 tlen = (uint32) tlen64;

/*
 * Point HeapTupleHeader directly into batchData (no copy).
 * batchData is MAXALIGN'd from MemoryContextAlloc, rowOffsets[i] is
 * MAXALIGN'd in the write path, and sizeof(uint64)==8 preserves
 * alignment. Using a direct pointer avoids a pfree that would leave
 * by-reference Datums (e.g., text) dangling.
 * batchData lives in scan->scanContext and survives between rows.
 */
HeapTupleData htup;
htup.t_len  = tlen;
htup.t_data = (HeapTupleHeader)(batchData + offset + sizeof(uint64));
ItemPointerSetInvalid(&htup.t_self);
htup.t_tableOid = InvalidOid;

heap_deform_tuple(&htup, tupdesc, values, isnull);
}

/*
 * RCTotalRowCount returns the total row count for a rowcompress table
 * by summing all batches.
 */
static uint64
RCTotalRowCount(uint64 storageId)
{
List     *batches = RCGetBatches(storageId);
uint64    total   = 0;
ListCell *lc;

foreach(lc, batches)
{
RowCompressBatchMetadata *b = (RowCompressBatchMetadata *) lfirst(lc);
total += b->rowCount;
}

list_free_deep(batches);
return total;
}


/* ================================================================
 * XACT CALLBACKS
 * ================================================================ */

static void
RowCompressXactCallback(XactEvent event, void *arg)
{
switch (event)
{
case XACT_EVENT_PRE_COMMIT:
case XACT_EVENT_PARALLEL_PRE_COMMIT:
case XACT_EVENT_PRE_PREPARE:
RCFlushAllWriteStates();
break;

case XACT_EVENT_ABORT:
case XACT_EVENT_PARALLEL_ABORT:
RCDiscardAllWriteStates();
break;

default:
break;
}
}

static void
RowCompressSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
   SubTransactionId parentSubid, void *arg)
{
switch (event)
{
case SUBXACT_EVENT_PRE_COMMIT_SUB:
RCFlushAllWriteStates();
break;

case SUBXACT_EVENT_ABORT_SUB:
RCDiscardAllWriteStates();
break;

default:
break;
}
}


/* ================================================================
 * TAM CALLBACKS
 * ================================================================ */

static const TupleTableSlotOps *
rowcompress_slot_callbacks(Relation relation)
{
return &TTSOpsVirtual;
}

static TableScanDesc
rowcompress_beginscan(Relation relation, Snapshot snapshot,
  int nkeys, ScanKey key,
  ParallelTableScanDesc parallel_scan,
  uint32 flags)
{
MemoryContext scanCtx = AllocSetContextCreate(CurrentMemoryContext,
  "RowCompress Scan",
  ALLOCSET_DEFAULT_SIZES);
MemoryContext oldCtx  = MemoryContextSwitchTo(scanCtx);

RowCompressScanDesc *scan = palloc0(sizeof(RowCompressScanDesc));
scan->rc_base.rs_rd        = relation;
scan->rc_base.rs_snapshot  = snapshot;
scan->rc_base.rs_nkeys     = nkeys;
scan->rc_base.rs_key       = key;
scan->rc_base.rs_flags     = flags;
scan->rc_base.rs_parallel  = parallel_scan;

scan->storageId        = RCStorageId(relation);
RCReadOptions(RelationGetRelid(relation), &scan->options);

scan->batchList        = RCGetBatches(scan->storageId);
scan->currentBatchCell = list_head(scan->batchList);

scan->batchData         = NULL;
scan->rowOffsets        = NULL;
scan->batchRowCount     = 0;
scan->currentRowIndex   = 0;
scan->currentFirstRowNumber = 0;
	scan->bitmapFetch       = NULL;

	/* Compute blocks-per-batch ratio for use in scan_analyze_next_block */
	{
		BlockNumber totalBlocks = smgrnblocks(RelationGetSmgr(relation), MAIN_FORKNUM);
		int numBatches = list_length(scan->batchList);
		if (numBatches > 0 && totalBlocks > 0)
			scan->analyzeBlocksPerBatch = Max(1, (uint32)(totalBlocks / numBatches));
		else
			scan->analyzeBlocksPerBatch = 1;
	}

#if PG_VERSION_NUM >= PG_VERSION_18
	scan->bmHavePage        = false;
	scan->bmNoffsets        = 0;
	scan->bmOffsets         = NULL;
#endif
	scan->bmOffidx          = 0;
	scan->bmRownum          = 0;
	scan->bmRownumEnd       = 0;

scan->scanContext = scanCtx;

MemoryContextSwitchTo(oldCtx);

return (TableScanDesc) scan;
}


static void
rowcompress_endscan(TableScanDesc sscan)
{
RowCompressScanDesc *scan = (RowCompressScanDesc *) sscan;
	if (scan->bitmapFetch)
		rowcompress_index_fetch_end((IndexFetchTableData *) scan->bitmapFetch);

if (scan->rc_base.rs_flags & SO_TEMP_SNAPSHOT)
UnregisterSnapshot(scan->rc_base.rs_snapshot);

MemoryContextDelete(scan->scanContext);
}


static void
rowcompress_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
RowCompressScanDesc *scan = (RowCompressScanDesc *) sscan;

if (scan->batchList)
list_free_deep(scan->batchList);

if (scan->batchData)  { pfree(scan->batchData);  scan->batchData  = NULL; }
if (scan->rowOffsets) { pfree(scan->rowOffsets); scan->rowOffsets = NULL; }

scan->batchList         = RCGetBatches(scan->storageId);
scan->currentBatchCell  = list_head(scan->batchList);
scan->batchRowCount     = 0;
scan->currentRowIndex   = 0;
	if (scan->bitmapFetch)
		rowcompress_index_fetch_reset((IndexFetchTableData *) scan->bitmapFetch);
#if PG_VERSION_NUM >= PG_VERSION_18
	scan->bmHavePage = false;
#endif
	scan->bmOffidx   = 0;
	scan->bmRownum   = 0;
	scan->bmRownumEnd = 0;
}


static bool
rowcompress_getnextslot(TableScanDesc sscan, ScanDirection direction,
TupleTableSlot *slot)
{
RowCompressScanDesc *scan = (RowCompressScanDesc *) sscan;

if (direction == BackwardScanDirection)
elog(ERROR, "rowcompress tables do not support backward scans");

/* Ensure the slot has a tuple descriptor (may be NULL when created by table_slot_create) */
if (unlikely(slot->tts_tupleDescriptor == NULL))
	ExecSetSlotDescriptor(slot, RelationGetDescr(scan->rc_base.rs_rd));

ExecClearTuple(slot);

for (;;)
{
if (scan->batchRowCount == 0 || scan->currentRowIndex >= scan->batchRowCount)
{
	RowCompressBatchMetadata *meta;
	TupleDesc td = RelationGetDescr(scan->rc_base.rs_rd);

	if (scan->rc_base.rs_parallel != NULL)
	{
		/* Parallel mode: atomically claim batch indices, skipping prunable ones */
		RowCompressParallelScanDesc pdscan =
			(RowCompressParallelScanDesc) scan->rc_base.rs_parallel;
		for (;;)
		{
			uint64 batchIdx =
				pg_atomic_fetch_add_u64(&pdscan->rc_next_batch_idx, 1);
			if (batchIdx >= (uint64) list_length(scan->batchList))
				return false;
			meta = (RowCompressBatchMetadata *)
				list_nth(scan->batchList, (int) batchIdx);
			if (!RCBatchCanBePruned(scan, meta, td))
				break;
			scan->batchesPruned++;
		}
	}
	else
	{
		/* Sequential mode: advance list cursor, skipping prunable batches */
		for (;;)
		{
			if (scan->currentBatchCell == NULL)
				return false;
			meta = (RowCompressBatchMetadata *) lfirst(scan->currentBatchCell);
			scan->currentBatchCell = lnext(scan->batchList, scan->currentBatchCell);

			if (!RCBatchCanBePruned(scan, meta, td))
				break;
			scan->batchesPruned++;
		}

		/* Async prefetch for the next (not-yet-decided) batch */
		if (scan->currentBatchCell != NULL)
		{
			RowCompressBatchMetadata *nextMeta =
				(RowCompressBatchMetadata *) lfirst(scan->currentBatchCell);
			ColumnarStoragePrefetch(scan->rc_base.rs_rd,
									nextMeta->fileOffset,
									(uint64) nextMeta->dataLength);
		}
	}

	if (!RCLoadBatch(scan, meta))
		return false;
}

Assert(scan->currentRowIndex < scan->batchRowCount);

/* Skip rows marked as deleted */
if (RC_IS_DELETED(scan->currentDeletedMask,
				  scan->currentDeletedMaskLen,
				  scan->currentRowIndex))
{
	scan->currentRowIndex++;
	continue;
}

uint64 rowNumber = scan->currentFirstRowNumber + scan->currentRowIndex;

RCExtractRow(slot->tts_tupleDescriptor,
 scan->batchData, scan->rowOffsets,
 scan->currentRowIndex,
 slot->tts_values, slot->tts_isnull);

scan->currentRowIndex++;

slot->tts_tableOid = RelationGetRelid(scan->rc_base.rs_rd);
slot->tts_tid      = row_number_to_tid(rowNumber);
ExecStoreVirtualTuple(slot);

return true;
}
}


static Size
rowcompress_parallelscan_estimate(Relation rel)
{
	return sizeof(RowCompressParallelScanDescData);
}

static Size
rowcompress_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	RowCompressParallelScanDesc rcscan = (RowCompressParallelScanDesc) pscan;
	pg_atomic_init_u64(&rcscan->rc_next_batch_idx, 0);
	return sizeof(RowCompressParallelScanDescData);
}

static void
rowcompress_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	RowCompressParallelScanDesc rcscan = (RowCompressParallelScanDesc) pscan;
	pg_atomic_write_u64(&rcscan->rc_next_batch_idx, 0);
}


/*
 * rc_tid_to_row_number converts a TID to a row number (same encoding as columnar).
 */
static inline uint64
rc_tid_to_row_number(ItemPointerData tid)
{
	return ItemPointerGetBlockNumber(&tid) * VALID_ITEMPOINTER_OFFSETS +
		   ItemPointerGetOffsetNumber(&tid) - FirstOffsetNumber;
}


/* ================================================================
 * INDEX FETCH
 * ================================================================ */

static IndexFetchTableData *
rowcompress_index_fetch_begin(Relation rel)
{
	IndexFetchRowCompressData *fetch =
		palloc0(sizeof(IndexFetchRowCompressData));
	fetch->rc_base.rel    = rel;
	fetch->storageId      = RCStorageId(rel);
	fetch->batchMetaArray = NULL;
	fetch->batchMetaCount = 0;
	fetch->lruClock       = 0;

	/* Mark all cache slots as empty */
	for (int i = 0; i < RC_DECOMP_CACHE_MAX; i++)
		fetch->decompCache[i].batchNum = UINT64_MAX;

	RCReadOptions(RelationGetRelid(rel), &fetch->options);
	return (IndexFetchTableData *) fetch;
}

static void
rowcompress_index_fetch_reset(IndexFetchTableData *scan)
{
	IndexFetchRowCompressData *fetch = (IndexFetchRowCompressData *) scan;

	/*
	 * Free decompressed batch data but keep the metadata cache: the batch
	 * metadata does not change within a query, so reusing it across
	 * rescan() calls is safe and avoids re-reading the catalog.
	 */
	for (int i = 0; i < RC_DECOMP_CACHE_MAX; i++)
	{
		if (fetch->decompCache[i].batchData)
		{
			pfree(fetch->decompCache[i].batchData);
			fetch->decompCache[i].batchData = NULL;
		}
		if (fetch->decompCache[i].rowOffsets)
		{
			pfree(fetch->decompCache[i].rowOffsets);
			fetch->decompCache[i].rowOffsets = NULL;
		}
		fetch->decompCache[i].batchNum = UINT64_MAX;
	}
}

static void
rowcompress_index_fetch_end(IndexFetchTableData *scan)
{
	IndexFetchRowCompressData *fetch = (IndexFetchRowCompressData *) scan;
	rowcompress_index_fetch_reset(scan);
	if (fetch->batchMetaArray)
		pfree(fetch->batchMetaArray);
	pfree(fetch);
}

static bool
rowcompress_index_fetch_tuple(struct IndexFetchTableData *scan,
							  ItemPointer tid,
							  Snapshot snapshot,
							  TupleTableSlot *slot,
							  bool *call_again, bool *all_dead)
{
	IndexFetchRowCompressData *fetch = (IndexFetchRowCompressData *) scan;
	Relation rel = fetch->rc_base.rel;

	uint64 rowNumber = rc_tid_to_row_number(*tid);

	/*
	 * Load batch metadata from the catalog once per scan, then use
	 * binary search for every subsequent TID lookup.
	 */
	RCEnsureBatchMetaCache(fetch, rel);

	RowCompressBatchMetadata *meta = RCBinaryFindBatch(fetch, rowNumber);
	if (meta == NULL)
		return false;

	/*
	 * Get the decompressed batch from the LRU cache (or decompress it
	 * now and add it to the cache).
	 */
	RCDecompCacheEntry *entry = RCGetOrLoadBatch(fetch, rel, meta);
	if (entry == NULL)
		return false;

	uint32 rowIndex = (uint32)(rowNumber - entry->batchFirstRowNumber);
	if (rowIndex >= entry->batchRowCount)
		return false;

	/* Skip if this row was deleted */
	if (RC_IS_DELETED(meta->deletedMask, meta->deletedMaskLen, rowIndex))
	{
		if (all_dead)   *all_dead   = true;
		if (call_again) *call_again = false;
		return false;
	}

	ExecClearTuple(slot);
	RCExtractRow(slot->tts_tupleDescriptor,
				 entry->batchData, entry->rowOffsets, rowIndex,
				 slot->tts_values, slot->tts_isnull);

	slot->tts_tableOid = RelationGetRelid(rel);
	slot->tts_tid      = *tid;
	ExecStoreVirtualTuple(slot);

	if (all_dead)   *all_dead   = false;
	if (call_again) *call_again = false;

	return true;
}


/* ================================================================
 * DML stubs (rowcompress is append-only; UPDATE/DELETE are no-ops)
 * ================================================================ */

static bool
rowcompress_fetch_row_version(Relation relation,
							  ItemPointer tid,
							  Snapshot snapshot,
							  TupleTableSlot *slot)
{
	IndexFetchRowCompressData fetch;
	memset(&fetch, 0, sizeof(fetch));
	fetch.rc_base.rel    = relation;
	fetch.storageId      = RCStorageId(relation);
	fetch.batchMetaArray = NULL;
	fetch.batchMetaCount = 0;
	fetch.lruClock       = 0;
	for (int i = 0; i < RC_DECOMP_CACHE_MAX; i++)
		fetch.decompCache[i].batchNum = UINT64_MAX;
	RCReadOptions(RelationGetRelid(relation), &fetch.options);

	bool found = rowcompress_index_fetch_tuple(&fetch.rc_base, tid, snapshot,
											   slot, NULL, NULL);

	/* Free any data allocated during the single-TID fetch */
	for (int i = 0; i < RC_DECOMP_CACHE_MAX; i++)
	{
		if (fetch.decompCache[i].batchData)  pfree(fetch.decompCache[i].batchData);
		if (fetch.decompCache[i].rowOffsets) pfree(fetch.decompCache[i].rowOffsets);
	}
	if (fetch.batchMetaArray) pfree(fetch.batchMetaArray);

	return found;
}

static void
rowcompress_get_latest_tid(TableScanDesc sscan, ItemPointer tid)
{
/* No updates in rowcompress; tid is always current */
}

static bool
rowcompress_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
return true;
}

static bool
rowcompress_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
  Snapshot snapshot)
{
	ItemPointerData tid = slot->tts_tid;
	uint64 rowNumber  = rc_tid_to_row_number(tid);
	uint64 storageId  = RCStorageId(rel);

	List *batches = RCGetBatches(storageId);
	RowCompressBatchMetadata *meta = RCFindBatchInList(batches, rowNumber);

	if (meta == NULL)
	{
		list_free_deep(batches);
		return false;
	}

	uint32 rowOffset = (uint32)(rowNumber - meta->firstRowNumber);
	bool deleted = RC_IS_DELETED(meta->deletedMask, meta->deletedMaskLen, rowOffset);

	list_free_deep(batches);
	return !deleted;
}

static TransactionId
rowcompress_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
return InvalidTransactionId;
}

#if PG_VERSION_NUM < PG_VERSION_14
static TransactionId
rowcompress_compute_xid_horizon_for_tuples(Relation rel,
   ItemPointerData *items,
   int nitems)
{
return InvalidTransactionId;
}
#endif


/* ================================================================
 * INSERT
 * ================================================================ */

static void
rowcompress_tuple_insert(Relation relation, TupleTableSlot *slot,
  CommandId cid, int options, BulkInsertState bistate)
{
RowCompressWriteState *ws = RCGetOrCreateWriteState(relation);

/* Convert virtual slot to a real heap tuple */
bool shouldFree = false;
HeapTuple htup = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

/* Detoast any toasted attributes */
Datum *detoasted = RCDetoastValues(ws->tupdesc, slot->tts_values, slot->tts_isnull);

/*
 * Rebuild the heap tuple with detoasted values so on-disk data has no
 * TOAST pointers.
 */
HeapTuple clean = heap_form_tuple(ws->tupdesc, detoasted, slot->tts_isnull);

/* Only free the detoasted array if it was freshly allocated (not the slot's own array) */
if (detoasted != slot->tts_values)
	pfree(detoasted);
if (shouldFree)
heap_freetuple(htup);

MemoryContext oldCtx = MemoryContextSwitchTo(ws->batchContext);

/* Reserve a row number */
if (ws->firstRowNumber == 0)
ws->firstRowNumber = ColumnarStorageReserveRowNumber(relation, 1);
else
ColumnarStorageReserveRowNumber(relation, 1);

/* Store the tuple in the batch */
RowDataEntry *entry = palloc(sizeof(RowDataEntry));
entry->tupleLen  = clean->t_len;
entry->tupleData = palloc(clean->t_len);
memcpy(entry->tupleData, clean->t_data, clean->t_len);

ws->rowList  = lappend(ws->rowList, entry);
ws->rowCount++;

MemoryContextSwitchTo(oldCtx);

/* Set TID for RETURNING */
uint64 rowNumber = ws->firstRowNumber + ws->rowCount - 1;
slot->tts_tid    = row_number_to_tid(rowNumber);

heap_freetuple(clean);

/* Flush if batch is full */
if (ws->rowCount >= ws->options.batchSize)
RCFlushBatch(ws, relation);
}

static void
rowcompress_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
  CommandId cid, int options,
  BulkInsertState bistate,
  uint32 specToken)
{
rowcompress_tuple_insert(relation, slot, cid, options, bistate);
}

static void
rowcompress_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
   uint32 specToken, bool succeeded)
{
/* Nothing to do */
}

static void
rowcompress_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
  CommandId cid, int options, BulkInsertState bistate)
{
for (int i = 0; i < ntuples; i++)
rowcompress_tuple_insert(relation, slots[i], cid, options, bistate);
}

/* ================================================================
 * DELETE / UPDATE helpers
 * ================================================================ */

/*
 * RCMarkRowDeleted sets the deletion bit for rowOffset in the engine.row_batch
 * catalog row identified by (storageId, batchNum).  If the row has no mask yet
 * a fresh zeroed-out mask is created; otherwise the existing mask is extended
 * and the bit is set.
 */
static void
RCMarkRowDeleted(uint64 storageId, uint64 batchNum, uint32 rowCount, uint32 rowOffset)
{
	Assert(rowOffset < rowCount);

	Oid batchOid   = RCBatchRelationId();
	Oid pkeyIdxOid = RCBatchPkeyIndexId();

	Relation  batchRel = table_open(batchOid, RowExclusiveLock);
	TupleDesc tupdesc  = RelationGetDescr(batchRel);

	/* Look up the catalog row by (storage_id, batch_num) pkey */
	ScanKeyData scanKey[2];
	ScanKeyInit(&scanKey[0], Anum_rowcompress_batch_storage_id,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum((int64) storageId));
	ScanKeyInit(&scanKey[1], Anum_rowcompress_batch_batch_num,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum((int64) batchNum));

	SysScanDesc scan = systable_beginscan(batchRel, pkeyIdxOid, true,
										  GetTransactionSnapshot(), 2, scanKey);
	HeapTuple tup = systable_getnext(scan);

	if (!HeapTupleIsValid(tup))
	{
		systable_endscan(scan);
		table_close(batchRel, RowExclusiveLock);
		elog(ERROR, "rowcompress: batch (%lu, %lu) not found during delete",
			 storageId, batchNum);
	}

	/* Build (or extend) the deleted_mask bytea */
	uint32 maskLen = (rowCount + 7) / 8;
	bytea *newMask = (bytea *) palloc0(VARHDRSZ + maskLen);
	SET_VARSIZE(newMask, VARHDRSZ + maskLen);
	uint8 *bits = (uint8 *) VARDATA(newMask);

	/* Copy any existing mask bits */
	bool isNull;
	Datum existingDatum = heap_getattr(tup, Anum_rowcompress_batch_deleted_mask,
									   tupdesc, &isNull);
	if (!isNull)
	{
		bytea *existing  = DatumGetByteaP(existingDatum);
		uint32 existLen  = VARSIZE_ANY_EXHDR(existing);
		memcpy(bits, VARDATA_ANY(existing), Min(existLen, maskLen));
	}

	/* Set the bit */
	bits[rowOffset / 8] |= (1u << (rowOffset % 8));

	/* Update the catalog row */
	Datum  newValues[Natts_rowcompress_batch];
	bool   newIsNull[Natts_rowcompress_batch];
	bool   doReplace[Natts_rowcompress_batch];
	memset(newValues,  0,     sizeof(newValues));
	memset(newIsNull,  false, sizeof(newIsNull));
	memset(doReplace,  false, sizeof(doReplace));

	newValues[Anum_rowcompress_batch_deleted_mask - 1] = PointerGetDatum(newMask);
	doReplace[Anum_rowcompress_batch_deleted_mask - 1] = true;

	HeapTuple newTup = heap_modify_tuple(tup, tupdesc, newValues, newIsNull, doReplace);
	CatalogTupleUpdate(batchRel, &tup->t_self, newTup);

	/*
	 * Make the deleted_mask update immediately visible to subsequent
	 * GetTransactionSnapshot() calls within the same transaction
	 * (e.g., unique constraint checks after UPDATE).
	 */
	CommandCounterIncrement();

	systable_endscan(scan);
	table_close(batchRel, RowExclusiveLock);
	heap_freetuple(newTup);
	pfree(newMask);
}

/*
 * RCFindBatchForRow scans the batch list (already loaded in RCGetBatches) and
 * returns the batch that contains rowNumber.
 */
static RowCompressBatchMetadata *
RCFindBatchInList(List *batches, uint64 rowNumber)
{
	ListCell *lc;
	foreach(lc, batches)
	{
		RowCompressBatchMetadata *m = (RowCompressBatchMetadata *) lfirst(lc);
		if (rowNumber >= m->firstRowNumber &&
			rowNumber <  m->firstRowNumber + m->rowCount)
			return m;
	}
	return NULL;
}

static TM_Result
rowcompress_tuple_delete(Relation relation, ItemPointer tid,
  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
  bool wait, TM_FailureData *tmfd, bool changingPart)
{
	uint64 rowNumber = rc_tid_to_row_number(*tid);
	uint64 storageId = RCStorageId(relation);

	/* Advisory lock: serialize concurrent DML on the same relation */
	DirectFunctionCall1(pg_advisory_xact_lock_int8,
						Int64GetDatum((int64) storageId));

	List *batches = RCGetBatches(storageId);
	RowCompressBatchMetadata *meta = RCFindBatchInList(batches, rowNumber);

	if (meta == NULL)
	{
		list_free_deep(batches);
		return TM_Deleted;
	}

	uint32 rowOffset = (uint32)(rowNumber - meta->firstRowNumber);
	RCMarkRowDeleted(storageId, meta->batchNum, meta->rowCount, rowOffset);

	pgstat_count_heap_delete(relation);
	list_free_deep(batches);
	return TM_Ok;
}

#if PG_VERSION_NUM >= PG_VERSION_16
static TM_Result
rowcompress_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
  bool wait, TM_FailureData *tmfd,
  LockTupleMode *lockmode, TU_UpdateIndexes *updateIndexes)
#else
static TM_Result
rowcompress_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
  bool wait, TM_FailureData *tmfd,
  LockTupleMode *lockmode, bool *updateIndexes)
#endif
{
	uint64 rowNumber = rc_tid_to_row_number(*otid);
	uint64 storageId = RCStorageId(relation);

	/* Advisory lock: serialize concurrent DML on the same relation */
	DirectFunctionCall1(pg_advisory_xact_lock_int8,
						Int64GetDatum((int64) storageId));

	List *batches = RCGetBatches(storageId);
	RowCompressBatchMetadata *meta = RCFindBatchInList(batches, rowNumber);

	if (meta == NULL)
	{
		list_free_deep(batches);
		return TM_Deleted;
	}

	uint32 rowOffset = (uint32)(rowNumber - meta->firstRowNumber);
	RCMarkRowDeleted(storageId, meta->batchNum, meta->rowCount, rowOffset);

	/* Insert the new version via the normal insert path */
	rowcompress_tuple_insert(relation, slot, cid, 0, NULL);

#if PG_VERSION_NUM >= PG_VERSION_16
	*updateIndexes = TU_All;
	pgstat_count_heap_update(relation, false, false);
#else
	*updateIndexes = true;
	pgstat_count_heap_update(relation, false);
#endif

	list_free_deep(batches);
	return TM_Ok;
}

static TM_Result
rowcompress_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
LockWaitPolicy wait_policy, uint8 flags,
TM_FailureData *tmfd)
{
	bool found = rowcompress_fetch_row_version(relation, tid, snapshot, slot);
	if (!found)
	{
		tmfd->cmax      = InvalidCommandId;
		tmfd->xmax      = InvalidTransactionId;
		tmfd->traversed = false;
		return TM_Deleted;
	}
	ExecMaterializeSlot(slot);  /* ensure values are safe to read */
	return TM_Ok;
}

static void
rowcompress_finish_bulk_insert(Relation relation, int options)
{
/* nothing to do; flush happens at commit via XactCallback */
}


/* ================================================================
 * DDL / STORAGE MANAGEMENT
 * ================================================================ */

static void
rowcompress_relation_set_new_filenode(Relation rel,
   const RelFileLocator *newrnode,
   char persistence,
   TransactionId *freezeXid,
   MultiXactId *minmulti)
{
*freezeXid = RecentXmin;
*minmulti  = GetOldestMultiXactId();

#if PG_VERSION_NUM >= PG_VERSION_15
SMgrRelation srel = RelationCreateStorage(*newrnode, persistence, true);
#else
SMgrRelation srel = RelationCreateStorage(*newrnode, persistence);
#endif

ColumnarStorageInit(srel, ColumnarMetadataNewStorageId());

/* Delete old options row if it exists (TRUNCATE reuses the same rel OID) */
RCDeleteOptions(rel->rd_id, true /* missingOk */);
RCInitOptions(rel->rd_id);

smgrclose(srel);
}


static void
rowcompress_relation_nontransactional_truncate(Relation rel)
{
uint64 storageId = ColumnarStorageGetStorageId(rel, false);
RCDeleteBatches(storageId);
RelationTruncate(rel, 0);

uint64 newStorageId = ColumnarMetadataNewStorageId();
EnsureRelationSmgrOpen(rel);
ColumnarStorageInit(rel->rd_smgr, newStorageId);
}


static void
rowcompress_relation_copy_data(Relation rel, const RelFileLocator *newrnode)
{
elog(ERROR, "rowcompress_relation_copy_data not implemented");
}

static void
rowcompress_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
   Relation OldIndex, bool use_sort,
   TransactionId OldestXmin,
   TransactionId *xid_cutoff,
   MultiXactId *multi_cutoff,
   double *num_tuples,
   double *tups_vacuumed,
   double *tups_recently_dead)
{
	if (OldIndex != NULL || use_sort)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("clustering rowcompress tables using indexes is not supported")));

	Assert(RelationGetDescr(OldHeap)->natts == RelationGetDescr(NewHeap)->natts);

	/*
	 * Scan OldHeap with SnapshotAny.  rowcompress_getnextslot already skips
	 * rows flagged in deleted_mask, so the copy is automatically compacted.
	 */
	TableScanDesc sscan = table_beginscan(OldHeap, SnapshotAny, 0, NULL);
	TupleTableSlot *slot = table_slot_create(OldHeap, NULL);

	*num_tuples    = 0;
	*tups_vacuumed = 0;

	while (rowcompress_getnextslot(sscan, ForwardScanDirection, slot))
	{
		rowcompress_tuple_insert(NewHeap, slot,
								 GetCurrentCommandId(true), 0, NULL);
		(*num_tuples)++;
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(sscan);

	/*
	 * Flush the write state for NewHeap directly.  We cannot rely on
	 * RCFlushAllWriteStates() here because it looks up the relation by
	 * relfilelocator OID, which may not be in the catalog yet during
	 * VACUUM FULL's file-swap phase.
	 */
#if PG_VERSION_NUM >= PG_VERSION_16
	RelFileLocator newRfl = NewHeap->rd_locator;
#else
	RelFileLocator newRfl = NewHeap->rd_node;
#endif
	RowCompressWriteState *ws = RCFindWriteState(newRfl);
	if (ws != NULL && ws->rowCount > 0)
		RCFlushBatch(ws, NewHeap);

	/* Tear down write state context so transaction commit doesn't double-flush */
	if (RCWriteStateContext)
	{
		MemoryContextDelete(RCWriteStateContext);
		RCWriteStateContext = NULL;
	}
	RCWriteStateList = NIL;
}

static bool
rowcompress_relation_needs_toast_table(Relation rel)
{
return false;
}


/* ================================================================
 * VACUUM / ANALYZE
 * ================================================================ */

static void
rowcompress_vacuum_rel(Relation rel, VacuumParams *params,
   BufferAccessStrategy bstrategy)
{
pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM, RelationGetRelid(rel));

uint64 storageId      = RCStorageId(rel);
double new_live_tuples = (double) RCTotalRowCount(storageId);

BlockNumber new_rel_pages   = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);
List    *indexList           = RelationGetIndexList(rel);
int      nindexes            = list_length(indexList);
BlockNumber new_rel_allvisible = 0;

#if PG_VERSION_NUM >= PG_VERSION_16
struct VacuumCutoffs cutoffs;
vacuum_get_cutoffs(rel, params, &cutoffs);

TransactionId newRelFrozenXid = cutoffs.OldestXmin;
MultiXactId   newRelminMxid   = cutoffs.OldestMxact;

bool frozenxid_updated, minmulti_updated;

#if PG_VERSION_NUM >= PG_VERSION_18
BlockNumber new_rel_allfrozen = 0;
vac_update_relstats(rel, new_rel_pages, new_live_tuples,
new_rel_allvisible, new_rel_allfrozen, nindexes > 0,
newRelFrozenXid, newRelminMxid,
&frozenxid_updated, &minmulti_updated, false);
#else
vac_update_relstats(rel, new_rel_pages, new_live_tuples,
new_rel_allvisible, nindexes > 0,
newRelFrozenXid, newRelminMxid,
&frozenxid_updated, &minmulti_updated, false);
#endif

#else /* PG < 16 */
TransactionId oldestXmin, freezeLimit;
MultiXactId   multiXactCutoff;

#if (PG_VERSION_NUM >= PG_VERSION_15) && (PG_VERSION_NUM < PG_VERSION_16)
MultiXactId oldestMxact;
vacuum_set_xid_limits(rel,
  params->freeze_min_age,
  params->freeze_table_age,
  params->multixact_freeze_min_age,
  params->multixact_freeze_table_age,
  &oldestXmin, &oldestMxact,
  &freezeLimit, &multiXactCutoff);
TransactionId newRelFrozenXid = oldestXmin;
MultiXactId   newRelminMxid   = oldestMxact;
bool frozenxid_updated, minmulti_updated;
vac_update_relstats(rel, new_rel_pages, new_live_tuples,
new_rel_allvisible, nindexes > 0,
newRelFrozenXid, newRelminMxid,
&frozenxid_updated, &minmulti_updated, false);
#else
TransactionId xidFullScanLimit;
MultiXactId   mxactFullScanLimit;
vacuum_set_xid_limits(rel,
  params->freeze_min_age,
  params->freeze_table_age,
  params->multixact_freeze_min_age,
  params->multixact_freeze_table_age,
  &oldestXmin, &freezeLimit, &xidFullScanLimit,
  &multiXactCutoff, &mxactFullScanLimit);
TransactionId newRelFrozenXid = oldestXmin;
MultiXactId   newRelminMxid   = multiXactCutoff;
vac_update_relstats(rel, new_rel_pages, new_live_tuples,
new_rel_allvisible, nindexes > 0,
newRelFrozenXid, newRelminMxid, false);
#endif
#endif /* PG < 16 */

#if PG_VERSION_NUM >= PG_VERSION_18
pgstat_report_vacuum(RelationGetRelid(rel),
 rel->rd_rel->relisshared,
 Max(new_live_tuples, 0),
 0,
 GetCurrentTimestamp());
#else
pgstat_report_vacuum(RelationGetRelid(rel),
 rel->rd_rel->relisshared,
 Max(new_live_tuples, 0),
 0);
#endif

pgstat_progress_end_command();
}


#if PG_VERSION_NUM >= PG_VERSION_17
static bool
rowcompress_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	RowCompressScanDesc *rc = (RowCompressScanDesc *) scan;

	if (rc->currentBatchCell == NULL)
		return false;   /* all batches exhausted */

	/*
	 * Consume analyzeBlocksPerBatch buffers per call.  relation_estimate_size
	 * reports the physical block count (nblocks), so ANALYZE's ReadStream
	 * contains nblocks sampled-block entries.  By consuming
	 * analyzeBlocksPerBatch buffers per batch, ANALYZE's pages-visited
	 * counter tracks the physical storage consumed by each batch.
	 *
	 * If the first buffer read returns InvalidBuffer the stream is
	 * exhausted — stop immediately so ANALYZE's extrapolation uses the
	 * correct pages_visited / total_pages ratio.
	 */
	Buffer first = read_stream_next_buffer(stream, NULL);
	if (!BufferIsValid(first))
		return false;   /* stream dry — stop sampling */
	ReleaseBuffer(first);

	for (uint32 i = 1; i < rc->analyzeBlocksPerBatch; i++)
	{
		Buffer buf = read_stream_next_buffer(stream, NULL);
		if (!BufferIsValid(buf))
			break;
		ReleaseBuffer(buf);
	}

	RowCompressBatchMetadata *meta =
		(RowCompressBatchMetadata *) lfirst(rc->currentBatchCell);
	rc->currentBatchCell = lnext(rc->batchList, rc->currentBatchCell);

	if (!RCLoadBatch(rc, meta))
		return false;

	return true;
}
#else
static bool
rowcompress_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
BufferAccessStrategy bstrategy)
{
return true;
}
#endif


static bool
rowcompress_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
double *liverows, double *deadrows,
TupleTableSlot *slot)
{
	RowCompressScanDesc *rc = (RowCompressScanDesc *) scan;

	/*
	 * Return the next row from the currently loaded batch.  When the
	 * batch is exhausted return false; scan_analyze_next_block will
	 * load the next batch on its next call.
	 */
	if (rc->batchRowCount == 0 || rc->currentRowIndex >= rc->batchRowCount)
		return false;

	uint64 rowNumber = rc->currentFirstRowNumber + rc->currentRowIndex;

	if (unlikely(slot->tts_tupleDescriptor == NULL))
		ExecSetSlotDescriptor(slot, RelationGetDescr(rc->rc_base.rs_rd));

	ExecClearTuple(slot);
	RCExtractRow(slot->tts_tupleDescriptor,
				 rc->batchData, rc->rowOffsets,
				 rc->currentRowIndex,
				 slot->tts_values, slot->tts_isnull);
	rc->currentRowIndex++;

	slot->tts_tableOid = RelationGetRelid(rc->rc_base.rs_rd);
	slot->tts_tid      = row_number_to_tid(rowNumber);
	ExecStoreVirtualTuple(slot);

	(*liverows)++;
	return true;
}


/* ================================================================
 * INDEX BUILD
 * ================================================================ */

static double
rowcompress_index_build_range_scan(Relation relation, Relation indexRelation,
   IndexInfo *indexInfo, bool allow_sync,
   bool anyvisible, bool progress,
   BlockNumber start_blockno, BlockNumber numblocks,
   IndexBuildCallback callback,
   void *callback_state, TableScanDesc scan)
{
if (start_blockno != 0 || numblocks != InvalidBlockNumber)
ereport(ERROR,
(errmsg("BRIN indexes on rowcompress tables are not supported")));

Snapshot snapshot;
bool snapshotRegisteredByUs = false;

if (scan != NULL)
{
	/*
	 * Parallel worker: caller already opened a parallel scan covering our
	 * slice of the table. Use it directly and borrow its snapshot for any
	 * cleanup at function exit.
	 */
	snapshot = ((RowCompressScanDesc *) scan)->rc_base.rs_snapshot;
}
else
{
	/*
	 * Serial index build: set up a snapshot and begin our own full-table
	 * scan.
	 */
	TransactionId OldestXmin = InvalidTransactionId;
	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
		OldestXmin = GetOldestNonRemovableTransactionId_compat(relation,
															   PROCARRAY_FLAGS_VACUUM);

	if (!TransactionIdIsValid(OldestXmin))
	{
		snapshot = RegisterSnapshot(GetTransactionSnapshot());
		snapshotRegisteredByUs = true;
	}
	else
	{
		snapshot = SnapshotAny;
	}

	int nkeys = 0;
	bool allowAccessStrategy = true;
	scan = table_beginscan_strat(relation, snapshot, nkeys, NULL,
								 allowAccessStrategy, allow_sync);
}

EState       *estate   = CreateExecutorState();
ExprContext  *econtext = GetPerTupleExprContext(estate);
econtext->ecxt_scantuple = table_slot_create(relation, NULL);
ExprState    *predicate  = ExecPrepareQual(indexInfo->ii_Predicate, estate);

double reltuples = 0.0;
TupleTableSlot *slot = econtext->ecxt_scantuple;

while (rowcompress_getnextslot(scan, ForwardScanDirection, slot))
{
CHECK_FOR_INTERRUPTS();
MemoryContextReset(econtext->ecxt_per_tuple_memory);

if (predicate != NULL && !ExecQual(predicate, econtext))
continue;

Datum  indexValues[INDEX_MAX_KEYS];
bool   indexNulls[INDEX_MAX_KEYS];
FormIndexDatum(indexInfo, slot, estate, indexValues, indexNulls);

ItemPointerData tid = slot->tts_tid;
bool tupleIsAlive   = true;

callback(indexRelation, &tid, indexValues, indexNulls,
 tupleIsAlive, callback_state);

reltuples++;
}

table_endscan(scan);

if (snapshotRegisteredByUs)
UnregisterSnapshot(snapshot);

ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
FreeExecutorState(estate);
indexInfo->ii_ExpressionsState = NIL;
indexInfo->ii_PredicateState   = NULL;

return reltuples;
}


static void
rowcompress_index_validate_scan(Relation relation, Relation indexRelation,
IndexInfo *indexInfo, Snapshot snapshot,
ValidateIndexState *validateIndexState)
{
EState      *estate   = CreateExecutorState();
ExprContext *econtext = GetPerTupleExprContext(estate);
econtext->ecxt_scantuple = table_slot_create(relation, NULL);
ExprState   *predicate   = ExecPrepareQual(indexInfo->ii_Predicate, estate);
(void) predicate; /* partial-index predicate filtering not yet implemented */

TableScanDesc scan = table_beginscan_strat(relation, snapshot, 0, NULL, true, false);

bool indexTupleSortEmpty = false;
ItemPointerData indexedItemPointerData;
ItemPointerSetInvalid(&indexedItemPointerData);

TupleTableSlot *slot = econtext->ecxt_scantuple;
while (rowcompress_getnextslot(scan, ForwardScanDirection, slot))
{
CHECK_FOR_INTERRUPTS();

ItemPointer rcItemPointer = &slot->tts_tid;
validateIndexState->htups += 1;

if (!indexTupleSortEmpty &&
(!ItemPointerIsValid(&indexedItemPointerData) ||
 ItemPointerCompare(&indexedItemPointerData, rcItemPointer) < 0))
{
bool forwardDirection = true;
Datum tsDatum;
bool tsDatumIsNull;
if (!tuplesort_getdatum_compat(validateIndexState->tuplesort,
   forwardDirection, false,
   &tsDatum, &tsDatumIsNull, NULL))
{
ItemPointerSetInvalid(&indexedItemPointerData);
indexTupleSortEmpty = true;
}
else
{
Assert(!tsDatumIsNull);
itemptr_decode(&indexedItemPointerData, DatumGetInt64(tsDatum));
#ifndef USE_FLOAT8_BYVAL
pfree(DatumGetPointer(tsDatum));
#endif
}
}

if (!indexTupleSortEmpty &&
ItemPointerIsValid(&indexedItemPointerData) &&
ItemPointerCompare(&indexedItemPointerData, rcItemPointer) == 0)
{
validateIndexState->tups_inserted += 1;
}
}

table_endscan(scan);

ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
FreeExecutorState(estate);
indexInfo->ii_ExpressionsState = NIL;
indexInfo->ii_PredicateState   = NULL;
}


/* ================================================================
 * RELATION ESTIMATE
 * ================================================================ */

static void
rowcompress_relation_estimate_size(Relation rel, int32 *attr_widths,
   BlockNumber *pages, double *tuples,
   double *allvisfrac)
{
	uint64 storageId = RCStorageId(rel);
	uint64 totalRows = RCTotalRowCount(storageId);
	BlockNumber nblocks = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);

	/*
	 * Report the physical block count.  ANALYZE creates a ReadStream over
	 * this many pages and uses it as the extrapolation denominator.  Our
	 * scan_analyze_next_block consumes analyzeBlocksPerBatch buffers per
	 * batch so that after visiting all batches the ratio is 1 and the
	 * estimated row count matches the true row count.
	 */
	*tuples    = (double) totalRows;
	*pages     = nblocks;
	*allvisfrac = 0.0;

	if (*pages == 0 && *tuples > 0)
		*pages = 1;
}


/* ================================================================
 * BITMAP SCAN / SAMPLE STUBS
 * ================================================================ */

static uint64
rowcompress_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64 nblocks = 0;

	EnsureRelationSmgrOpen(rel);

	if (forkNumber == InvalidForkNumber)
	{
		for (int i = 0; i < MAX_FORKNUM; i++)
			nblocks += smgrnblocks(rel->rd_smgr, i);
	}
	else
	{
		nblocks = smgrnblocks(rel->rd_smgr, forkNumber);
	}

	/* TableAM.relation_size must return bytes, not block count. */
	return nblocks * BLCKSZ;
}

static bool
rowcompress_scan_sample_next_block(TableScanDesc scan,
   SampleScanState *scanstate)
{
return false;
}

static bool
rowcompress_scan_sample_next_tuple(TableScanDesc scan,
   SampleScanState *scanstate,
   TupleTableSlot *slot)
{
return false;
}

/*
 * rowcompress_fetch_bitmap_tuple
 *
 * Lazy-initializes index-fetch state and fetches one tuple by TID.
 */
static bool
rowcompress_fetch_bitmap_tuple(RowCompressScanDesc *scan,
							   ItemPointerData tid,
							   TupleTableSlot *slot)
{
	if (scan->bitmapFetch == NULL)
		scan->bitmapFetch = (IndexFetchRowCompressData *)
			rowcompress_index_fetch_begin(scan->rc_base.rs_rd);

	return rowcompress_index_fetch_tuple((IndexFetchTableData *) scan->bitmapFetch,
								 &tid,
								 scan->rc_base.rs_snapshot,
								 slot,
								 NULL,
								 NULL);
}

#if PG_VERSION_NUM < PG_VERSION_18
static bool
rowcompress_scan_bitmap_next_block(TableScanDesc sscan,
								   struct TBMIterateResult *tbmres)
{
	RowCompressScanDesc *scan = (RowCompressScanDesc *) sscan;

	scan->tbmres  = tbmres;
	scan->bmOffidx = 0;

	if (tbmres->ntuples < 0)
	{
		scan->bmRownum = (uint64) tbmres->blockno * VALID_ITEMPOINTER_OFFSETS;
		scan->bmRownumEnd = scan->bmRownum + VALID_ITEMPOINTER_OFFSETS;
	}

	return true;
}

static bool
rowcompress_scan_bitmap_next_tuple(TableScanDesc sscan,
								   struct TBMIterateResult *tbmres,
								   TupleTableSlot *slot)
{
	RowCompressScanDesc *scan = (RowCompressScanDesc *) sscan;

	if (unlikely(slot->tts_tupleDescriptor == NULL))
		ExecSetSlotDescriptor(slot, RelationGetDescr(scan->rc_base.rs_rd));

	while (true)
	{
		uint64 rowNumber;

		if (tbmres->ntuples < 0)
		{
			if (scan->bmRownum >= scan->bmRownumEnd)
				return false;
			rowNumber = scan->bmRownum++;
		}
		else
		{
			if (scan->bmOffidx >= tbmres->ntuples)
				return false;
			OffsetNumber off = tbmres->offsets[scan->bmOffidx++];
			rowNumber = (uint64) tbmres->blockno * VALID_ITEMPOINTER_OFFSETS +
				(uint64) (off - FirstOffsetNumber);
		}

		ItemPointerData tid = row_number_to_tid(rowNumber);
		ExecClearTuple(slot);
		if (!rowcompress_fetch_bitmap_tuple(scan, tid, slot))
			continue;

		return true;
	}
}
#else
static bool
rowcompress_scan_bitmap_next_tuple(TableScanDesc sscan,
								   TupleTableSlot *slot,
								   bool *recheck,
								   uint64 *lossy_pages,
								   uint64 *exact_pages)
{
	RowCompressScanDesc *scan = (RowCompressScanDesc *) sscan;

	if (unlikely(slot->tts_tupleDescriptor == NULL))
		ExecSetSlotDescriptor(slot, RelationGetDescr(scan->rc_base.rs_rd));

	if (scan->bmOffsets == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(scan->scanContext);
		scan->bmOffsets = (OffsetNumber *)
			palloc(VALID_ITEMPOINTER_OFFSETS * sizeof(OffsetNumber));
		MemoryContextSwitchTo(old);
	}

	while (true)
	{
		if (!scan->bmHavePage)
		{
			if (!tbm_iterate(&sscan->st.rs_tbmiterator, &scan->tbmres))
				return false;

			scan->bmHavePage = true;
			*recheck = scan->tbmres.recheck;

			if (scan->tbmres.lossy)
			{
				scan->bmRownum = (uint64) scan->tbmres.blockno *
					VALID_ITEMPOINTER_OFFSETS;
				scan->bmRownumEnd = scan->bmRownum + VALID_ITEMPOINTER_OFFSETS;
				scan->bmNoffsets = -1;
				(*lossy_pages)++;
			}
			else
			{
				scan->bmNoffsets = tbm_extract_page_tuple(
					&scan->tbmres,
					scan->bmOffsets,
					VALID_ITEMPOINTER_OFFSETS);
				scan->bmOffidx = 0;
				(*exact_pages)++;
			}
		}

		while (scan->bmHavePage)
		{
			uint64 rowNumber;

			if (scan->bmNoffsets < 0)
			{
				if (scan->bmRownum >= scan->bmRownumEnd)
				{
					scan->bmHavePage = false;
					break;
				}
				rowNumber = scan->bmRownum++;
			}
			else
			{
				if (scan->bmOffidx >= scan->bmNoffsets)
				{
					scan->bmHavePage = false;
					break;
				}

				OffsetNumber off = scan->bmOffsets[scan->bmOffidx++];
				rowNumber = (uint64) scan->tbmres.blockno *
					VALID_ITEMPOINTER_OFFSETS +
					(uint64) (off - FirstOffsetNumber);
			}

			ItemPointerData tid = row_number_to_tid(rowNumber);
			ExecClearTuple(slot);
			if (!rowcompress_fetch_bitmap_tuple(scan, tid, slot))
				continue;

			return true;
		}
	}
}
#endif


/* ================================================================
 * TABLE AM HANDLER
 * ================================================================ */

static const TableAmRoutine rowcompress_methods = {
.type = T_TableAmRoutine,

.slot_callbacks         = rowcompress_slot_callbacks,

.scan_begin             = rowcompress_beginscan,
.scan_end               = rowcompress_endscan,
.scan_rescan            = rowcompress_rescan,
.scan_getnextslot       = rowcompress_getnextslot,

.parallelscan_estimate  = rowcompress_parallelscan_estimate,
.parallelscan_initialize = rowcompress_parallelscan_initialize,
.parallelscan_reinitialize = rowcompress_parallelscan_reinitialize,

.index_fetch_begin      = rowcompress_index_fetch_begin,
.index_fetch_reset      = rowcompress_index_fetch_reset,
.index_fetch_end        = rowcompress_index_fetch_end,
.index_fetch_tuple      = rowcompress_index_fetch_tuple,

.tuple_fetch_row_version  = rowcompress_fetch_row_version,
.tuple_get_latest_tid     = rowcompress_get_latest_tid,
.tuple_tid_valid          = rowcompress_tuple_tid_valid,
.tuple_satisfies_snapshot = rowcompress_tuple_satisfies_snapshot,
#if PG_VERSION_NUM >= PG_VERSION_14
.index_delete_tuples      = rowcompress_index_delete_tuples,
#else
.compute_xid_horizon_for_tuples = rowcompress_compute_xid_horizon_for_tuples,
#endif

.tuple_insert           = rowcompress_tuple_insert,
.tuple_insert_speculative = rowcompress_tuple_insert_speculative,
.tuple_complete_speculative = rowcompress_tuple_complete_speculative,
.multi_insert           = rowcompress_multi_insert,
.tuple_delete           = rowcompress_tuple_delete,
.tuple_update           = rowcompress_tuple_update,
.tuple_lock             = rowcompress_tuple_lock,
.finish_bulk_insert     = rowcompress_finish_bulk_insert,

.relation_set_new_filelocator = rowcompress_relation_set_new_filenode,
.relation_nontransactional_truncate = rowcompress_relation_nontransactional_truncate,
.relation_copy_data    = rowcompress_relation_copy_data,
.relation_copy_for_cluster = rowcompress_relation_copy_for_cluster,
.relation_vacuum        = rowcompress_vacuum_rel,
.scan_analyze_next_block = rowcompress_scan_analyze_next_block,
.scan_analyze_next_tuple = rowcompress_scan_analyze_next_tuple,
.index_build_range_scan = rowcompress_index_build_range_scan,
.index_validate_scan    = rowcompress_index_validate_scan,

	.relation_size          = rowcompress_relation_size,
.relation_needs_toast_table = rowcompress_relation_needs_toast_table,
.relation_estimate_size = rowcompress_relation_estimate_size,

#if PG_VERSION_NUM < PG_VERSION_18
	.scan_bitmap_next_block = rowcompress_scan_bitmap_next_block,
#endif
	.scan_bitmap_next_tuple = rowcompress_scan_bitmap_next_tuple,
	.scan_sample_next_block = rowcompress_scan_sample_next_block,
.scan_sample_next_tuple = rowcompress_scan_sample_next_tuple,
};

/* ================================================================
 * USER-FACING MANAGEMENT FUNCTIONS
 * ================================================================ */

/*
 * alter_rowcompress_table_set — change options on a rowcompress table.
 *
 * SQL signature:
 *   engine.alter_rowcompress_table_set(
 *       table_name        regclass,
 *       batch_size        int  DEFAULT NULL,
 *       compression       name DEFAULT NULL,
 *       compression_level int  DEFAULT NULL,
 *       pruning_column    text DEFAULT NULL)
 *
 * Only non-NULL arguments are changed; the rest keep their current value.
 * Pass pruning_column = '' (empty string) to disable pruning.
 */
PG_FUNCTION_INFO_V1(alter_rowcompress_table_set);
Datum
alter_rowcompress_table_set(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	Relation rel = table_open(relationId, AccessExclusiveLock);
	if (rel->rd_tableam != &rowcompress_methods)
	{
		ereport(ERROR, (errmsg("table %s is not a rowcompress table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

#if PG_VERSION_NUM >= PG_VERSION_16
	if (!object_ownercheck(RelationRelationId, relationId, GetUserId()))
#else
	if (!pg_class_ownercheck(relationId, GetUserId()))
#endif
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relationId));
	}

	RowCompressOptions options = { 0 };
	if (!RCReadOptions(relationId, &options))
	{
		ereport(ERROR, (errmsg("unable to read current options for table")));
	}

	/* batch_size => not null */
	if (!PG_ARGISNULL(1))
	{
		options.batchSize = PG_GETARG_INT32(1);
		if (options.batchSize < ROWCOMPRESS_BATCH_SIZE_MIN ||
			options.batchSize > ROWCOMPRESS_BATCH_SIZE_MAX)
		{
			ereport(ERROR, (errmsg("batch size out of range"),
							errhint("batch size must be between %d and %d",
									ROWCOMPRESS_BATCH_SIZE_MIN,
									ROWCOMPRESS_BATCH_SIZE_MAX)));
		}
		ereport(DEBUG1, (errmsg("updating batch size to %d", options.batchSize)));
	}

	/* compression => not null */
	if (!PG_ARGISNULL(2))
	{
		Name compressionName = PG_GETARG_NAME(2);
		options.compression = ParseCompressionType(NameStr(*compressionName));
		if (options.compression == COMPRESSION_TYPE_INVALID)
		{
			ereport(ERROR, (errmsg("unknown compression type for rowcompress table: %s",
								   quote_identifier(NameStr(*compressionName)))));
		}
		ereport(DEBUG1, (errmsg("updating compression to %s",
								CompressionTypeStr(options.compression))));
	}

	/* compression_level => not null */
	if (!PG_ARGISNULL(3))
	{
		options.compressionLevel = PG_GETARG_INT32(3);
		if (options.compressionLevel < COMPRESSION_LEVEL_MIN ||
			options.compressionLevel > COMPRESSION_LEVEL_MAX)
		{
			ereport(ERROR, (errmsg("compression level out of range"),
							errhint("compression level must be between %d and %d",
									COMPRESSION_LEVEL_MIN,
									COMPRESSION_LEVEL_MAX)));
		}
		ereport(DEBUG1, (errmsg("updating compression level to %d",
								options.compressionLevel)));
	}

	/* pruning_column => not null */
	if (!PG_ARGISNULL(4))
	{
		text *colNameText = PG_GETARG_TEXT_PP(4);
		char *colNameStr  = text_to_cstring(colNameText);

		if (colNameStr[0] == '\0')
		{
			/* Empty string = disable pruning */
			options.pruningAttnum = 0;
			ereport(DEBUG1, (errmsg("disabling rowcompress batch pruning")));
		}
		else
		{
			TupleDesc td = RelationGetDescr(rel);
			int16     attno = 0;

			for (int i = 0; i < td->natts; i++)
			{
				Form_pg_attribute a = TupleDescAttr(td, i);
				if (!a->attisdropped &&
					strcmp(NameStr(a->attname), colNameStr) == 0)
				{
					attno = a->attnum;
					break;
				}
			}

			if (attno == 0)
				ereport(ERROR,
						(errmsg("column \"%s\" does not exist in table \"%s\"",
								colNameStr,
								RelationGetRelationName(rel))));

			/* Verify the type has a btree comparator */
			Form_pg_attribute pattrForm = TupleDescAttr(td, attno - 1);
			TypeCacheEntry *tc = lookup_type_cache(pattrForm->atttypid,
												   TYPECACHE_CMP_PROC_FINFO);
			if (!OidIsValid(tc->cmp_proc))
				ereport(ERROR,
						(errmsg("column \"%s\" has type %s which has no btree comparator",
								colNameStr,
								format_type_be(pattrForm->atttypid)),
						 errhint("choose a column with a btree-comparable data type")));

			options.pruningAttnum = attno;
			ereport(DEBUG1, (errmsg("setting pruning column to \"%s\" (attnum %d)",
									colNameStr, attno)));
		}
	}

	RCSetOptions(relationId, &options);

	table_close(rel, NoLock);

	PG_RETURN_VOID();
}

/*
 * alter_rowcompress_table_reset — reset options on a rowcompress table to defaults.
 *
 * SQL signature:
 *   engine.alter_rowcompress_table_reset(
 *       table_name        regclass,
 *       batch_size        bool DEFAULT false,
 *       compression       bool DEFAULT false,
 *       compression_level bool DEFAULT false)
 *
 * Set an argument to true to reset it to the system default.
 */
PG_FUNCTION_INFO_V1(alter_rowcompress_table_reset);
Datum
alter_rowcompress_table_reset(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	Relation rel = table_open(relationId, AccessExclusiveLock);
	if (rel->rd_tableam != &rowcompress_methods)
	{
		ereport(ERROR, (errmsg("table %s is not a rowcompress table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

#if PG_VERSION_NUM >= PG_VERSION_16
	if (!object_ownercheck(RelationRelationId, relationId, GetUserId()))
#else
	if (!pg_class_ownercheck(relationId, GetUserId()))
#endif
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relationId));
	}

	RowCompressOptions options = { 0 };
	if (!RCReadOptions(relationId, &options))
	{
		ereport(ERROR, (errmsg("unable to read current options for table")));
	}

	/* batch_size => true */
	if (!PG_ARGISNULL(1) && PG_GETARG_BOOL(1))
	{
		options.batchSize = ROWCOMPRESS_DEFAULT_BATCH_SIZE;
		ereport(DEBUG1, (errmsg("resetting batch size to %d", options.batchSize)));
	}

	/* compression => true */
	if (!PG_ARGISNULL(2) && PG_GETARG_BOOL(2))
	{
		options.compression = ROWCOMPRESS_DEFAULT_COMPRESSION;
		ereport(DEBUG1, (errmsg("resetting compression to %s",
								CompressionTypeStr(options.compression))));
	}

	/* compression_level => true */
	if (!PG_ARGISNULL(3) && PG_GETARG_BOOL(3))
	{
		options.compressionLevel = ROWCOMPRESS_DEFAULT_COMPRESSION_LEVEL;
		ereport(DEBUG1, (errmsg("resetting compression level to %d",
								options.compressionLevel)));
	}

	RCSetOptions(relationId, &options);

	table_close(rel, NoLock);

	PG_RETURN_VOID();
}

/*
 * rowcompress_repack rewrites all rows of a rowcompress table using the
 * current row options (batch_size/compression/compression_level).
 *
 * Usage:
 *   SELECT engine.rowcompress_repack('schema.table'::regclass);
 */
PG_FUNCTION_INFO_V1(rowcompress_repack);
Datum
rowcompress_repack(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	RowCompressOptions options = { 0 };

	Relation rel = table_open(relationId, AccessExclusiveLock);
	if (rel->rd_tableam != &rowcompress_methods)
	{
		table_close(rel, AccessExclusiveLock);
		ereport(ERROR, (errmsg("table %s is not a rowcompress table",
							   quote_identifier(RelationGetRelationName(rel)))));
	}

#if PG_VERSION_NUM >= PG_VERSION_16
	if (!object_ownercheck(RelationRelationId, relationId, GetUserId()))
#else
	if (!pg_class_ownercheck(relationId, GetUserId()))
#endif
	{
		table_close(rel, AccessExclusiveLock);
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(relationId));
	}

	if (!RCReadOptions(relationId, &options))
	{
		table_close(rel, AccessExclusiveLock);
		ereport(ERROR,
				(errmsg("rowcompress_repack: unable to read options for table")));
	}

	char *nspname = get_namespace_name(RelationGetNamespace(rel));
	char *relname = pstrdup(RelationGetRelationName(rel));
	char *qualname = quote_qualified_identifier(nspname, relname);

	char tmpname[64];
	snprintf(tmpname, sizeof(tmpname), "_rcrepack_%u", relationId);

	table_close(rel, NoLock);

	SPI_connect();

	StringInfoData buf;
	initStringInfo(&buf);

	appendStringInfo(&buf,
		"CREATE TEMP TABLE %s AS SELECT * FROM %s",
		tmpname, qualname);
	int ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("rowcompress_repack: CREATE TEMP TABLE failed (code %d)", ret)));

	resetStringInfo(&buf);
	appendStringInfo(&buf, "TRUNCATE %s", qualname);
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("rowcompress_repack: TRUNCATE failed (code %d)", ret)));

	/* TRUNCATE recreates rowcompress options row with defaults; restore previous options. */
	RCSetOptions(relationId, &options);

	resetStringInfo(&buf);
	appendStringInfo(&buf, "INSERT INTO %s SELECT * FROM %s", qualname, tmpname);
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_INSERT)
		ereport(ERROR, (errmsg("rowcompress_repack: INSERT failed (code %d)", ret)));

	uint64 rowsRepacked = SPI_processed;

	resetStringInfo(&buf);
	appendStringInfo(&buf, "DROP TABLE %s", tmpname);
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		ereport(ERROR, (errmsg("rowcompress_repack: DROP TABLE failed (code %d)", ret)));

	SPI_finish();

	ereport(NOTICE,
			(errmsg("rowcompress_repack: " UINT64_FORMAT " rows rewritten into %s",
					rowsRepacked, qualname)));

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(rowcompress_handler);

Datum
rowcompress_handler(PG_FUNCTION_ARGS)
{
PG_RETURN_POINTER(&rowcompress_methods);
}

/*
 * IsRowCompressTableAmTable returns true if the relation uses the
 * rowcompress table access method.
 */
bool
IsRowCompressTableAmTable(Oid relationId)
{
	if (!OidIsValid(relationId))
		return false;

	Relation rel = relation_open(relationId, AccessShareLock);
	bool result = (rel->rd_tableam == &rowcompress_methods);
	relation_close(rel, NoLock);

	return result;
}

void
rowcompress_tableam_init(void)
{
RegisterXactCallback(RowCompressXactCallback, NULL);
RegisterSubXactCallback(RowCompressSubXactCallback, NULL);

/* Install object access hook to clean up metadata on DROP TABLE */
PrevRCObjectAccessHook = object_access_hook;
object_access_hook = RCObjectAccessHook;
}

/*
 * RCObjectAccessHook cleans up rowcompress metadata when a relation is dropped.
 */
static void
RCObjectAccessHook(ObjectAccessType access, Oid classId, Oid objectId,
				   int subId, void *arg)
{
	if (PrevRCObjectAccessHook)
		PrevRCObjectAccessHook(access, classId, objectId, subId, arg);

	if (access != OAT_DROP || classId != RelationRelationId || OidIsValid(subId))
		return;

	/*
	 * Lock to prevent the relation from being dropped out from under us and
	 * to avoid race conditions (mirrors ColumnarTableDropHook pattern).
	 */
	LockRelationOid(objectId, AccessShareLock);

	/* Open with share lock to check the table access method */
	Relation rel = relation_open(objectId, AccessShareLock);
	bool isRowCompress = (rel->rd_tableam == &rowcompress_methods);

	if (!isRowCompress)
	{
		relation_close(rel, NoLock);
		return;
	}

	/*
	 * Upgrade to exclusive lock, read the storage ID from the file header,
	 * then close. The physical file still exists at OAT_DROP time.
	 */
	relation_close(rel, NoLock);
	rel = table_open(objectId, AccessExclusiveLock);
	uint64 storageId = RCStorageId(rel);
	/* Keep the lock since we do physical metadata removal */
	table_close(rel, NoLock);

	/* Delete all batch rows for this storage and the options row */
	RCDeleteBatches(storageId);
	RCDeleteOptions(objectId, true);
}

/* ================================================================
 * PUBLIC PRUNING / SCAN API (called from RowcompressScan custom node)
 * ================================================================ */

/*
 * RowCompressGetPruningAttnum — return the 1-based pruning attnum configured
 * for the given rowcompress relation, or 0 if pruning is not configured.
 * Returns 0 when the options row does not exist or the extension is not
 * initialised.
 */
int16
RowCompressGetPruningAttnum(Oid relid)
{
	RowCompressOptions opts;
	if (!RCReadOptions(relid, &opts))
		return 0;
	return opts.pruningAttnum;
}

/*
 * rowcompress_set_pushdown_clauses — attach WHERE-clause pruning context to
 * an open rowcompress scan.
 *
 * Called by RowcompressScan_BeginCustomScan after table_beginscan().  The
 * clauses list should contain RestrictInfo-unwrapped Expr nodes (plain Expr
 * pointers, not RestrictInfo wrappers).
 *
 * The function looks for the Var that references pruningAttnum and builds the
 * (var >= min) AND (var <= max) template used by RCBatchCanBePruned.
 */
void
rowcompress_set_pushdown_clauses(TableScanDesc sscan,
								 List *clauses,
								 TupleDesc tupdesc)
{
	RowCompressScanDesc *scan = (RowCompressScanDesc *) sscan;

	scan->pruningCtx = NULL;

	if (clauses == NIL || scan->options.pruningAttnum <= 0)
		return;

	int16 pattnum = scan->options.pruningAttnum;
	if (pattnum > tupdesc->natts)
		return;

	Form_pg_attribute attrForm = TupleDescAttr(tupdesc, pattnum - 1);

	/*
	 * Find a Var in the clause list that references the pruning column.
	 * We look for a Var at any level (pull_var_clause with default flags).
	 */
	Var *pruningVar = NULL;
	ListCell *lc;
	foreach(lc, clauses)
	{
		Node *clause = (Node *) lfirst(lc);
		List *vars = pull_var_clause(clause,
									 PVC_RECURSE_AGGREGATES |
									 PVC_RECURSE_WINDOWFUNCS |
									 PVC_INCLUDE_PLACEHOLDERS);
		ListCell *vlc;
		foreach(vlc, vars)
		{
			Var *v = (Var *) lfirst(vlc);
			if (!IsA(v, Var))
				continue;
			if (v->varattno == pattnum)
			{
				pruningVar = copyObject(v);
				break;
			}
		}
		list_free(vars);
		if (pruningVar != NULL)
			break;
	}

	if (pruningVar == NULL)
		return; /* pruning column not referenced in WHERE */

	Node *baseConstraint = RCBuildBaseConstraint(pruningVar);
	if (baseConstraint == NULL)
		return; /* no btree opfamily for this type */

	RCPruningCtx *ctx = palloc(sizeof(RCPruningCtx));
	ctx->clauses         = clauses;
	ctx->pruningAttnum   = pattnum;
	ctx->pruningAttrForm = attrForm;
	ctx->pruningVar      = pruningVar;
	ctx->baseConstraint  = baseConstraint;

	scan->pruningCtx    = ctx;
	scan->batchesPruned = 0;
}

/*
 * rowcompress_get_batches_pruned — return the number of batches skipped by
 * the pruning optimisation during this scan (for EXPLAIN output).
 */
int64
rowcompress_get_batches_pruned(TableScanDesc sscan)
{
	RowCompressScanDesc *scan = (RowCompressScanDesc *) sscan;
	return scan->batchesPruned;
}
