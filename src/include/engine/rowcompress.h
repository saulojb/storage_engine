/*-------------------------------------------------------------------------
 *
 * rowcompress.h
 *
 * Public types and declarations for the rowcompress Table Access Method.
 * Row-based compressed storage: rows are grouped into batches and each
 * batch is compressed as a unit using the columnar compression library.
 *
 * Copyright (c) Hydra, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ROWCOMPRESS_H
#define ROWCOMPRESS_H

#include "postgres.h"
#include "engine/engine_compression.h"

/* Module-level constants */
#define ROWCOMPRESS_NAMESPACE_NAME       "engine"
#define ROWCOMPRESS_BATCH_TABLE_NAME     "row_batch"
#define ROWCOMPRESS_OPTIONS_TABLE_NAME   "row_options"

/* Default options */
#define ROWCOMPRESS_DEFAULT_BATCH_SIZE         10000
#define ROWCOMPRESS_DEFAULT_COMPRESSION_LEVEL  3

#if HAVE_LIBZSTD
#define ROWCOMPRESS_DEFAULT_COMPRESSION COMPRESSION_ZSTD
#elif HAVE_CITUS_LIBLZ4
#define ROWCOMPRESS_DEFAULT_COMPRESSION COMPRESSION_LZ4
#else
#define ROWCOMPRESS_DEFAULT_COMPRESSION COMPRESSION_PG_LZ
#endif

/* Minimum and maximum batch sizes */
#define ROWCOMPRESS_BATCH_SIZE_MIN  100
#define ROWCOMPRESS_BATCH_SIZE_MAX  100000000

/*
 * RowCompressOptions holds per-table configuration for rowcompress tables.
 */
typedef struct RowCompressOptions
{
	uint32         batchSize;       /* max rows per compressed batch */
	CompressionType compression;    /* compression algorithm */
	int            compressionLevel; /* compression level */
} RowCompressOptions;

/*
 * RowCompressBatchMetadata represents information about one compressed batch.
 * This mirrors what is stored in engine.row_batch.
 */
typedef struct RowCompressBatchMetadata
{
	uint64  batchNum;        /* monotonically increasing batch ID */
	uint64  firstRowNumber;  /* row number of the first row in this batch */
	uint32  rowCount;        /* number of rows stored */
	uint64  fileOffset;      /* logical offset in the relation file */
	uint64  dataLength;      /* total on-disk size (header + offsets + data) */
	uint8  *deletedMask;     /* per-row deletion bitmask; NULL = no deleted rows */
	uint32  deletedMaskLen;  /* number of bytes in deletedMask */
} RowCompressBatchMetadata;

/* Public API */
extern void rowcompress_tableam_init(void);
extern bool IsRowCompressTableAmTable(Oid relationId);

/* User-facing management functions (exposed as SQL UDFs) */
extern Datum alter_rowcompress_table_set(PG_FUNCTION_ARGS);
extern Datum alter_rowcompress_table_reset(PG_FUNCTION_ARGS);
extern Datum rowcompress_repack(PG_FUNCTION_ARGS);

#endif /* ROWCOMPRESS_H */
