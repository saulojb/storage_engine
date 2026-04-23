/*-------------------------------------------------------------------------
 *
 * columnar.c
 *
 * This file contains...
 *
 * Copyright (c) 2016, Citus Data, Inc.
 * Copyright (c) Hydra, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/rel.h"

#include "engine/engine.h"
#include "engine/engine_tableam.h"

/* Default values for option parameters */
#define DEFAULT_STRIPE_ROW_COUNT 150000
#define DEFAULT_CHUNK_ROW_COUNT 10000

#if HAVE_LIBZSTD
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_ZSTD
#elif HAVE_LIBLZ4
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_LZ4
#else
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_PG_LZ
#endif

static void engine_guc_init(void);

int engine_compression = DEFAULT_COMPRESSION_TYPE;
int engine_stripe_row_limit = DEFAULT_STRIPE_ROW_COUNT;
int engine_chunk_group_row_limit = DEFAULT_CHUNK_ROW_COUNT;
int engine_compression_level = 3;
bool engine_enable_parallel_execution = true;
int engine_min_parallel_processes = 8;
bool engine_enable_vectorization = true;
bool engine_enable_dml = true;
bool engine_enable_page_cache = false;
int engine_page_cache_size = 200U;
bool engine_index_scan = false;

static const struct config_enum_entry engine_compression_options[] =
{
	{ "none", COMPRESSION_NONE, false },
	{ "pglz", COMPRESSION_PG_LZ, false },
#if HAVE_LIBLZ4
	{ "lz4", COMPRESSION_LZ4, false },
#endif
#if HAVE_LIBZSTD
	{ "zstd", COMPRESSION_ZSTD, false },
#endif
	{ NULL, 0, false }
};

void
engine_init(void)
{
	engine_guc_init();
	engine_tableam_init();
	engine_planner_init();
}


static void
engine_guc_init()
{
	/*
	 * Guard against double-registration (e.g. shared_preload_libraries path
	 * differs from the path resolved at CREATE EXTENSION / LOAD time, causing
	 * PostgreSQL to call _PG_init() twice for the same physical .so file).
	 */
	if (GetConfigOption("storage_engine.compression", true, false) != NULL)
		return;

	DefineCustomEnumVariable("storage_engine.compression",
							 "Compression type for columnar.",
							 NULL,
							 &engine_compression,
							 DEFAULT_COMPRESSION_TYPE,
							 engine_compression_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("storage_engine.compression_level",
							"Compression level to be used with zstd.",
							NULL,
							&engine_compression_level,
							3,
							COMPRESSION_LEVEL_MIN,
							COMPRESSION_LEVEL_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("storage_engine.stripe_row_limit",
							"Maximum number of tuples per stripe.",
							NULL,
							&engine_stripe_row_limit,
							DEFAULT_STRIPE_ROW_COUNT,
							STRIPE_ROW_COUNT_MINIMUM,
							STRIPE_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("storage_engine.chunk_group_row_limit",
							"Maximum number of rows per chunk.",
							NULL,
							&engine_chunk_group_row_limit,
							DEFAULT_CHUNK_ROW_COUNT,
							CHUNK_ROW_COUNT_MINIMUM,
							CHUNK_ROW_COUNT_MAXIMUM,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("storage_engine.enable_parallel_execution",
							 "Enables parallel execution",
							 NULL,
							 &engine_enable_parallel_execution,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, 
							 NULL, 
							 NULL);

	DefineCustomIntVariable("storage_engine.min_parallel_processes",
							"Minimum number of parallel processes",
							NULL,
							&engine_min_parallel_processes,
							8,
							1,
							32,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("storage_engine.enable_vectorization",
							 "Enables vectorized execution",
							 NULL,
							 &engine_enable_vectorization,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, 
							 NULL, 
							 NULL);

	DefineCustomBoolVariable("storage_engine.enable_dml",
							"Enables DML",
							NULL,
							&engine_enable_dml,
							true,
							PGC_USERSET,
							0,
							NULL, 
							NULL, 
							NULL);

	DefineCustomBoolVariable("storage_engine.enable_column_cache",
							"Enables column based caching",
							NULL,
							&engine_enable_page_cache,
							false,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("storage_engine.column_cache_size",
							"Size of the column based cache in megabytes",
							NULL,
							&engine_page_cache_size,
							200U,
							20U,
							20000U,
							PGC_USERSET,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("storage_engine.enable_engine_index_scan",
							 "Enables custom columnar index scan",
							 NULL,
							 &engine_index_scan,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, 
							 NULL, 
							 NULL);
}


/*
 * ParseCompressionType converts a string to a compression type.
 * For compression algorithms that are invalid or not compiled, it
 * returns COMPRESSION_TYPE_INVALID.
 */
CompressionType
ParseCompressionType(const char *compressionTypeString)
{
	Assert(compressionTypeString != NULL);

	for (int compressionIndex = 0;
		 engine_compression_options[compressionIndex].name != NULL;
		 compressionIndex++)
	{
		const char *compressionName = engine_compression_options[compressionIndex].name;
		if (strncmp(compressionTypeString, compressionName, NAMEDATALEN) == 0)
		{
			return engine_compression_options[compressionIndex].val;
		}
	}

	return COMPRESSION_TYPE_INVALID;
}


/*
 * CompressionTypeStr returns string representation of a compression type.
 * For compression algorithms that are invalid or not compiled, it
 * returns NULL.
 */
const char *
CompressionTypeStr(CompressionType requestedType)
{
	for (int compressionIndex = 0;
		 engine_compression_options[compressionIndex].name != NULL;
		 compressionIndex++)
	{
		CompressionType compressionType =
			engine_compression_options[compressionIndex].val;
		if (compressionType == requestedType)
		{
			return engine_compression_options[compressionIndex].name;
		}
	}

	return NULL;
}
