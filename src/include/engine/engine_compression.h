/*-------------------------------------------------------------------------
 *
 * engine_compression.h
 *
 * Type and function declarations for compression methods.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ENGINE_COMPRESSION_H
#define ENGINE_COMPRESSION_H

/* Enumaration for columnar table's compression method */
typedef enum
{
	COMPRESSION_TYPE_INVALID = -1,
	COMPRESSION_NONE = 0,
	COMPRESSION_PG_LZ = 1,
	COMPRESSION_LZ4 = 2,
	COMPRESSION_ZSTD = 3,

	COMPRESSION_COUNT
} CompressionType;

extern bool CompressBuffer(StringInfo inputBuffer,
						   StringInfo outputBuffer,
						   CompressionType compressionType,
						   int compressionLevel);
extern StringInfo DecompressBuffer(StringInfo buffer, CompressionType compressionType,
								   uint64 decompressedSize);

#endif /* ENGINE_COMPRESSION_H */
