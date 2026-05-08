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

#ifndef COLUMNAR_COMPRESSION_H
#define COLUMNAR_COMPRESSION_H

/* Enumaration for columnar table's compression method */
typedef enum
{
	COMPRESSION_TYPE_INVALID = -1,
	COMPRESSION_NONE = 0,
	COMPRESSION_PG_LZ = 1,
	COMPRESSION_LZ4 = 2,
	COMPRESSION_ZSTD = 3,
	COMPRESSION_DEFLATE = 4,
	COMPRESSION_ZXC = 5,    /* ZXC asymmetric codec: https://github.com/hellobertrand/zxc */

	/*
	 * ClickHouse-style transform codecs (v2.3).
	 * These apply a reversible numeric transform before LZ4/ZSTD compression.
	 * Effective for columns with slowly-changing values (timestamps, counters,
	 * sensor readings, floating-point metrics).
	 *
	 * All three codecs require 8-byte-aligned data (float8, int8, timestamptz).
	 * For 4-byte types, use the _4 variants (future work).
	 * Data length must be a multiple of 8 bytes; falls back gracefully otherwise.
	 */
	COMPRESSION_DELTA = 6,        /* delta(i) = v[i]-v[i-1] → LZ4/ZSTD       */
	COMPRESSION_DOUBLEDELTA = 7,  /* dd(i) = delta[i]-delta[i-1] → LZ4/ZSTD  */
	COMPRESSION_GORILLA = 8,      /* xor(i) = v[i]^v[i-1] → LZ4/ZSTD        */

	COMPRESSION_COUNT
} CompressionType;

extern bool CompressBuffer(StringInfo inputBuffer,
						   StringInfo outputBuffer,
						   CompressionType compressionType,
						   int compressionLevel);
extern StringInfo DecompressBuffer(StringInfo buffer, CompressionType compressionType,
								   uint64 decompressedSize);

#endif /* COLUMNAR_COMPRESSION_H */
