/*-------------------------------------------------------------------------
 *
 * engine_compression.c
 *
 * This file contains compression/decompression functions definitions
 * used for columnar.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_version_compat.h"

#include "common/pg_lzcompress.h"
#include "lib/stringinfo.h"

#include "engine/engine_compression.h"

#if HAVE_LIBLZ4
#include <lz4.h>
#endif

#if HAVE_LIBZSTD
#include <zstd.h>
#endif

#if HAVE_LIBDEFLATE
#include <libdeflate.h>
#endif

#if HAVE_LIBZXC
#include <zxc.h>
#endif

#if PG_VERSION_NUM >= PG_VERSION_16
#include "varatt.h"
#endif

/* -----------------------------------------------------------------------
 * ClickHouse-style numeric transform codecs (Delta, DoubleDelta, Gorilla)
 *
 * These codecs apply a reversible numeric transform over the raw byte buffer
 * (treated as an array of uint64 elements), then compress the result with
 * LZ4 (preferred) or ZSTD (fallback) or DEFLATE (last resort).
 *
 * The codecs are most effective for 8-byte typed columns:
 *   - Delta:       monotonically increasing integers, counters, unix timestamps
 *   - DoubleDelta: regularly-sampled timestamps (near-constant intervals)
 *   - Gorilla:     float8 sensor readings, price ticks, slowly-drifting metrics
 *
 * Data requirements:
 *   - Input length must be a multiple of 8 bytes.
 *   - If not, CompressBuffer returns false and the caller falls back to NONE.
 *
 * Wire format: [LZ4/ZSTD/DEFLATE compressed transform array]
 *   The transform array has exactly the same byte length as the original data.
 *   DecompressBuffer decompresses it and then applies the inverse transform
 *   using the decompressedSize passed in from the chunk metadata.
 * -----------------------------------------------------------------------*/

/*
 * ch_compress_inner - compress a transform buffer using whatever is available.
 * Priority: LZ4 > ZSTD > DEFLATE.  Returns false if nothing is available.
 */
static bool
ch_compress_inner(StringInfo transformedBuf, StringInfo outputBuffer,
				  int compressionLevel)
{
#if HAVE_LIBLZ4
	{
		int maxOut = LZ4_compressBound(transformedBuf->len);
		resetStringInfo(outputBuffer);
		enlargeStringInfo(outputBuffer, maxOut);
		int compressedSize = LZ4_compress_default(transformedBuf->data,
												  outputBuffer->data,
												  transformedBuf->len,
												  maxOut);
		if (compressedSize > 0)
		{
			outputBuffer->len = compressedSize;
			return true;
		}
	}
#endif
#if HAVE_LIBZSTD
	{
		int maxOut = ZSTD_compressBound(transformedBuf->len);
		resetStringInfo(outputBuffer);
		enlargeStringInfo(outputBuffer, maxOut);
		size_t compressedSize = ZSTD_compress(outputBuffer->data,
											  outputBuffer->maxlen,
											  transformedBuf->data,
											  transformedBuf->len,
											  compressionLevel > 0 ? compressionLevel : 3);
		if (!ZSTD_isError(compressedSize))
		{
			outputBuffer->len = compressedSize;
			return true;
		}
	}
#endif
#if HAVE_LIBDEFLATE
	{
		struct libdeflate_compressor *cmp =
			libdeflate_alloc_compressor(compressionLevel > 0 ? compressionLevel : 6);
		if (cmp)
		{
			size_t maxOut = libdeflate_deflate_compress_bound(cmp, transformedBuf->len);
			resetStringInfo(outputBuffer);
			enlargeStringInfo(outputBuffer, maxOut);
			size_t compressedSize = libdeflate_deflate_compress(cmp,
																transformedBuf->data,
																transformedBuf->len,
																outputBuffer->data,
																maxOut);
			libdeflate_free_compressor(cmp);
			if (compressedSize > 0)
			{
				outputBuffer->len = compressedSize;
				return true;
			}
		}
	}
#endif
	return false;   /* no compression library available */
}

/*
 * ch_decompress_inner - decompress to a buffer of exactly `decompressedSize`.
 * Tries LZ4, then ZSTD, then DEFLATE in order.
 */
static StringInfo
ch_decompress_inner(StringInfo buffer, uint64 decompressedSize)
{
	StringInfo out = makeStringInfo();
	enlargeStringInfo(out, decompressedSize);

#if HAVE_LIBLZ4
	{
		int r = LZ4_decompress_safe(buffer->data, out->data,
									buffer->len, (int) decompressedSize);
		if (r == (int) decompressedSize)
		{
			out->len = decompressedSize;
			return out;
		}
	}
#endif
#if HAVE_LIBZSTD
	{
		size_t r = ZSTD_decompress(out->data, decompressedSize,
								   buffer->data, buffer->len);
		if (!ZSTD_isError(r) && r == decompressedSize)
		{
			out->len = decompressedSize;
			return out;
		}
	}
#endif
#if HAVE_LIBDEFLATE
	{
		struct libdeflate_decompressor *dc = libdeflate_alloc_decompressor();
		if (dc)
		{
			size_t actual = 0;
			enum libdeflate_result r =
				libdeflate_deflate_decompress(dc,
											  buffer->data, buffer->len,
											  out->data, decompressedSize,
											  &actual);
			libdeflate_free_decompressor(dc);
			if (r == LIBDEFLATE_SUCCESS && actual == decompressedSize)
			{
				out->len = decompressedSize;
				return out;
			}
		}
	}
#endif
	pfree(out->data);
	pfree(out);
	ereport(ERROR,
			(errmsg("ch codec: inner decompression failed — no matching library"),
			 errdetail("compressed size=%d, expected decompressed=%lu",
					   buffer->len, (unsigned long) decompressedSize)));
}

/* ---------- Delta -------------------------------------------------------- */

/*
 * ChDeltaCompress: store v[0] unchanged; for i>0 store v[i]-v[i-1] (uint64).
 * Effective for monotonically increasing int8/timestamptz columns.
 */
static bool
ChDeltaCompress(StringInfo inputBuffer, StringInfo outputBuffer, int compressionLevel)
{
	if (inputBuffer->len % 8 != 0 || inputBuffer->len == 0)
		return false;

	size_t		n = inputBuffer->len / 8;
	uint64	   *in = (uint64 *) inputBuffer->data;
	uint64	   *tmp = palloc(inputBuffer->len);

	tmp[0] = in[0];
	for (size_t i = 1; i < n; i++)
		tmp[i] = in[i] - in[i - 1];

	StringInfoData transformedBuf;
	initStringInfo(&transformedBuf);
	transformedBuf.data = (char *) tmp;
	transformedBuf.len = inputBuffer->len;
	transformedBuf.maxlen = inputBuffer->len;

	bool ok = ch_compress_inner(&transformedBuf, outputBuffer, compressionLevel);
	pfree(tmp);
	return ok;
}

static StringInfo
ChDeltaDecompress(StringInfo buffer, uint64 decompressedSize)
{
	if (decompressedSize % 8 != 0)
		ereport(ERROR, (errmsg("Delta codec: decompressedSize=%lu is not a multiple of 8",
							   (unsigned long) decompressedSize)));

	StringInfo	out = ch_decompress_inner(buffer, decompressedSize);

	/* Inverse: prefix sum over uint64 values */
	uint64	   *data = (uint64 *) out->data;
	size_t		n = decompressedSize / 8;

	for (size_t i = 1; i < n; i++)
		data[i] += data[i - 1];

	return out;
}

/* ---------- DoubleDelta -------------------------------------------------- */

/*
 * ChDoubleDeltaCompress: second-order differences.
 * dd[0] = v[0], dd[1] = v[1]-v[0], dd[i] = (v[i]-v[i-1])-(v[i-1]-v[i-2])
 * Effective for timestamps with near-constant sampling intervals.
 */
static bool
ChDoubleDeltaCompress(StringInfo inputBuffer, StringInfo outputBuffer,
					  int compressionLevel)
{
	if (inputBuffer->len % 8 != 0 || inputBuffer->len < 8)
		return false;

	size_t		n = inputBuffer->len / 8;
	uint64	   *in = (uint64 *) inputBuffer->data;
	uint64	   *tmp = palloc(inputBuffer->len);

	tmp[0] = in[0];
	if (n > 1)
		tmp[1] = in[1] - in[0];
	for (size_t i = 2; i < n; i++)
		tmp[i] = (in[i] - in[i - 1]) - (in[i - 1] - in[i - 2]);

	StringInfoData transformedBuf;
	initStringInfo(&transformedBuf);
	transformedBuf.data = (char *) tmp;
	transformedBuf.len = inputBuffer->len;
	transformedBuf.maxlen = inputBuffer->len;

	bool ok = ch_compress_inner(&transformedBuf, outputBuffer, compressionLevel);
	pfree(tmp);
	return ok;
}

static StringInfo
ChDoubleDeltaDecompress(StringInfo buffer, uint64 decompressedSize)
{
	if (decompressedSize % 8 != 0)
		ereport(ERROR, (errmsg("DoubleDelta codec: decompressedSize=%lu not multiple of 8",
							   (unsigned long) decompressedSize)));

	StringInfo	out = ch_decompress_inner(buffer, decompressedSize);

	uint64	   *data = (uint64 *) out->data;
	size_t		n = decompressedSize / 8;

	/* Reconstruct: out[i] = 2*out[i-1] - out[i-2] + dd[i] */
	if (n > 1)
		data[1] = data[0] + data[1];
	for (size_t i = 2; i < n; i++)
		data[i] = 2 * data[i - 1] - data[i - 2] + data[i];

	return out;
}

/* ---------- Gorilla ------------------------------------------------------- */

/*
 * ChGorillaCompress: XOR consecutive 8-byte values.
 * xor[0] = v[0]; xor[i] = v[i] ^ v[i-1].
 * XOR of similar float64 values has many leading zero bytes, which LZ4
 * encodes very efficiently.  Best for slowly-drifting float8 columns.
 */
static bool
ChGorillaCompress(StringInfo inputBuffer, StringInfo outputBuffer, int compressionLevel)
{
	if (inputBuffer->len % 8 != 0 || inputBuffer->len == 0)
		return false;

	size_t		n = inputBuffer->len / 8;
	uint64	   *in = (uint64 *) inputBuffer->data;
	uint64	   *tmp = palloc(inputBuffer->len);

	tmp[0] = in[0];
	for (size_t i = 1; i < n; i++)
		tmp[i] = in[i] ^ in[i - 1];

	StringInfoData transformedBuf;
	initStringInfo(&transformedBuf);
	transformedBuf.data = (char *) tmp;
	transformedBuf.len = inputBuffer->len;
	transformedBuf.maxlen = inputBuffer->len;

	bool ok = ch_compress_inner(&transformedBuf, outputBuffer, compressionLevel);
	pfree(tmp);
	return ok;
}

static StringInfo
ChGorillaDecompress(StringInfo buffer, uint64 decompressedSize)
{
	if (decompressedSize % 8 != 0)
		ereport(ERROR, (errmsg("Gorilla codec: decompressedSize=%lu not multiple of 8",
							   (unsigned long) decompressedSize)));

	StringInfo	out = ch_decompress_inner(buffer, decompressedSize);

	/* Inverse XOR: running accumulator */
	uint64	   *data = (uint64 *) out->data;
	size_t		n = decompressedSize / 8;

	for (size_t i = 1; i < n; i++)
		data[i] ^= data[i - 1];

	return out;
}

/* ---------------------------------------------------------------------- */
typedef struct ColumnarCompressHeader
{
	int32 vl_len_;              /* varlena header (do not touch directly!) */
	int32 rawsize;
} ColumnarCompressHeader;

/*
 * Utilities for manipulation of header information for compressed data
 */

#define COLUMNAR_COMPRESS_HDRSZ ((int32) sizeof(ColumnarCompressHeader))
#define COLUMNAR_COMPRESS_RAWSIZE(ptr) (((ColumnarCompressHeader *) (ptr))->rawsize)
#define COLUMNAR_COMPRESS_RAWDATA(ptr) (((char *) (ptr)) + COLUMNAR_COMPRESS_HDRSZ)
#define COLUMNAR_COMPRESS_SET_RAWSIZE(ptr, \
									  len) (((ColumnarCompressHeader *) (ptr))->rawsize = \
												(len))


/*
 * CompressBuffer compresses the given buffer with the given compression type
 * outputBuffer enlarged to contain compressed data. The function returns true
 * if compression is done, returns false if compression is not done.
 * outputBuffer is valid only if the function returns true.
 */
bool
CompressBuffer(StringInfo inputBuffer,
			   StringInfo outputBuffer,
			   CompressionType compressionType,
			   int compressionLevel)
{
	switch (compressionType)
	{
#if HAVE_LIBLZ4
		case COMPRESSION_LZ4:
		{
			int maximumLength = LZ4_compressBound(inputBuffer->len);

			resetStringInfo(outputBuffer);
			enlargeStringInfo(outputBuffer, maximumLength);

			int compressedSize = LZ4_compress_default(inputBuffer->data,
													  outputBuffer->data,
													  inputBuffer->len, maximumLength);
			if (compressedSize <= 0)
			{
				elog(DEBUG1,
					 "failure in LZ4_compress_default, input size=%d, output size=%d",
					 inputBuffer->len, maximumLength);
				return false;
			}

			elog(DEBUG1, "compressed %d bytes to %d bytes", inputBuffer->len,
				 compressedSize);

			outputBuffer->len = compressedSize;
			return true;
		}
#endif

#if HAVE_LIBZSTD
		case COMPRESSION_ZSTD:
		{
			int maximumLength = ZSTD_compressBound(inputBuffer->len);

			resetStringInfo(outputBuffer);
			enlargeStringInfo(outputBuffer, maximumLength);

			size_t compressedSize = ZSTD_compress(outputBuffer->data,
												  outputBuffer->maxlen,
												  inputBuffer->data,
												  inputBuffer->len,
												  compressionLevel);

			if (ZSTD_isError(compressedSize))
			{
				ereport(WARNING, (errmsg("zstd compression failed"),
								  (errdetail("%s", ZSTD_getErrorName(compressedSize)))));
				return false;
			}

			outputBuffer->len = compressedSize;
			return true;
		}
#endif

#if HAVE_LIBDEFLATE
		case COMPRESSION_DEFLATE:
		{
			struct libdeflate_compressor *compressor =
				libdeflate_alloc_compressor(compressionLevel > 0 ? compressionLevel : 6);
			if (!compressor)
			{
				elog(WARNING, "libdeflate_alloc_compressor failed");
				return false;
			}

			size_t maximumLength = libdeflate_deflate_compress_bound(compressor,
																	 inputBuffer->len);
			resetStringInfo(outputBuffer);
			enlargeStringInfo(outputBuffer, maximumLength);

			size_t compressedSize = libdeflate_deflate_compress(compressor,
																inputBuffer->data,
																inputBuffer->len,
																outputBuffer->data,
																maximumLength);
			libdeflate_free_compressor(compressor);

			if (compressedSize == 0)
			{
				elog(DEBUG1, "libdeflate_deflate_compress returned 0 for input size=%d",
					 inputBuffer->len);
				return false;
			}

			elog(DEBUG1, "deflate compressed %d bytes to %zu bytes",
				 inputBuffer->len, compressedSize);

			outputBuffer->len = compressedSize;
			return true;
		}
#endif

#if HAVE_LIBZXC
		case COMPRESSION_ZXC:
		{
			zxc_compress_opts_t opts = {
				.level = compressionLevel > 0 ? compressionLevel : ZXC_LEVEL_DEFAULT,
			};

			uint64_t maximumLength = zxc_compress_bound(inputBuffer->len);

			resetStringInfo(outputBuffer);
			enlargeStringInfo(outputBuffer, maximumLength);

			int64_t compressedSize = zxc_compress(inputBuffer->data, inputBuffer->len,
												  outputBuffer->data, maximumLength,
												  &opts);
			if (compressedSize <= 0)
			{
				elog(DEBUG1, "zxc_compress failed for input size=%d", inputBuffer->len);
				return false;
			}

			elog(DEBUG1, "zxc compressed %d bytes to %lld bytes",
				 inputBuffer->len, (long long) compressedSize);

			outputBuffer->len = compressedSize;
			return true;
		}
#endif

		case COMPRESSION_PG_LZ:
		{
			uint64 maximumLength = PGLZ_MAX_OUTPUT(inputBuffer->len) +
								   COLUMNAR_COMPRESS_HDRSZ;
			bool compressionResult = false;

			resetStringInfo(outputBuffer);
			enlargeStringInfo(outputBuffer, maximumLength);

			int32 compressedByteCount = pglz_compress((const char *) inputBuffer->data,
													  inputBuffer->len,
													  COLUMNAR_COMPRESS_RAWDATA(
														  outputBuffer->data),
													  PGLZ_strategy_always);
			if (compressedByteCount >= 0)
			{
				COLUMNAR_COMPRESS_SET_RAWSIZE(outputBuffer->data, inputBuffer->len);
				SET_VARSIZE_COMPRESSED(outputBuffer->data,
									   compressedByteCount + COLUMNAR_COMPRESS_HDRSZ);
				compressionResult = true;
			}

			if (compressionResult)
			{
				outputBuffer->len = VARSIZE(outputBuffer->data);
			}

			return compressionResult;
		}

		/* --- ClickHouse-style transform codecs (v2.3) --- */
		case COMPRESSION_DELTA:
			return ChDeltaCompress(inputBuffer, outputBuffer, compressionLevel);

		case COMPRESSION_DOUBLEDELTA:
			return ChDoubleDeltaCompress(inputBuffer, outputBuffer, compressionLevel);

		case COMPRESSION_GORILLA:
			return ChGorillaCompress(inputBuffer, outputBuffer, compressionLevel);

		default:
		{
			return false;
		}
	}
}


/*
 * DecompressBuffer decompresses the given buffer with the given compression
 * type. This function returns the buffer as-is when no compression is applied.
 */
StringInfo
DecompressBuffer(StringInfo buffer,
				 CompressionType compressionType,
				 uint64 decompressedSize)
{
	switch (compressionType)
	{
		case COMPRESSION_NONE:
		{
			return buffer;
		}

#if HAVE_LIBLZ4
		case COMPRESSION_LZ4:
		{
			StringInfo decompressedBuffer = makeStringInfo();
			enlargeStringInfo(decompressedBuffer, decompressedSize);

			int lz4DecompressSize = LZ4_decompress_safe(buffer->data,
														decompressedBuffer->data,
														buffer->len,
														decompressedSize);

			if (lz4DecompressSize != decompressedSize)
			{
				ereport(ERROR, (errmsg("cannot decompress the buffer"),
								errdetail("Expected %lu bytes, but received %d bytes",
										  decompressedSize, lz4DecompressSize)));
			}

			decompressedBuffer->len = decompressedSize;

			return decompressedBuffer;
		}
#endif

#if HAVE_LIBZSTD
		case COMPRESSION_ZSTD:
		{
			StringInfo decompressedBuffer = makeStringInfo();
			enlargeStringInfo(decompressedBuffer, decompressedSize);

			size_t zstdDecompressSize = ZSTD_decompress(decompressedBuffer->data,
														decompressedSize,
														buffer->data,
														buffer->len);
			if (ZSTD_isError(zstdDecompressSize))
			{
				ereport(ERROR, (errmsg("zstd decompression failed"),
								(errdetail("%s", ZSTD_getErrorName(
											   zstdDecompressSize)))));
			}

			if (zstdDecompressSize != decompressedSize)
			{
				ereport(ERROR, (errmsg("unexpected decompressed size"),
								errdetail("Expected %ld, received %ld", decompressedSize,
										  zstdDecompressSize)));
			}

			decompressedBuffer->len = decompressedSize;

			return decompressedBuffer;
		}
#endif

#if HAVE_LIBDEFLATE
		case COMPRESSION_DEFLATE:
		{
			struct libdeflate_decompressor *decompressor =
				libdeflate_alloc_decompressor();
			if (!decompressor)
				ereport(ERROR, (errmsg("libdeflate_alloc_decompressor failed")));

			StringInfo decompressedBuffer = makeStringInfo();
			enlargeStringInfo(decompressedBuffer, decompressedSize);

			size_t actualSize = 0;
			enum libdeflate_result result =
				libdeflate_deflate_decompress(decompressor,
											  buffer->data, buffer->len,
											  decompressedBuffer->data, decompressedSize,
											  &actualSize);
			libdeflate_free_decompressor(decompressor);

			if (result != LIBDEFLATE_SUCCESS)
			{
				ereport(ERROR, (errmsg("libdeflate decompression failed"),
								errdetail("error code: %d", (int) result)));
			}

			if (actualSize != decompressedSize)
			{
				ereport(ERROR, (errmsg("unexpected decompressed size"),
								errdetail("Expected %lu, received %zu",
										  decompressedSize, actualSize)));
			}

			decompressedBuffer->len = decompressedSize;
			return decompressedBuffer;
		}
#endif

#if HAVE_LIBZXC
		case COMPRESSION_ZXC:
		{
			StringInfo decompressedBuffer = makeStringInfo();
			enlargeStringInfo(decompressedBuffer, decompressedSize);

			zxc_decompress_opts_t opts = { 0 };
			int64_t actualSize = zxc_decompress(buffer->data, buffer->len,
												decompressedBuffer->data, decompressedSize,
												&opts);
			if (actualSize < 0)
			{
				ereport(ERROR, (errmsg("zxc decompression failed"),
								errdetail("error code: %lld", (long long) actualSize)));
			}

			if ((uint64_t) actualSize != decompressedSize)
			{
				ereport(ERROR, (errmsg("unexpected decompressed size"),
								errdetail("Expected %lu, received %lld",
										  decompressedSize, (long long) actualSize)));
			}

			decompressedBuffer->len = decompressedSize;
			return decompressedBuffer;
		}
#endif

		case COMPRESSION_PG_LZ:
		{
			uint32 compressedDataSize = VARSIZE(buffer->data) - COLUMNAR_COMPRESS_HDRSZ;
			uint32 decompressedDataSize = COLUMNAR_COMPRESS_RAWSIZE(buffer->data);

			if (compressedDataSize + COLUMNAR_COMPRESS_HDRSZ != buffer->len)
			{
				ereport(ERROR, (errmsg("cannot decompress the buffer"),
								errdetail("Expected %u bytes, but received %u bytes",
										  compressedDataSize, buffer->len)));
			}

			char *decompressedData = palloc0(decompressedDataSize);

			int32 decompressedByteCount = pglz_decompress(COLUMNAR_COMPRESS_RAWDATA(
															  buffer->data),
														  compressedDataSize,
														  decompressedData,
														  decompressedDataSize, true);

			if (decompressedByteCount < 0)
			{
				ereport(ERROR, (errmsg("cannot decompress the buffer"),
								errdetail("compressed data is corrupted")));
			}

			StringInfo decompressedBuffer = palloc0(sizeof(StringInfoData));
			decompressedBuffer->data = decompressedData;
			decompressedBuffer->len = decompressedDataSize;
			decompressedBuffer->maxlen = decompressedDataSize;

			return decompressedBuffer;
		}

		/* --- ClickHouse-style transform codecs (v2.3) --- */
		case COMPRESSION_DELTA:
			return ChDeltaDecompress(buffer, decompressedSize);

		case COMPRESSION_DOUBLEDELTA:
			return ChDoubleDeltaDecompress(buffer, decompressedSize);

		case COMPRESSION_GORILLA:
			return ChGorillaDecompress(buffer, decompressedSize);

		default:
		{
			ereport(ERROR, (errmsg("unexpected compression type: %d", compressionType)));
		}
	}
}
