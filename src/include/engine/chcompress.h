/*-------------------------------------------------------------------------
 *
 * chcompress.h
 *
 * Public types and declarations for the chcompress Table Access Method.
 *
 * chcompress is an experimental, non-transactional Table AM that uses
 * chDB (embedded ClickHouse) as the storage and query engine.  Each
 * chcompress table is backed by a ClickHouse MergeTree (or a variant)
 * table stored under <DataDir>/chdb/<db_oid>/.
 *
 * Design notes:
 *   - NON-TRANSACTIONAL: writes are not rolled back on transaction abort.
 *     Use only for append-only, bulk-load, or analytics workloads where
 *     ACID is not required for the individual writes.
 *   - SINGLE-WRITER per backend: chDB allows only one active connection
 *     per process.  Multiple readers are safe.  For concurrent writes from
 *     multiple backends, the caller must hold the table-level lock
 *     (SHARE ROW EXCLUSIVE MODE).
 *   - PARALLEL READ: parallel sequential scan is supported (each worker
 *     opens its own read-only connection to the same chDB path using the
 *     read-only flag).
 *
 * Reference: https://clickhouse.com/docs/chdb/install/c
 *
 * Copyright (c) Saulo J. Benvenutti, 2026
 *
 *-------------------------------------------------------------------------
 */

#ifndef CHCOMPRESS_H
#define CHCOMPRESS_H

#include "postgres.h"
#include "access/tableam.h"
#include "utils/relcache.h"

/* -----------------------------------------------------------------------
 * Catalog constants
 * ----------------------------------------------------------------------- */
#define CHCOMPRESS_NAMESPACE_NAME       "engine"
#define CHCOMPRESS_OPTIONS_TABLE_NAME   "ch_options"

/* -----------------------------------------------------------------------
 * Default options
 * ----------------------------------------------------------------------- */
#define CHCOMPRESS_DEFAULT_ENGINE       "MergeTree"
#define CHCOMPRESS_DEFAULT_ORDER_BY     "tuple()"

/* -----------------------------------------------------------------------
 * Per-table options stored in engine.ch_options
 * ----------------------------------------------------------------------- */
typedef struct ChcompressOptions
{
	Oid		relation_id;		/* PG relation OID */
	char   *ch_engine;			/* MergeTree / CollapsingMergeTree / ReplacingMergeTree */
	char   *order_by;			/* ORDER BY expression (required for MergeTree) */
	char   *partition_by;		/* PARTITION BY expression (optional) */
	char   *primary_key;		/* PRIMARY KEY expression (optional) */
	char   *settings;			/* Additional ENGINE SETTINGS clause (optional) */
} ChcompressOptions;

/* -----------------------------------------------------------------------
 * Scan state (opaque, heap-allocated per scan)
 * ----------------------------------------------------------------------- */
typedef struct ChcompressScanDesc
{
	TableScanDescData rs_base;  /* must be first */

	/* chDB streaming result handle */
	void   *stream_result;		/* chdb_result* from chdb_stream_query */
	void   *current_chunk;		/* chdb_result* from chdb_stream_fetch_result */

	/* CSV parsing position within current chunk */
	char   *chunk_pos;
	char   *chunk_end;

	/* Tuple descriptor cache */
	TupleDesc tupdesc;

	/* Are we done? */
	bool	eof;
} ChcompressScanDesc;

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

/* Module init — called from _PG_init */
extern void chcompress_tableam_init(void);

/* TAM handler (registered as PG function) */
extern Datum chcompress_handler(PG_FUNCTION_ARGS);

/* Connection management — returns struct se_chdb_conn ** (defined in chcompress_tableam.c) */
extern void se_chdb_close_connection(void);

/* Table management */
extern void se_chdb_create_table(Relation rel, ChcompressOptions *opts);
extern void se_chdb_drop_table(Oid relid);
extern void se_chdb_truncate_table(Oid relid);

/* Options I/O */
extern ChcompressOptions *se_chdb_get_options(Oid relid);
extern void se_chdb_save_options(ChcompressOptions *opts);
extern void se_chdb_delete_options(Oid relid);

/* Utility */
extern const char *se_chdb_pg_type_to_ch(Oid typid);
extern char *se_chdb_build_ddl(Relation rel, ChcompressOptions *opts);

#endif /* CHCOMPRESS_H */
