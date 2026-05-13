/*-------------------------------------------------------------------------
 *
 * chcompress_tableam.c
 *
 * chcompress — Table Access Method backed by embedded ClickHouse (chDB).
 *
 * libchdb is loaded at runtime via dlopen(RTLD_LAZY|RTLD_LOCAL) to avoid
 * symbol conflicts between chDB's bundled libraries and the PostgreSQL
 * backend.  The extension SO does NOT directly link against libchdb.so;
 * it only needs -ldl at link time.
 *
 * Supported: INSERT, sequential scan, VACUUM (→ OPTIMIZE TABLE FINAL),
 *            size reporting.
 * Not supported: UPDATE, DELETE, index scan, parallel write, transactions.
 *
 * Copyright (c) Saulo J. Benvenutti, 2026
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <dlfcn.h>

#include "pg_version_constants.h"
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "executor/executor.h"

#include "commands/vacuum.h"
#include "engine/chcompress.h"

PG_FUNCTION_INFO_V1(chcompress_handler);

/* -----------------------------------------------------------------------
 * chDB type definitions (subset of chdb.h, C-only, no C++ defaults)
 * These mirror the structs in chdb.h so we can work with chDB results
 * without including the header at compile time.
 * ----------------------------------------------------------------------- */

struct se_chdb_result {
	char    *buf;
	size_t   len;
	void    *_vec;
	double   elapsed;
	uint64   rows_read;
	uint64   bytes_read;
	char    *error_message;
};

struct se_chdb_conn {
	void *server;
	bool  connected;
};

typedef struct { void *internal_data; } se_chdb_streaming_result;

/* -----------------------------------------------------------------------
 * Function pointer types for chDB API
 * ----------------------------------------------------------------------- */
typedef struct se_chdb_conn **(*pfn_connect_chdb_t)(int argc, char **argv);
typedef void (*pfn_close_conn_t)(struct se_chdb_conn **conn);
typedef struct se_chdb_result *(*pfn_query_conn_t)(struct se_chdb_conn *conn, const char *query, const char *format);
typedef se_chdb_streaming_result *(*pfn_query_conn_streaming_t)(struct se_chdb_conn *conn, const char *query, const char *format);
typedef const char *(*pfn_streaming_result_error_t)(se_chdb_streaming_result *result);
typedef struct se_chdb_result *(*pfn_streaming_fetch_result_t)(struct se_chdb_conn *conn, se_chdb_streaming_result *result);
typedef void (*pfn_streaming_cancel_query_t)(struct se_chdb_conn *conn, se_chdb_streaming_result *result);
typedef void (*pfn_destroy_result_t)(se_chdb_streaming_result *result);
typedef void (*pfn_free_result_t)(struct se_chdb_result *result);

/* -----------------------------------------------------------------------
 * dlopen state — loaded once per process
 * ----------------------------------------------------------------------- */

#define CHDB_LIB_PATH  "/usr/local/lib/libchdb.so"

static void                         *se_chdb_dlhandle          = NULL;
static pfn_connect_chdb_t            pfn_connect_chdb           = NULL;
static pfn_close_conn_t              pfn_close_conn             = NULL;
static pfn_query_conn_t              pfn_query_conn             = NULL;
static pfn_query_conn_streaming_t    pfn_query_conn_streaming   = NULL;
static pfn_streaming_result_error_t  pfn_streaming_result_error = NULL;
static pfn_streaming_fetch_result_t  pfn_streaming_fetch_result = NULL;
static pfn_streaming_cancel_query_t  pfn_streaming_cancel_query = NULL;
static pfn_destroy_result_t          pfn_destroy_result         = NULL;
static pfn_free_result_t             pfn_free_result            = NULL;

/* Load symbol from dlhandle, raise ERROR if not found */
#define CHDB_LOAD_SYM(var, sym) \
	do { \
		*(void **)(&(var)) = dlsym(se_chdb_dlhandle, (sym)); \
		if ((var) == NULL) \
			ereport(ERROR, (errmsg("chcompress: symbol '%s' not found in libchdb: %s", \
								   (sym), dlerror()))); \
	} while(0)

/*
 * se_chdb_load — load libchdb.so via dlopen and resolve all function pointers.
 * Idempotent; raises ERROR if the library is not found.
 */
static void
se_chdb_load(void)
{
	if (se_chdb_dlhandle != NULL)
		return;

	se_chdb_dlhandle = dlopen(CHDB_LIB_PATH, RTLD_LAZY | RTLD_LOCAL);
	if (se_chdb_dlhandle == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("chcompress: failed to load libchdb: %s", dlerror()),
				 errhint("Install libchdb: curl -sL https://lib.chdb.io | bash")));

	CHDB_LOAD_SYM(pfn_connect_chdb,           "connect_chdb");
	CHDB_LOAD_SYM(pfn_close_conn,             "close_conn");
	CHDB_LOAD_SYM(pfn_query_conn,             "query_conn");
	CHDB_LOAD_SYM(pfn_query_conn_streaming,   "query_conn_streaming");
	CHDB_LOAD_SYM(pfn_streaming_result_error, "chdb_streaming_result_error");
	CHDB_LOAD_SYM(pfn_streaming_fetch_result, "chdb_streaming_fetch_result");
	CHDB_LOAD_SYM(pfn_streaming_cancel_query, "chdb_streaming_cancel_query");
	CHDB_LOAD_SYM(pfn_destroy_result,         "chdb_destroy_result");
	CHDB_LOAD_SYM(pfn_free_result,            "free_result_v2");

	elog(DEBUG1, "chcompress: libchdb loaded from %s", CHDB_LIB_PATH);
}

/* -----------------------------------------------------------------------
 * Connection management
 * ----------------------------------------------------------------------- */

static struct se_chdb_conn **se_chdb_conn = NULL;
static char se_chdb_conn_path[MAXPGPATH] = {0};

/*
 * se_chdb_is_loaded — true only if libchdb has been loaded and connected.
 * Used in callbacks that are called during DDL (CREATE TABLE, etc.) where
 * we must NOT trigger initialization.
 */
static inline bool
se_chdb_is_loaded(void)
{
	return se_chdb_conn != NULL;
}

static void
se_chdb_on_proc_exit(int code, Datum arg)
{
	if (se_chdb_conn != NULL && pfn_close_conn != NULL)
	{
		pfn_close_conn(se_chdb_conn);
		se_chdb_conn = NULL;
	}
}

struct se_chdb_conn **
se_chdb_get_connection(void)
{
	if (se_chdb_conn != NULL)
		return se_chdb_conn;

	se_chdb_load();

	/* <DataDir>/chdb/<db_oid> */
	char path[MAXPGPATH];
	snprintf(path, sizeof(path), "%s/chdb/" UINT64_FORMAT,
			 DataDir, (uint64) MyDatabaseId);

	char parent[MAXPGPATH];
	snprintf(parent, sizeof(parent), "%s/chdb", DataDir);
	mkdir(parent, 0700);
	mkdir(path, 0700);

	char *path_arg = psprintf("--path=%s", path);
	char *argv[]   = { "chdb", path_arg };
	int   argc     = 2;

	se_chdb_conn = pfn_connect_chdb(argc, argv);
	pfree(path_arg);

	if (se_chdb_conn == NULL || *se_chdb_conn == NULL || !(*se_chdb_conn)->connected)
		ereport(ERROR,
				(errmsg("chcompress: failed to open chDB connection"),
				 errdetail("path: %s", path)));

	strlcpy(se_chdb_conn_path, path, sizeof(se_chdb_conn_path));
	on_proc_exit(se_chdb_on_proc_exit, (Datum) 0);
	elog(DEBUG1, "chcompress: opened chDB connection at %s", path);
	return se_chdb_conn;
}

void
se_chdb_close_connection(void)
{
	if (se_chdb_conn != NULL && pfn_close_conn != NULL)
	{
		pfn_close_conn(se_chdb_conn);
		se_chdb_conn = NULL;
		se_chdb_conn_path[0] = '\0';
	}
}

/* -----------------------------------------------------------------------
 * Query helpers
 * ----------------------------------------------------------------------- */

static void
se_chdb_exec(const char *sql)
{
	struct se_chdb_conn **conn = se_chdb_get_connection();
	struct se_chdb_result *res = pfn_query_conn(*conn, sql, "CSV");

	if (res == NULL)
		ereport(ERROR, (errmsg("chcompress: chDB query returned NULL"),
						errdetail("SQL: %s", sql)));

	if (res->error_message != NULL)
	{
		char *ch_errmsg = pstrdup(res->error_message);
		pfn_free_result(res);
		ereport(ERROR, (errmsg("chcompress: chDB error: %s", ch_errmsg),
						errdetail("SQL: %s", sql)));
	}
	pfn_free_result(res);
}

static struct se_chdb_result *
se_chdb_query(const char *sql, const char *format)
{
	struct se_chdb_conn **conn = se_chdb_get_connection();
	struct se_chdb_result *res = pfn_query_conn(*conn, sql, format);

	if (res == NULL)
		ereport(ERROR, (errmsg("chcompress: chDB query returned NULL"),
						errdetail("SQL: %s", sql)));

	if (res->error_message != NULL)
	{
		char *ch_errmsg = pstrdup(res->error_message);
		pfn_free_result(res);
		ereport(ERROR, (errmsg("chcompress: chDB error: %s", ch_errmsg),
						errdetail("SQL: %s", sql)));
	}
	return res;
}

/* -----------------------------------------------------------------------
 * Type mapping
 * ----------------------------------------------------------------------- */

const char *
se_chdb_pg_type_to_ch(Oid typid)
{
	switch (typid)
	{
		case BOOLOID:		return "UInt8";
		case INT2OID:		return "Int16";
		case INT4OID:		return "Int32";
		case INT8OID:		return "Int64";
		case FLOAT4OID:		return "Float32";
		case FLOAT8OID:		return "Float64";
		case NUMERICOID:	return "Decimal(38, 10)";
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:		return "String";
		case BYTEAOID:		return "String";
		case DATEOID:		return "Date32";
		case TIMESTAMPOID:	return "DateTime64(6)";
		case TIMESTAMPTZOID:return "DateTime64(6, 'UTC')";
		case TIMEOID:
		case TIMETZOID:
		case INTERVALOID:	return "String";
		case OIDOID:		return "UInt32";
		case UUIDOID:		return "UUID";
		case JSONOID:
		case JSONBOID:		return "String";
		default:			return "String";
	}
}

/* -----------------------------------------------------------------------
 * DDL helpers
 * ----------------------------------------------------------------------- */

static char *
se_chdb_ch_table_name(Oid relid)
{
	return psprintf("ch_" UINT64_FORMAT, (uint64) relid);
}

char *
se_chdb_build_ddl(Relation rel, ChcompressOptions *opts)
{
	TupleDesc   tupdesc = RelationGetDescr(rel);
	StringInfoData buf;
	initStringInfo(&buf);

	char *ch_table = se_chdb_ch_table_name(RelationGetRelid(rel));
	appendStringInfo(&buf, "CREATE TABLE IF NOT EXISTS %s (", ch_table);

	bool first = true;
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (attr->attisdropped)
			continue;
		if (!first)
			appendStringInfoString(&buf, ", ");
		first = false;

		const char *ch_type = se_chdb_pg_type_to_ch(attr->atttypid);
		if (!attr->attnotnull)
			appendStringInfo(&buf, "%s Nullable(%s)", NameStr(attr->attname), ch_type);
		else
			appendStringInfo(&buf, "%s %s", NameStr(attr->attname), ch_type);
	}
	if (first)
		appendStringInfoString(&buf, "_dummy UInt8");

	appendStringInfo(&buf, ") ENGINE = %s()", opts->ch_engine);
	if (opts->order_by && opts->order_by[0])
		appendStringInfo(&buf, " ORDER BY (%s)", opts->order_by);
	else
		appendStringInfoString(&buf, " ORDER BY tuple()");
	if (opts->partition_by && opts->partition_by[0])
		appendStringInfo(&buf, " PARTITION BY (%s)", opts->partition_by);
	if (opts->primary_key && opts->primary_key[0])
		appendStringInfo(&buf, " PRIMARY KEY (%s)", opts->primary_key);
	if (opts->settings && opts->settings[0])
		appendStringInfo(&buf, " SETTINGS %s", opts->settings);

	return buf.data;
}

/* -----------------------------------------------------------------------
 * Table lifecycle
 * ----------------------------------------------------------------------- */

void
se_chdb_create_table(Relation rel, ChcompressOptions *opts)
{
	char *ddl = se_chdb_build_ddl(rel, opts);
	elog(DEBUG1, "chcompress CREATE: %s", ddl);
	se_chdb_exec(ddl);
	pfree(ddl);
}

void
se_chdb_drop_table(Oid relid)
{
	char *ch_table = se_chdb_ch_table_name(relid);
	char *sql = psprintf("DROP TABLE IF EXISTS %s", ch_table);
	se_chdb_exec(sql);
	pfree(sql);
	pfree(ch_table);
	se_chdb_delete_options(relid);
}

void
se_chdb_truncate_table(Oid relid)
{
	char *ch_table = se_chdb_ch_table_name(relid);
	char *sql = psprintf("TRUNCATE TABLE IF EXISTS %s", ch_table);
	se_chdb_exec(sql);
	pfree(sql);
	pfree(ch_table);
}

/* -----------------------------------------------------------------------
 * Options catalog stubs
 * ----------------------------------------------------------------------- */

ChcompressOptions *
se_chdb_get_options(Oid relid)
{
	ChcompressOptions *opts = palloc0(sizeof(ChcompressOptions));
	opts->relation_id  = relid;
	opts->ch_engine    = pstrdup(CHCOMPRESS_DEFAULT_ENGINE);
	opts->order_by     = pstrdup(CHCOMPRESS_DEFAULT_ORDER_BY);
	opts->partition_by = NULL;
	opts->primary_key  = NULL;
	opts->settings     = NULL;
	return opts;
}

void se_chdb_save_options(ChcompressOptions *opts) { (void) opts; }
void se_chdb_delete_options(Oid relid) { (void) relid; }

/* -----------------------------------------------------------------------
 * CSV helpers
 * ----------------------------------------------------------------------- */

static char *
se_chdb_escape_csv_field(const char *val)
{
	if (val == NULL)
		return pstrdup("\\N");

	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfoChar(&buf, '"');
	for (const char *p = val; *p; p++)
	{
		if (*p == '"')
			appendStringInfoChar(&buf, '"');
		appendStringInfoChar(&buf, *p);
	}
	appendStringInfoChar(&buf, '"');
	return buf.data;
}

static char *
se_chdb_slot_to_csv(TupleTableSlot *slot)
{
	TupleDesc tupdesc = slot->tts_tupleDescriptor;
	StringInfoData buf;
	initStringInfo(&buf);

	slot_getallattrs(slot);

	bool first = true;
	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (attr->attisdropped)
			continue;
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;

		if (slot->tts_isnull[i])
		{
			appendStringInfoString(&buf, "\\N");
			continue;
		}

		Oid  outputFuncOid;
		bool isVarlena;
		getTypeOutputInfo(attr->atttypid, &outputFuncOid, &isVarlena);
		char *str = OidOutputFunctionCall(outputFuncOid, slot->tts_values[i]);
		char *escaped = se_chdb_escape_csv_field(str);
		appendStringInfoString(&buf, escaped);
		pfree(str);
		pfree(escaped);
	}
	return buf.data;
}

/*
 * se_chdb_ensure_table - lazily create the ClickHouse table on first write.
 *
 * Called from tuple_insert / multi_insert.  The chDB library is NOT loaded
 * during CREATE TABLE (to avoid C++ global-state conflicts in a freshly-forked
 * PG backend).  We load it here instead, where we actually need data access.
 */
static void
se_chdb_ensure_table(Relation rel)
{
	Oid relid = RelationGetRelid(rel);
	char *ch_table = se_chdb_ch_table_name(relid);

	/*
	 * Quick existence check: try "SELECT 1 FROM <table> LIMIT 0".
	 * If it fails, the table doesn't exist yet → create it.
	 */
	struct se_chdb_conn **conn = se_chdb_get_connection();
	char *check_sql = psprintf("SELECT 1 FROM %s LIMIT 0", ch_table);
	struct se_chdb_result *res = pfn_query_conn(*conn, check_sql, "CSV");
	pfree(check_sql);

	bool needs_create = (res == NULL || res->error_message != NULL);
	pfn_free_result(res);

	if (needs_create)
	{
		ChcompressOptions *opts = se_chdb_get_options(relid);
		se_chdb_create_table(rel, opts);
	}

	pfree(ch_table);
}

/* -----------------------------------------------------------------------
 * INSERT
 * ----------------------------------------------------------------------- */

#if PG_VERSION_NUM >= PG_VERSION_19
static void
chcompress_tuple_insert(Relation relation, TupleTableSlot *slot,
						CommandId cid, uint32 options, BulkInsertStateData *bistate)
#else
static void
chcompress_tuple_insert(Relation relation, TupleTableSlot *slot,
						CommandId cid, int options, BulkInsertState bistate)
#endif
{
	se_chdb_ensure_table(relation);
	ExecMaterializeSlot(slot);
	char *ch_table = se_chdb_ch_table_name(RelationGetRelid(relation));
	char *csv_row  = se_chdb_slot_to_csv(slot);
	char *sql = psprintf("INSERT INTO %s FORMAT CSV\n%s\n", ch_table, csv_row);
	se_chdb_exec(sql);
	pfree(sql);
	pfree(csv_row);
	pfree(ch_table);
	slot->tts_tid = (ItemPointerData){ .ip_blkid = {0xff, 0xff}, .ip_posid = 1 };
}

#if PG_VERSION_NUM >= PG_VERSION_19
static void
chcompress_multi_insert(Relation relation, TupleTableSlot **slots, int nslots,
						CommandId cid, uint32 options, BulkInsertStateData *bistate)
#else
static void
chcompress_multi_insert(Relation relation, TupleTableSlot **slots, int nslots,
						CommandId cid, int options, BulkInsertState bistate)
#endif
{
	if (nslots == 0)
		return;

	se_chdb_ensure_table(relation);

	char *ch_table = se_chdb_ch_table_name(RelationGetRelid(relation));
	StringInfoData payload;
	initStringInfo(&payload);

	for (int i = 0; i < nslots; i++)
	{
		ExecMaterializeSlot(slots[i]);
		char *row = se_chdb_slot_to_csv(slots[i]);
		appendStringInfo(&payload, "%s\n", row);
		pfree(row);
		slots[i]->tts_tid = (ItemPointerData){ .ip_blkid = {0xff, 0xff}, .ip_posid = (OffsetNumber)(i + 1) };
	}

	char *sql = psprintf("INSERT INTO %s FORMAT CSV\n%s", ch_table, payload.data);
	se_chdb_exec(sql);
	pfree(sql);
	pfree(payload.data);
	pfree(ch_table);
}

/* -----------------------------------------------------------------------
 * Scan
 * ----------------------------------------------------------------------- */

static TableScanDesc
chcompress_scan_begin(Relation rel, Snapshot snapshot, int nkeys, ScanKey key,
					  ParallelTableScanDesc parallel_scan, uint32 flags)
{
	ChcompressScanDesc *scan = palloc0(sizeof(ChcompressScanDesc));
	scan->rs_base.rs_rd       = rel;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys    = nkeys;
	scan->rs_base.rs_flags    = flags;
	scan->tupdesc = RelationGetDescr(rel);
	scan->eof = false;

	/*
	 * Use the non-streaming API for reliability.  All rows are fetched into
	 * a single local_result_v2 buffer and iterated by the slot callbacks.
	 * Streaming can be added later as an optimisation.
	 */
	char *ch_table = se_chdb_ch_table_name(RelationGetRelid(rel));
	char *sql      = psprintf("SELECT * FROM %s FORMAT CSV", ch_table);
	pfree(ch_table);

	struct se_chdb_result *res = se_chdb_query(sql, "CSV");
	pfree(sql);

	if (res == NULL)
		ereport(ERROR, (errmsg("chcompress: scan query returned NULL")));

	if (res->error_message != NULL)
	{
		char *errcopy = pstrdup(res->error_message);
		pfn_free_result(res);
		ereport(ERROR, (errmsg("chcompress scan error: %s", errcopy)));
	}

	/* Store the full result as the single "chunk" */
	scan->current_chunk = res;
	scan->chunk_pos = res->buf;
	scan->chunk_end = res->buf + res->len;
	scan->stream_result = NULL;   /* unused in non-streaming mode */
	return (TableScanDesc) scan;
}

static void
chcompress_scan_end(TableScanDesc sscan)
{
	ChcompressScanDesc *scan = (ChcompressScanDesc *) sscan;

	/* stream_result is NULL in non-streaming mode — nothing to do there */
	if (scan->current_chunk != NULL)
	{
		pfn_free_result((struct se_chdb_result *) scan->current_chunk);
		scan->current_chunk = NULL;
	}
	pfree(scan);
}

static void
chcompress_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
					   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	ereport(WARNING, (errmsg("chcompress: rescan not supported")));
	((ChcompressScanDesc *) sscan)->eof = true;
}

/*
 * parse_csv_field — parse one CSV field from *pos, advance past separator.
 */
static char *
parse_csv_field(char **pos, char *end, bool *isnull)
{
	*isnull = false;
	char *p = *pos;

	if (p >= end) { *isnull = true; return NULL; }

	if (*p == '"')
	{
		p++;
		StringInfoData buf;
		initStringInfo(&buf);
		while (p < end)
		{
			if (*p == '"')
			{
				p++;
				if (p < end && *p == '"') { appendStringInfoChar(&buf, '"'); p++; }
				else break;
			}
			else appendStringInfoChar(&buf, *p++);
		}
		if (p < end && (*p == ',' || *p == '\n' || *p == '\r')) p++;
		if (p < end && *p == '\n') p++;
		*pos = p;
		return buf.data;
	}

	char *start = p;
	while (p < end && *p != ',' && *p != '\n' && *p != '\r') p++;
	size_t flen = p - start;
	char *field = palloc(flen + 1);
	memcpy(field, start, flen);
	field[flen] = '\0';
	if (p < end && *p == ',') p++;
	else if (p < end && *p == '\r') p++;
	if (p < end && *p == '\n') p++;
	*pos = p;
	if (flen == 2 && field[0] == '\\' && field[1] == 'N') { pfree(field); *isnull = true; return NULL; }
	return field;
}

static bool
chcompress_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
							TupleTableSlot *slot)
{
	ChcompressScanDesc *scan = (ChcompressScanDesc *) sscan;

	/* EOF: no data, or we've consumed the full result buffer */
	if (scan->eof ||
		scan->chunk_pos == NULL ||
		scan->chunk_pos >= scan->chunk_end)
	{
		scan->eof = true;
		ExecClearTuple(slot);
		return false;
	}

	TupleDesc tupdesc = scan->tupdesc;
	ExecClearTuple(slot);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (attr->attisdropped)
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
			continue;
		}

		bool isnull;
		char *field = parse_csv_field(&scan->chunk_pos, scan->chunk_end, &isnull);

		if (isnull || field == NULL)
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
		else
		{
			Oid inputFuncOid;
			Oid typioParam;
			getTypeInputInfo(attr->atttypid, &inputFuncOid, &typioParam);
			slot->tts_values[i] = OidInputFunctionCall(inputFuncOid, field, typioParam, attr->atttypmod);
			slot->tts_isnull[i] = false;
			pfree(field);
		}
	}

	/* Advance past any remaining bytes on this CSV line (e.g. extra columns) */
	while (scan->chunk_pos < scan->chunk_end && scan->chunk_pos[0] != '\n')
		scan->chunk_pos++;
	if (scan->chunk_pos < scan->chunk_end && scan->chunk_pos[0] == '\n')
		scan->chunk_pos++;

	ExecStoreVirtualTuple(slot);
	return true;
}

/* -----------------------------------------------------------------------
 * DML stubs
 * ----------------------------------------------------------------------- */

#if PG_VERSION_NUM >= PG_VERSION_19
static TM_Result
chcompress_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
						uint32 options, Snapshot snapshot, Snapshot crosscheck,
						bool wait, TM_FailureData *tmfd)
#else
static TM_Result
chcompress_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
						Snapshot snapshot, Snapshot crosscheck, bool wait,
						TM_FailureData *tmfd, bool changingPart)
#endif
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("chcompress: DELETE is not supported"),
					errhint("Use CollapsingMergeTree engine with a sign column.")));
}

#if PG_VERSION_NUM >= PG_VERSION_19
static TM_Result
chcompress_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
						CommandId cid, uint32 options, Snapshot snapshot,
						Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
						LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
#elif PG_VERSION_NUM >= PG_VERSION_16
static TM_Result
chcompress_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
						CommandId cid, Snapshot snapshot, Snapshot crosscheck,
						bool wait, TM_FailureData *tmfd,
						LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
#else
static TM_Result
chcompress_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
						CommandId cid, Snapshot snapshot, Snapshot crosscheck,
						bool wait, TM_FailureData *tmfd,
						LockTupleMode *lockmode, bool *update_indexes)
#endif
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("chcompress: UPDATE is not supported"),
					errhint("Use ReplacingMergeTree engine.")));
}

static TM_Result
chcompress_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
					  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					  LockWaitPolicy wait_policy, uint8 flags,
					  TM_FailureData *tmfd)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("chcompress: row-level locking is not supported")));
}

/* -----------------------------------------------------------------------
 * VACUUM
 * ----------------------------------------------------------------------- */

#if PG_VERSION_NUM >= PG_VERSION_19
static void
chcompress_relation_vacuum(Relation rel, const VacuumParams *params,
						   BufferAccessStrategy bstrategy)
#else
static void
chcompress_relation_vacuum(Relation rel, struct VacuumParams *params,
						   BufferAccessStrategy bstrategy)
#endif
{
	char *ch_table = se_chdb_ch_table_name(RelationGetRelid(rel));
	char *sql = psprintf("OPTIMIZE TABLE %s FINAL", ch_table);
	se_chdb_exec(sql);
	pfree(sql);
	pfree(ch_table);
}

/* -----------------------------------------------------------------------
 * DDL callbacks
 * ----------------------------------------------------------------------- */

#if PG_VERSION_NUM >= PG_VERSION_16
static void
chcompress_relation_set_new_filelocator(Relation rel,
										const RelFileLocator *newrlocator,
										char persistence,
										TransactionId *freezeXid,
										MultiXactId *minmulti)
#else
static void
chcompress_relation_set_new_filenode(Relation rel,
									 const RelFileNode *newrnode,
									 char persistence,
									 TransactionId *freezeXid,
									 MultiXactId *minmulti)
#endif
{
	/*
	 * We intentionally do NOT create the ClickHouse table here.  Loading
	 * libchdb.so inside a freshly-forked PostgreSQL backend causes a crash
	 * due to C++ global state / thread-pool initialization conflicts.
	 *
	 * The chDB table is created lazily on the first INSERT (see
	 * chcompress_multi_insert / chcompress_tuple_insert).
	 */
	*freezeXid = InvalidTransactionId;
	*minmulti  = InvalidMultiXactId;
}

static void
chcompress_relation_nontransactional_truncate(Relation rel)
{
	/* Do nothing if chDB is not yet initialized (e.g. DROP TABLE on empty table) */
	if (!se_chdb_is_loaded())
		return;
	se_chdb_truncate_table(RelationGetRelid(rel));
}

#if PG_VERSION_NUM >= PG_VERSION_16
static void
chcompress_relation_copy_data(Relation rel, const RelFileLocator *newrlocator) { }
#else
static void
chcompress_relation_copy_data(Relation rel, const RelFileNode *newrnode) { }
#endif

#if PG_VERSION_NUM >= PG_VERSION_19
static void
chcompress_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
									 Relation OldIndex, bool use_sort,
									 TransactionId OldestXmin,
									 Snapshot snapshot,
									 TransactionId *xid_cutoff,
									 MultiXactId *multi_cutoff,
									 double *num_tuples,
									 double *tups_vacuumed,
									 double *tups_recently_dead)
#else
static void
chcompress_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
									 Relation OldIndex, bool use_sort,
									 TransactionId OldestXmin,
									 TransactionId *xid_cutoff,
									 MultiXactId *multi_cutoff,
									 double *num_tuples,
									 double *tups_vacuumed,
									 double *tups_recently_dead)
#endif
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("chcompress: CLUSTER is not supported")));
}

/* -----------------------------------------------------------------------
 * Size
 * ----------------------------------------------------------------------- */

static uint64
chcompress_relation_size(Relation rel, ForkNumber forkNumber)
{
	if (forkNumber != MAIN_FORKNUM || !se_chdb_is_loaded())
		return 0;

	char *ch_table = se_chdb_ch_table_name(RelationGetRelid(rel));
	char *sql = psprintf("SELECT sum(bytes_on_disk) FROM system.parts WHERE table = '%s' AND active", ch_table);
	pfree(ch_table);
	struct se_chdb_result *res = se_chdb_query(sql, "CSV");
	pfree(sql);
	if (res->len == 0 || res->buf == NULL) { pfn_free_result(res); return 0; }
	uint64 sz = (uint64) strtoul(res->buf, NULL, 10);
	pfn_free_result(res);
	return sz;
}

static void
chcompress_estimate_rel_size(Relation rel, int32 *attr_widths,
							 BlockNumber *pages, double *tuples,
							 double *allvisfrac)
{
	/* Return safe defaults when chDB is not yet connected */
	if (!se_chdb_is_loaded())
	{
		*tuples     = 0.0;
		*pages      = 0;
		*allvisfrac = 0.0;
		return;
	}

	char *ch_table = se_chdb_ch_table_name(RelationGetRelid(rel));
	char *sql = psprintf("SELECT sum(rows), sum(bytes_on_disk) FROM system.parts WHERE table = '%s' AND active", ch_table);
	pfree(ch_table);
	struct se_chdb_result *res = se_chdb_query(sql, "CSV");
	pfree(sql);

	uint64 nrows = 0, nbytes = 0;
	if (res->len > 0 && res->buf != NULL)
	{
		char *p = res->buf;
		nrows  = (uint64) strtoul(p, &p, 10);
		if (*p == ',') p++;
		nbytes = (uint64) strtoul(p, NULL, 10);
	}
	pfn_free_result(res);

	*tuples     = (double) nrows;
	*pages      = (BlockNumber) ((nbytes + BLCKSZ - 1) / BLCKSZ);
	*allvisfrac = 0.0;
	if (attr_widths)
		for (int i = 0; i < RelationGetDescr(rel)->natts; i++)
			attr_widths[i] = 0;
}

/* -----------------------------------------------------------------------
 * Tuple visibility
 * ----------------------------------------------------------------------- */

static bool
chcompress_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
									Snapshot snapshot)
{ return true; }

static TransactionId
chcompress_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{ return InvalidTransactionId; }

/* -----------------------------------------------------------------------
 * Analyze
 * ----------------------------------------------------------------------- */

#if PG_VERSION_NUM >= PG_VERSION_17
static bool
chcompress_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{ return false; }
#else
static bool
chcompress_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								   BufferAccessStrategy bstrategy)
{ return false; }
#endif

#if PG_VERSION_NUM >= PG_VERSION_19
static bool
chcompress_scan_analyze_next_tuple(TableScanDesc scan,
								   double *liverows, double *deadrows,
								   TupleTableSlot *slot)
{ return false; }
#else
static bool
chcompress_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								   double *liverows, double *deadrows,
								   TupleTableSlot *slot)
{ return false; }
#endif

/* -----------------------------------------------------------------------
 * Bitmap scan (PG18 API)
 * ----------------------------------------------------------------------- */

#if PG_VERSION_NUM < PG_VERSION_18
static bool
chcompress_scan_bitmap_next_block(TableScanDesc scan,
								  struct TBMIterateResult *tbmres)
{ return false; }
static bool
chcompress_scan_bitmap_next_tuple(TableScanDesc scan,
								  struct TBMIterateResult *tbmres,
								  TupleTableSlot *slot)
{ return false; }
#else
static bool
chcompress_scan_bitmap_next_tuple(TableScanDesc scan,
								  TupleTableSlot *slot,
								  bool *recheck,
								  uint64 *lossy_pages,
								  uint64 *exact_pages)
{ return false; }
#endif

/* -----------------------------------------------------------------------
 * Sample scan
 * ----------------------------------------------------------------------- */

static bool
chcompress_scan_sample_next_block(TableScanDesc scan, struct SampleScanState *scanstate)
{ return false; }

static bool
chcompress_scan_sample_next_tuple(TableScanDesc scan, struct SampleScanState *scanstate,
								  TupleTableSlot *slot)
{ return false; }

/* -----------------------------------------------------------------------
 * Parallel scan
 * ----------------------------------------------------------------------- */

static Size
chcompress_parallelscan_estimate(Relation rel)
{ return sizeof(ParallelBlockTableScanDescData); }

static Size
chcompress_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{ return sizeof(ParallelBlockTableScanDescData); }

static void
chcompress_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{ }

/* -----------------------------------------------------------------------
 * Index (not supported)
 * ----------------------------------------------------------------------- */

#if PG_VERSION_NUM >= PG_VERSION_19
static IndexFetchTableData *
chcompress_index_fetch_begin(Relation rel, uint32 flags)
#else
static IndexFetchTableData *
chcompress_index_fetch_begin(Relation rel)
#endif
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("chcompress: index scan not supported")));
}

static void chcompress_index_fetch_reset(IndexFetchTableData *scan) { }
static void chcompress_index_fetch_end(IndexFetchTableData *scan) { }

static bool
chcompress_index_fetch_tuple(struct IndexFetchTableData *scan,
							 ItemPointer tid, Snapshot snapshot,
							 TupleTableSlot *slot,
							 bool *call_again, bool *all_dead)
{ return false; }

/* -----------------------------------------------------------------------
 * Slot
 * ----------------------------------------------------------------------- */

static const TupleTableSlotOps *
chcompress_slot_callbacks(Relation rel)
{ return &TTSOpsVirtual; }

static bool
chcompress_relation_needs_toast_table(Relation rel)
{
	/* chDB handles its own storage; no PostgreSQL TOAST table needed */
	return false;
}

static Oid
chcompress_relation_toast_am(Relation rel)
{
	/* Should never be called since relation_needs_toast_table returns false */
	return InvalidOid;
}

/* -----------------------------------------------------------------------
 * Module init
 * ----------------------------------------------------------------------- */

void
chcompress_tableam_init(void)
{
	/* connection and library loaded lazily on first use */
}

/* -----------------------------------------------------------------------
 * TAM handler
 * ----------------------------------------------------------------------- */

Datum
chcompress_handler(PG_FUNCTION_ARGS)
{
	TableAmRoutine *amroutine = makeNode(TableAmRoutine);

	amroutine->slot_callbacks                     = chcompress_slot_callbacks;
	amroutine->scan_begin                         = chcompress_scan_begin;
	amroutine->scan_end                           = chcompress_scan_end;
	amroutine->scan_rescan                        = chcompress_scan_rescan;
	amroutine->scan_getnextslot                   = chcompress_scan_getnextslot;

	amroutine->parallelscan_estimate              = chcompress_parallelscan_estimate;
	amroutine->parallelscan_initialize            = chcompress_parallelscan_initialize;
	amroutine->parallelscan_reinitialize          = chcompress_parallelscan_reinitialize;

	amroutine->index_fetch_begin                  = chcompress_index_fetch_begin;
	amroutine->index_fetch_reset                  = chcompress_index_fetch_reset;
	amroutine->index_fetch_end                    = chcompress_index_fetch_end;
	amroutine->index_fetch_tuple                  = chcompress_index_fetch_tuple;

	amroutine->tuple_insert                       = chcompress_tuple_insert;
	amroutine->tuple_insert_speculative           = NULL;
	amroutine->tuple_complete_speculative         = NULL;
	amroutine->multi_insert                       = chcompress_multi_insert;
	amroutine->tuple_delete                       = chcompress_tuple_delete;
	amroutine->tuple_update                       = chcompress_tuple_update;
	amroutine->tuple_lock                         = chcompress_tuple_lock;

	amroutine->tuple_fetch_row_version            = NULL;
	amroutine->tuple_get_latest_tid               = NULL;
	amroutine->tuple_tid_valid                    = NULL;
	amroutine->tuple_satisfies_snapshot           = chcompress_tuple_satisfies_snapshot;
	amroutine->index_delete_tuples                = chcompress_index_delete_tuples;

#if PG_VERSION_NUM >= PG_VERSION_16
	amroutine->relation_set_new_filelocator       = chcompress_relation_set_new_filelocator;
#else
	amroutine->relation_set_new_filenode          = chcompress_relation_set_new_filenode;
#endif
	amroutine->relation_nontransactional_truncate = chcompress_relation_nontransactional_truncate;
	amroutine->relation_copy_data                 = chcompress_relation_copy_data;
	amroutine->relation_copy_for_cluster          = chcompress_relation_copy_for_cluster;
	amroutine->relation_vacuum                    = chcompress_relation_vacuum;
	amroutine->scan_analyze_next_block            = chcompress_scan_analyze_next_block;
	amroutine->scan_analyze_next_tuple            = chcompress_scan_analyze_next_tuple;
#if PG_VERSION_NUM < PG_VERSION_18
	amroutine->scan_bitmap_next_block             = chcompress_scan_bitmap_next_block;
#endif
	amroutine->scan_bitmap_next_tuple             = chcompress_scan_bitmap_next_tuple;
	amroutine->scan_sample_next_block             = chcompress_scan_sample_next_block;
	amroutine->scan_sample_next_tuple             = chcompress_scan_sample_next_tuple;
	amroutine->relation_size                      = chcompress_relation_size;
	amroutine->relation_needs_toast_table         = chcompress_relation_needs_toast_table;
	amroutine->relation_toast_am                  = chcompress_relation_toast_am;
	amroutine->relation_estimate_size             = chcompress_estimate_rel_size;

	PG_RETURN_POINTER(amroutine);
}
