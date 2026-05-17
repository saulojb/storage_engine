/*-------------------------------------------------------------------------
 *
 * engine_maintenance_bgw.c
 *
 * Background Worker that periodically calls
 * engine.storage_maintenance_auto() to dispatch incremental-merge and
 * full-repack operations for all colcompress/rowcompress tables whose
 * storage_health.recommended_action is not 'ok'.
 *
 * Configuration (postgresql.conf):
 *
 *   storage_engine.maintenance_auto_enabled  = off   # master switch
 *   storage_engine.maintenance_auto_naptime  = 300   # seconds between cycles
 *   storage_engine.maintenance_auto_database = ''    # database name (empty = skip)
 *
 * The extension must be listed in shared_preload_libraries for the BGW to
 * start automatically.  Without shared_preload_libraries the GUCs are still
 * available but the BGW will not register.
 *
 * Copyright (c) Saulo José Benvenutti.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pg_version_constants.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "lib/stringinfo.h"
#include "utils/snapmgr.h"

#include "engine/engine_maintenance_bgw.h"

#if PG_VERSION_NUM >= PG_VERSION_19
extern void pqsignal_be(int signo, pqsigfunc func);
#endif

/* ================================================================
 * GUC variables
 * ================================================================ */

bool  se_maintenance_auto_enabled  = false;
int   se_maintenance_auto_naptime  = 300;
char *se_maintenance_auto_database = NULL;

/* ================================================================
 * Signal handling
 * ================================================================ */

static volatile sig_atomic_t got_sigterm = false;

static void
se_bgw_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sigterm = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

/* ================================================================
 * BGW entry point
 * ================================================================ */

PGDLLEXPORT void se_maintenance_bgworker_main(Datum arg);

void
se_maintenance_bgworker_main(Datum arg)
{
	/* Set up signal handlers */
	#if PG_VERSION_NUM >= PG_VERSION_19
	pqsignal_be(SIGTERM, se_bgw_sigterm);
	#else
	pqsignal(SIGTERM, se_bgw_sigterm);
	#endif
	BackgroundWorkerUnblockSignals();

	/* If no database configured, nothing to do — sleep until SIGTERM */
	if (se_maintenance_auto_database == NULL || se_maintenance_auto_database[0] == '\0')
	{
		ereport(LOG,
				(errmsg("storage_engine maintenance BGW: no database configured "
						"(storage_engine.maintenance_auto_database is empty); "
						"set it in postgresql.conf and restart")));
		while (!got_sigterm)
		{
			WaitLatch(MyLatch,
					  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					  60000L,
					  PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
		}
		proc_exit(0);
	}

	/* Connect to the configured database */
	BackgroundWorkerInitializeConnection(se_maintenance_auto_database, NULL, 0);

	ereport(LOG,
			(errmsg("storage_engine maintenance BGW started on database \"%s\" "
					"(naptime=%ds)",
					se_maintenance_auto_database, se_maintenance_auto_naptime)));

	while (!got_sigterm)
	{
		int rc;

		/* Sleep for naptime (or until latch is set / PM dies) */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   (long) se_maintenance_auto_naptime * 1000L,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (rc & WL_LATCH_SET)
			continue;	/* interrupted — re-check got_sigterm */

		if (!se_maintenance_auto_enabled)
			continue;	/* disabled at runtime — skip this cycle */

		/*
		 * Run one maintenance cycle via SPI.
		 * We use a subtransaction so a failure in one table doesn't abort
		 * the whole cycle; SPI_execute rolls back on error.
		 */
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();

		PushActiveSnapshot(GetTransactionSnapshot());

		if (SPI_connect() != SPI_OK_CONNECT)
		{
			ereport(WARNING,
					(errmsg("storage_engine maintenance BGW: SPI_connect failed")));
			PopActiveSnapshot();
			AbortCurrentTransaction();
			continue;
		}

		{
			int ret = SPI_execute(
				"CALL engine.storage_maintenance_auto("
				"    dry_run   => false,"
				"    max_tables => NULL,"
				"    am_filter  => NULL,"
				"    p_verbose  => false)",
				false, 0);

			if (ret < 0)
				ereport(WARNING,
						(errmsg("storage_engine maintenance BGW: "
								"storage_maintenance_auto() returned SPI code %d", ret)));
		}

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	ereport(LOG, (errmsg("storage_engine maintenance BGW shutting down")));
	proc_exit(0);
}

/* ================================================================
 * Registration + GUC definitions
 * ================================================================ */

void
se_maintenance_bgw_register(void)
{
	BackgroundWorker worker;

	/* ---- GUCs ----------------------------------------------------------- */

	DefineCustomBoolVariable(
		"storage_engine.maintenance_auto_enabled",
		"Enable automatic storage maintenance background worker.",
		"When on, the storage_engine BGW periodically calls "
		"engine.storage_maintenance_auto() on the configured database.",
		&se_maintenance_auto_enabled,
		false,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"storage_engine.maintenance_auto_naptime",
		"Seconds between automatic maintenance cycles.",
		"The storage_engine maintenance background worker sleeps this many "
		"seconds between each call to engine.storage_maintenance_auto().",
		&se_maintenance_auto_naptime,
		300,	/* default: 5 minutes */
		1,
		86400,	/* max: 24 hours */
		PGC_SIGHUP,
		GUC_UNIT_S,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"storage_engine.maintenance_auto_database",
		"Database where automatic storage maintenance runs.",
		"The storage_engine maintenance BGW connects to this database to call "
		"engine.storage_maintenance_auto(). Empty string disables the BGW.",
		&se_maintenance_auto_database,
		"",
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	/* ---- BGW registration ----------------------------------------------- */

	/*
	 * Only register the BGW if loaded via shared_preload_libraries
	 * (IsUnderPostmaster is false during postmaster startup).
	 */
	if (IsUnderPostmaster)
		return;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags        = BGWORKER_SHMEM_ACCESS |
							   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;	/* restart 10 s after unexpected exit */

	snprintf(worker.bgw_library_name, BGW_MAXLEN, "storage_engine");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "se_maintenance_bgworker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "storage_engine maintenance");
	snprintf(worker.bgw_type, BGW_MAXLEN, "storage_engine maintenance");

	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}
