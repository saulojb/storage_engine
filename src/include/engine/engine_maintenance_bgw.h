/*-------------------------------------------------------------------------
 *
 * engine_maintenance_bgw.h
 *
 * Declarations for the storage_engine storage maintenance background worker.
 *
 * Copyright (c) Saulo José Benvenutti.
 *
 *-------------------------------------------------------------------------
 */

#ifndef ENGINE_MAINTENANCE_BGW_H
#define ENGINE_MAINTENANCE_BGW_H

/* GUC variables (defined in engine_maintenance_bgw.c) */
extern bool  se_maintenance_auto_enabled;
extern int   se_maintenance_auto_naptime;
extern char *se_maintenance_auto_database;

/* Registration: call from _PG_init */
extern void se_maintenance_bgw_register(void);

/* BGW entry point (exported for postmaster) */
extern void se_maintenance_bgworker_main(Datum arg);

#endif /* ENGINE_MAINTENANCE_BGW_H */
