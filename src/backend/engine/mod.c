/*-------------------------------------------------------------------------
 *
 * mod.c
 *
 * This file contains module-level definitions.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "citus_version.h"

#include "engine/engine.h"
#include "engine/engine_tableam.h"
#include "engine/rowcompress.h"


PG_MODULE_MAGIC;

void _PG_init(void);

void
_PG_init(void)
{
	engine_init();
	rowcompress_tableam_init();
}
