/*-------------------------------------------------------------------------
 *
 * engine_customscan.h
 *
 * Forward declarations of functions to hookup the custom scan feature of
 * columnar.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_CUSTOMSCAN_H
#define COLUMNAR_CUSTOMSCAN_H

#include "nodes/extensible.h"

/* Flag to indicate is vectorized aggregate used in execution */
#define CUSTOM_SCAN_VECTORIZED_AGGREGATE 1

extern void engine_customscan_init(void);
extern const CustomScanMethods * engine_customscan_methods(void);
extern Bitmapset * ColumnarAttrNeeded(ScanState *ss, List *customList);

extern const struct CustomPathMethods ColumnarScanPathMethods;
extern const struct CustomScanMethods ColumnarScanScanMethods;
extern const struct CustomExecMethods ColumnarScanExecuteMethods;

#endif /* COLUMNAR_CUSTOMSCAN_H */
