-- storage_engine--2.2.0--2.3.0.sql
--
-- Upgrade script: 2.2.0 → 2.3.0
--
-- New in 2.3.0:
--   • VecAgg sum(expression): VECGAGG_SUM_EXPR — sum(col OP col), sum(col + const)
--   • VecAgg post-aggregation arithmetic: VECGAGG_MULTI_EXPR — sum(a) + count(*)
--   • avg(int8) parallel correctness fix
--   • Multiple SIGSEGV crash fixes in planner hook and aggregate executor
--
-- No catalog changes in this release.
--
-- Copyright (c) Saulo J. Benvenutti, 2026
-- License: PostgreSQL License


