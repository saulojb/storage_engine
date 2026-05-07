-- storage_engine--2.0.1--2.1.0.sql
--
-- Upgrade script: storage_engine 2.0.1 → 2.1.0
--
-- Phase 1: Lazy/Eager Maintenance + Incremental Merge — catalog scaffold
--
-- What this release adds:
--   • engine.col_maintenance_options  — per-table maintenance config for colcompress
--   • engine.row_maintenance_options  — per-table maintenance config for rowcompress
--   • engine.storage_health           — unified operational health view (both AMs)
--   • engine.colcompress_set_maintenance()  — setter for colcompress maintenance options
--   • engine.rowcompress_set_maintenance()  — setter for rowcompress maintenance options
--   • engine.storage_maintenance_recommendation() — prioritized recommendation function
--   • engine.storage_maintenance_stats()          — per-table stats function
--
-- No behavior changes. All new columns and tables default to 'eager' mode
-- (the only mode that existed before), so existing tables are unaffected.
-- No C code changes required for Phase 1.
--
-- Phase 2 (future): C code updates dirty_units counters during UPDATE/DELETE
-- Phase 3 (future): engine.colcompress_merge_incremental(), auto-maintenance
--

-- ============================================================
-- 1. Per-table maintenance options — colcompress
-- ============================================================

CREATE TABLE engine.col_maintenance_options (
    regclass                         regclass NOT NULL PRIMARY KEY,
    maintenance_mode                 text     NOT NULL DEFAULT 'eager',
    maintenance_target_pruning_ratio real     NOT NULL DEFAULT 0.70,
    maintenance_merge_trigger_ratio  real     NOT NULL DEFAULT 0.20,
    CONSTRAINT col_maintenance_mode_check
        CHECK (maintenance_mode IN ('eager', 'lazy')),
    CONSTRAINT col_maintenance_ratios_check
        CHECK (
            maintenance_target_pruning_ratio BETWEEN 0.0 AND 1.0
            AND maintenance_merge_trigger_ratio  BETWEEN 0.0 AND 1.0
            AND maintenance_merge_trigger_ratio  <= maintenance_target_pruning_ratio
        )
) WITH (user_catalog_table = true);

COMMENT ON TABLE engine.col_maintenance_options
    IS 'colcompress per-table maintenance settings: eager/lazy mode and pruning/merge thresholds (Phase 1 scaffold; no behavior change until Phase 2)';

COMMENT ON COLUMN engine.col_maintenance_options.maintenance_mode
    IS '''eager'' (default) = update pruning metadata on every write; ''lazy'' = defer metadata updates (Phase 2)';
COMMENT ON COLUMN engine.col_maintenance_options.maintenance_target_pruning_ratio
    IS 'minimum fraction of stripes with valid pruning metadata before full repack is recommended (default 0.70)';
COMMENT ON COLUMN engine.col_maintenance_options.maintenance_merge_trigger_ratio
    IS 'fraction of dirty stripes at which incremental merge is recommended (default 0.20)';

-- ============================================================
-- 2. Per-table maintenance options — rowcompress
-- ============================================================

CREATE TABLE engine.row_maintenance_options (
    regclass                         regclass NOT NULL PRIMARY KEY,
    maintenance_mode                 text     NOT NULL DEFAULT 'eager',
    maintenance_target_pruning_ratio real     NOT NULL DEFAULT 0.70,
    maintenance_merge_trigger_ratio  real     NOT NULL DEFAULT 0.20,
    CONSTRAINT row_maintenance_mode_check
        CHECK (maintenance_mode IN ('eager', 'lazy')),
    CONSTRAINT row_maintenance_ratios_check
        CHECK (
            maintenance_target_pruning_ratio BETWEEN 0.0 AND 1.0
            AND maintenance_merge_trigger_ratio  BETWEEN 0.0 AND 1.0
            AND maintenance_merge_trigger_ratio  <= maintenance_target_pruning_ratio
        )
) WITH (user_catalog_table = true);

COMMENT ON TABLE engine.row_maintenance_options
    IS 'rowcompress per-table maintenance settings: eager/lazy mode and pruning/merge thresholds (Phase 1 scaffold; no behavior change until Phase 2)';

COMMENT ON COLUMN engine.row_maintenance_options.maintenance_mode
    IS '''eager'' (default) = update pruning metadata on every write; ''lazy'' = defer metadata updates (Phase 2)';
COMMENT ON COLUMN engine.row_maintenance_options.maintenance_target_pruning_ratio
    IS 'minimum fraction of batches with valid pruning metadata before full repack is recommended (default 0.70)';
COMMENT ON COLUMN engine.row_maintenance_options.maintenance_merge_trigger_ratio
    IS 'fraction of dirty batches at which incremental merge is recommended (default 0.20)';

-- ============================================================
-- 3. Update ensure_am_depends_catalog to include new tables
-- ============================================================

CREATE OR REPLACE FUNCTION engine.ensure_am_depends_catalog()
    RETURNS void
    LANGUAGE plpgsql
    SET search_path = pg_catalog
AS $func$
BEGIN
    INSERT INTO pg_depend
    SELECT
        'pg_am'::regclass::oid    AS classid,
        am.oid                    AS objid,
        0                         AS objsubid,
        'pg_class'::regclass::oid AS refclassid,
        members.relname::regclass::oid AS refobjid,
        0                         AS refobjsubid,
        'n'                       AS deptype
    FROM pg_am am,
         (VALUES ('engine.chunk'),
                 ('engine.chunk_group'),
                 ('engine.chunk_group_pkey'),
                 ('engine.chunk_pkey'),
                 ('engine.col_options'),
                 ('engine.col_options_pkey'),
                 ('engine.col_maintenance_options'),
                 ('engine.col_maintenance_options_pkey'),
                 ('engine.storageid_seq'),
                 ('engine.stripe'),
                 ('engine.stripe_first_row_number_idx'),
                 ('engine.stripe_pkey'),
                 ('engine.row_mask'),
                 ('engine.row_mask_pkey'),
                 ('engine.row_mask_seq'),
                 ('engine.row_mask_stripe_unique'),
                 ('engine.row_batch'),
                 ('engine.row_batch_pkey'),
                 ('engine.row_options'),
                 ('engine.row_options_pkey'),
                 ('engine.row_maintenance_options'),
                 ('engine.row_maintenance_options_pkey')
         ) AS members(relname)
    WHERE am.amname IN ('colcompress', 'rowcompress')
    EXCEPT TABLE pg_depend;
END;
$func$;

SELECT engine.ensure_am_depends_catalog();

-- ============================================================
-- 4. Maintenance option setters (pure SQL — no C required)
-- ============================================================

--
-- engine.colcompress_set_maintenance — set maintenance mode and thresholds
--   for a colcompress table.  NULL arguments inherit defaults.
--
--   maintenance_mode:
--     'eager' (default) — pruning metadata kept up-to-date on every write
--     'lazy'            — defer metadata maintenance (Phase 2 behavior)
--   maintenance_target_pruning_ratio (0–1, default 0.70):
--     minimum fraction of stripes with valid pruning metadata before full
--     repack is recommended.
--   maintenance_merge_trigger_ratio (0–maintenance_target, default 0.20):
--     fraction of dirty stripes at which incremental merge is recommended.
--
CREATE OR REPLACE FUNCTION engine.colcompress_set_maintenance(
    p_table                          regclass,
    maintenance_mode                 text DEFAULT NULL,
    maintenance_target_pruning_ratio real DEFAULT NULL,
    maintenance_merge_trigger_ratio  real DEFAULT NULL)
    RETURNS void
    LANGUAGE plpgsql
AS $$
DECLARE
    _mode   text;
    _target real;
    _merge  real;
BEGIN
    _mode   := COALESCE(maintenance_mode, 'eager');
    _target := COALESCE(maintenance_target_pruning_ratio, 0.70);
    _merge  := COALESCE(maintenance_merge_trigger_ratio,  0.20);
    -- Verify target table is a colcompress table
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_am a ON a.oid = c.relam
        WHERE c.oid = p_table AND a.amname = 'colcompress'
    ) THEN
        RAISE EXCEPTION 'colcompress_set_maintenance: % is not a colcompress table', p_table::text;
    END IF;

    -- Validate maintenance_mode
    IF _mode NOT IN ('eager', 'lazy') THEN
        RAISE EXCEPTION 'maintenance_mode must be ''eager'' or ''lazy'', got: %', _mode;
    END IF;

    -- Validate ratios
    IF _target < 0.0 OR _target > 1.0 THEN
        RAISE EXCEPTION
            'maintenance_target_pruning_ratio must be in [0.0, 1.0], got: %', _target;
    END IF;
    IF _merge < 0.0 OR _merge > _target THEN
        RAISE EXCEPTION
            'maintenance_merge_trigger_ratio must be in [0.0, maintenance_target_pruning_ratio=%], got: %',
            round(_target::numeric, 2), round(_merge::numeric, 2);
    END IF;

    IF EXISTS (SELECT 1 FROM engine.col_maintenance_options WHERE regclass = p_table) THEN
        UPDATE engine.col_maintenance_options
            SET maintenance_mode                  = _mode,
                maintenance_target_pruning_ratio  = _target,
                maintenance_merge_trigger_ratio   = _merge
          WHERE regclass = p_table;
    ELSE
        INSERT INTO engine.col_maintenance_options
            (regclass, maintenance_mode, maintenance_target_pruning_ratio, maintenance_merge_trigger_ratio)
        VALUES
            (p_table, _mode, _target, _merge);
    END IF;
END;
$$;

COMMENT ON FUNCTION engine.colcompress_set_maintenance(regclass, text, real, real)
    IS 'set maintenance mode (eager/lazy) and pruning/merge thresholds for a colcompress table; NULL = keep default';

GRANT EXECUTE ON FUNCTION engine.colcompress_set_maintenance(regclass, text, real, real) TO PUBLIC;

--
-- engine.rowcompress_set_maintenance — same for rowcompress tables
--
CREATE OR REPLACE FUNCTION engine.rowcompress_set_maintenance(
    p_table                          regclass,
    maintenance_mode                 text DEFAULT NULL,
    maintenance_target_pruning_ratio real DEFAULT NULL,
    maintenance_merge_trigger_ratio  real DEFAULT NULL)
    RETURNS void
    LANGUAGE plpgsql
AS $$
DECLARE
    _mode   text;
    _target real;
    _merge  real;
BEGIN
    _mode   := COALESCE(maintenance_mode, 'eager');
    _target := COALESCE(maintenance_target_pruning_ratio, 0.70);
    _merge  := COALESCE(maintenance_merge_trigger_ratio,  0.20);
    -- Verify target table is a rowcompress table
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_am a ON a.oid = c.relam
        WHERE c.oid = p_table AND a.amname = 'rowcompress'
    ) THEN
        RAISE EXCEPTION 'rowcompress_set_maintenance: % is not a rowcompress table', p_table::text;
    END IF;

    -- Validate maintenance_mode
    IF _mode NOT IN ('eager', 'lazy') THEN
        RAISE EXCEPTION 'maintenance_mode must be ''eager'' or ''lazy'', got: %', _mode;
    END IF;

    -- Validate ratios
    IF _target < 0.0 OR _target > 1.0 THEN
        RAISE EXCEPTION
            'maintenance_target_pruning_ratio must be in [0.0, 1.0], got: %', _target;
    END IF;
    IF _merge < 0.0 OR _merge > _target THEN
        RAISE EXCEPTION
            'maintenance_merge_trigger_ratio must be in [0.0, maintenance_target_pruning_ratio=%], got: %',
            round(_target::numeric, 2), round(_merge::numeric, 2);
    END IF;

    IF EXISTS (SELECT 1 FROM engine.row_maintenance_options WHERE regclass = p_table) THEN
        UPDATE engine.row_maintenance_options
            SET maintenance_mode                  = _mode,
                maintenance_target_pruning_ratio  = _target,
                maintenance_merge_trigger_ratio   = _merge
          WHERE regclass = p_table;
    ELSE
        INSERT INTO engine.row_maintenance_options
            (regclass, maintenance_mode, maintenance_target_pruning_ratio, maintenance_merge_trigger_ratio)
        VALUES
            (p_table, _mode, _target, _merge);
    END IF;
END;
$$;

COMMENT ON FUNCTION engine.rowcompress_set_maintenance(regclass, text, real, real)
    IS 'set maintenance mode (eager/lazy) and pruning/merge thresholds for a rowcompress table; NULL = keep default';

GRANT EXECUTE ON FUNCTION engine.rowcompress_set_maintenance(regclass, text, real, real) TO PUBLIC;

-- ============================================================
-- 5a. engine.rowcompress_relation_storageid — C-backed helper
-- ============================================================
-- Returns the internal storage_id used by rowcompress (from ColumnarStorageGetStorageId).
-- This differs from pg_relation_filenode and is required for correct JOINs
-- against engine.row_batch.storage_id.

CREATE OR REPLACE FUNCTION engine.rowcompress_relation_storageid(relation regclass)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', 'se_rowcompress_relation_storageid';

COMMENT ON FUNCTION engine.rowcompress_relation_storageid(regclass)
    IS 'returns the internal storage_id for a rowcompress relation (from ColumnarStorageGetStorageId)';

GRANT EXECUTE ON FUNCTION engine.rowcompress_relation_storageid(regclass) TO PUBLIC;

-- ============================================================
-- 5. engine.storage_health — unified operational health view
-- ============================================================
--
-- Provides per-table health statistics for both colcompress and rowcompress
-- tables, including recommended maintenance actions based on configured
-- thresholds.
--
-- Phase 1 notes:
--   • dirty_units: stripes/batches known to have modifications (deletions).
--     For colcompress: stripes where sum(chunk_group.deleted_rows) > 0.
--     For rowcompress: batches where deleted_mask IS NOT NULL.
--   • tombstone_rows:
--     colcompress — exact sum of chunk_group.deleted_rows per table.
--     rowcompress — upper bound: sum of row_count for dirty batches.
--                   Phase 2 will add exact per-batch popcount support.
--   • effective_pruning_ratio_est: fraction of units with no known dirty rows.
--     Phase 2 will replace this with per-unit pruning_valid flags.
--   • recommended_action:
--     'ok'                  — below all thresholds
--     'run_incremental_merge' — dirty_ratio > maintenance_merge_trigger_ratio
--     'run_full_repack'     — dirty_ratio > (1 - maintenance_target_pruning_ratio)
--
CREATE OR REPLACE VIEW engine.storage_health AS

-- colcompress segment
SELECT
    co.regclass::text                                    AS table_name,
    'colcompress'::text                                  AS am_name,
    COALESCE(cmo.maintenance_mode,                'eager') AS maintenance_mode,
    COALESCE(cmo.maintenance_target_pruning_ratio, 0.70)  AS maintenance_target_pruning_ratio,
    COALESCE(cmo.maintenance_merge_trigger_ratio,  0.20)  AS maintenance_merge_trigger_ratio,
    -- unit = stripe
    count(s.stripe_num)::bigint                          AS total_units,
    count(s.stripe_num)
        FILTER (WHERE COALESCE(cg_s.stripe_deleted_rows, 0) > 0)::bigint
                                                         AS dirty_units,
    CASE WHEN count(s.stripe_num) > 0
         THEN count(s.stripe_num)
                  FILTER (WHERE COALESCE(cg_s.stripe_deleted_rows, 0) > 0)::real
              / count(s.stripe_num)
         ELSE 0.0
    END                                                  AS dirty_ratio,
    -- exact tombstone count for colcompress (chunk_group.deleted_rows is maintained eagerly)
    COALESCE(sum(cg_s.stripe_deleted_rows), 0)::bigint   AS tombstone_rows,
    COALESCE(
        sum(s.row_count) - sum(COALESCE(cg_s.stripe_deleted_rows, 0)),
        0
    )::bigint                                            AS live_rows,
    -- pruning efficiency estimate: fraction of units with no dirty rows
    CASE WHEN count(s.stripe_num) > 0
         THEN 1.0::real
              - count(s.stripe_num)
                    FILTER (WHERE COALESCE(cg_s.stripe_deleted_rows, 0) > 0)::real
              / count(s.stripe_num)
         ELSE 1.0
    END                                                  AS effective_pruning_ratio_est,
    -- maintenance recommendation
    CASE
        WHEN count(s.stripe_num) = 0 THEN 'ok'
        WHEN count(s.stripe_num)
                 FILTER (WHERE COALESCE(cg_s.stripe_deleted_rows, 0) > 0)::real
             / NULLIF(count(s.stripe_num), 0)
             > (1.0 - COALESCE(cmo.maintenance_target_pruning_ratio, 0.70))
            THEN 'run_full_repack'
        WHEN count(s.stripe_num)
                 FILTER (WHERE COALESCE(cg_s.stripe_deleted_rows, 0) > 0)::real
             / NULLIF(count(s.stripe_num), 0)
             > COALESCE(cmo.maintenance_merge_trigger_ratio, 0.20)
            THEN 'run_incremental_merge'
        ELSE 'ok'
    END                                                  AS recommended_action
FROM engine.col_options co
LEFT JOIN engine.col_maintenance_options cmo
    ON cmo.regclass = co.regclass
LEFT JOIN engine.stripe s
    ON s.storage_id = engine.colcompress_relation_storageid(co.regclass)
LEFT JOIN (
    SELECT storage_id, stripe_num, sum(deleted_rows) AS stripe_deleted_rows
    FROM engine.chunk_group
    GROUP BY storage_id, stripe_num
) cg_s ON cg_s.storage_id = s.storage_id
      AND cg_s.stripe_num  = s.stripe_num
GROUP BY
    co.regclass,
    cmo.maintenance_mode,
    cmo.maintenance_target_pruning_ratio,
    cmo.maintenance_merge_trigger_ratio

UNION ALL

-- rowcompress segment
SELECT
    ro.regclass::text                                    AS table_name,
    'rowcompress'::text                                  AS am_name,
    COALESCE(rmo.maintenance_mode,                'eager') AS maintenance_mode,
    COALESCE(rmo.maintenance_target_pruning_ratio, 0.70)  AS maintenance_target_pruning_ratio,
    COALESCE(rmo.maintenance_merge_trigger_ratio,  0.20)  AS maintenance_merge_trigger_ratio,
    -- unit = batch
    count(rb.batch_num)::bigint                          AS total_units,
    count(rb.batch_num)
        FILTER (WHERE rb.deleted_mask IS NOT NULL)::bigint
                                                         AS dirty_units,
    CASE WHEN count(rb.batch_num) > 0
         THEN count(rb.batch_num)
                  FILTER (WHERE rb.deleted_mask IS NOT NULL)::real
              / count(rb.batch_num)
         ELSE 0.0
    END                                                  AS dirty_ratio,
    -- tombstone upper bound: row_count of dirty batches (Phase 2: exact popcount)
    COALESCE(
        sum(rb.row_count) FILTER (WHERE rb.deleted_mask IS NOT NULL),
        0
    )::bigint                                            AS tombstone_rows,
    COALESCE(
        sum(rb.row_count) - sum(rb.row_count) FILTER (WHERE rb.deleted_mask IS NOT NULL),
        sum(rb.row_count),
        0
    )::bigint                                            AS live_rows,
    CASE WHEN count(rb.batch_num) > 0
         THEN 1.0::real
              - count(rb.batch_num)
                    FILTER (WHERE rb.deleted_mask IS NOT NULL)::real
              / count(rb.batch_num)
         ELSE 1.0
    END                                                  AS effective_pruning_ratio_est,
    CASE
        WHEN count(rb.batch_num) = 0 THEN 'ok'
        WHEN count(rb.batch_num)
                 FILTER (WHERE rb.deleted_mask IS NOT NULL)::real
             / NULLIF(count(rb.batch_num), 0)
             > (1.0 - COALESCE(rmo.maintenance_target_pruning_ratio, 0.70))
            THEN 'run_full_repack'
        WHEN count(rb.batch_num)
                 FILTER (WHERE rb.deleted_mask IS NOT NULL)::real
             / NULLIF(count(rb.batch_num), 0)
             > COALESCE(rmo.maintenance_merge_trigger_ratio, 0.20)
            THEN 'run_incremental_merge'
        ELSE 'ok'
    END                                                  AS recommended_action
FROM engine.row_options ro
LEFT JOIN engine.row_maintenance_options rmo
    ON rmo.regclass = ro.regclass
LEFT JOIN engine.row_batch rb
    ON rb.storage_id = engine.rowcompress_relation_storageid(ro.regclass)
GROUP BY
    ro.regclass,
    rmo.maintenance_mode,
    rmo.maintenance_target_pruning_ratio,
    rmo.maintenance_merge_trigger_ratio;

COMMENT ON VIEW engine.storage_health
    IS 'unified operational health view for all colcompress and rowcompress tables; shows dirty_ratio, tombstone rows and maintenance recommendation based on configured thresholds';

GRANT SELECT ON engine.storage_health TO PUBLIC;

-- ============================================================
-- 6. engine.storage_maintenance_recommendation
-- ============================================================

CREATE OR REPLACE FUNCTION engine.storage_maintenance_recommendation(
    p_table regclass)
    RETURNS TABLE (
        status            text,
        reason            text,
        suggested_command text,
        priority          text)
    LANGUAGE plpgsql
    STABLE
AS $$
DECLARE
    _h record;
BEGIN
    SELECT
        h.am_name,
        h.recommended_action,
        h.dirty_ratio,
        h.total_units,
        h.dirty_units,
        h.tombstone_rows,
        h.live_rows
    INTO _h
    FROM engine.storage_health h
    WHERE h.table_name = p_table::text;

    IF NOT FOUND THEN
        RETURN QUERY SELECT
            'unknown'::text                                              AS status,
            format('table %s not registered in storage_health; '
                   'is it a colcompress or rowcompress table?',
                   p_table::text)                                        AS reason,
            NULL::text                                                   AS suggested_command,
            'none'::text                                                 AS priority;
        RETURN;
    END IF;

    CASE _h.recommended_action

        WHEN 'ok' THEN
            RETURN QUERY SELECT
                'ok'::text,
                format('%s/%s %ss dirty (%.1f%%), below all thresholds',
                       _h.dirty_units, _h.total_units,
                       CASE _h.am_name WHEN 'colcompress' THEN 'stripe' ELSE 'batch' END,
                       _h.dirty_ratio * 100)::text,
                NULL::text,
                'none'::text;

        WHEN 'run_incremental_merge' THEN
            RETURN QUERY SELECT
                'degraded'::text,
                format('%s/%s %ss dirty (%.1f%%), above merge_trigger_ratio threshold',
                       _h.dirty_units, _h.total_units,
                       CASE _h.am_name WHEN 'colcompress' THEN 'stripe' ELSE 'batch' END,
                       _h.dirty_ratio * 100)::text,
                CASE _h.am_name
                    WHEN 'colcompress' THEN
                        format('SELECT engine.colcompress_merge(''%s'');',
                               p_table::text)
                    ELSE
                        format('SELECT engine.rowcompress_repack(''%s'');',
                               p_table::text)
                END,
                'medium'::text;

        WHEN 'run_full_repack' THEN
            RETURN QUERY SELECT
                'critical'::text,
                format('%s/%s %ss dirty (%.1f%%), exceeds (1 - target_pruning_ratio) threshold',
                       _h.dirty_units, _h.total_units,
                       CASE _h.am_name WHEN 'colcompress' THEN 'stripe' ELSE 'batch' END,
                       _h.dirty_ratio * 100)::text,
                CASE _h.am_name
                    WHEN 'colcompress' THEN
                        format('SELECT engine.colcompress_merge(''%s'');',
                               p_table::text)
                    ELSE
                        format('SELECT engine.rowcompress_repack(''%s'');',
                               p_table::text)
                END,
                'high'::text;

        ELSE
            RETURN QUERY SELECT
                'unknown'::text,
                format('unexpected recommended_action: %s', _h.recommended_action)::text,
                NULL::text,
                'none'::text;
    END CASE;
END;
$$;

COMMENT ON FUNCTION engine.storage_maintenance_recommendation(regclass)
    IS 'returns status, reason, suggested command and priority for a colcompress or rowcompress table based on dirty_ratio vs configured thresholds';

GRANT EXECUTE ON FUNCTION engine.storage_maintenance_recommendation(regclass) TO PUBLIC;

-- ============================================================
-- 7. engine.storage_maintenance_stats
-- ============================================================

CREATE OR REPLACE FUNCTION engine.storage_maintenance_stats(
    p_table regclass)
    RETURNS TABLE (
        am_name                      text,
        maintenance_mode             text,
        total_units                  bigint,
        dirty_units                  bigint,
        dirty_ratio                  real,
        live_rows                    bigint,
        tombstone_rows               bigint,
        effective_pruning_ratio_est  real,
        recommended_action           text)
    LANGUAGE sql
    STABLE
AS $$
    SELECT
        h.am_name,
        h.maintenance_mode,
        h.total_units,
        h.dirty_units,
        h.dirty_ratio,
        h.live_rows,
        h.tombstone_rows,
        h.effective_pruning_ratio_est,
        h.recommended_action
    FROM engine.storage_health h
    WHERE h.table_name = p_table::text;
$$;

COMMENT ON FUNCTION engine.storage_maintenance_stats(regclass)
    IS 'returns maintenance statistics for a single colcompress or rowcompress table (subset of engine.storage_health)';

GRANT EXECUTE ON FUNCTION engine.storage_maintenance_stats(regclass) TO PUBLIC;

-- ============================================================
-- Phase 2: pruning_valid flag (colcompress) + exact deleted_count (rowcompress)
-- ============================================================

-- 1. Add pruning_valid to engine.stripe.
--    DEFAULT true: all existing stripes start as pruning-valid.
--    FlushRowMaskCache (C code) sets this to false on the first delete of a stripe.
ALTER TABLE engine.stripe
    ADD COLUMN IF NOT EXISTS pruning_valid boolean NOT NULL DEFAULT true;

COMMENT ON COLUMN engine.stripe.pruning_valid
    IS 'false when the stripe has been modified by DELETE/UPDATE; reset to true after colcompress_merge';

-- 2. Backfill pruning_valid = false for stripes that already have deleted rows
--    (detected via engine.chunk_group.deleted_rows, maintained by the C code).
UPDATE engine.stripe s
   SET pruning_valid = false
 WHERE EXISTS (
       SELECT 1
         FROM engine.chunk_group cg
        WHERE cg.storage_id = s.storage_id
          AND cg.stripe_num  = s.stripe_num
          AND cg.deleted_rows > 0
   );

-- 3. Add deleted_count to engine.row_batch.
--    DEFAULT 0: existing batches start with 0; backfilled below.
--    The C code (batch delete path) increments this on each row deletion.
ALTER TABLE engine.row_batch
    ADD COLUMN IF NOT EXISTS deleted_count integer NOT NULL DEFAULT 0;

COMMENT ON COLUMN engine.row_batch.deleted_count
    IS 'exact count of deleted rows in this batch (popcount of deleted_mask); maintained by the C code since v2.1.0 Phase 2';

-- 4. Utility: count set bits in a rowcompress deletion bitmask.
CREATE OR REPLACE FUNCTION engine.se_bytea_popcount(mask bytea)
RETURNS integer LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT COALESCE(sum(
        ((get_byte(mask, i) >> 0) & 1) +
        ((get_byte(mask, i) >> 1) & 1) +
        ((get_byte(mask, i) >> 2) & 1) +
        ((get_byte(mask, i) >> 3) & 1) +
        ((get_byte(mask, i) >> 4) & 1) +
        ((get_byte(mask, i) >> 5) & 1) +
        ((get_byte(mask, i) >> 6) & 1) +
        ((get_byte(mask, i) >> 7) & 1)
    )::integer, 0)
    FROM generate_series(0, octet_length(mask) - 1) AS gs(i)
$$;

COMMENT ON FUNCTION engine.se_bytea_popcount(bytea)
    IS 'count set bits (deleted rows) in a rowcompress deletion bitmask; used during migration backfill and available as a utility for manual inspection';

GRANT EXECUTE ON FUNCTION engine.se_bytea_popcount(bytea) TO PUBLIC;

-- 5. Backfill deleted_count for existing batches that have a deleted_mask.
UPDATE engine.row_batch
   SET deleted_count = engine.se_bytea_popcount(deleted_mask)
 WHERE deleted_mask IS NOT NULL;

-- 6. Update storage_health view: use pruning_valid for colcompress dirty detection
--    and exact deleted_count for rowcompress tombstone count.
CREATE OR REPLACE VIEW engine.storage_health AS

-- colcompress segment
SELECT
    co.regclass::text                                    AS table_name,
    'colcompress'::text                                  AS am_name,
    COALESCE(cmo.maintenance_mode,                'eager') AS maintenance_mode,
    COALESCE(cmo.maintenance_target_pruning_ratio, 0.70)  AS maintenance_target_pruning_ratio,
    COALESCE(cmo.maintenance_merge_trigger_ratio,  0.20)  AS maintenance_merge_trigger_ratio,
    -- unit = stripe; dirty when pruning_valid = false (set by FlushRowMaskCache on any delete)
    count(s.stripe_num)::bigint                          AS total_units,
    count(s.stripe_num)
        FILTER (WHERE NOT s.pruning_valid)::bigint
                                                         AS dirty_units,
    CASE WHEN count(s.stripe_num) > 0
         THEN count(s.stripe_num)
                  FILTER (WHERE NOT s.pruning_valid)::real
              / count(s.stripe_num)
         ELSE 0.0
    END                                                  AS dirty_ratio,
    -- exact tombstone count: chunk_group.deleted_rows (maintained eagerly by C code)
    COALESCE(sum(cg_s.stripe_deleted_rows), 0)::bigint   AS tombstone_rows,
    COALESCE(
        sum(s.row_count) - sum(COALESCE(cg_s.stripe_deleted_rows, 0)),
        0
    )::bigint                                            AS live_rows,
    -- effective pruning ratio: fraction of clean stripes (pruning_valid = true)
    CASE WHEN count(s.stripe_num) > 0
         THEN 1.0::real
              - count(s.stripe_num)
                    FILTER (WHERE NOT s.pruning_valid)::real
              / count(s.stripe_num)
         ELSE 1.0
    END                                                  AS effective_pruning_ratio_est,
    CASE
        WHEN count(s.stripe_num) = 0 THEN 'ok'
        WHEN count(s.stripe_num)
                 FILTER (WHERE NOT s.pruning_valid)::real
             / NULLIF(count(s.stripe_num), 0)
             > (1.0 - COALESCE(cmo.maintenance_target_pruning_ratio, 0.70))
            THEN 'run_full_repack'
        WHEN count(s.stripe_num)
                 FILTER (WHERE NOT s.pruning_valid)::real
             / NULLIF(count(s.stripe_num), 0)
             > COALESCE(cmo.maintenance_merge_trigger_ratio, 0.20)
            THEN 'run_incremental_merge'
        ELSE 'ok'
    END                                                  AS recommended_action
FROM engine.col_options co
LEFT JOIN engine.col_maintenance_options cmo
    ON cmo.regclass = co.regclass
LEFT JOIN engine.stripe s
    ON s.storage_id = engine.colcompress_relation_storageid(co.regclass)
LEFT JOIN (
    SELECT storage_id, stripe_num, sum(deleted_rows) AS stripe_deleted_rows
    FROM engine.chunk_group
    GROUP BY storage_id, stripe_num
) cg_s ON cg_s.storage_id = s.storage_id
      AND cg_s.stripe_num  = s.stripe_num
GROUP BY
    co.regclass,
    cmo.maintenance_mode,
    cmo.maintenance_target_pruning_ratio,
    cmo.maintenance_merge_trigger_ratio

UNION ALL

-- rowcompress segment
SELECT
    ro.regclass::text                                    AS table_name,
    'rowcompress'::text                                  AS am_name,
    COALESCE(rmo.maintenance_mode,                'eager') AS maintenance_mode,
    COALESCE(rmo.maintenance_target_pruning_ratio, 0.70)  AS maintenance_target_pruning_ratio,
    COALESCE(rmo.maintenance_merge_trigger_ratio,  0.20)  AS maintenance_merge_trigger_ratio,
    -- unit = batch; dirty when deleted_mask IS NOT NULL (at least one row deleted)
    count(rb.batch_num)::bigint                          AS total_units,
    count(rb.batch_num)
        FILTER (WHERE rb.deleted_mask IS NOT NULL)::bigint
                                                         AS dirty_units,
    CASE WHEN count(rb.batch_num) > 0
         THEN count(rb.batch_num)
                  FILTER (WHERE rb.deleted_mask IS NOT NULL)::real
              / count(rb.batch_num)
         ELSE 0.0
    END                                                  AS dirty_ratio,
    -- exact tombstone count via deleted_count (maintained by C code since v2.1.0 Phase 2)
    COALESCE(sum(rb.deleted_count), 0)::bigint           AS tombstone_rows,
    COALESCE(
        sum(rb.row_count) - sum(rb.deleted_count),
        sum(rb.row_count),
        0
    )::bigint                                            AS live_rows,
    CASE WHEN count(rb.batch_num) > 0
         THEN 1.0::real
              - count(rb.batch_num)
                    FILTER (WHERE rb.deleted_mask IS NOT NULL)::real
              / count(rb.batch_num)
         ELSE 1.0
    END                                                  AS effective_pruning_ratio_est,
    CASE
        WHEN count(rb.batch_num) = 0 THEN 'ok'
        WHEN count(rb.batch_num)
                 FILTER (WHERE rb.deleted_mask IS NOT NULL)::real
             / NULLIF(count(rb.batch_num), 0)
             > (1.0 - COALESCE(rmo.maintenance_target_pruning_ratio, 0.70))
            THEN 'run_full_repack'
        WHEN count(rb.batch_num)
                 FILTER (WHERE rb.deleted_mask IS NOT NULL)::real
             / NULLIF(count(rb.batch_num), 0)
             > COALESCE(rmo.maintenance_merge_trigger_ratio, 0.20)
            THEN 'run_incremental_merge'
        ELSE 'ok'
    END                                                  AS recommended_action
FROM engine.row_options ro
LEFT JOIN engine.row_maintenance_options rmo
    ON rmo.regclass = ro.regclass
LEFT JOIN engine.row_batch rb
    ON rb.storage_id = engine.rowcompress_relation_storageid(ro.regclass)
GROUP BY
    ro.regclass,
    rmo.maintenance_mode,
    rmo.maintenance_target_pruning_ratio,
    rmo.maintenance_merge_trigger_ratio;

COMMENT ON VIEW engine.storage_health
    IS 'unified operational health view for all colcompress and rowcompress tables; shows dirty_ratio, tombstone rows and maintenance recommendation based on configured thresholds';

-- ============================================================
-- Phase 2.5: stripe.dirty_rows (colcompress) + row_batch.pruning_valid (rowcompress)
-- ============================================================

-- 1. Add dirty_rows to engine.stripe.
--    DEFAULT 0: all existing stripes start at 0; backfilled below.
--    MarkStripePruningInvalid (C code) accumulates deleted rows here on each flush.
ALTER TABLE engine.stripe
    ADD COLUMN IF NOT EXISTS dirty_rows bigint NOT NULL DEFAULT 0;

COMMENT ON COLUMN engine.stripe.dirty_rows
    IS 'cumulative count of deleted rows in this stripe; accumulated by C code on each FlushRowMaskCache call; avoids aggregating chunk_group.deleted_rows for tombstone reporting';

-- 2. Backfill dirty_rows from chunk_group.deleted_rows for stripes that already have deleted rows.
UPDATE engine.stripe s
   SET dirty_rows = COALESCE((
       SELECT sum(cg.deleted_rows)
         FROM engine.chunk_group cg
        WHERE cg.storage_id = s.storage_id
          AND cg.stripe_num  = s.stripe_num
   ), 0)
 WHERE EXISTS (
       SELECT 1 FROM engine.chunk_group cg
        WHERE cg.storage_id = s.storage_id
          AND cg.stripe_num  = s.stripe_num
          AND cg.deleted_rows > 0
   );

-- 3. Add pruning_valid to engine.row_batch.
--    DEFAULT true: all existing batches start as pruning-valid.
--    RCMarkRowDeleted (C code) sets this to false on the first delete of a batch.
ALTER TABLE engine.row_batch
    ADD COLUMN IF NOT EXISTS pruning_valid boolean NOT NULL DEFAULT true;

COMMENT ON COLUMN engine.row_batch.pruning_valid
    IS 'false when the batch has been modified by DELETE/UPDATE, so min/max stats may be stale; reset to true after rowcompress_merge';

-- 4. Backfill pruning_valid = false for batches that already have deletions.
UPDATE engine.row_batch
   SET pruning_valid = false
 WHERE deleted_mask IS NOT NULL;

-- 5. Update storage_health view to use stripe.dirty_rows and row_batch.pruning_valid.
CREATE OR REPLACE VIEW engine.storage_health AS

-- colcompress segment
SELECT
    co.regclass::text                                    AS table_name,
    'colcompress'::text                                  AS am_name,
    COALESCE(cmo.maintenance_mode,                'eager') AS maintenance_mode,
    COALESCE(cmo.maintenance_target_pruning_ratio, 0.70)  AS maintenance_target_pruning_ratio,
    COALESCE(cmo.maintenance_merge_trigger_ratio,  0.20)  AS maintenance_merge_trigger_ratio,
    -- unit = stripe; dirty when pruning_valid = false (set by FlushRowMaskCache on any delete)
    count(s.stripe_num)::bigint                          AS total_units,
    count(s.stripe_num)
        FILTER (WHERE NOT s.pruning_valid)::bigint
                                                         AS dirty_units,
    CASE WHEN count(s.stripe_num) > 0
         THEN count(s.stripe_num)
                  FILTER (WHERE NOT s.pruning_valid)::real
              / count(s.stripe_num)
         ELSE 0.0
    END                                                  AS dirty_ratio,
    -- exact tombstone count: stripe.dirty_rows (denormalized sum, maintained by C code)
    COALESCE(sum(s.dirty_rows), 0)::bigint               AS tombstone_rows,
    COALESCE(
        sum(s.row_count) - sum(s.dirty_rows),
        0
    )::bigint                                            AS live_rows,
    -- effective pruning ratio: fraction of clean stripes (pruning_valid = true)
    CASE WHEN count(s.stripe_num) > 0
         THEN 1.0::real
              - count(s.stripe_num)
                    FILTER (WHERE NOT s.pruning_valid)::real
              / count(s.stripe_num)
         ELSE 1.0
    END                                                  AS effective_pruning_ratio_est,
    CASE
        WHEN count(s.stripe_num) = 0 THEN 'ok'
        WHEN count(s.stripe_num)
                 FILTER (WHERE NOT s.pruning_valid)::real
             / NULLIF(count(s.stripe_num), 0)
             > (1.0 - COALESCE(cmo.maintenance_target_pruning_ratio, 0.70))
            THEN 'run_full_repack'
        WHEN count(s.stripe_num)
                 FILTER (WHERE NOT s.pruning_valid)::real
             / NULLIF(count(s.stripe_num), 0)
             > COALESCE(cmo.maintenance_merge_trigger_ratio, 0.20)
            THEN 'run_incremental_merge'
        ELSE 'ok'
    END                                                  AS recommended_action
FROM engine.col_options co
LEFT JOIN engine.col_maintenance_options cmo
    ON cmo.regclass = co.regclass
LEFT JOIN engine.stripe s
    ON s.storage_id = engine.colcompress_relation_storageid(co.regclass)
GROUP BY
    co.regclass,
    cmo.maintenance_mode,
    cmo.maintenance_target_pruning_ratio,
    cmo.maintenance_merge_trigger_ratio

UNION ALL

-- rowcompress segment
SELECT
    ro.regclass::text                                    AS table_name,
    'rowcompress'::text                                  AS am_name,
    COALESCE(rmo.maintenance_mode,                'eager') AS maintenance_mode,
    COALESCE(rmo.maintenance_target_pruning_ratio, 0.70)  AS maintenance_target_pruning_ratio,
    COALESCE(rmo.maintenance_merge_trigger_ratio,  0.20)  AS maintenance_merge_trigger_ratio,
    -- unit = batch; dirty when pruning_valid = false (set by RCMarkRowDeleted on any delete)
    count(rb.batch_num)::bigint                          AS total_units,
    count(rb.batch_num)
        FILTER (WHERE NOT rb.pruning_valid)::bigint
                                                         AS dirty_units,
    CASE WHEN count(rb.batch_num) > 0
         THEN count(rb.batch_num)
                  FILTER (WHERE NOT rb.pruning_valid)::real
              / count(rb.batch_num)
         ELSE 0.0
    END                                                  AS dirty_ratio,
    -- exact tombstone count via deleted_count (maintained by C code since v2.1.0 Phase 2)
    COALESCE(sum(rb.deleted_count), 0)::bigint           AS tombstone_rows,
    COALESCE(
        sum(rb.row_count) - sum(rb.deleted_count),
        sum(rb.row_count),
        0
    )::bigint                                            AS live_rows,
    CASE WHEN count(rb.batch_num) > 0
         THEN 1.0::real
              - count(rb.batch_num)
                    FILTER (WHERE NOT rb.pruning_valid)::real
              / count(rb.batch_num)
         ELSE 1.0
    END                                                  AS effective_pruning_ratio_est,
    CASE
        WHEN count(rb.batch_num) = 0 THEN 'ok'
        WHEN count(rb.batch_num)
                 FILTER (WHERE NOT rb.pruning_valid)::real
             / NULLIF(count(rb.batch_num), 0)
             > (1.0 - COALESCE(rmo.maintenance_target_pruning_ratio, 0.70))
            THEN 'run_full_repack'
        WHEN count(rb.batch_num)
                 FILTER (WHERE NOT rb.pruning_valid)::real
             / NULLIF(count(rb.batch_num), 0)
             > COALESCE(rmo.maintenance_merge_trigger_ratio, 0.20)
            THEN 'run_incremental_merge'
        ELSE 'ok'
    END                                                  AS recommended_action
FROM engine.row_options ro
LEFT JOIN engine.row_maintenance_options rmo
    ON rmo.regclass = ro.regclass
LEFT JOIN engine.row_batch rb
    ON rb.storage_id = engine.rowcompress_relation_storageid(ro.regclass)
GROUP BY
    ro.regclass,
    rmo.maintenance_mode,
    rmo.maintenance_target_pruning_ratio,
    rmo.maintenance_merge_trigger_ratio;

COMMENT ON VIEW engine.storage_health
    IS 'unified operational health view for all colcompress and rowcompress tables; shows dirty_ratio, tombstone rows and maintenance recommendation based on configured thresholds (Phase 2.5: uses stripe.dirty_rows and row_batch.pruning_valid)';

