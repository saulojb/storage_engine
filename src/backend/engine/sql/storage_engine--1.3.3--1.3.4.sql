-- storage_engine--1.3.3--1.3.4.sql
-- Upgrade script: v1.3.3 → v1.3.4
--
-- v1.3.4 — new: engine.colcompress_repack PROCEDURE — online stripe defragmentation for colcompress tables
--         removed: engine.colcompress_repack(regclass) FUNCTION alias (use engine.colcompress_merge instead)
--
-- Adds engine.colcompress_repack(table_name regclass, min_fill_ratio float8 DEFAULT 0.9).
-- Compacts fragmented stripes caused by DELETE/UPDATE without requiring orderby
-- or a SET clause. Works stripe-by-stripe with one COMMIT per stripe (crash-safe).

-- Drop the pg_repack-compatible alias function (superseded by engine.colcompress_merge)
DROP FUNCTION IF EXISTS engine.colcompress_repack(regclass);

-- ============================================================
-- engine.colcompress_repack — online defragmentation for colcompress tables
-- ============================================================
--
-- Eliminates fragmentation (deleted-row "holes") caused by UPDATE or DELETE
-- operations on colcompress tables, without requiring orderby or a SET clause.
--
-- How it works:
--   For each stripe, the procedure counts live rows (rows not masked by
--   deleted_mask). If the fill ratio is below min_fill_ratio the stripe is
--   repacked: its live rows are buffered into a temporary heap, the entire
--   stripe is deleted, and the rows are re-inserted as a new fully-packed
--   stripe with fresh min/max chunk statistics. One COMMIT per repacked
--   stripe — crash-safe; re-run CALL to resume after an interruption.
--
-- Parameters:
--   table_name     — regclass of the colcompress table to repack
--   min_fill_ratio — stripes with live_rows / row_count >= this value are
--                    considered healthy and are skipped (default 0.9, i.e.
--                    repack stripes that lost >10 % of their rows to deletes)
--
-- Usage:
--   CALL engine.colcompress_repack('myschema.mytable');
--   CALL engine.colcompress_repack('mytable', min_fill_ratio => 0.8);
--
-- Notes:
--   • row_count in engine.stripe counts the rows as written originally;
--     live_rows is the count of rows currently visible (not deleted).
--   • Stripes with zero live rows are skipped — they are already fully masked
--     and produce no I/O during scans.
--   • After repack, TIDs of moved rows change. Run:
--       REINDEX TABLE CONCURRENTLY <table>;
--     if any B-tree indexes exist on the table.
--   • For tables with an orderby option, smart_update or a fresh INSERT/DELETE
--     cycle preserves sort order; repack does NOT re-sort rows.

CREATE OR REPLACE PROCEDURE engine.colcompress_repack(
    IN table_name      regclass,
    IN min_fill_ratio  float8   DEFAULT 0.9
)
LANGUAGE plpgsql
AS $procedure$
DECLARE
    _nspname             text;
    _relname             text;
    _qualname            text;
    _collist             text;
    _storage_id          bigint;
    _stripe_num          bigint;
    _first_rn            bigint;
    _last_rn             bigint;
    _row_count           bigint;   -- rows written into the stripe originally
    _chunk_row_count     int;      -- rows per chunk (= standard stripe size)
    _tid_min             tid;
    _tid_max             tid;
    _live_rows           bigint;   -- currently visible rows in the stripe
    _n_ins               bigint;
    _total_ins           bigint  := 0;
    _n_stripes_repacked  int     := 0;
    _n_stripes_skipped   int     := 0;
BEGIN
    -- ----------------------------------------------------------------
    -- Validate table type
    -- ----------------------------------------------------------------
    SELECT n.nspname, c.relname
      INTO _nspname, _relname
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = table_name;

    _qualname := quote_ident(_nspname) || '.' || quote_ident(_relname);

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_am a ON a.oid = c.relam
        WHERE c.oid = table_name AND a.amname = 'colcompress'
    ) THEN
        RAISE EXCEPTION 'repack: % is not a colcompress table', _qualname;
    END IF;

    IF min_fill_ratio < 0.0 OR min_fill_ratio > 1.0 THEN
        RAISE EXCEPTION 'repack: min_fill_ratio must be between 0.0 and 1.0 (got %)', min_fill_ratio;
    END IF;

    -- ----------------------------------------------------------------
    -- Setup
    -- ----------------------------------------------------------------
    RAISE NOTICE 'repack: starting on % (min_fill_ratio=%)', _qualname, min_fill_ratio;

    PERFORM set_config('statement_timeout', '0', false);
    PERFORM set_config('lock_timeout',      '0', false);

    -- Use at most half the available parallel workers (same policy as smart_update)
    PERFORM set_config(
        'max_parallel_workers_per_gather',
        (current_setting('max_parallel_workers')::int / 2)::text,
        false
    );

    _storage_id := engine.colcompress_relation_storageid(table_name);

    -- Build full column list for INSERT … SELECT
    SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum)
      INTO _collist
      FROM pg_attribute
     WHERE attrelid = table_name AND attnum > 0 AND NOT attisdropped;

    -- ----------------------------------------------------------------
    -- Iterate over stripes in physical order
    -- ----------------------------------------------------------------
    FOR _stripe_num, _first_rn, _last_rn, _row_count, _chunk_row_count IN
        SELECT stripe_num,
               first_row_number,
               first_row_number + row_count - 1,
               row_count,
               chunk_row_count
          FROM engine.stripe
         WHERE storage_id = _storage_id
         ORDER BY stripe_num
    LOOP
        PERFORM set_config('statement_timeout', '0', false);
        PERFORM set_config('lock_timeout',      '0', false);

        _tid_min := engine.row_number_to_tid(_first_rn);
        _tid_max := engine.row_number_to_tid(_last_rn);

        -- Count live (visible) rows in this stripe
        EXECUTE format(
            'SELECT count(*) FROM %s WHERE ctid BETWEEN $1 AND $2',
            _qualname
        ) USING _tid_min, _tid_max
          INTO _live_rows;

        -- Skip if stripe is already healthy
        IF _live_rows = 0 THEN
            -- Fully dead stripe — no visible rows, nothing to recover
            _n_stripes_skipped := _n_stripes_skipped + 1;
            CONTINUE;
        END IF;

        IF _live_rows::float8 / _row_count::float8 >= min_fill_ratio THEN
            -- Fill ratio is acceptable — stripe is healthy
            _n_stripes_skipped := _n_stripes_skipped + 1;
            CONTINUE;
        END IF;

        -- ---- Repack this stripe ----

        -- a. Buffer live rows into a temp heap (bounded memory; dropped on COMMIT)
        EXECUTE format(
            'CREATE TEMP TABLE _se_repack_tmp ON COMMIT DROP AS'
            ' SELECT %s FROM %s WHERE ctid BETWEEN $1 AND $2',
            _collist, _qualname
        ) USING _tid_min, _tid_max;

        -- b. Delete all live rows from this stripe in colcompress
        --    (marks their bits in the deleted_mask — no decompression of other stripes)
        EXECUTE format(
            'DELETE FROM %s WHERE ctid BETWEEN $1 AND $2',
            _qualname
        ) USING _tid_min, _tid_max;

        -- c. Re-insert live rows as a new, fully-packed stripe with
        --    fresh min/max chunk statistics
        EXECUTE format(
            'INSERT INTO %s (%s) SELECT %s FROM _se_repack_tmp',
            _qualname, _collist, _collist
        );
        GET DIAGNOSTICS _n_ins = ROW_COUNT;

        -- d. COMMIT — ON COMMIT DROP destroys _se_repack_tmp atomically.
        --    If the server dies here the entire stripe transaction rolls back
        --    and the original rows are restored (crash-safe).
        COMMIT;

        _total_ins           := _total_ins + _n_ins;
        _n_stripes_repacked  := _n_stripes_repacked + 1;

        RAISE NOTICE 'repack: stripe % — % live / % original rows (fill=%.0f%%) → repacked',
            _stripe_num, _live_rows, _row_count,
            (_live_rows::float8 / _row_count::float8 * 100);
    END LOOP;

    COMMIT;

    RAISE NOTICE
        E'repack: complete — % stripe(s) repacked (% rows moved), % stripe(s) skipped.\n'
        'If B-tree indexes exist on %, run:\n'
        '  REINDEX TABLE CONCURRENTLY %;',
        _n_stripes_repacked, _total_ins, _n_stripes_skipped,
        _qualname, _qualname;
END;
$procedure$;

COMMENT ON PROCEDURE engine.colcompress_repack(regclass, float8)
IS 'Online defragmentation for colcompress tables. Identifies fragmented stripes (fill ratio below min_fill_ratio, default 0.9) caused by DELETE/UPDATE operations, and rewrites them stripe-by-stripe: reads live rows into a temporary heap, deletes the stripe, and re-inserts rows as a new compact stripe with fresh min/max statistics. No SET clause or orderby required. One COMMIT per stripe; crash-safe (re-run CALL to resume). Run REINDEX TABLE CONCURRENTLY after completion if indexes exist.';

GRANT EXECUTE ON PROCEDURE engine.colcompress_repack(regclass, float8) TO PUBLIC;
