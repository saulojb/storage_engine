-- ============================================================
-- storage_engine Benchmark Setup
-- Database: bench_am  •  1 000 000 rows
--
-- Creates four tables with identical schemas using different
-- storage access methods for fair side-by-side comparison:
--   events_heap   — standard PostgreSQL heap
--   events_col    — storage_engine colcompress (columnar)
--   events_row    — storage_engine rowcompress  (row-level compressed)
--   events_cit    — citus_columnar (requires pg_citus extension)
--
-- Usage:
--   createdb bench_am
--   psql -d bench_am -f setup.sql
-- ============================================================

-- ── Extensions ───────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS storage_engine;
-- Uncomment if citus is installed and you want events_cit:
-- CREATE EXTENSION IF NOT EXISTS citus_columnar;

-- ── Helper: random JSONB metadata ────────────────────────────
CREATE OR REPLACE FUNCTION _bench_meta(seed bigint) RETURNS jsonb
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
SELECT jsonb_build_object(
    'os',        (ARRAY['android','ios','windows','macos','linux'])[(seed % 5)::int + 1],
    'version',   (12 + (seed % 8))::text,
    'app',       (ARRAY['mobile','web','desktop','tv'])[(seed % 4)::int + 1],
    'campaign',  (ARRAY['summer','black_friday','organic','referral','email'])[(seed % 5)::int + 1],
    'revenue',   round((random() * 500)::numeric, 2),
    'ab_group',  (ARRAY['A','B','C'])[(seed % 3)::int + 1],
    'premium',   (seed % 7 = 0)
)
$$;

-- ── Drop existing tables ──────────────────────────────────────
DROP TABLE IF EXISTS events_heap CASCADE;
DROP TABLE IF EXISTS events_col  CASCADE;
DROP TABLE IF EXISTS events_row  CASCADE;
DROP TABLE IF EXISTS events_cit  CASCADE;

-- ── Heap ─────────────────────────────────────────────────────
CREATE TABLE events_heap (
    id              bigserial       PRIMARY KEY,
    created_at      timestamptz     NOT NULL,
    event_date      date            NOT NULL,
    user_id         bigint          NOT NULL,
    session_id      text            NOT NULL,
    amount          numeric(15,4),
    price           double precision,
    quantity        integer,
    duration_ms     integer,
    score           real,
    country_code    char(2)         NOT NULL,
    browser         text            NOT NULL,
    url             text,
    is_mobile       boolean         NOT NULL,
    event_type      text            NOT NULL,
    metadata        jsonb,
    tags            text[]
);

-- ── colcompress (storage_engine columnar) ────────────────────
CREATE TABLE events_col (
    id              bigserial,
    created_at      timestamptz     NOT NULL,
    event_date      date            NOT NULL,
    user_id         bigint          NOT NULL,
    session_id      text            NOT NULL,
    amount          numeric(15,4),
    price           double precision,
    quantity        integer,
    duration_ms     integer,
    score           real,
    country_code    char(2)         NOT NULL,
    browser         text            NOT NULL,
    url             text,
    is_mobile       boolean         NOT NULL,
    event_type      text            NOT NULL,
    metadata        jsonb,
    tags            text[]
) USING colcompress;

-- events_col: columnar storage settings
-- index_scan=false avoids random-access stripe reads that bypass stripe pruning.
-- orderby ensures data is physically sorted by event_date so date-range
-- queries (Q5) hit only the relevant stripe(s).
ALTER TABLE events_col SET (index_scan = false);
ALTER TABLE events_col SET (orderby = 'event_date ASC');

-- ── rowcompress (storage_engine row-level compressed) ────────
CREATE TABLE events_row (
    id              bigserial,
    created_at      timestamptz     NOT NULL,
    event_date      date            NOT NULL,
    user_id         bigint          NOT NULL,
    session_id      text            NOT NULL,
    amount          numeric(15,4),
    price           double precision,
    quantity        integer,
    duration_ms     integer,
    score           real,
    country_code    char(2)         NOT NULL,
    browser         text            NOT NULL,
    url             text,
    is_mobile       boolean         NOT NULL,
    event_type      text            NOT NULL,
    metadata        jsonb,
    tags            text[]
) USING rowcompress;

-- ── citus_columnar ────────────────────────────────────────────
-- Uncomment this block if citus is installed.
/*
CREATE TABLE events_cit (
    id              bigserial,
    created_at      timestamptz     NOT NULL,
    event_date      date            NOT NULL,
    user_id         bigint          NOT NULL,
    session_id      text            NOT NULL,
    amount          numeric(15,4),
    price           double precision,
    quantity        integer,
    duration_ms     integer,
    score           real,
    country_code    char(2)         NOT NULL,
    browser         text            NOT NULL,
    url             text,
    is_mobile       boolean         NOT NULL,
    event_type      text            NOT NULL,
    metadata        jsonb,
    tags            text[]
) USING columnar;
*/

-- ── Load 1 000 000 rows ───────────────────────────────────────
\echo 'Loading events_heap (1M rows)...'
\timing on

INSERT INTO events_heap
    (created_at, event_date, user_id, session_id, amount, price, quantity,
     duration_ms, score, country_code, browser, url, is_mobile, event_type,
     metadata, tags)
SELECT
    '2024-01-01'::timestamptz + (g * interval '30 seconds'),
    ('2024-01-01'::date + (g / 2880)::int),
    (g % 50000) + 1,
    md5(g::text),
    round((random() * 9999.99)::numeric, 4),
    (random() * 999.99),
    (g % 100) + 1,
    (random() * 30000)::int + 50,
    (random())::real,
    (ARRAY['BR','US','DE','FR','IN','GB','JP','CN','RU','CA'])[(g % 10) + 1],
    (ARRAY['Chrome','Firefox','Safari','Edge','Opera','Samsung'])[(g % 6) + 1],
    '/page/' || (g % 5000)::text,
    (g % 3 <> 0),
    (ARRAY['click','pageview','purchase','signup','search','logout'])[(g % 6) + 1],
    _bench_meta(g),
    ARRAY[
        'tag_' || (g % 20)::text,
        'cat_' || (g % 8)::text
    ]
FROM generate_series(1, 1000000) g;

\echo 'Loading events_col (1M rows)...'

INSERT INTO events_col
    (created_at, event_date, user_id, session_id, amount, price, quantity,
     duration_ms, score, country_code, browser, url, is_mobile, event_type,
     metadata, tags)
SELECT
    created_at, event_date, user_id, session_id, amount, price, quantity,
    duration_ms, score, country_code, browser, url, is_mobile, event_type,
    metadata, tags
FROM events_heap
ORDER BY event_date;  -- physical sort enables stripe pruning on date-range queries

\echo 'Loading events_row (1M rows)...'

INSERT INTO events_row
    (created_at, event_date, user_id, session_id, amount, price, quantity,
     duration_ms, score, country_code, browser, url, is_mobile, event_type,
     metadata, tags)
SELECT
    created_at, event_date, user_id, session_id, amount, price, quantity,
    duration_ms, score, country_code, browser, url, is_mobile, event_type,
    metadata, tags
FROM events_heap;

-- ── Indexes ───────────────────────────────────────────────────
\echo 'Building indexes...'

-- Heap: full index suite
CREATE INDEX ON events_heap (event_date);
CREATE INDEX ON events_heap (user_id);
CREATE INDEX ON events_heap (event_type);
CREATE INDEX ON events_heap USING gin (metadata jsonb_path_ops);
CREATE INDEX ON events_heap USING gin (tags);

-- colcompress: GIN indexes only.
-- NOTE: do NOT add btree indexes on events_col. A btree index causes the
-- planner to choose IndexScan with randomAccess=true, which disables stripe
-- pruning in the columnar engine and makes Q5 (date-range) ~10x slower.
CREATE INDEX ON events_col USING gin (metadata jsonb_path_ops);
CREATE INDEX ON events_col USING gin (tags);

-- rowcompress: standard indexes
CREATE INDEX ON events_row (event_date);
CREATE INDEX ON events_row (user_id);
CREATE INDEX ON events_row USING gin (metadata jsonb_path_ops);
CREATE INDEX ON events_row USING gin (tags);

-- citus_columnar: uncomment alongside the table above
/*
CREATE INDEX ON events_cit (event_date);
CREATE INDEX ON events_cit (user_id);
CREATE INDEX ON events_cit USING gin (metadata jsonb_path_ops);
CREATE INDEX ON events_cit USING gin (tags);
*/

-- ── Statistics ────────────────────────────────────────────────
ANALYZE events_heap;
ANALYZE events_col;
ANALYZE events_row;
-- ANALYZE events_cit;

\timing off
\echo ''
\echo '=== Table sizes ==='
SELECT
    relname,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total,
    pg_size_pretty(pg_relation_size(c.oid))        AS table_only
FROM pg_class c
WHERE relname IN ('events_heap','events_col','events_row','events_cit')
ORDER BY relname;
