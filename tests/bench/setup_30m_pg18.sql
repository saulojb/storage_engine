\set ON_ERROR_STOP on
\timing on

DROP DATABASE IF EXISTS bench_am_30m;
CREATE DATABASE bench_am_30m;
\connect bench_am_30m

CREATE EXTENSION IF NOT EXISTS storage_engine;

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

INSERT INTO events_heap
    (created_at, event_date, user_id, session_id, amount, price, quantity,
     duration_ms, score, country_code, browser, url, is_mobile, event_type,
     metadata, tags)
SELECT
    '2024-01-01'::timestamptz + (g * interval '1 second'),
    ('2024-01-01'::date + ((g - 1) / 86400)::int),
    (g % 500000) + 1,
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
FROM generate_series(1, 30000000) g;

INSERT INTO events_col
    (created_at, event_date, user_id, session_id, amount, price, quantity,
     duration_ms, score, country_code, browser, url, is_mobile, event_type,
     metadata, tags)
SELECT
    created_at, event_date, user_id, session_id, amount, price, quantity,
    duration_ms, score, country_code, browser, url, is_mobile, event_type,
    metadata, tags
FROM events_heap
ORDER BY event_date;

CREATE INDEX ON events_heap (event_date);
CREATE INDEX ON events_heap (event_type);
ANALYZE events_heap;
ANALYZE events_col;
