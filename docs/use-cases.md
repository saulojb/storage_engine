---
layout: default
title: Use Cases
nav_order: 5
---

# Use Cases
{: .no_toc }

Real-world patterns and worked examples for `colcompress` and `rowcompress`.
{: .fs-6 .fw-300 }

<details open markdown="block">
<summary>Table of contents</summary>
{: .text-delta }
1. TOC
{:toc}
</details>

---

## Analytics / Data Warehouse

The primary use case for `colcompress`. Queries aggregate over millions of rows touching a small subset of the table's columns.

```sql
CREATE TABLE sales (
    sale_id     bigserial,
    sold_at     timestamptz NOT NULL,
    customer_id bigint,
    product_id  bigint,
    category    text,
    region      text,
    quantity    int,
    unit_price  numeric(15,4),
    discount    numeric(5,2),
    revenue     numeric(15,4),
    cost        numeric(15,4),
    margin      numeric(15,4),
    channel     text,
    currency    text
) USING colcompress;

-- Sort by date for optimal range pruning
SELECT engine.alter_colcompress_table_set(
    'sales'::regclass,
    orderby           => 'sold_at ASC',
    compression       => 'zstd',
    compression_level => 9
);

-- Bulk load, then compact into globally sorted stripes
INSERT INTO sales SELECT ... FROM raw_sales;
SELECT engine.colcompress_merge('sales');
```

```sql
-- Monthly revenue by category — touches only 4 of 14 columns
SELECT category, SUM(revenue), AVG(margin), COUNT(*)
FROM sales
WHERE sold_at BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY category
ORDER BY SUM(revenue) DESC;

-- Year-over-year comparison
SELECT
    date_trunc('month', sold_at) AS month,
    region,
    SUM(revenue),
    SUM(revenue) / SUM(SUM(revenue)) OVER (PARTITION BY date_trunc('month', sold_at)) AS share
FROM sales
WHERE sold_at >= '2023-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;
```

**Why colcompress wins here:**
- Column projection reads only `sold_at`, `category`, `revenue`, `margin` — 4/14 columns
- Stripe pruning with `orderby = 'sold_at ASC'` skips entire months before decompression
- Vectorized GROUP BY processes each column chunk in batches of 10,000 values

---

## HTAP — Mixed Workload

`colcompress` and `heap` tables in the same database, each serving the query type it's best at.

```sql
-- OLTP: heap table for transactional writes and point lookups
CREATE TABLE orders (
    id          bigserial PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now(),
    customer_id bigint NOT NULL,
    status      text NOT NULL,
    total       numeric(15,4)
);
CREATE INDEX ON orders (customer_id);
CREATE INDEX ON orders (status) WHERE status != 'delivered';

-- Analytics: colcompress replica / aggregation target
CREATE TABLE orders_analytics (
    LIKE orders  -- same schema
) USING colcompress;

SELECT engine.alter_colcompress_table_set(
    'orders_analytics'::regclass,
    orderby => 'created_at ASC'
);
```

```sql
-- OLTP: fast point lookup via heap + index (microseconds)
SELECT * FROM orders WHERE customer_id = 42 ORDER BY created_at DESC LIMIT 10;

-- Analytics: colcompress for aggregation (milliseconds over millions of rows)
SELECT
    date_trunc('day', created_at) AS day,
    status,
    COUNT(*),
    SUM(total)
FROM orders_analytics
WHERE created_at >= now() - interval '90 days'
GROUP BY 1, 2
ORDER BY 1, 2;
```

**Sync pattern** (simple incremental load):
```sql
-- Run periodically via pg_cron
INSERT INTO orders_analytics
SELECT * FROM orders
WHERE created_at > (SELECT MAX(created_at) FROM orders_analytics)
ON CONFLICT DO NOTHING;
```

---

## Document / File Repository

colcompress with `index_scan = true`: columnar compression for storage savings, B-tree index for point-lookup speed. Ideal for tables storing large binary or text blobs.

```sql
CREATE TABLE documents (
    id           bigserial PRIMARY KEY,
    created_at   timestamptz NOT NULL DEFAULT now(),
    owner_id     bigint NOT NULL,
    doc_type     text NOT NULL,       -- 'invoice', 'contract', 'receipt', ...
    filename     text NOT NULL,
    mime_type    text,
    file_size    bigint,
    content      bytea,              -- compressed at storage level by colcompress
    metadata     jsonb,
    tags         text[]
) USING colcompress;

-- Enable index scan — point lookups by PK are the primary access pattern
SELECT engine.alter_colcompress_table_set(
    'documents'::regclass,
    index_scan        => true,
    compression       => 'zstd',
    compression_level => 19   -- maximum compression for cold blobs
);
```

```sql
-- Point lookup by PK — decompresses only the matching row (index_scan = true)
SELECT filename, mime_type, content
FROM documents
WHERE id = 98765;

-- Search and aggregation — sequential scan with column projection
SELECT doc_type, COUNT(*), SUM(file_size)
FROM documents
WHERE owner_id = 42
  AND metadata @> '{"status": "approved"}'
GROUP BY doc_type;
```

**Storage savings**: zstd level 19 typically compresses PDFs 30–60%, XML/JSON 80–95%, already-compressed formats (ZIP, MP4) ~0%.

---

## Time-Series Events

Append-only event stream with time-based range queries. colcompress with `orderby = 'ts ASC'` is the natural fit.

```sql
CREATE TABLE events (
    ts          timestamptz NOT NULL,
    user_id     bigint,
    session_id  text,
    event_type  text,
    page_url    text,
    duration_ms int,
    amount      numeric(15,4),
    metadata    jsonb
) USING colcompress;

SELECT engine.alter_colcompress_table_set(
    'events'::regclass,
    orderby           => 'ts ASC',
    compression       => 'zstd',
    compression_level => 6
);
```

```sql
-- Real-time dashboard: last 24 hours
SELECT
    date_trunc('hour', ts) AS hour,
    event_type,
    COUNT(*),
    COUNT(DISTINCT user_id),
    SUM(amount)
FROM events
WHERE ts >= now() - interval '24 hours'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Funnel analysis: 7-day window
SELECT
    event_type,
    COUNT(DISTINCT session_id) AS sessions,
    SUM(amount) AS revenue
FROM events
WHERE ts BETWEEN '2024-06-01' AND '2024-06-07'
GROUP BY event_type;
```

Maintenance with auto-scheduler (runs via background worker or pg_cron):
```sql
-- Manually trigger maintenance for all tables needing it
CALL engine.storage_maintenance_auto();

-- Or check recommendations first
SELECT table_name, recommended_action
FROM engine.storage_health
WHERE recommended_action != 'ok';
```

---

## Audit Log (LGPD / GDPR Compliance)

rowcompress for an immutable append-only audit trail. Writes are frequent, reads are occasional and always sequential.

```sql
CREATE TABLE audit_log (
    id          bigserial,
    logged_at   timestamptz NOT NULL DEFAULT now(),
    user_id     bigint,
    session_id  uuid,
    ip_address  inet,
    action      text NOT NULL,
    resource    text,
    resource_id bigint,
    before_data jsonb,
    after_data  jsonb,
    result      text
) USING rowcompress;

SELECT engine.alter_rowcompress_table_set(
    'audit_log'::regclass,
    batch_size        => 10000,
    compression       => 'zstd',
    compression_level => 3  -- fast writes, good ratio
);

-- Trigger-based capture
CREATE OR REPLACE FUNCTION log_change() RETURNS trigger AS $$
BEGIN
    INSERT INTO audit_log (user_id, action, resource, resource_id, before_data, after_data)
    VALUES (
        current_setting('app.user_id', true)::bigint,
        TG_OP,
        TG_TABLE_NAME,
        COALESCE(NEW.id, OLD.id),
        to_jsonb(OLD),
        to_jsonb(NEW)
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

```sql
-- Compliance query: all actions by a user in a date range
SELECT logged_at, action, resource, resource_id, after_data
FROM audit_log
WHERE user_id = 12345
  AND logged_at BETWEEN '2024-01-01' AND '2024-03-31'
ORDER BY logged_at;

-- Storage health
SELECT table_name, live_rows,
       pg_size_pretty(pg_total_relation_size('audit_log')) AS disk_size
FROM engine.storage_health
WHERE table_name = 'audit_log';
```

---

## ClickBench-Style Analytics (engine.uint8)

For workloads with unsigned 64-bit identifiers (e.g., ClickHouse's `WatchID`, `UserID`):

```sql
SET search_path TO engine, public;

CREATE TABLE hits (
    WatchID    engine.uint8 NOT NULL,
    JavaEnable smallint,
    Title      text,
    GoodEvent  smallint,
    EventTime  timestamptz,
    EventDate  date,
    CounterID  int,
    ClientIP   int,
    RegionID   int,
    UserID     engine.uint8 NOT NULL,
    CounterClass smallint,
    OS         smallint,
    UserAgent  smallint,
    URL        text,
    Referer    text,
    IsRefresh  smallint,
    RefererCategoryID smallint,
    RefererRegionID int,
    URLCategoryID smallint,
    URLRegionID int,
    ResolutionWidth smallint,
    ResolutionHeight smallint,
    ResolutionDepth smallint,
    FlashMajor smallint,
    FlashMinor smallint
    -- ...
) USING colcompress;

-- ClickBench Q1 equivalent
SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';

-- ClickBench Q6 equivalent
SELECT
    MIN(EventDate), MAX(EventDate), CounterID,
    COUNT(DISTINCT UserID), COUNT(*),
    AVG(ResolutionWidth)
FROM hits
GROUP BY CounterID
ORDER BY COUNT(*) DESC
LIMIT 20;
```
