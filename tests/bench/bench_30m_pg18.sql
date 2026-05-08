\set ON_ERROR_STOP on
\timing on
SET jit=off;
SET max_parallel_workers_per_gather=0;

\echo 'Q1 heap'
SELECT COUNT(*) FROM events_heap;
\echo 'Q1 colcompress'
SELECT COUNT(*) FROM events_col;

\echo 'Q4 heap'
SELECT event_type,COUNT(*),SUM(amount),AVG(duration_ms),percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_ms)
FROM events_heap GROUP BY event_type ORDER BY SUM(amount) DESC NULLS LAST;
\echo 'Q4 colcompress'
SELECT event_type,COUNT(*),SUM(amount),AVG(duration_ms),percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_ms)
FROM events_col GROUP BY event_type ORDER BY SUM(amount) DESC NULLS LAST;

\echo 'Q5 heap'
SELECT event_date,COUNT(*),SUM(amount),AVG(price)
FROM events_heap
WHERE event_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY event_date ORDER BY event_date;
\echo 'Q5 colcompress'
SELECT event_date,COUNT(*),SUM(amount),AVG(price)
FROM events_col
WHERE event_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY event_date ORDER BY event_date;

\echo 'Q10 heap'
SELECT browser,is_mobile,COUNT(*),SUM(amount),AVG(amount),MIN(amount),MAX(amount),SUM(price*quantity),AVG(duration_ms),COUNT(DISTINCT user_id),SUM(CASE WHEN event_type='purchase' THEN amount END)
FROM events_heap GROUP BY browser,is_mobile ORDER BY COUNT(*) DESC;
\echo 'Q10 colcompress'
SELECT browser,is_mobile,COUNT(*),SUM(amount),AVG(amount),MIN(amount),MAX(amount),SUM(price*quantity),AVG(duration_ms),COUNT(DISTINCT user_id),SUM(CASE WHEN event_type='purchase' THEN amount END)
FROM events_col GROUP BY browser,is_mobile ORDER BY COUNT(*) DESC;
