SELECT
  dt,
  tenant,
  count(*) AS events,
  approx_count_distinct(user_id) AS users
FROM read_parquet('data/events/*/*/*.parquet', hive_partitioning=1)
WHERE dt >= DATE '2026-01-01'
  AND dt <  DATE '2026-01-08'
  AND tenant = 'acme'
GROUP BY dt, tenant
ORDER BY dt;


-- Create a local DuckDB table as a hot cache
CREATE TABLE IF NOT EXISTS events_hot AS
SELECT * FROM read_parquet('data/events/*/*/*.parquet', hive_partitioning=1)
WHERE dt >= CURRENT_DATE - INTERVAL 14 DAY;

-- Refresh pattern (simple version)
DELETE FROM events_hot WHERE dt >= CURRENT_DATE - INTERVAL 14 DAY;
INSERT INTO events_hot
SELECT * FROM read_parquet('data/events/*/*/*.parquet', hive_partitioning=1)
WHERE dt >= CURRENT_DATE - INTERVAL 14 DAY;


select * from events_hot;

