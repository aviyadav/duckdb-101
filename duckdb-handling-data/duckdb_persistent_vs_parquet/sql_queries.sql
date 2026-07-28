-- duckdb vpc_analytics.duckdb

DROP VIEW IF EXISTS flow_logs;
CREATE OR REPLACE VIEW flow_logs AS
SELECT
    "year",
    "month",
    v, acc, id, src, dst,
    TRY_CAST(sp AS INTEGER) AS sp,
    TRY_CAST(dp AS INTEGER) AS dp,
    pr,
    TRY_CAST(pkt AS BIGINT) AS pkt,
    -- This TRY_CAST handles the '-' by turning it into NULL
    TRY_CAST(byt AS BIGINT) AS byt,
    TRY_CAST("start" AS BIGINT) AS "start",
    TRY_CAST("end" AS BIGINT) AS "end",
    act, st
FROM read_parquet(
    'base_flow/year=*/month=*/*.parquet',
    hive_partitioning = true,
    union_by_name = true -- This is the key fix
);


SELECT
to_timestamp(start)::DATE AS flow_date, -- Total Gigabytes
round(SUM(byt) / 1024.0 / 1024.0 / 1024.0, 3) AS total_gb, -- Break down by action to see Accept vs Reject ratio
round(SUM(CASE WHEN act = 'ACCEPT' THEN byt ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, 3) AS accepted_gb,
round(SUM(CASE WHEN act = 'REJECT' THEN byt ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, 3) AS rejected_gb,
count(*) AS total_flow_records
FROM flow_logs
WHERE to_timestamp(start) >= CURRENT_DATE - INTERVAL 100 DAY
GROUP BY 1
ORDER BY 1 DESC;


-- duckdb analytics.duckdb

CREATE TABLE flow_logs AS
    SELECT
        "year",
        "month",
        v, acc, id, src, dst,
        TRY_CAST(sp AS INTEGER) AS sp,
        TRY_CAST(dp AS INTEGER) AS dp,
        pr,
        TRY_CAST(pkt AS BIGINT) AS pkt,
        TRY_CAST(byt AS BIGINT) AS byt,
        TRY_CAST("start" AS BIGINT) AS "start",
        TRY_CAST("end" AS BIGINT) AS "end",
        act, st
    FROM read_parquet(
        'base_flow/year=*/month=*/*.parquet',
        hive_partitioning = true,
        union_by_name = true
    );
