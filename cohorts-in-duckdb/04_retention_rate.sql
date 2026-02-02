WITH events AS (
    SELECT
        *
    FROM "data/events.parquet"
),

purchases AS (
    SELECT
        user_id,
        event_at,
        revenue
    FROM events
    WHERE event_type = 'purchase'
),

cohorted AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(event_at) OVER (PARTITION BY user_id)) AS cohort_month,
        DATE_TRUNC('month', event_at) AS activity_month
    FROM purchases
),

indexed AS (
    SELECT
        cohort_month,
        DATE_DIFF('month', cohort_month, activity_month) AS month_index,
        user_id
    FROM cohorted
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM indexed
    WHERE month_index = 0
    GROUP BY 1
),

active AS (
    SELECT
        cohort_month,
        month_index,
        COUNT(DISTINCT user_id) AS active_users
    FROM indexed
    WHERE month_index >= 0
    GROUP BY 1, 2
)

SELECT
    a.cohort_month,
    a.month_index,
    a.active_users,
    s.cohort_size,
    ROUND(a.active_users * 1.0 / s.cohort_size, 4) AS retention_rate
FROM active a
JOIN cohort_sizes s USING (cohort_month)
ORDER BY 1, 2;
