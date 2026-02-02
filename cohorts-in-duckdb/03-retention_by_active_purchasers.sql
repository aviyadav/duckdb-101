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
)

SELECT
    cohort_month,
    month_index,
    COUNT(DISTINCT user_id) AS active_users
FROM indexed
WHERE month_index >= 0
GROUP BY 1, 2
ORDER BY 1, 2;
