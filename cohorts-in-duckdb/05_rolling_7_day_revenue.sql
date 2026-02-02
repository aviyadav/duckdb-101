WITH events AS (
    SELECT
        *
    FROM "data/events.parquet"
),

daily AS (
    SELECT
        DATE_TRUNC('day', event_at) AS day,
        SUM(revenue) AS revenue
    FROM events
    WHERE event_type = 'purchase'
    GROUP BY 1
) 

SELECT
    day,
    revenue,
    SUM(revenue) OVER (
        ORDER BY day
        RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_revenue
FROM daily
ORDER BY day;

