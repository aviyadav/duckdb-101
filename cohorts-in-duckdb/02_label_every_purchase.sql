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

enriched AS (
    SELECT
        user_id,
        event_at,
        revenue,
        
        MIN(event_at) OVER (PARTITION BY user_id) AS first_purchase_at,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_at) AS purchase_number
    FROM purchases
)

SELECT
    user_id,
    event_at,
    revenue,
    first_purchase_at,
    purchase_number,

    DATE_TRUNC('month', first_purchase_at) AS cohort_month,
    DATE_DIFF('month', DATE_TRUNC('month', first_purchase_at), DATE_TRUNC('month', event_at)) AS month_since_cohort
FROM enriched;
