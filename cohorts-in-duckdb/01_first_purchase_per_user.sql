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

first_purchase AS (
  SELECT
    user_id,
    MIN(event_at) AS first_purchase_at
  FROM purchases
  GROUP BY 1
)

SELECT * FROM first_purchase;