WITH events AS (
    SELECT
        *
    FROM "data/events.parquet"
)

SELECT
  user_id,
  event_at,
  event_type,
  revenue,
  MIN(event_at) OVER (PARTITION BY user_id) AS first_purchase_at,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_at) AS purchase_number
FROM events
WHERE user_id IN ('u_0319', 'u_0508', 'u_0186')
  AND event_type = 'purchase'
ORDER BY user_id, event_at;