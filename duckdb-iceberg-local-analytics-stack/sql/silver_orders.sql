SELECT
    CAST(order_id AS BIGINT) AS order_id,
    CAST(customer_id AS BIGINT) AS customer_id,
    CAST(order_time AS TIMESTAMP) AS order_time,
    CAST(order_time AS DATE) AS order_date,
    product_category,
    CAST(total_amount AS DECIMAL(12, 2)) AS total_amount
FROM bronze_orders
WHERE status = 'completed'
  AND total_amount > 0
  AND customer_id IS NOT NULL
