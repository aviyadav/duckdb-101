SELECT
    customer_id,
    COUNT(*) AS lifetime_orders,
    ROUND(SUM(total_amount), 2) AS lifetime_revenue,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
FROM silver_orders
GROUP BY customer_id;
