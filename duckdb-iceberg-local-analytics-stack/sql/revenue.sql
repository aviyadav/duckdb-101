SELECT
    order_date,
    ROUND(SUM(total_amount), 2) AS revenue,
    COUNT(*) AS orders,
    ROUND(AVG(total_amount), 2) AS average_order_value
FROM silver_orders
GROUP BY order_date
ORDER BY order_date;
