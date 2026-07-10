WITH customer_revenue AS (
    SELECT
        customer_id,
        COUNT(*) AS orders,
        ROUND(SUM(total_amount), 2) AS revenue,
        MAX(order_date) AS last_order_date
    FROM silver_orders
    GROUP BY customer_id
)
SELECT
    revenue.customer_id,
    customers.customer_name,
    customers.region,
    customers.segment,
    revenue.orders,
    revenue.revenue,
    revenue.last_order_date
FROM customer_revenue AS revenue
JOIN silver_customers AS customers USING (customer_id)
ORDER BY revenue.revenue DESC
LIMIT $limit;
