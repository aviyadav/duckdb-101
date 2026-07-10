WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        ROUND(SUM(total_amount), 2) AS revenue,
        COUNT(*) AS orders
    FROM silver_orders
    GROUP BY month
),
with_previous AS (
    SELECT
        month,
        revenue,
        orders,
        LAG(revenue) OVER (ORDER BY month) AS previous_revenue
    FROM monthly
)
SELECT
    month,
    revenue,
    orders,
    ROUND(100.0 * (revenue - previous_revenue) / NULLIF(previous_revenue, 0), 2) AS growth_percent
FROM with_previous
ORDER BY month;
