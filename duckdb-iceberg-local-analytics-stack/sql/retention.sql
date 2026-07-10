WITH first_orders AS (
    SELECT customer_id, DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM silver_orders
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT DISTINCT customer_id, DATE_TRUNC('month', order_date) AS activity_month
    FROM silver_orders
),
cohort_activity AS (
    SELECT
        first_orders.cohort_month,
        DATE_DIFF('month', first_orders.cohort_month, monthly_activity.activity_month) AS month_number,
        COUNT(DISTINCT monthly_activity.customer_id) AS active_customers
    FROM monthly_activity
    JOIN first_orders USING (customer_id)
    GROUP BY first_orders.cohort_month, month_number
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM first_orders
    GROUP BY cohort_month
)
SELECT
    activity.cohort_month,
    activity.month_number,
    sizes.cohort_size,
    activity.active_customers,
    ROUND(100.0 * activity.active_customers / sizes.cohort_size, 2) AS retention_percent
FROM cohort_activity AS activity
JOIN cohort_sizes AS sizes USING (cohort_month)
ORDER BY activity.cohort_month, activity.month_number;
