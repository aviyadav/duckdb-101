WITH order_items AS (
    SELECT * FROM read_parquet('data/order_items.parquet')
),
products AS (
    SELECT * FROM read_parquet('data/products.parquet')
),
refunds AS (
    SELECT * FROM read_parquet('data/refunds.parquet')
)
SELECT 
    p.product_name,
    SUM(oi.quantity * oi.unit_price) - COALESCE(SUM(r.amount), 0) AS net_revenue
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
LEFT JOIN refunds r ON r.order_id = oi.order_id
WHERE oi.order_date >= DATE '2025-10-01' AND oi.order_date < DATE '2026-01-01'
GROUP BY 1
ORDER BY net_revenue DESC;


CREATE OR REPLACE TABLE order_items AS (
    SELECT * FROM read_parquet('data/order_items.parquet')
);
CREATE OR REPLACE TABLE products AS (
    SELECT * FROM read_parquet('data/products.parquet')
);
CREATE OR REPLACE TABLE refunds AS (
    SELECT * FROM read_parquet('data/refunds.parquet')
);
CREATE OR REPLACE TABLE orders AS (
    SELECT * FROM read_parquet('data/orders.parquet')
);
