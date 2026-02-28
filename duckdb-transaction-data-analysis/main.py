import time

import duckdb

print("-" * 60)
print("DuckDB Transaction Data Analysis")
print("-" * 60)

# Example 1: Quick peek at the data
print("\n1. Quick peek at the transaction data:")
print("-" * 60)
start_time = time.time()
result = duckdb.sql("""
    SELECT * FROM 'data/transaction-data.csv' LIMIT 10;
""").df()
elapsed_time = time.time() - start_time
print(result.to_string(index=False))
print(f"\n⏱️  Query executed in {elapsed_time:.3f} seconds")


# Example 2: Sales by product
print("\n2. Total sales by product:")
print("-" * 60)
start_time = time.time()

result = duckdb.sql("""
    SELECT
        product,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_revenue,
        ROUND(AVG(amount), 2) AS avg_transaction_revenue,
        SUM(quantity) AS units_sold
    FROM 'data/transaction-data.csv'
    GROUP BY product
    ORDER BY total_revenue DESC;
""").df()
elapsed_time = time.time() - start_time
print(result.to_string(index=False))
print(f"\n⏱️  Query executed in {elapsed_time:.3f} seconds")


# Example 3: Top customers by spending
print("\n\n3. Top 10 customers by spending:")
print("-" * 60)
start_time = time.time()

result = duckdb.sql("""
    SELECT
        customer_id,
        COUNT(*) AS num_transactions,
        ROUND(SUM(amount), 2) AS total_spent,
        ROUND(AVG(amount), 2) AS avg_per_transaction_spent,
    FROM 'data/transaction-data.csv'
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 10;
""").df()
elapsed_time = time.time() - start_time
print(result.to_string(index=False))
print(f"\n⏱️  Query executed in {elapsed_time:.3f} seconds")

# Example 4: Monthly trends
print("\n\n4. Monthly sales trend (2024):")
print("-" * 60)
start = time.time()
result = duckdb.sql("""
    SELECT
        strftime(date, '%Y-%m') as month,
        COUNT(*) as transactions,
        ROUND(SUM(amount), 2) as revenue
    FROM 'data/transaction-data.csv'
    WHERE date >= '2024-01-01'
    GROUP BY month
    ORDER BY month
""").df()
elapsed = time.time() - start
print(result.to_string(index=False))
print(f"\n⏱️  Query executed in {elapsed:.3f} seconds")

# Example 5: Store performance comparison
print("\n\n5. Store performance comparison:")
print("-" * 60)
start = time.time()
result = duckdb.sql("""
    SELECT
        store,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(*) as transactions,
        ROUND(SUM(amount), 2) as total_revenue,
        ROUND(AVG(amount), 2) as avg_transaction
    FROM 'data/transaction-data.csv'
    GROUP BY store
    ORDER BY total_revenue DESC
""").df()
elapsed = time.time() - start
print(result.to_string(index=False))
print(f"\n⏱️  Query executed in {elapsed:.3f} seconds")

# Example 6: Complex analysis with window functions
print("\n\n6. Payment method preferences by store:")
print("-" * 60)
start = time.time()
result = duckdb.sql("""
    SELECT
        store,
        payment_method,
        COUNT(*) as usage_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY store), 2) as percentage
    FROM 'data/transaction-data.csv'
    GROUP BY store, payment_method
    ORDER BY store, usage_count DESC
""").df()
elapsed = time.time() - start
print(result.to_string(index=False))
print(f"\n⏱️  Query executed in {elapsed:.3f} seconds")

print("\n" + "=" * 60)
print("✓ Analysis complete!")
print("=" * 60)
