import pandas as pd
import numpy as np
import polars as pl
import os
from datetime import datetime, timedelta

def generate_mock_data_pd():

    # Set seed for reproducibility
    np.random.seed(42)
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # --- PANDAS IMPLEMENTATION (COMMENTED OUT) ---
    # 1. Generate Products
    num_products = 50
    products_data = {
        'product_id': range(1, num_products + 1),
        'product_name': [f'Product_{i}' for i in range(1, num_products + 1)]
    }
    products_df = pd.DataFrame(products_data)
    products_df.to_parquet('data/products.parquet', index=False)
    
    # 2. Generate Orders
    # User requested at least 1000 orders
    num_orders = 1500 
    orders_data = {
        'order_id': range(1, num_orders + 1),
        'customer_id': np.random.randint(1, 300, num_orders),
    }
    orders_df = pd.DataFrame(orders_data)
    orders_df.to_parquet('data/orders.parquet', index=False)
    
    # Generate random dates for each order (spanning around 2025-10-01 to 2026-01-01 to ensure some match)
    start_date = datetime(2025, 6, 1)
    end_date = datetime(2026, 4, 1)
    date_range = (end_date - start_date).days
    random_days = np.random.randint(0, date_range, num_orders)
    order_dates = [start_date + timedelta(days=int(d)) for d in random_days]
    order_date_map = dict(zip(orders_df['order_id'], order_dates))
    
    # 3. Generate Order Items
    # Each order has 1 to 5 items
    num_items = np.random.randint(1, 6, num_orders)
    total_order_items = np.sum(num_items)
    
    order_ids = np.repeat(orders_df['order_id'].values, num_items)
    item_dates = [order_date_map[oid] for oid in order_ids]
    
    order_items_data = {
        'order_id': order_ids,
        'product_id': np.random.randint(1, num_products + 1, total_order_items),
        'quantity': np.random.randint(1, 10, total_order_items),
        'unit_price': np.random.uniform(10.0, 500.0, total_order_items).round(2),
        'order_date': item_dates
    }
    order_items_df = pd.DataFrame(order_items_data)
    # The SQL query uses oi.order_date, so we format this as a pure date
    order_items_df['order_date'] = pd.to_datetime(order_items_df['order_date']).dt.date
    order_items_df.to_parquet('data/order_items.parquet', index=False)
    
    # 4. Generate Refunds
    # Let's say 15% of orders have refunds
    num_refunds = int(num_orders * 0.15)
    refunded_order_ids = np.random.choice(orders_df['order_id'], size=num_refunds, replace=False)
    
    refunds_data = {
        'refund_id': range(1, num_refunds + 1),
        'order_id': refunded_order_ids,
        'amount': np.random.uniform(5.0, 150.0, num_refunds).round(2)
    }
    refunds_df = pd.DataFrame(refunds_data)
    refunds_df.to_parquet('data/refunds.parquet', index=False)
    
    print(f"Data generation complete.")
    print(f"Generated {len(orders_df)} orders.")
    print(f"Generated {len(order_items_df)} order items.")
    print(f"Generated {len(products_df)} products.")
    print(f"Generated {len(refunds_df)} refunds.")
    print("Files saved in 'data' directory.")



def generate_mock_data_pl():
    # Set seed for reproducibility
    np.random.seed(42)
    
    # Create data directory
    os.makedirs('data', exist_ok=True)    

    # --- POLARS IMPLEMENTATION ---
    # 1. Generate Products
    num_products = 50
    products_df = pl.DataFrame({
        'product_id': list(range(1, num_products + 1)),
        'product_name': [f'Product_{i}' for i in range(1, num_products + 1)]
    })
    products_df.write_parquet('data/products.parquet')
    
    # 2. Generate Orders
    num_orders = 1500 
    orders_df = pl.DataFrame({
        'order_id': list(range(1, num_orders + 1)),
        'customer_id': np.random.randint(1, 300, num_orders)
    })
    orders_df.write_parquet('data/orders.parquet')
    
    # Generate random dates for each order
    start_date = datetime(2025, 6, 1)
    end_date = datetime(2026, 4, 1)
    date_range = (end_date - start_date).days
    random_days = np.random.randint(0, date_range, num_orders)
    order_dates = [start_date + timedelta(days=int(d)) for d in random_days]
    
    # Create mapping DataFrame for dates for easy join
    order_date_df = pl.DataFrame({
        'order_id': orders_df['order_id'],
        'order_date': order_dates
    })
    
    # 3. Generate Order Items
    num_items = np.random.randint(1, 6, num_orders)
    total_order_items = np.sum(num_items)
    
    order_ids = np.repeat(orders_df['order_id'].to_numpy(), num_items)
    
    order_items_df = pl.DataFrame({
        'order_id': order_ids,
        'product_id': np.random.randint(1, num_products + 1, total_order_items),
        'quantity': np.random.randint(1, 10, total_order_items),
        'unit_price': np.random.uniform(10.0, 500.0, total_order_items).round(2)
    })
    
    # Map dates to items and convert to date type
    order_items_df = order_items_df.join(order_date_df, on="order_id", how="left")
    order_items_df = order_items_df.with_columns(pl.col('order_date').dt.date())
    
    order_items_df.write_parquet('data/order_items.parquet')
    
    # 4. Generate Refunds
    num_refunds = int(num_orders * 0.15)
    refunded_order_ids = np.random.choice(orders_df['order_id'].to_numpy(), size=num_refunds, replace=False)
    
    refunds_df = pl.DataFrame({
        'refund_id': list(range(1, num_refunds + 1)),
        'order_id': refunded_order_ids,
        'amount': np.random.uniform(5.0, 150.0, num_refunds).round(2)
    })
    refunds_df.write_parquet('data/refunds.parquet')
    
    print(f"Data generation complete using Polars.")
    print(f"Generated {orders_df.height} orders.")
    print(f"Generated {order_items_df.height} order items.")
    print(f"Generated {products_df.height} products.")
    print(f"Generated {refunds_df.height} refunds.")
    print("Files saved in 'data' directory.")


if __name__ == "__main__":
    generate_mock_data_pd()
    # generate_mock_data_pl()