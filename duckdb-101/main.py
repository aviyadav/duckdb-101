import pandas as pd
import polars as pl
import glob
import time
import duckdb
import concurrent.futures


def using_pandas():
    cur_time = time.time()
    df = pd.read_csv("dataset/olist_customers_dataset.csv")
    print(f"time: {time.time() - cur_time}")
    print(df.head(10))

    print(df.describe())

    print(len(df))

    print(df[df["customer_id"] == "06b8999e2fba1a1fbc88172c00ba8bc7"])

    customer = df.rename(columns={
        "customer_id": "id_customer",
        "customer_unique_id": "unique_id_customer",
        "customer_zip_code_prefix": "zip_code_prefix",
        "customer_city": "city",
        "customer_state": "state"
    })
    
    print(customer)

    print(customer.drop(columns=["id_customer", "zip_code_prefix"]))

    print(customer.drop(columns=["id_customer", "zip_code_prefix"]).min())

    aggregated_customers = customer.groupby("state").size().reset_index(name="nm_customer")
    print(aggregated_customers)

    aggregated_customers.to_parquet("aggregated_customers_pandas.parquet")
    print(pd.read_parquet("aggregated_customers_pandas.parquet"))

def using_duckdb(conn):
    cur_time = time.time()
    df = conn.execute("""
        SELECT * FROM read_csv_auto('dataset/olist_customers_dataset.csv', header=True)
    """).df()
    print(f"time: {time.time() - cur_time}")
    print(df)

    conn.register("df_view", df)
    df2 = conn.execute("""DESCRIBE df_view""").df()
    print(df2)

    df3 = conn.execute("""SELECT COUNT(1) FROM df""").df()
    print(df3)

    df4 = conn.execute("""SELECT * FROM df WHERE "customer_id" = '06b8999e2fba1a1fbc88172c00ba8bc7' """).df()
    print(df4)

    conn.execute("""
        CREATE OR REPLACE TABLE customer AS 
        SELECT
            "customer_id" AS id_customer,
            "customer_unique_id" AS unique_id_customer,
            "customer_zip_code_prefix" AS zip_code_prefix,
            "customer_city" AS city,
            "customer_state" AS state
        FROM df
        """)

    df5 = conn.execute("FROM customer").df()
    print(df5)

    df6 = conn.execute("SELECT * EXCLUDE (id_customer, zip_code_prefix) FROM customer").df()
    print(df6)

    df7 = conn.execute("""
                 SELECT 
                    MIN(COLUMNS(* EXCLUDE (id_customer, zip_code_prefix))) 
                 FROM customer
                """).df()
    print(df7)

    conn.execute("""
                CREATE OR REPLACE VIEW aggregated_customers AS
                SELECT
                    COUNT(1) AS nm_customer,
                    state
                FROM customer
                GROUP BY state
                """).df

    df8 = conn.execute("FROM aggregated_customers").df()
    print(df8)

    conn.execute("COPY (FROM aggregated_customers) TO 'aggregated_customers_duckdb.parquet' (FORMAT 'parquet')")
    df9 = conn.execute("FROM aggregated_customers_duckdb.parquet").df()
    print(df9)


def using_polars():
    cur_time = time.time()
    df = pl.read_csv("dataset/olist_customers_dataset.csv")
    print(f"time: {time.time() - cur_time}")
    print(df)

    print(df.describe())

    print(df.select(pl.len()))

    print(df.filter(pl.col("customer_id") == "06b8999e2fba1a1fbc88172c00ba8bc7"))

    customer = df.select(
        pl.col("customer_id").alias("id_customer"),
        pl.col("customer_unique_id").alias("unique_id_customer"),
        pl.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        pl.col("customer_city").alias("city"),
        pl.col("customer_state").alias("state"),
    )

    print(customer)

    print(customer.select(pl.exclude("id_customer", "zip_code_prefix")))

    print(customer.select(pl.all().exclude("id_customer", "zip_code_prefix").min()))

    aggregated_customers = customer.group_by("state").agg(pl.len().alias("nm_customer"))
    print(aggregated_customers)

    aggregated_customers.write_parquet("aggregated_customers_polars.parquet")
    print(pl.read_parquet("aggregated_customers_polars.parquet"))

def write_to_parquet():
    # using polars write olist_customers_dataset.csv to parquet
    df = pl.read_csv("dataset/olist_customers_dataset.csv")
    df.write_parquet("dataset/olist_customers_dataset.parquet")
    print(pl.read_parquet("dataset/olist_customers_dataset.parquet"))

    # using duckdb write olist_customers_dataset.csv to parquet
    conn = duckdb.connect("mydb.db")
    conn.execute("COPY (FROM 'dataset/olist_customers_dataset.csv') TO 'dataset/olist_customers_dataset_duckdb.parquet' (FORMAT 'parquet')")
    df = conn.execute("FROM 'dataset/olist_customers_dataset_duckdb.parquet'").df()
    print(df)
    
if __name__ == '__main__':
    conn = duckdb.connect("mydb.db")
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        f1 = executor.submit(using_pandas)
        f2 = executor.submit(using_duckdb, conn)
        f3 = executor.submit(using_polars)
        
        concurrent.futures.wait([f1, f2, f3])
    # write_to_parquet()
