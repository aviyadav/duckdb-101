import pandas as pd
import glob
import time
import duckdb


def using_pandas():
    cur_time = time.time()
    df = pd.read_csv("dataset/olist_customers_dataset.csv")
    print(f"time: {time.time() - cur_time}")
    print(df.head(10))

def using_duckdb(conn):
    cur_time = time.time()
    df = conn.execute("""
        SELECT * FROM read_csv_auto('dataset/olist_customers_dataset.csv', header=True)
    """).df()
    print(f"time: {time.time() - cur_time}")
    # print(df)

    conn.register("df_view", df)
    df2 = conn.execute("""DESCRIBE df_view""").df()
    # print(df2)

    df3 = conn.execute("""SELECT COUNT(1) FROM df""").df()
    # print(df3)

    df4 = conn.execute("""SELECT * FROM df WHERE "customer_id" = '06b8999e2fba1a1fbc88172c00ba8bc7' """).df()
    # print(df4)

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
    # print(df5)

    df6 = conn.execute("SELECT * EXCLUDE (id_customer, zip_code_prefix) FROM customer").df()
    # print(df6)

    df7 = conn.execute("""
                 SELECT 
                    MIN(COLUMNS(* EXCLUDE (id_customer, zip_code_prefix))) 
                 FROM customer
                """).df()
    # print(df7)

    conn.execute("""
                CREATE OR REPLACE VIEW aggregated_customers AS
                SELECT
                    COUNT(1) AS nm_customer,
                    state
                FROM customer
                GROUP BY state
                """).df

    df8 = conn.execute("FROM aggregated_customers").df()
    # print(df8)

    conn.execute("COPY (FROM aggregated_customers) TO 'aggregated_customers.parquet' (FORMAT 'parquet')")
    df9 = conn.execute("FROM aggregated_customers.parquet").df()
    print(df9)




if __name__ == '__main__':
    conn = duckdb.connect("mydb.db")
    # using_pandas()
    using_duckdb(conn)



