import duckdb


def query_my_bucket() -> None:
    # 1. Connect to an in-memory DuckDB instance
    con = duckdb.connect()

    # 2. Create the non-persistent S3 secret for the local RustFS server.
    #    Note: with DuckDB 1.5.x the bucket must be part of the ENDPOINT
    #    ("host:port/bucket") for path-style URLs, otherwise S3 LIST requests
    #    (used for glob patterns) are sent without the bucket and match nothing.
    con.execute(
        """
        CREATE SECRET my_s3_secret (
            TYPE S3,
            KEY_ID 'rustfsadmin',
            SECRET 'rustfsadmin',
            ENDPOINT 'localhost:9000/warehouse',
            REGION 'us-east-1',
            URL_STYLE 'path-style',
            USE_SSL false
        );
    """
    )

    # 3. Query the S3 bucket (DuckDB automatically applies 'my_s3_secret')
    query = "SELECT * FROM read_parquet('s3://warehouse/shop/orders/data/**/*.parquet') LIMIT 5;"
    result = con.sql(query)

    # 4. Display the results (.show() avoids the pandas/pytz dependency
    #    that .df() would need for the timestamptz column)
    result.show()


if __name__ == "__main__":
    query_my_bucket()
