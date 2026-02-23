import duckdb


def main(qry: str):
    with duckdb.connect() as conn:
        print(conn.query(qry))


if __name__ == "__main__":
    query = """
    SELECT * FROM read_parquet('data/*.parquet') LIMIT 100
    """
    main(query)

    qr_ex_1 = """
    SELECT
        transaction_id ,
        timestamp ,
        -- customer_id,
        merchant ,
        category ,
        amount ,
        status
    FROM read_parquet('data/*.parquet')
    """
    main(qr_ex_1)

    qr_ex_2 = """
    SELECT * EXCLUDE(customer_id)
    FROM read_parquet('data/*.parquet')
    """

    main(qr_ex_2)
