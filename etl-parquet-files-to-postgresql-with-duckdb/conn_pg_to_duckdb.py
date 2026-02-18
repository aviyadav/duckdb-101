import duckdb

# Database connection
DB_HOST= 'localhost'
DB_NAME= 'demodb'
DB_USER= 'demouser'
DB_PASSWORD= 'password'
DB_SCHEMA= 'public'
DUCKDB_SCHEMA= 'pg'
TARGET_TABLE= 'fake_table'

# Connection String
connection_string= f"dbname={DB_NAME} user={DB_USER} host={DB_HOST} password={DB_PASSWORD}"
duckdb_connection_string= f"ATTACH '{connection_string}' AS {DUCKDB_SCHEMA} (TYPE postgres, SCHEMA '{DB_SCHEMA}');"

# Create table query
# Fix: LIMIT must be in a subquery inside CREATE TABLE AS SELECT
create_table_query= f"""
CREATE TABLE IF NOT EXISTS {DUCKDB_SCHEMA}.{TARGET_TABLE}
AS
SELECT * FROM (
    SELECT * FROM read_parquet('data/*.parquet')
    LIMIT 0
)
"""

with duckdb.connect() as con:
    # Fix: ATTACH must be called first so the 'pg' schema exists
    con.execute(duckdb_connection_string)

    # Create the table in PostgreSQL (schema only, no data due to LIMIT 0)
    con.execute(create_table_query)

    # Fix: use con.sql() instead of deprecated con.query()
    result = con.sql(f'SELECT * FROM {DUCKDB_SCHEMA}.{TARGET_TABLE} LIMIT 10')
    print(result)


query = f"""
    INSERT INTO {DUCKDB_SCHEMA}.{TARGET_TABLE}
    SELECT * FROM read_parquet('data/*.parquet')
"""

with duckdb.connect() as con:
    con.execute(duckdb_connection_string)
    con.execute(query)
    result = con.sql(f'SELECT * FROM {DUCKDB_SCHEMA}.{TARGET_TABLE} LIMIT 10')
    print(result)


PKEY= 'transaction_id'
PKEY_QUERY= f"ALTER TABLE {DB_SCHEMA}.{TARGET_TABLE} ADD PRIMARY KEY ({PKEY})"
query= f"""CALL postgres_execute('{DUCKDB_SCHEMA}', '{PKEY_QUERY}')"""

with duckdb.connect() as conn :
    conn.execute(duckdb_connection_string)
    conn.execute(query)
    print(conn.sql(f'SELECT * FROM {DUCKDB_SCHEMA}.{TARGET_TABLE} LIMIT 3'))


query= "SELECT * FROM read_parquet('data/*.parquet') LIMIT 0" 
# Could be select on a target table or a parquet file, depend on your preference

with duckdb.connect() as conn :
    conn.execute(duckdb_connection_string)
    description= conn.execute(query).description
    col= [x[0] for x in description]

print(col)

set_update= [f"{x} = EXCLUDED.{x}" for x in col]
on_conflict= "UPDATE SET " + ' ,'.join(set_update)

query= f"""
INSERT INTO {DUCKDB_SCHEMA}.{TARGET_TABLE}
SELECT * FROM read_parquet('data/*.parquet')
ON CONFLICT ({PKEY})
DO {on_conflict}
"""

with duckdb.connect() as conn :
    conn.execute(duckdb_connection_string)
    conn.execute(query)
    print(conn.sql(f'SELECT * FROM {DUCKDB_SCHEMA}.{TARGET_TABLE} LIMIT 3'))