-- =============================================================================
--  02_create_table_and_seed.sql
--  Create the Iceberg table in the local warehouse and load three rows.
--
--  In the AWS version of this exercise the table was created in an S3 Tables
--  namespace; here it is created through Lakekeeper (metadata -> PostgreSQL)
--  while the Parquet/Avro/metadata.json files land in the RustFS bucket
--  s3://warehouse/iceberg/lab1/customers2/.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS lake.lab1;

CREATE TABLE IF NOT EXISTS lake.lab1.customers2 (
    customer_id INTEGER,
    name        VARCHAR,
    city        VARCHAR,
    balance     DOUBLE
);

-- Baseline state used by the MERGE examples below.
-- (Delete first so re-running this script always starts from the same seed.)
DELETE FROM lake.lab1.customers2;

INSERT INTO lake.lab1.customers2 VALUES
    (1, 'Alice', 'Boston',  100.00),
    (2, 'Bob',   'Seattle', 200.00),
    (3, 'Carol', 'Austin',  300.00);

SELECT '01-load' AS step, * FROM lake.lab1.customers2 ORDER BY customer_id;
