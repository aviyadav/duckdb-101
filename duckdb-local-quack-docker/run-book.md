#### Build and start the workers

```bash
docker compose build
```

Start the three workers:

```bash
docker compose up -d worker-1 worker-2 worker-3
```

Watch the logs:

```bash
docker compose logs -f worker-1
```

On first startup, each worker seeds its own DuckDB database.


Default row count is 100000. If you want the article’s 10 million rows per table:

```bash
docker compose down -v
ROW_COUNT=10000000 docker compose up -d worker-1 worker-2 worker-3
```


#### Example 1: Simple concurrent queries

```bash
docker compose run --rm coordinator \
  --query "worker-1=SELECT sale_status, COUNT(*) AS count_star FROM sales GROUP BY sale_status ORDER BY sale_status" \
  --query "worker-2=SELECT country, COUNT(*) AS count_star FROM customers GROUP BY country ORDER BY country" \
  --query "worker-3=SELECT category, COUNT(*) AS count_star FROM products GROUP BY category ORDER BY category"
```


You should see output similar in spirit to the article:

```text
Concurrent remote queries
worker     table      start_offset_ms duration_seconds
---------- ---------- --------------- ------------------
worker-1   query-1              1.123              0.431
worker-3   query-3              0.976              0.442
worker-2   query-2              1.325              0.455

Start spread: 0.349 ms

query-1 (worker-1)
sale_status  count_star
----------   ----------
cancelled         20000
completed         20000
processing        20000
returned          20000
shipped           20000

query-2 (worker-2)
country  count_star
-------  ----------
AU            16666
CA            16667
DE            16667
FR            16666
UK            16667
US            16667

query-3 (worker-3)
category     count_star
----------   ----------
clothing          16667
electronics       16666
food              16666
garden            16667
home              16667
sports            16667
```

#### Example 2: More complex concurrent analytics

```bash
docker compose run --rm coordinator \
  --query "worker-1=WITH daily AS (SELECT sale_date, sales_channel, sale_status, COUNT(*) AS transaction_count, SUM(quantity) AS units, SUM(quantity * sold_unit_price) AS revenue FROM sales GROUP BY ALL) SELECT * FROM daily ORDER BY revenue DESC LIMIT 20" \
  --query "worker-2=WITH customer_groups AS (SELECT country, segment, membership_tier, COUNT(*) AS customer_count, AVG(credit_limit) AS average_credit_limit FROM customers GROUP BY ALL) SELECT * FROM customer_groups ORDER BY customer_count DESC LIMIT 20" \
  --query "worker-3=WITH inventory_groups AS (SELECT category, brand, supplier_region, COUNT(*) AS product_count, SUM(stock_quantity) AS stock_units, SUM(stock_quantity * catalogue_price) AS inventory_value FROM products GROUP BY ALL) SELECT * FROM inventory_groups ORDER BY inventory_value DESC LIMIT 20"
```

#### Example 3: Concurrent writes and reads

First clean up any previous test rows:

```bash
docker compose run --rm coordinator \
  --allow-write \
  --query "worker-1=DELETE FROM sales WHERE sale_id BETWEEN 30000001 AND 30000020"
```

Now run several inserts and reads concurrently.
This example uses 5 inserts and 3 reads. To reproduce the article exactly, add 15 more insert statements using the same pattern.

```bash
docker compose run --rm coordinator \
  --show-sql \
  --allow-write \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (30000001, 1, 1, 1, 'online', 'card', 'completed', DATE '2026-09-24', 10.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (30000002, 2, 2, 2, 'store', 'bank_transfer', 'processing', DATE '2026-09-24', 20.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (30000003, 3, 3, 3, 'marketplace', 'wallet', 'shipped', DATE '2026-09-24', 30.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (30000004, 4, 4, 4, 'telephone', 'invoice', 'returned', DATE '2026-09-24', 40.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (30000005, 5, 5, 5, 'online', 'card', 'cancelled', DATE '2026-09-24', 50.00, 0.00)" \
  --query "worker-1=SELECT COUNT(*) AS visible_rows FROM sales WHERE sale_id BETWEEN 30000001 AND 30000020" \
  --query "worker-1=SELECT COUNT(*) AS visible_rows FROM sales WHERE sale_id BETWEEN 30000001 AND 30000020" \
  --query "worker-1=SELECT COUNT(*) AS visible_rows FROM sales WHERE sale_id BETWEEN 30000001 AND 30000020"
```

#### Example 4: Concurrent DDL

Create aggregate tables concurrently:

```bash
docker compose run --rm coordinator \
  --allow-write \
  --query "worker-1=CREATE OR REPLACE TABLE sales_agg AS SELECT sale_status, COUNT(*) AS sale_count FROM sales GROUP BY sale_status" \
  --query "worker-2=CREATE OR REPLACE TABLE customers_agg AS SELECT country, COUNT(*) AS customer_count FROM customers GROUP BY country" \
  --query "worker-3=CREATE OR REPLACE TABLE products_agg AS SELECT category, COUNT(*) AS product_count FROM products GROUP BY category"
```

Then query them concurrently:

```bash
docker compose run --rm coordinator \
  --query "worker-1=SELECT * FROM sales_agg ORDER BY sale_count DESC" \
  --query "worker-2=SELECT * FROM customers_agg ORDER BY customer_count DESC" \
  --query "worker-3=SELECT * FROM products_agg ORDER BY product_count DESC"
```

#### Using SQL files instead of inline SQL

The coordinator also supports:
```bash
--query-file "worker-1=/queries/sales.sql"
```

If you want to use files, mount a host directory into the coordinator.
For example, add this to the coordinator service in docker-compose.yml:

```yaml

    volumes:
      - ./queries:/queries:ro

```

then run:

```bash
docker compose run --rm coordinator \
  --query-file "worker-1=/queries/sales.sql" \
  --query-file "worker-2=/queries/customers.sql" \
  --query-file "worker-3=/queries/products.sql"
```


#### Switching to real Quack client mode


set
.env
    SERVER_MODE=quack


restart:


```bash

docker compose down
docker compose up -d worker-1 worker-2 worker-3

```

In Quack mode, the coordinator executes remote SQL using the article’s pattern:


```sql
INSTALL quack;
LOAD quack;

ATTACH 'worker-1:9494' AS remote (
    TYPE quack,
    TOKEN 'local-dev-token',
    DISABLE_SSL true
);

SELECT * FROM remote.query('SELECT ...');
```

#### Cleanup

```bash
docker compose down
```

Remove containers and volumes:

```bash
docker compose down -v
```

What this project reproduces from the article

This Docker project reproduces the core behavior:
Three separate DuckDB databases.
One coordinator.
Multiple SQL fragments supplied on the command line.
Each fragment assigned to a specific worker.
Concurrent execution using threads and a barrier.
Start offset timing.
Duration timing.
Result collection on the coordinator.
Support for reads, writes, and DDL.
No AWS dependencies.
No CloudFormation.
No EC2.
No SSM Parameter Store.
No Lambda token manager.
It intentionally does not implement distributed query processing. Like the article, it is a coordinator that fires independent SQL statements at separate DuckDB servers and gathers the results.




## Quack pattern demo

change the SERVER_MODE to quack

```bash
docker compose build
```

Start the three workers:

```bash
docker compose up -d worker-1 worker-2 worker-3
```

watch logs:

```bash
docker compose logs -f worker-1
```

expected output

```text
Seeding worker 1 with 100000 rows into /data/worker.duckdb
Starting Quack server mode
Starting Quack server on 0.0.0.0:9494, db=/data/worker.duckdb
```

Check the other workers in separate terminals if needed:

```bash
docker compose logs -f worker-2
```

```bash
docker compose logs -f worker-3
```


#### Verify that the compose environment is in Quack mode

```bash

docker compose config | grep SERVER_MODE

```

output

```yaml
SERVER_MODE: quack
```

Run the demo script inside the coordinator container:

```bash
docker compose run --rm \
  --entrypoint python \
  coordinator \
  /app/quack_pattern_demo.py
```

This is the main demonstration that the coordinator is using the article’s Quack pattern.

You should see output similar to this:

```text
[query-1] worker-1: ATTACH 'worker-1:9494' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
[query-2] worker-2: ATTACH 'worker-2:9494' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
[query-3] worker-3: ATTACH 'worker-3:9494' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)

Concurrent remote queries using Quack article pattern
worker     table      start_offset_ms duration_seconds
---------- ---------- --------------- ------------------
worker-3   query-3              0.948              0.531
worker-1   query-1              1.133              0.542
worker-2   query-2              1.257              0.551

Start spread: 0.309 ms

query-1 (worker-1)
Coordinator-side Quack pattern:
ATTACH 'worker-1:9494' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
SELECT * FROM remote.query('SELECT sale_status, COUNT(*) AS count_star FROM sales GROUP BY sale_status ORDER BY sale_status')
Result:
sale_status  count_star
-----------  ----------
cancelled         20000
completed         20000
processing        20000
returned          20000
shipped           20000

query-2 (worker-2)
Coordinator-side Quack pattern:
ATTACH 'worker-2:9494' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
SELECT * FROM remote.query('SELECT country, COUNT(*) AS count_star FROM customers GROUP BY country ORDER BY country')
Result:
country  count_star
-------  ----------
AU            16666
CA            16667
DE            16667
FR            16666
UK            16667
US            16667

query-3 (worker-3)
Coordinator-side Quack pattern:
ATTACH 'worker-3:9494' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
SELECT * FROM remote.query('SELECT category, COUNT(*) AS count_star FROM products GROUP BY category ORDER BY category')
Result:
category     count_star
-----------  ----------
clothing          16667
electronics       16666
food              16666
garden            16667
home              16667
sports            16667
```


The key proof is this part of the output:

```text
ATTACH 'worker-1:9494' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
SELECT * FROM remote.query('...')

```

#### Test Quack mode using the original coordinator script

You can also test using the existing related_cluster_sql.py coordinator.
First run a simple connectivity test:

```bash
docker compose run --rm coordinator \
  --mode quack \
  --wait-timeout 900 \
  --query "worker-1=SELECT 1 AS worker_1_ok" \
  --query "worker-2=SELECT 2 AS worker_2_ok" \
  --query "worker-3=SELECT 3 AS worker_3_ok"
```

Expected result:

```text
query-1 (worker-1)
worker_1_ok
-----------
1

query-2 (worker-2)
worker_2_ok
-----------
2

query-3 (worker-3)
worker_3_ok
-----------
3
```


Then run the article-style simple concurrent query example:

```bash
docker compose run --rm coordinator \
  --mode quack \
  --wait-timeout 900 \
  --show-sql \
  --query "worker-1=SELECT sale_status, COUNT(*) AS count_star FROM sales GROUP BY sale_status ORDER BY sale_status" \
  --query "worker-2=SELECT country, COUNT(*) AS count_star FROM customers GROUP BY country ORDER BY country" \
  --query "worker-3=SELECT category, COUNT(*) AS count_star FROM products GROUP BY category ORDER BY category"
```

This runs three remote SQL statements concurrently through Quack and collects the results on the coordinator.


#### Test concurrent writes and reads in Quack mode

clean up test rows:

```bash
docker compose run --rm coordinator \
  --mode quack \
  --allow-write \
  --wait-timeout 900 \
  --query "worker-1=DELETE FROM sales WHERE sale_id BETWEEN 40000001 AND 40000005"
```

run five inserts and two reads concurrently:

```bash
docker compose run --rm coordinator \
  --mode quack \
  --allow-write \
  --show-sql \
  --wait-timeout 900 \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (40000001, 1, 1, 1, 'online', 'card', 'completed', DATE '2026-09-24', 10.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (40000002, 2, 2, 2, 'store', 'bank_transfer', 'processing', DATE '2026-09-24', 20.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (40000003, 3, 3, 3, 'marketplace', 'wallet', 'shipped', DATE '2026-09-24', 30.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (40000004, 4, 4, 4, 'telephone', 'invoice', 'returned', DATE '2026-09-24', 40.00, 0.00)" \
  --query "worker-1=INSERT INTO sales (sale_id, customer_id, product_id, quantity, sales_channel, payment_method, sale_status, sale_date, sold_unit_price, discount_pct) VALUES (40000005, 5, 5, 5, 'online', 'card', 'cancelled', DATE '2026-09-24', 50.00, 0.00)" \
  --query "worker-1=SELECT COUNT(*) AS visible_rows FROM sales WHERE sale_id BETWEEN 40000001 AND 40000005" \
  --query "worker-1=SELECT COUNT(*) AS visible_rows FROM sales WHERE sale_id BETWEEN 40000001 AND 40000005"
```

Depending on timing, the two reads may see different numbers of committed inserts.


random expected response

```text
visible_rows
------------
3
```

or 

```text
visible_rows
------------
5
```

#### Test concurrent DDL in Quack mode

Create aggregate tables concurrently:

```bash
docker compose run --rm coordinator \
  --mode quack \
  --allow-write \
  --wait-timeout 900 \
  --query "worker-1=CREATE OR REPLACE TABLE sales_agg AS SELECT sale_status, COUNT(*) AS sale_count FROM sales GROUP BY sale_status" \
  --query "worker-2=CREATE OR REPLACE TABLE customers_agg AS SELECT country, COUNT(*) AS customer_count FROM customers GROUP BY country" \
  --query "worker-3=CREATE OR REPLACE TABLE products_agg AS SELECT category, COUNT(*) AS product_count FROM products GROUP BY category"
```

Then query them concurrently:

```bash
docker compose run --rm coordinator \
  --mode quack \
  --wait-timeout 900 \
  --query "worker-1=SELECT * FROM sales_agg ORDER BY sale_count DESC" \
  --query "worker-2=SELECT * FROM customers_agg ORDER BY customer_count DESC" \
  --query "worker-3=SELECT * FROM products_agg ORDER BY product_count DESC"
```

#### What proves that this is using the article’s Quack pattern?

The demo script prints the actual coordinator-side SQL pattern.
For each query, you should see something like:

```text
ATTACH 'worker-1:9494' AS remote (TYPE quack, TOKEN 'REDACTED', DISABLE_SSL true)
SELECT * FROM remote.query('SELECT sale_status, COUNT(*) AS count_star FROM sales GROUP BY sale_status ORDER BY sale_status')
```

This matches the article’s pattern:

```python
with duckdb.connect() as connection:
    connection.execute("INSTALL quack")
    connection.execute("LOAD quack")

    connection.execute(
        f"ATTACH {endpoint} AS remote "
        f"(TYPE quack, TOKEN {token}, DISABLE_SSL true)"
    )

    cursor = connection.execute(
        f"SELECT * FROM remote.query({sql_string(sql)})"
    )
```

The coordinator is not executing the SQL locally. It is sending the SQL to the remote DuckDB worker through Quack and materializing the result on the coordinator.


#### Troubleshooting

If the demo fails, check the worker logs first:

```bash
docker compose logs worker-1
```

```bash
docker compose logs worker-2
```

```bash
docker compose logs worker-3
```

Look for:

```text
Starting Quack server mode

Starting Quack server on quack:0.0.0.0:9494, db=/data/worker.duckdb

```

If you see `Catalog Error: Table Function with name serve does not exist`, the script is
calling a Quack function that your DuckDB build does not provide. Since DuckDB v1.5.3 the
server entry point is the `quack_serve` table function, and it takes a `quack:` URI rather
than a database path:

```sql
CALL quack_serve('quack:0.0.0.0:9494', allow_other_hostname => true, token => 'local-dev-token');
```

`allow_other_hostname => true` is required under Docker. By default Quack refuses to bind
anything other than a local hostname, so without it the workers are unreachable from the
coordinator container.

`quack_serve` starts the listener on a background thread and returns immediately, so
`app/quack_server.py` keeps the process alive with a sleep loop afterwards.

To see which Quack functions your build actually exposes, run:

```sql
SELECT DISTINCT function_name
FROM duckdb_functions()
WHERE function_name ILIKE '%quack%'
ORDER BY 1;
```


Also verify:

docker compose config | grep SERVER_MODE

make sure SERVER_MODE: quack

If the coordinator times out while waiting for workers, increase the wait timeout:

```bash
docker compose run --rm coordinator \
  --mode quack \
  --wait-timeout 1800 \
  --query "worker-1=SELECT 1 AS ok"
```


#### Cleanup

```bash
docker compose down

docker compose down -v
```
