import re

with open('benchmark_sqlite.py', 'r') as f:
    text = f.read()

# 1. Update docstring
text = text.replace('MySQL vs Parquet', 'SQLite vs Parquet')
text = text.replace('results_mysql.txt', 'results_sqlite.txt')
text = text.replace('raw MySQL query', 'raw SQLite query')
text = text.replace('keep MySQL database', 'keep SQLite database')

# 2. Config
mysql_conf = """MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")"""
sqlite_conf = 'SQLITE_DB_PATH = os.path.join(DATA_DIR, f"{DB_NAME}.db")'
text = text.replace(mysql_conf, sqlite_conf)

# 3. File paths
text = text.replace('MYSQL_RESULTS_FILE = os.path.join(SCRIPT_DIR, "results_mysql.txt")',
                    'SQLITE_RESULTS_FILE = os.path.join(SCRIPT_DIR, "results_sqlite.txt")')
text = text.replace('MYSQL_RESULTS_FILE', 'SQLITE_RESULTS_FILE')

# 4. load function
old_load = """def load_mysql(rows):
    \"\"\"Create the MySQL table and bulk-insert all rows.
    Args:
        rows: The generated data rows.
    \"\"\"
    print("\\n=== Step 2: Loading into MySQL ===\\n")
    import mysql.connector

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=True,
    )
    cursor = conn.cursor()

    cursor.execute(f"DROP DATABASE IF EXISTS `{DB_NAME}`")
    cursor.execute(f"CREATE DATABASE `{DB_NAME}`")
    conn.database = DB_NAME

    cursor.execute(\"\"\"
        CREATE TABLE ad_insights (
            id BIGINT NOT NULL,
            client_id INT NOT NULL,
            channel_id INT NOT NULL,
            ad_account_id VARCHAR(64) NOT NULL,
            campaign_id VARCHAR(128) NOT NULL,
            campaign_name VARCHAR(256) NOT NULL,
            ad_id VARCHAR(128) NOT NULL,
            ad_name VARCHAR(256) NOT NULL,
            impressions BIGINT NOT NULL,
            clicks BIGINT NOT NULL,
            spend DOUBLE NOT NULL,
            conversions INT NOT NULL,
            date DATE NOT NULL,
            PRIMARY KEY (id),
            INDEX idx_client_channel_date (client_id, channel_id, date),
            INDEX idx_client_date (client_id, date)
        )
    \"\"\")

    t0 = time.perf_counter()
    batch_size = 10_000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(
            \"\"\"
            INSERT INTO ad_insights
            (id, client_id, channel_id, ad_account_id, campaign_id, campaign_name,
             ad_id, ad_name, impressions, clicks, spend, conversions, date)
            VALUES (%(id)s, %(client_id)s, %(channel_id)s, %(ad_account_id)s,
                    %(campaign_id)s, %(campaign_name)s, %(ad_id)s, %(ad_name)s,
                    %(impressions)s, %(clicks)s, %(spend)s, %(conversions)s, %(date)s)
            \"\"\",
            batch,
        )
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"MySQL load: {elapsed_ms:,.0f} ms ({len(rows):,} rows)\\n")
    cursor.close()
    conn.close()"""

new_load = """def load_sqlite(rows):
    \"\"\"Create the SQLite table and bulk-insert all rows.
    Args:
        rows: The generated data rows.
    \"\"\"
    print("\\n=== Step 2: Loading into SQLite ===\\n")
    import sqlite3
    
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(SQLITE_DB_PATH):
        os.remove(SQLITE_DB_PATH)

    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    cursor.execute(\"\"\"
        CREATE TABLE ad_insights (
            id BIGINT NOT NULL PRIMARY KEY,
            client_id INT NOT NULL,
            channel_id INT NOT NULL,
            ad_account_id VARCHAR(64) NOT NULL,
            campaign_id VARCHAR(128) NOT NULL,
            campaign_name VARCHAR(256) NOT NULL,
            ad_id VARCHAR(128) NOT NULL,
            ad_name VARCHAR(256) NOT NULL,
            impressions BIGINT NOT NULL,
            clicks BIGINT NOT NULL,
            spend DOUBLE NOT NULL,
            conversions INT NOT NULL,
            date DATE NOT NULL
        )
    \"\"\")
    cursor.execute("CREATE INDEX idx_client_channel_date ON ad_insights(client_id, channel_id, date)")
    cursor.execute("CREATE INDEX idx_client_date ON ad_insights(client_id, date)")

    t0 = time.perf_counter()
    batch_size = 10_000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(
            \"\"\"
            INSERT INTO ad_insights
            (id, client_id, channel_id, ad_account_id, campaign_id, campaign_name,
             ad_id, ad_name, impressions, clicks, spend, conversions, date)
            VALUES (:id, :client_id, :channel_id, :ad_account_id,
                    :campaign_id, :campaign_name, :ad_id, :ad_name,
                    :impressions, :clicks, :spend, :conversions, :date)
            \"\"\",
            batch,
        )
    conn.commit()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"SQLite load: {elapsed_ms:,.0f} ms ({len(rows):,} rows)\\n")
    cursor.close()
    conn.close()"""
text = text.replace(old_load, new_load)

text = text.replace('load_mysql(rows)', 'load_sqlite(rows)')

# 5. Helper queries
text = text.replace('run_mysql_query', 'run_sqlite_query')
text = text.replace('time_query_mysql', 'time_query_sqlite')

text = text.replace('MySQL query and return', 'SQLite query and return')
text = text.replace('MySQL cursor', 'SQLite cursor')

# 6. Definition replacements
text = re.sub(r'(\s+)"mysql":', r'\1"sqlite":', text)

# 7. MONTH(date) -> CAST(strftime('%m', date) AS INTEGER) for SQLITE entries.
# Since duckdb queries also have MONTH(date), we shouldn't change them globally if not needed...
# But actually DuckDB supports MONTH(date) and strftime. We can just replace MONTH(date) globally, but maybe just for sqlite part.
import ast
import json

# Instead of complex parsing, since formatting is consistent query-by-query let's try replacing MONTH(date) everywhere. DuckDB supports strftime, maybe? Wait, duckdb has `month(date)`. Let's just do a regex that finds the "sqlite": """ ... """ and replaces it.
def repl_sqlite_month(m):
    return m.group(0).replace('MONTH(date)', "CAST(strftime('%m', date) AS INTEGER)")

text = re.sub(r'"sqlite": """.*?"""', repl_sqlite_month, text, flags=re.DOTALL)

# 8. run_benchmarks logic setup
old_run = """    import duckdb
    import mysql.connector

    mysql_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=DB_NAME,
    )
    mysql_cursor = mysql_conn.cursor()"""
new_run = """    import duckdb
    import sqlite3

    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_cursor = sqlite_conn.cursor()"""

text = text.replace(old_run, new_run)
text = text.replace('mysql_cursor, q["mysql"]', 'sqlite_cursor, q["sqlite"]')
text = text.replace('ms_mysql, rows_mysql =', 'ms_sqlite, rows_sqlite =')
text = text.replace('ms_mysql / ms_duck', 'ms_sqlite / ms_duck')
text = text.replace('MySQL {ms_mysql', 'SQLite {ms_sqlite')
text = text.replace('"ms_mysql": ms_mysql', '"ms_sqlite": ms_sqlite')
text = text.replace('"rows_mysql": rows_mysql', '"rows_sqlite": rows_sqlite')

text = text.replace('mysql_cursor.close()', 'sqlite_cursor.close()')
text = text.replace('mysql_conn.close()', 'sqlite_conn.close()')

# 9. write results
text = text.replace('write_single_engine_results(MYSQL_RESULTS_FILE, "MySQL", bench_results)',
                    'write_single_engine_results(SQLITE_RESULTS_FILE, "SQLite", bench_results)')
text = text.replace('MySQL results  -> {MYSQL_RESULTS', 'SQLite results -> {SQLITE_RESULTS')

text = text.replace('if engine_name == "MySQL"', 'if engine_name == "SQLite"')
text = text.replace('rows_mysql', 'rows_sqlite')
text = text.replace('ms_mysql', 'ms_sqlite')
text = text.replace('MySQL (ms)', 'SQLite (ms)')
text = text.replace('MySQL ', 'SQLite ')
text = text.replace('MySQL:', 'SQLite:')
text = text.replace('>>> MySQL Result', '>>> SQLite Result')

# 10. printing results
text = text.replace('total_mysql', 'total_sqlite')

# 11. Cleanup
old_cleanup = """    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            autocommit=True,
        )
        conn.cursor().execute(f"DROP DATABASE IF EXISTS `{DB_NAME}`")
        conn.close()
        print(f"Dropped SQLite database `{DB_NAME}`.")
    except Exception as e:
        print(f"SQLite cleanup warning: {e}")"""
        
# Because my text already had 'MySQL' replaced with 'SQLite' in some places, let's be careful.
# Actually I haven't done bulk MySQL -> SQLite yet.

# Let's cleanly replace cleanup first:
# I will use regex for cleanup
cleanup_regex = r'def cleanup\(\):.*?(?=def main\(\):)'
new_cleanup = """def cleanup():
    \"\"\"Remove the SQLite database and Parquet data directory.\"\"\"
    if SKIP_CLEANUP:
        print("Skipping cleanup (SKIP_CLEANUP=1).\\n")
        return
    print("\\n=== Cleanup ===\\n")
    if os.path.exists(SQLITE_DB_PATH):
        os.remove(SQLITE_DB_PATH)
        print(f"Removed SQLite database at {SQLITE_DB_PATH}.")
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
        print(f"Removed {DATA_DIR}\\n")


"""
text = re.sub(cleanup_regex, new_cleanup, text, flags=re.DOTALL)

with open('benchmark_sqlite.py', 'w') as f:
    f.write(text)

