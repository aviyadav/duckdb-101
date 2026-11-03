Your setup becomes:

 ```text
   PostgreSQL + pg_duckdb container
             │ S3 API requests
             ▼
   RustFS running in openSUSE WSL
             ▼
   Bucket / files
 ```

 ### 1. Find RustFS’s S3 endpoint

 Your URL on port 9001 is the management console, not the S3 API. RustFS normally serves its S3 API on port 9000—check your RustFS configuration.

 From PowerShell, find the openSUSE WSL IP:

 ```powershell
   # Find the exact distro name
   wsl -l -v

   # Get its IP address
   wsl -d openSUSE-Tumbleweed -- hostname -I
 ```

 For example, suppose it returns 172.25.100.20. Your endpoint would be:

 ```text
   172.25.100.20:9000
 ```

 Do not use localhost:9000 inside the PostgreSQL container: that refers to the container itself, not your openSUSE distro.

 RustFS must listen on a reachable interface, such as 0.0.0.0:9000, rather than only 127.0.0.1:9000. Firewall rules must also allow the connection. Reachability between WSLc and
 your distro depends on your WSL networking setup.

 ### 2. Run PostgreSQL without the local-file mount

 Using WSLc:

 ```powershell
   wslc run -d --name pgduckdb -e POSTGRES_PASSWORD=password -p 5432:5432 pgduckdb/pgduckdb:18-main
 ```

 If the pgduckdb container already exists, use it—you don’t need to recreate it just to access RustFS.

 Open its SQL shell:

 ```powershell
   wslc exec -it pgduckdb psql -U postgres
 ```

 ### 3. Configure RustFS credentials in PostgreSQL

 Run this SQL, replacing the IP, access key, secret key, and bucket name:

 ```sql
   CREATE EXTENSION IF NOT EXISTS pg_duckdb;

   SELECT duckdb.create_simple_secret(
       type      := 'S3',
       key_id    := 'YOUR_RUSTFS_ACCESS_KEY',
       secret    := 'YOUR_RUSTFS_SECRET_KEY',
       region    := 'us-east-1',
       endpoint  := '172.25.100.20:9000',
       url_style := 'path',
       use_ssl   := 'false',
       scope     := 's3://my-bucket/'
   );
 ```

 Important details:

 - endpoint: host and port only, without http:// or the console path.
 - url_style := 'path': uses path-style bucket addressing.
 - use_ssl := 'false': for your local HTTP endpoint; use 'true' for HTTPS.
 - region: use the region configured in RustFS, commonly us-east-1.
 - Prefer a read-only access key with list/read permissions for the bucket.

 ### 4. Query files directly

 For an object named sales/orders.parquet in my-bucket:

 ```sql
   SELECT *
   FROM read_parquet('s3://my-bucket/sales/orders.parquet')
   LIMIT 10;
 ```

 Read multiple Parquet files:

 ```sql
   SELECT *
   FROM read_parquet('s3://my-bucket/sales/*.parquet')
   LIMIT 10;
 ```

 In short:

 ┌─────────────────────────────────┬──────────────────────────────────────────┐
 │ Local folder                    │ RustFS bucket                            │
 ├─────────────────────────────────┼──────────────────────────────────────────┤
 │ Mount with -v "./data:/data:ro" │ Configure an S3 endpoint and credentials │
 ├─────────────────────────────────┼──────────────────────────────────────────┤
 │ Query /data/orders.parquet      │ Query s3://my-bucket/orders.parquet      │
 └─────────────────────────────────┴──────────────────────────────────────────┘

 Avoid bind-mounting RustFS’s internal storage directory—it is managed by RustFS and is not intended to be accessed as an ordinary folder of objects.

 Note: A WSL distro IP may change after restarting WSL, so you may need to update the endpoint.

 pg_duckdb S3 credential documentation