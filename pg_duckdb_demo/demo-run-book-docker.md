Site for data - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Download a parquet file to the data folder
```
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-09.parquet
mv yellow_tripdata_2026-09.parquet data
```

Run the docker instance withe pg_duckdb extension available, map the local data folder to the postgresql's machine

```bash
docker run -d --name pgduckdb -e POSTGRES_PASSWORD=password -p 5432:5432 -v "./data:/data:ro"  pgduckdb/pgduckdb:18-main
```

Check the postgresql instance

```bash
docker ps
```

Log into the postgresql using the psql cli:

```bash
docker exec -ti pgduckdb psql -U postgres
```

logged in -

```text
postgres=#
```

create pg_duckdb extension - in postgresql

```sql
CREATE EXTENSION IF NOT EXISTS pg_duckdb;
```

Verify if the data folder was added and the parquet file is available in the container:


```bash
docker exec -ti pgduckdb ls /

docker exec -ti pgduckdb ls /data
```


```sql

create table yellow_taxi_2026_09 as 
select * from read_parquet('/data/yellow_tripdata_2026-09.parquet');


select * from public.yellow_taxi_2026_09 limit 100;


select count(*) from public.yellow_taxi_2026_09;

select AVG("trip_distance") from public.yellow_taxi_2026_09;


use duckdb engine

SET duckdb.force_execution = false; --disable duckdb engine

SET duckdb.force_execution = true; --enable duckdb engine

select AVG("trip_distance") from public.yellow_taxi_2026_09; -- duckdb vectorized engine

select AVG(r['trip_distance']) from read_parquet('/data/yellow_tripdata_2026-09.parquet') r;



Cleanup

```bash
   # Stop and remove the container
   docker rm -f pgduckdb

   # Remove the image
   docker rmi pgduckdb/pgduckdb:18-main
 ```
