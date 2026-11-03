 ### Run PostgreSQL + DuckDB

 In PowerShell, from your project folder:

 ```powershell
   # Stop the Docker instance first, if still running, to free port 5432
   docker stop pgduckdb

   # Run the same image using WSL Containers
   wslc run -d --name pgduckdb -e POSTGRES_PASSWORD=password -p 5432:5432 -v "./data:/data:ro" pgduckdb/pgduckdb:18-main
 ```

 If the relative bind-mount path causes an error, use an absolute path:

 ```powershell
   wslc run -d --name pgduckdb -e POSTGRES_PASSWORD=password -p 5432:5432 -v "C:/Users/avina/codebase/db-world/pg_duckdb_demo/data:/data:ro" pgduckdb/pgduckdb:18-main
 ```

 ### Check and connect

 ```powershell
   # Check startup logs
   wslc logs pgduckdb

   # Open PostgreSQL's SQL shell
   wslc exec -it pgduckdb psql -U postgres
 ```

 Your application can continue using localhost:5432, username postgres, password password.

 ### Stop, restart, or remove

 ```powershell
   # Stop without deleting
   wslc stop pgduckdb

   # Start again
   wslc start pgduckdb

   # Stop and remove the container
   wslc stop pgduckdb
   wslc rm pgduckdb

   # Remove the image
   wslc image rm pgduckdb/pgduckdb:18-main
 ```

 Important: Docker and WSLc have separate storage. This starts a fresh database, not your existing Docker database. Your bind-mounted ./data files remain unchanged.

 WSLc has Docker-like commands, but not complete Docker feature parity—for example, Compose and restart-policy support depend on the version.