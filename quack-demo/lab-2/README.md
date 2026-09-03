# Lab 2 — Federated DuckDB with QUACK and Docker

Lab 2 builds on the single-server demo from Lab 1. Here we run **multiple DuckDB servers**, each holding its own local data, using **Docker**. A **coordinator** DuckDB instance communicates with all servers over the **QUACK** protocol, attaches to each of them, and exposes a federated view of their data. Your **local DuckDB client** then connects to the coordinator and can query data from *all* servers through it — without ever talking to the shards directly.

## Architecture

```
┌──────────────┐   ┌──────────────┐
│   server1    │   │   server2    │
│  (east shard)│   │ (west shard) │
│  port 9491   │   │  port 9492   │
└──────▲───────┘   └──────▲───────┘
       │ quack            │ quack
       └────────┬─────────┘
                │
        ┌───────┴────────┐
        │  coordinator   │
        │   port 9493    │
        └───────▲────────┘
                │ quack
        ┌───────┴────────┐
        │  local client  │
        │  (your duckdb) │
        └────────────────┘
```

## Files

| File                | Role                                                                                     |
| ------------------- | ---------------------------------------------------------------------------------------- |
| `Dockerfile`        | Image based on `duckdb/duckdb:1.5.3` with the SQL scripts and entrypoint                 |
| `entrypoint.sh`     | Starts DuckDB with the SQL script for the container's role (`QUACK_ROLE`)                |
| `docker-compose.yml`| Defines `server1`, `server2` and `coordinator` services, ports, tokens and healthchecks  |
| `sql/server1.sql`   | Creates the `east` shard data, identifies itself, and serves QUACK on port 9494          |
| `sql/server2.sql`   | Creates the `west` shard data, identifies itself, and serves QUACK on port 9494          |
| `sql/coordinator.sql`| Attaches to both servers, builds the federated `all_orders` view, and serves QUACK itself |
| `client.sql`        | Local client: attaches to the coordinator and queries all servers through it             |

## How it works

**Shard servers (`sql/server1.sql`, `sql/server2.sql`)**

1. Each server creates a local `orders` table with static data (server1 = `east` region, server2 = `west` region).
2. `CALL quack_identify(...)` gives each server a name, region and metadata so clients can discover who they are talking to.
3. `CALL quack_serve('quack:0.0.0.0:9494', token = ..., allow_other_hostname => true)` starts the QUACK server inside the container. Each shard has its own token (`server1_secret`, `server2_secret`).

**Coordinator (`sql/coordinator.sql`)**

1. Creates one `quack` secret per shard, scoped to that shard's URI (`quack:server1:9494`, `quack:server2:9494`) with the matching token.
2. Attaches to both shards: `ATTACH 'quack:server1:9494' AS server1 (TYPE quack, DISABLE_SSL true);` (same for `server2`).
3. Builds a federated view `all_orders` that `UNION ALL`s both shards' `orders` tables, tagging each row with its `source`.
4. Identifies itself via `quack_identify` and starts its own QUACK server on `quack:0.0.0.0:9494` with token `coord_secret` — so the coordinator is itself a QUACK server that local clients can attach to.

**Client (`client.sql`)**

1. Creates a scoped secret for the coordinator (`quack:localhost:9493`, token `coord_secret`).
2. Attaches to the coordinator: `ATTACH 'quack:localhost:9493' AS coord (TYPE quack, DISABLE_SSL true);`
3. Queries the federated view and individual shards through the coordinator session using `coord.query(...)`.

## Step-by-step guide

### Prerequisites

- Docker (with Docker Compose).
- DuckDB CLI available locally as `duckdb` (install the `quack` extension with `INSTALL quack` if prompted).

### 1. Start the cluster

From the `lab-2` folder, build and start all three containers:

```sh
docker compose up --build -d
```

This starts:

| Service       | Role                        | Host port |
| ------------- | --------------------------- | --------- |
| `server1`     | East shard (`orders`)       | `9491`    |
| `server2`     | West shard (`orders`)       | `9492`    |
| `coordinator` | Attaches to both shards     | `9493`    |

The coordinator waits for both shards to become healthy (via `entrypoint.sh` and the compose healthchecks) before attaching to them and starting its own QUACK server.

Check that everything is up:

```sh
docker compose ps
```

All three services should show `healthy`.

### 2. Connect a local DuckDB client

In a local terminal, open DuckDB:

```sh
duckdb
```

Run the client script:

```sql
.read client.sql
```

Or run the steps manually:

Create a secret for the coordinator:

```sql
CREATE SECRET coordinator_credentials (
    TYPE quack,
    SCOPE 'quack:localhost:9493',
    TOKEN 'coord_secret'
);
```

Attach to the coordinator:

```sql
ATTACH 'quack:localhost:9493' AS coord (TYPE quack, DISABLE_SSL true);
```

### 3. Query data from all servers via the coordinator

The federated view assembled by the coordinator combines both shards:

```sql
FROM coord.all_orders ORDER BY id;
```

You should see rows from both servers, tagged with their source:

```
┌───────┬─────────┬──────────┬────────┬─────────┐
│  id   │ region  │ customer │ amount │ source  │
│ int32 │ varchar │ varchar  │ int32  │ varchar │
├───────┼─────────┼──────────┼────────┼─────────┤
│ 1     │ east    │ alice    │ 100    │ server1 │
│ 2     │ east    │ bob      │ 200    │ server1 │
│ 3     │ east    │ carol    │ 150    │ server1 │
│ 4     │ east    │ dave     │ 250    │ server1 │
│ 5     │ east    │ eve      │ 180    │ server1 │
│ 11    │ west    │ carol    │ 150    │ server2 │
│ 12    │ west    │ dave     │ 250    │ server2 │
│ 13    │ west    │ eve      │ 180    │ server2 │
│ 14    │ west    │ frank    │ 200    │ server2 │
│ 15    │ west    │ george   │ 160    │ server2 │
└───────┴─────────┴──────────┴────────┴─────────┘
```

### 4. Explore further

Find out who you are talking to:

```sql
FROM coord.query('FROM whoami()');
```

Reach individual shards through the coordinator session:

```sql
FROM coord.query('FROM server1.orders ORDER BY id');
FROM coord.query('FROM server2.orders ORDER BY id');
```

Run arbitrary federated queries, e.g. totals per region across all servers:

```sql
FROM coord.query("
    SELECT region, sum(amount) AS total
    FROM all_orders
    GROUP BY region ORDER BY region
");
```

### 5. Tear down

```sh
docker compose down
```

## Notes

- Inside the Docker network, every container serves QUACK on port `9494`; the compose file maps them to host ports `9491` (server1), `9492` (server2) and `9493` (coordinator).
- `DISABLE_SSL true` is used because the containers run plain HTTP inside the local Docker network.
- Each hop is authenticated with its own token: the coordinator holds the shard tokens (`server1_secret`, `server2_secret`), and the local client holds the coordinator token (`coord_secret`).
- The client only ever needs to reach the coordinator — the shards can stay on a private network.
