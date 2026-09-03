# DuckDB QUACK Protocol Demo

This folder contains a minimal demo of the **QUACK** protocol in DuckDB — a protocol that lets one DuckDB instance attach to and query another DuckDB instance over the network, as if the remote database were local. In this example we use `localhost`, but the server and client could just as easily run on entirely different servers / machines.

## Files

| File         | Role                                                                    |
| ------------ | ----------------------------------------------------------------------- |
| `server.sql` | Starts the QUACK server and creates a demo table                        |
| `client.sql` | Connects to the server as a client, authenticates, and queries the data |

## Step-by-step guide

### Prerequisites

- DuckDB CLI available as `duckdb` (install the `quack` extension with `INSTALL quack` if prompted).

### 1. Start the server (Terminal 1)

Open DuckDB in the first terminal:

```sh
duckdb
```

In the DuckDB CLI, start the QUACK server:

```sql
CALL quack_serve(
    'quack:localhost',
    token = 'super_secret'
);
```

This starts the QUACK server on the given host (`localhost` here). It is confirmed by the output:

```
┌─────────────────┬───────────────────────┬──────────────┐
│   listen_uri    │      listen_url       │  auth_token  │
│     varchar     │        varchar        │   varchar    │
├─────────────────┼───────────────────────┼──────────────┤
│ quack:localhost │ http://localhost:9494 │ super_secret │
└─────────────────┴───────────────────────┴──────────────┘
```

Then create any table — with static data or loaded from files. For example:

```sql
CREATE TABLE hello AS
    FROM VALUES ('world') v(s);
```

Leave this terminal open so the server keeps running.

### 2. Start the client (Terminal 2)

In a **second terminal**, open another DuckDB session:

```sh
duckdb
```

Create a secret of type `quack` with the token the server expects:

```sql
CREATE SECRET (
    TYPE quack,
    TOKEN 'super_secret'
);
```

### 3. Attach the remote database

Attach the DuckDB instance running in the first terminal:

```sql
ATTACH 'quack:localhost' AS remote;
```

### 4. Query the remote data

Now you can query the database created in the first terminal from here:

```sql
FROM remote.hello;
```

or

```sql
SELECT * FROM remote.hello;
```

You should see the remote data returned:

```
┌─────────┐
│    s    │
│ varchar │
├─────────┤
│ world   │
└─────────┘
```

### Running across machines

We used `localhost` in this demo, but the server and client do not have to be on the same machine. Point the client at the server's hostname or IP instead:

- Server: `CALL quack_serve('quack:0.0.0.0', token = 'super_secret');` (or the server's reachable host)
- Client: `ATTACH 'quack:<server-host-or-ip>' AS remote;`

The same auth token must be used on both sides.
