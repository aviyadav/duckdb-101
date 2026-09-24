# Host-side clients

Scripts for driving the Dockerised DuckDB workers **from your local Linux shell**
instead of from inside the coordinator container.

| File | Mode | Dependencies |
| --- | --- | --- |
| `http_client.py` | `SERVER_MODE=http` | none — standard library only |
| `quack_client.py` | `SERVER_MODE=quack` | `duckdb==1.5.5` (must match the image) |
| `requirements.txt` | Quack mode only | `pip install -r local/requirements.txt` |
| `.env.local` | both | `source local/.env.local` |

For the full step-by-step sequence, see [`../run-book-from-local.md`](../run-book-from-local.md).

## The one rule

**The workers choose the protocol, not your script.** `SERVER_MODE` in the project-root `.env`
decides which server process runs inside each container, and only one runs at a time:

- `SERVER_MODE=http` → `localhost:9491/query` speaks JSON. Quack is not listening.
- `SERVER_MODE=quack` → `localhost:9491` speaks Quack. There is no `/health` or `/query`.

Check before you start:

```bash
docker compose config | grep SERVER_MODE
```

## Published ports

| Worker | Host endpoint | Container port | Table |
| --- | --- | --- | --- |
| `worker-1` | `localhost:9491` | 9494 | `sales` |
| `worker-2` | `localhost:9492` | 9494 | `customers` |
| `worker-3` | `localhost:9493` | 9494 | `products` |

Both servers bind `0.0.0.0` inside their container, so no compose changes are needed to reach
them from the host.

## Quick start — HTTP mode

```bash
source local/.env.local

python local/http_client.py --health
python local/http_client.py --query "worker-1=SELECT COUNT(*) AS n FROM sales"
```

## Quick start — Quack mode

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r local/requirements.txt
source local/.env.local

python local/quack_client.py --check
python local/quack_client.py --query "worker-1=SELECT COUNT(*) AS n FROM sales"
```

## Using them as libraries

```python
import sys

sys.path.insert(0, "local")

from http_client import HttpClient          # or: from quack_client import QuackClient

print(HttpClient("worker-1").query("SELECT sale_status, COUNT(*) FROM sales GROUP BY 1"))
```

`quack_client.QuackClient` gives you something HTTP mode cannot: a real remote catalog. Once
attached, remote tables behave like local ones and transactions are forwarded to the server.

```python
with QuackClient() as client:
    client.attach_all()                                        # w1, w2, w3
    client.fetch("SELECT COUNT(*) FROM w1.sales")              # remote table
    client.fetch("FROM w1.query('SELECT COUNT(*) FROM sales')")  # pushed-down SQL
```

## What does not work

Do not open the `.duckdb` files directly from the host. The data lives in Docker **named**
volumes, which on Docker Desktop / WSL sit inside the VM rather than at a usable host path —
and in Quack mode `quack_server.py` holds a persistent read-write connection, so a second
process gets a lock conflict. Use the network protocol; that is what the published ports are for.
