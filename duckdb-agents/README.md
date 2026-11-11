# duckdbAgents

A Claude-powered data analyst agent that answers natural-language questions by querying a read-only DuckDB database of the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## How the agent works with DuckDB

The agent connects to any DuckDB file in **read-only** mode and never writes to it. It runs a standard tool-use loop:

1. You ask a question in plain English.
2. Claude decides which tools to call:
   | Tool | What it does |
   | --- | --- |
   | `list_tables` | Lists every table in the database |
   | `describe_table` | Column names and types for one table |
   | `run_sql` | Runs ONE read-only `SELECT` on DuckDB |
3. `run_sql` is guarded: results are capped at 20 rows (the agent is told to aggregate instead of paging), queries time out after 15 seconds, and only `SELECT` statements are allowed.
4. The system prompt teaches the agent the Olist gotchas: `customer_unique_id` vs `customer_id`, the multi-row `order_items`/`order_payments` double-join trap, LEFT JOIN for optional reviews, the many-rows-per-zip `geolocation` table, and "revenue = delivered orders only" by default.
5. After 3 SQL errors in a row the agent is nudged to stop and ask a clarifying question instead of thrashing.
6. Every step (question, model calls, tool calls, final answer) is logged as JSON lines to `logs/agent.jsonl`.

## Setup

```bash
uv sync
```

Create a `.env` file:

```
ANTHROPIC_API_KEY=...
```

Build the database either from CSVs in `data/`:

```bash
uv run build_db.py
```

or straight from Kaggle (no local copy needed):

```bash
uv run build_db_kaggle.py
```

This uses `kagglehub`, which downloads the dataset once to `~/.cache/kagglehub` and reuses it on later runs.

## Usage

Ask a question:

```bash
uv run agent.py olist.duckdb "Which product categories have the highest revenue?"
```

## Example questions and answers

All answers verified against the database (rows in the underlying tables):

| Question | Answer |
| --- | --- |
| `uv run agent.py olist.duckdb "How many orders arrived late?"` | 7,826 delivered orders arrived after their estimated delivery date |
| `uv run agent.py olist.duckdb "Which product category has the most items sold?"` | `cama_mesa_banho` (bed/table/bath) with 11,115 items — next are `beleza_saude` (9,670) and `esporte_lazer` (8,641) |
| `uv run agent.py olist.duckdb "What is the total revenue from delivered orders?"` | R$ 15,419,773.75 (price + freight, delivered orders only) |
| `uv run agent.py olist.duckdb "How many orders were placed in 2018?"` | 54,011 orders |
| `uv run agent.py olist.duckdb "What is the average review score, and how many reviews is it based on?"` | 4.09 based on 99,224 reviews |
| `uv run agent.py olist.duckdb "Which payment type is used most often?"` | `credit_card` (76,795 payments) |
| `uv run agent.py olist.duckdb "Which city has the most customers?"` | São Paulo with 15,540 customers |

## Eval

Run the evals (optionally pass a case number to run one):

```bash
uv run eval.py olist.duckdb [case_number]
```

The 10 cases and their expected answers:

| # | Case | Question | Expected result |
| --- | --- | --- | --- |
| 1 | `order_count` | How many orders are there? | 99,441 |
| 2 | `distinct_customers` | How many distinct customers are there? | 96,096 unique `customer_unique_id` |
| 3 | `payment_by_status` | Total payment value per order status | Sum of `payment_value` grouped by `order_status` |
| 4 | `orders_without_review` | How many orders have no review at all? | 768 orders |
| 5 | `late_deliveries` | How many delivered orders arrived later than estimated? | 7,826 |
| 6 | `revenue_delivered` | Total revenue (price + freight) of delivered orders | 15,419,773.75 |
| 7 | `orders_2018` | How many orders were placed in 2018? | 54,011 |
| 8 | `avg_review_score` | What is the average review score? | 4.086 |
| 9 | `top_city` | Which city has the most customers? | `sao paulo` (15,540) |
| 10 | `top_payment_type` | Which payment type is used most often? | `credit_card` (76,795) |

Numbers compare with a small tolerance, extra columns in the agent's rows are fine, and the agent passes if any of its queries matches — so sanity-check queries don't cause failures.

## Files

| File | Purpose |
| --- | --- |
| `agent.py` | Agent loop, system prompt, JSON-lines logging to `logs/` |
| `tools.py` | `list_tables`, `describe_table`, `run_sql` (row cap and timeout) |
| `build_db.py` | Loads local CSVs into `olist.duckdb` |
| `build_db_kaggle.py` | Downloads the dataset from Kaggle and loads it into `olist.duckdb` |
| `eval.py` | Compares agent answers against reference SQL |
