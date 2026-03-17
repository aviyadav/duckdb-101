# DuckDB SQL Agents Demo

This is a demonstration project showing how to use LLMs (like Google's Gemini) as "SQL Agents" that query structured DuckDB data. It includes a basic mechanism to request a natural language query, translate it into DuckDB SQL via an LLM, validate the safety of the generated SQL, and execute it against a mock dataset.

## Project Structure

- `generate_data.py`: A Python script containing mock data generation logic using Polars and Numpy. Generates parquet files for `orders`, `order_items`, `products`, and `refunds`.
- `query.sql`: A DuckDB SQL script demonstrating how to read those Parquet files directly using `read_parquet` and Common Table Expressions (CTEs). 
- `main.py`: The main program loop. It contains:
  - `agent_generate_sql`: A fallback/mock agent containing a static sample query.
  - `gemini_generate_sql`: A live integration with Google's Gemini model via the `google-genai` SDK to translate questions into DuckDB SQL.
  - `validate_sql`: A safety mechanism to prevent destructive queries (like INSERT, UPDATE, DROP) and enforce safe limits (LIMIT 200).
- `.env`: Environment variables file containing API keys.

## Prerequisites

This project uses [uv](https://github.com/astral-sh/uv) for fast Python package management.

Make sure you have `uv` installed. If you don't, you can install it via:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

You'll also need a Gemini API Key to use the live LLM integration. You can get one from [Google AI Studio](https://aistudio.google.com/).

## Installation

1. **Clone the repository** (or navigate to your local copy).
2. **Install dependencies** using `uv sync` (which uses `pyproject.toml`):
   ```bash
   uv sync
   ```
3. **Configure the environment**:
   - Create a `.env` file in the root directory if it does not already exist.
   - Add your Gemini API key:
     ```env
     GEMINI_API_KEY=your_actual_api_key_here
     ```

## Running the Project

### 1. Generate Mock Data
First, generate the Parquet files into the `data/` directory:
```bash
uv run python generate_data.py
```
This will populate ~1500 mock orders alongside items, products, and refunds.

### 2. Run the Main Agent App
Execute `main.py` using `uv`:
```bash
uv run main.py
```

By default, the script asks the LLM (or mock agent) to answer a business question (e.g., "Top 10 products by revenue last quarter"). The script will validate the query, execute it, and print the resulting rows.

## Customizing

You can modify the `question` variable inside `main.py` -> `main()` to query the LLM about anything else supported by the attached `schema_info`. If you wish to toggle the live LLM off for testing, you can comment out the `proposal = gemini_generate_sql(question)` line and uncomment the `agent_generate_sql` fallback.
