import os
import json
import duckdb
import re
from dataclasses import dataclass
from tabulate import tabulate
import google.genai as genai
from dotenv import load_dotenv

# --- 1) Define a strict contract for the agent output ---
@dataclass
class SqlProposal:
    sql: str
    rationale: str
    expected_columns: list[str]

# --- 2) Guardrails: allowlist + denylist ---
ALLOWED_TABLES = {"orders", "order_items", "products", "refunds"}
DENY_KEYWORDS = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|COPY)\b", re.IGNORECASE)

def validate_sql(sql: str) -> str:
    if DENY_KEYWORDS.search(sql):
        raise ValueError("Blocked: non-SELECT operation detected")

    # no multi-statement
    if ";" in sql.strip().rstrip(";"):
        raise ValueError("Blocked: multi-statement SQL not allowed")
    
    # crude table reference check (good enough to start: harden later)
    mentioned = set(re.findall(r"\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)|\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, re.IGNORECASE))
    flat = {t for pair in mentioned for t in pair if t}
    unknown = {t.lower() for t in flat} - ALLOWED_TABLES
    if unknown:
        raise ValueError(f"Blocked: unknown tables referenced: {sorted(unknown)}")
        
    # enforce LIMIT if missing
    if re.search(r"\blimit\b", sql, re.IGNORECASE) is None:
        sql = sql.rstrip() + "\nLIMIT 200"
    
    return sql

# --- 3) Example "agent" {replace with a real LLM call}
def agent_generate_sql(question: str) -> SqlProposal:
    # sql = """
    # SELECT p.product_name,
    #        SUM(oi.quantity * oi.unit_price) - COALESCE(SUM(r.amount), 0) AS net_revenue
    # FROM order_items oi
    # JOIN products p ON p.product_id = oi.product_id
    # LEFT JOIN refunds r ON r.order_id = oi.order_id
    # WHERE oi.order_date >= DATE '2025-10-01' AND oi.order_date < DATE '2026-01-01'
    # GROUP BY 1
    # ORDER BY net_revenue DESC
    # LIMIT 10
    # """.strip()

    sql = """
    SELECT 
    p.product_name, 
    SUM(oi.quantity) AS total_quantity_sold 
    FROM order_items AS oi 
    JOIN products AS p ON oi.product_id = p.product_id 
    GROUP BY p.product_name 
    ORDER BY total_quantity_sold DESC 
    LIMIT 15
    """.strip()

    return SqlProposal(
        sql=sql,
        rationale="Compute net revenue per product for last quarter using items minus refunds.",
        expected_columns=["product_name", "net_revenue"]
    )


def gemini_generate_sql(question: str) -> SqlProposal:
    load_dotenv()
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Warning: GEMINI_API_KEY environment variable not set. Falling back to mock agent.")
        return agent_generate_sql(question)
        
    client = genai.Client(api_key=api_key)
    
    schema_info = '''
    Tables available:
    - orders: order_id, customer_id
    - order_items: order_id, product_id, quantity, unit_price, order_date
    - products: product_id, product_name
    - refunds: refund_id, order_id, amount
    '''
    
    prompt = f'''
    You are an expert DuckDB SQL assistant. Given the following schema:
    {schema_info}
    
    Translate the following question into a valid DuckDB SQL SELECT query. 
    Make sure to match the question's date range and requirements.
    Return your response exactly as a JSON object with the following keys:
    - "sql": the raw SQL query string
    - "rationale": a brief explanation of how the query works
    - "expected_columns": a list of string column names expected in the result
    
    Question: {question}
    '''
    
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt,
        config=genai.types.GenerateContentConfig(
            response_mime_type="application/json"
        )
    )
    
    try:
        data = json.loads(response.text)
        return SqlProposal(
            sql=data.get("sql", ""),
            rationale=data.get("rationale", ""),
            expected_columns=data.get("expected_columns", [])
        )
    except Exception as e:
        print(f"Error parsing Gemini response: {e}")
        return agent_generate_sql(question)

def print_rows(rows: list[tuple], columns: list[str]) -> None:
    if not rows:
        print("No rows returned.")
        return
        
    print("\nResults:")
    print(tabulate(rows, headers=columns, tablefmt="grid"))


# --- 4) Run ---
def main():
    con = duckdb.connect("salesdb.ddb")

    question = "Top 10 products by revenue last quarter (assume Q4 2025)"
    # question = "fetch top 15 most sold items"

    proposal = agent_generate_sql(question)
    # proposal = gemini_generate_sql(question)

    safe_sql = validate_sql(proposal.sql)
    rows = con.execute(safe_sql).fetchall()

    print(f"Question: {question}")
    print(f"SQL: {safe_sql}")
    print(f"Rationale: {proposal.rationale}")
    print(f"Expected columns: {proposal.expected_columns}")
    print_rows(rows, proposal.expected_columns)


if __name__ == "__main__":
    main()