import duckdb
import re
from duckdb import typing as dtypes

con = duckdb.connect()

def parse_money(s: str) -> float:
    """Parse a money string and return its float value."""
    if s is None or s == "":
        return None
    # handle "1,299" vs "1.299" ambiguities in a predictable way
    s = s.replace(',', '')
    m = re.search(r"\d+(\.\d+)?", s)
    return float(m.groups(0)) if m else None

con.create_function(
    "parse_money",
    parse_money,
    parameters = [dtypes.VARCHAR],
    return_type=dtypes.DOUBLE,
    side_effects=False
)