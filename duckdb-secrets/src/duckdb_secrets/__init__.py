from .query_my_bucket import query_my_bucket
from .scoped_persistent_secrets import query_scoped_secrets
from .setup_scoped_secrets import setup_scoped_secrets

__all__ = [
    "main",
    "query_my_bucket",
    "query_scoped_secrets",
    "setup_scoped_secrets",
]


def main() -> None:
    print("Hello from duckdb-secrets!")
