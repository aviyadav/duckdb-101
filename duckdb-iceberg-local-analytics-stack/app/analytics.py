import argparse
import json

from app.pipeline import bootstrap
from app.services import AnalyticsService
from app.utils import configure_logging

REPORTS = {
    "revenue": "daily_revenue",
    "customers": "top_customers",
    "growth": "monthly_growth",
    "retention": "retention",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a local DuckDB analytics report")
    parser.add_argument("report", choices=REPORTS, nargs="?", default="revenue")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    configure_logging()
    bootstrap()

    service = AnalyticsService()
    try:
        if args.report == "customers":
            rows = service.top_customers(args.limit)
        else:
            rows = getattr(service, REPORTS[args.report])()
        print(json.dumps(rows, default=str, indent=2))
    finally:
        service.close()


if __name__ == "__main__":
    main()
