from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Query, Request

from app.pipeline import bootstrap
from app.services import AnalyticsService
from app.utils import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    bootstrap()
    app.state.analytics = AnalyticsService()
    yield
    app.state.analytics.close()


app = FastAPI(
    title="Local DuckDB Analytics",
    description="Parquet-backed e-commerce analytics using a bronze/silver/gold lakehouse layout.",
    version="1.0.0",
    lifespan=lifespan,
)


def service(request: Request) -> AnalyticsService:
    return request.app.state.analytics


@app.get("/health", tags=["operations"])
def health(request: Request):
    return service(request).health()


@app.get("/analytics/revenue", tags=["analytics"])
def revenue(request: Request):
    return service(request).daily_revenue()


@app.get("/analytics/top-customers", tags=["analytics"])
def customers(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
):
    return service(request).top_customers(limit)


@app.get("/analytics/monthly-growth", tags=["analytics"])
def monthly_growth(request: Request):
    return service(request).monthly_growth()


@app.get("/analytics/retention", tags=["analytics"])
def retention(request: Request):
    return service(request).retention()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000)
