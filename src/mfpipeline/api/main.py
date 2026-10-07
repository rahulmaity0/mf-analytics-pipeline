"""Read API over the analytics marts, with a Redis response cache."""
from functools import lru_cache
from typing import Annotated, Literal

import redis
from fastapi import Depends, FastAPI, HTTPException, Query, Response
from prometheus_fastapi_instrumentator import Instrumentator

from mfpipeline.api.cache import ResponseCache
from mfpipeline.api.repository import SCORECARD_SORT_COLUMNS, FundRepository
from mfpipeline.config import get_settings

app = FastAPI(
    title="Mutual Fund Analytics API",
    description="Performance metrics for Indian mutual funds, built from AMFI NAV history.",
    version="0.1.0",
)
Instrumentator().instrument(app).expose(app, include_in_schema=False)


@lru_cache
def get_repository() -> FundRepository:
    return FundRepository(get_settings())


@lru_cache
def get_cache() -> ResponseCache:
    return ResponseCache(redis.Redis.from_url(get_settings().redis_url))


Repo = Annotated[FundRepository, Depends(get_repository)]
Cache = Annotated[ResponseCache, Depends(get_cache)]


def _cached(response: Response, cache: ResponseCache, key: str, loader):
    value, hit = cache.get_or_load(key, loader)
    response.headers["X-Cache"] = "HIT" if hit else "MISS"
    return value


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/funds")
def list_funds(
    response: Response,
    repo: Repo,
    cache: Cache,
    category: str | None = None,
    fund_house: str | None = None,
    plan: Literal["Direct", "Regular"] | None = None,
    sort_by: str = Query("ret_1y", description=f"one of {sorted(SCORECARD_SORT_COLUMNS)}"),
    limit: int = Query(20, ge=1, le=200),
):
    if sort_by not in SCORECARD_SORT_COLUMNS:
        raise HTTPException(422, f"sort_by must be one of {sorted(SCORECARD_SORT_COLUMNS)}")
    key = f"funds:{category}:{fund_house}:{plan}:{sort_by}:{limit}"
    return _cached(response, cache, key, lambda: repo.list_funds(category, fund_house, plan, sort_by, limit))


@app.get("/funds/{scheme_code}")
def get_fund(scheme_code: int, response: Response, repo: Repo, cache: Cache):
    def load():
        registry = repo.get_registry_entry(scheme_code)
        if registry is None:
            return None
        return {"scorecard": repo.get_fund(scheme_code), "registry": registry}

    fund = _cached(response, cache, f"fund:{scheme_code}", load)
    if fund is None:
        raise HTTPException(404, f"scheme {scheme_code} not found")
    return fund


@app.get("/funds/{scheme_code}/history")
def get_history(scheme_code: int, response: Response, repo: Repo, cache: Cache,
                months: int = Query(60, ge=1, le=240)):
    history = _cached(response, cache, f"history:{scheme_code}:{months}",
                      lambda: repo.get_history(scheme_code, months))
    if not history:
        raise HTTPException(404, f"no metrics for scheme {scheme_code}")
    return history


@app.get("/categories")
def list_categories(response: Response, repo: Repo, cache: Cache):
    return _cached(response, cache, "categories", repo.list_categories)
