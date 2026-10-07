import pytest

pytest.importorskip("fastapi")
pytest.importorskip("prometheus_fastapi_instrumentator")

from fastapi.testclient import TestClient  # noqa: E402

from mfpipeline.api.cache import VERSION_KEY, ResponseCache  # noqa: E402
from mfpipeline.api.main import app, get_cache, get_repository  # noqa: E402


class DictBackend:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ex=None):
        self.data[key] = value


class FakeRepository:
    def __init__(self):
        self.calls = 0

    def list_funds(self, category, fund_house, plan, sort_by, limit):
        self.calls += 1
        return [{"scheme_code": 120716, "category": category, "sort_by": sort_by}][:limit]

    def get_registry_entry(self, scheme_code):
        if scheme_code != 120716:
            return None
        return {"scheme_code": scheme_code, "current": {"scheme_name": "UTI Nifty 50"}}

    def get_fund(self, scheme_code):
        return {"scheme_code": scheme_code, "ret_1y": 0.1}

    def get_history(self, scheme_code, months):
        return [{"month": "2026-09-01"}] if scheme_code == 120716 else []

    def list_categories(self):
        return []


@pytest.fixture
def client():
    backend = DictBackend()
    repo = FakeRepository()
    app.dependency_overrides[get_repository] = lambda: repo
    app.dependency_overrides[get_cache] = lambda: ResponseCache(backend)
    yield TestClient(app), repo, backend
    app.dependency_overrides.clear()


def test_second_request_is_served_from_cache(client):
    http, repo, _ = client
    first = http.get("/funds", params={"category": "Liquid Fund"})
    second = http.get("/funds", params={"category": "Liquid Fund"})
    assert first.headers["X-Cache"] == "MISS"
    assert second.headers["X-Cache"] == "HIT"
    assert second.json() == first.json()
    assert repo.calls == 1


def test_bumping_cache_version_invalidates_old_entries(client):
    http, repo, backend = client
    http.get("/funds")
    backend.data[VERSION_KEY] = b"2"  # what the pipeline does after dbt build
    assert http.get("/funds").headers["X-Cache"] == "MISS"
    assert repo.calls == 2


def test_unknown_sort_column_is_rejected(client):
    http, repo, _ = client
    response = http.get("/funds", params={"sort_by": "nav; drop table x"})
    assert response.status_code == 422
    assert repo.calls == 0


def test_limit_is_validated(client):
    http, _, _ = client
    assert http.get("/funds", params={"limit": 0}).status_code == 422
    assert http.get("/funds", params={"limit": 500}).status_code == 422


def test_fund_detail_combines_scorecard_and_registry(client):
    http, _, _ = client
    body = http.get("/funds/120716").json()
    assert body["registry"]["current"]["scheme_name"] == "UTI Nifty 50"
    assert body["scorecard"]["ret_1y"] == 0.1


def test_unknown_fund_returns_404(client):
    http, _, _ = client
    assert http.get("/funds/999").status_code == 404
    assert http.get("/funds/999/history").status_code == 404
