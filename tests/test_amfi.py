from datetime import date

import pytest
import requests

from mfpipeline.ingest import amfi
from mfpipeline.ingest.amfi import AmfiResponseError, bronze_key, fetch_nav_history, ingest_range, month_windows

VALID_BODY = (
    b"Scheme Code;NAV Name;Plan;Option;ISIN1;ISIN2;Net Asset Value;Date\r\n\r\n"
    b"Axis Mutual Fund\r\n1;A;;;;;10.0;01-Sep-2026\r\n"
)


class FakeResponse:
    def __init__(self, content: bytes, status: int = 200):
        self.content = content
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append(params)
        return self.responses.pop(0)


class FakeStorage:
    def __init__(self):
        self.objects = {}

    def put_bytes(self, key, body, content_type="text/plain"):
        self.objects[key] = body


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(amfi.time, "sleep", lambda _: None)


def test_month_windows_split_at_month_boundaries():
    assert month_windows(date(2026, 1, 15), date(2026, 3, 2)) == [
        (date(2026, 1, 15), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 2, 28)),
        (date(2026, 3, 1), date(2026, 3, 2)),
    ]


def test_month_windows_handles_single_day_and_year_end():
    assert month_windows(date(2025, 12, 31), date(2026, 1, 1)) == [
        (date(2025, 12, 31), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 1, 1)),
    ]


def test_month_windows_rejects_inverted_range():
    with pytest.raises(ValueError):
        month_windows(date(2026, 2, 1), date(2026, 1, 1))


def test_bronze_key_is_deterministic_and_partitioned_by_period():
    assert bronze_key(date(2026, 9, 1), date(2026, 9, 30)) == (
        "bronze/amfi_nav/period=2026-09/amfi_nav_20260901_20260930.txt"
    )


def test_fetch_uses_amfi_date_format():
    session = FakeSession([FakeResponse(VALID_BODY)])
    fetch_nav_history(date(2026, 9, 1), date(2026, 9, 30), session)
    assert session.calls == [{"frmdt": "01-Sep-2026", "todt": "30-Sep-2026"}]


def test_fetch_retries_html_error_page_then_succeeds():
    session = FakeSession([FakeResponse(b"<html>Service Unavailable</html>"), FakeResponse(VALID_BODY)])
    assert fetch_nav_history(date(2026, 9, 1), date(2026, 9, 2), session) == VALID_BODY
    assert len(session.calls) == 2


def test_fetch_gives_up_after_retries():
    session = FakeSession([FakeResponse(b"<html>oops</html>")] * 3)
    with pytest.raises(AmfiResponseError):
        fetch_nav_history(date(2026, 9, 1), date(2026, 9, 2), session)


def test_fetch_raises_on_http_error():
    session = FakeSession([FakeResponse(b"", status=503)] * 3)
    with pytest.raises(requests.HTTPError):
        fetch_nav_history(date(2026, 9, 1), date(2026, 9, 2), session)


def test_ingest_range_lands_one_file_per_month_and_clamps_to_earliest_date():
    session = FakeSession([FakeResponse(VALID_BODY)] * 2)
    storage = FakeStorage()
    landed = ingest_range(date(2006, 3, 1), date(2006, 5, 10), storage, session)
    # Starts at 1 Apr 2006 (AMFI's first date), not 1 Mar.
    assert [f.period for f in landed] == ["2006-04", "2006-05"]
    assert set(storage.objects) == {f.key for f in landed}
    assert landed[0].data_lines == 1
