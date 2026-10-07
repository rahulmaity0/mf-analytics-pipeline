from datetime import UTC, date, datetime

import pandas as pd
import pytest

from mfpipeline.quality.checks import DataQualityError, check_freshness, check_quarantine_ratio
from mfpipeline.registry.schemes import build_scheme_documents, to_warehouse_row

NOW = datetime(2026, 10, 8, tzinfo=UTC)


def version(code, name, category, first, last, fund_house="Axis Mutual Fund"):
    return {
        "scheme_code": code, "scheme_name": name, "scheme_type": "Open Ended Schemes", "category": category,
        "fund_house": fund_house, "isin_growth": "INF1", "isin_reinvest": None,
        "first_seen": first, "last_seen": last, "nav_rows": 10,
    }


def test_registry_keeps_rename_history_and_uses_latest_version_as_current():
    versions = pd.DataFrame([
        version(1, "Axis Equity Fund - Growth", "Growth", date(2010, 1, 4), date(2018, 5, 31)),
        version(1, "Axis Bluechip Fund - Growth", "Equity Scheme - Large Cap Fund",
                date(2018, 6, 1), date(2026, 10, 7)),
        version(2, "HDFC Top 100 Fund", "Equity Scheme - Large Cap Fund", date(2013, 1, 1), date(2026, 10, 7)),
    ])
    docs = {d["_id"]: d for d in build_scheme_documents(versions, now=NOW)}

    axis = docs[1]
    assert axis["current"]["scheme_name"] == "Axis Bluechip Fund - Growth"
    assert [h["scheme_name"] for h in axis["history"]] == ["Axis Equity Fund - Growth", "Axis Bluechip Fund - Growth"]
    assert axis["name_changes"] == 1
    assert axis["first_nav_date"] == datetime(2010, 1, 4, tzinfo=UTC)
    assert docs[2]["name_changes"] == 0


def test_warehouse_row_flattens_current_view():
    versions = pd.DataFrame([version(7, "Fund X", "Liquid Fund", date(2020, 1, 1), date(2026, 10, 7))])
    doc = next(build_scheme_documents(versions, now=NOW))
    assert to_warehouse_row(doc) == (
        7, "Fund X", "Open Ended Schemes", "Liquid Fund", "Axis Mutual Fund", "INF1", None,
        date(2020, 1, 1), date(2026, 10, 7), 0,
    )


def test_quarantine_ratio_within_limit_passes():
    assert check_quarantine_ratio(1000, {"non_numeric_nav": 10}, 0.02) == pytest.approx(0.01)
    assert check_quarantine_ratio(0, {}, 0.02) == 0.0


def test_quarantine_ratio_above_limit_fails():
    with pytest.raises(DataQualityError, match="unexpected reasons"):
        check_quarantine_ratio(1000, {"negative_nav": 30}, 0.02)


def test_expected_zero_navs_do_not_count_towards_the_limit():
    # Segregated portfolios report NAV 0 every day; 5% of rows here, still a healthy run.
    assert check_quarantine_ratio(1000, {"zero_nav": 50, "bad_date": 5}, 0.02) == pytest.approx(0.005)


def test_freshness_allows_weekends_but_not_stale_feeds():
    check_freshness(date(2026, 10, 2), run_date=date(2026, 10, 5))  # Friday NAV on Monday
    with pytest.raises(DataQualityError, match="more than 5 days"):
        check_freshness(date(2026, 9, 25), run_date=date(2026, 10, 5))
    with pytest.raises(DataQualityError, match="no NAV data"):
        check_freshness(None, run_date=date(2026, 10, 5))
