import math
from datetime import date

import numpy as np
import pandas as pd
import pytest

from mfpipeline.analytics import OUTPUT_COLUMNS, scheme_metrics


def business_day_series(start: str, end: str, daily_growth: float, scheme_code: int = 1) -> pd.DataFrame:
    dates = pd.bdate_range(start, end)
    navs = 10.0 * (1 + daily_growth) ** np.arange(len(dates))
    return pd.DataFrame({"scheme_code": scheme_code, "nav_date": dates.date, "nav": navs})


def latest(result: pd.DataFrame) -> pd.Series:
    return result[result["is_latest"]].iloc[0]


def test_one_row_per_month_end_plus_latest():
    result = scheme_metrics(business_day_series("2024-01-01", "2024-03-15", 0.0), risk_free_rate=0.0)
    assert list(result.columns) == OUTPUT_COLUMNS
    assert list(result["as_of_date"]) == [date(2024, 1, 31), date(2024, 2, 29), date(2024, 3, 15)]
    assert list(result["is_latest"]) == [False, False, True]


def test_steady_growth_gives_matching_cagr_and_zero_drawdown():
    yearly = 0.12
    daily = (1 + yearly) ** (1 / 260) - 1
    row = latest(scheme_metrics(business_day_series("2019-01-01", "2025-06-30", daily), risk_free_rate=0.065))
    assert row["cagr_3y"] == pytest.approx(yearly, abs=0.005)
    assert row["cagr_5y"] == pytest.approx(yearly, abs=0.005)
    assert row["max_drawdown_1y"] == pytest.approx(0.0)
    assert row["vol_1y"] == pytest.approx(0.0, abs=1e-9)
    # Zero volatility: Sharpe is undefined, not infinite.
    assert row["sharpe_1y"] is None or math.isnan(row["sharpe_1y"])


def test_drawdown_measures_peak_to_trough_fall():
    df = business_day_series("2024-01-01", "2024-12-31", 0.0)
    df.loc[df.index[100]:, "nav"] = 8.0   # 20% fall
    df.loc[df.index[200]:, "nav"] = 9.0   # partial recovery
    row = latest(scheme_metrics(df, risk_free_rate=0.0))
    assert row["max_drawdown_1y"] == pytest.approx(-0.2)


def test_lookback_metrics_are_null_when_history_is_too_short():
    row = latest(scheme_metrics(business_day_series("2025-01-01", "2025-05-30", 0.001), risk_free_rate=0.0))
    assert row["ret_1m"] == pytest.approx((1.001 ** 22) - 1, rel=0.1)
    for column in ["ret_1y", "cagr_3y", "cagr_5y", "vol_1y", "max_drawdown_1y", "sharpe_1y"]:
        assert row[column] is None or math.isnan(row[column]), column


def test_lookback_is_null_when_scheme_had_no_nav_near_target_date():
    df = business_day_series("2023-01-01", "2025-06-30", 0.0005)
    # A three-month suspension around the 1y lookback date.
    gap = (df["nav_date"] > date(2024, 5, 1)) & (df["nav_date"] < date(2024, 8, 1))
    row = latest(scheme_metrics(df[~gap], risk_free_rate=0.0))
    assert row["ret_1y"] is None or math.isnan(row["ret_1y"])


def test_returns_across_long_gaps_are_not_counted_as_daily_returns():
    df = business_day_series("2024-01-01", "2025-06-30", 0.0)
    df.loc[df["nav_date"] >= date(2025, 3, 1), "nav"] = 20.0
    gap = (df["nav_date"] > date(2025, 1, 31)) & (df["nav_date"] < date(2025, 3, 1))
    row = latest(scheme_metrics(df[~gap], risk_free_rate=0.0))
    # The 100% jump spans a month-long gap, so it is excluded and volatility stays zero.
    assert row["vol_1y"] == pytest.approx(0.0, abs=1e-9)


def test_sharpe_uses_excess_return_over_risk_free_rate():
    rng = np.random.default_rng(7)
    df = business_day_series("2023-01-01", "2025-06-30", 0.0)
    df["nav"] = 10.0 * np.exp(np.cumsum(rng.normal(0.0005, 0.01, len(df))))
    row = latest(scheme_metrics(df, risk_free_rate=0.05))
    assert row["sharpe_1y"] == pytest.approx((row["ret_1y"] - 0.05) / row["vol_1y"])
