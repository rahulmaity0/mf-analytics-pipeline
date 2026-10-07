"""Per-scheme performance metrics.

Run once per scheme through Spark's ``applyInPandas``: Spark shuffles each
scheme's full NAV history to one task, and this function does the
path-dependent maths (drawdown, point-in-time lookbacks) in NumPy, which is
awkward to express with SQL window frames.

NAVs are stored as exact decimals in silver; metrics are ratios, so they are
computed in float64.
"""
import math

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252
# A lookback NAV must be within this many days of the target date, otherwise
# the scheme had no NAV around then (not launched yet, or suspended).
MAX_LOOKUP_GAP_DAYS = 7
# Consecutive NAVs further apart than this are not a "daily" return.
MAX_RETURN_GAP_DAYS = 10
# Need most of a year of daily returns before quoting 1y volatility.
MIN_OBSERVATIONS_1Y = 200
# Below this annualised volatility, Sharpe is meaningless: float rounding on a
# near-flat NAV series would otherwise produce ratios in the trillions.
MIN_VOLATILITY_FOR_SHARPE = 1e-4

OUTPUT_SCHEMA = (
    "scheme_code int, as_of_date date, nav double, is_latest boolean, "
    "ret_1m double, ret_1y double, cagr_3y double, cagr_5y double, "
    "vol_1y double, max_drawdown_1y double, sharpe_1y double, obs_1y int"
)
OUTPUT_COLUMNS = [part.strip().split(" ")[0] for part in OUTPUT_SCHEMA.split(",")]


def _nav_on_or_before(dates: np.ndarray, navs: np.ndarray, target: np.datetime64) -> float | None:
    i = np.searchsorted(dates, target, side="right") - 1
    if i < 0 or (target - dates[i]) > np.timedelta64(MAX_LOOKUP_GAP_DAYS, "D"):
        return None
    return float(navs[i])


def _growth(nav_now: float, nav_then: float | None, years: float = 1.0) -> float | None:
    if nav_then is None:
        return None
    return (nav_now / nav_then) ** (1.0 / years) - 1.0


def as_of_positions(dates: pd.DatetimeIndex) -> np.ndarray:
    """Positions of each month's last NAV, plus the overall latest NAV."""
    months = dates.to_period("M")
    is_last_in_month = np.append(months[1:] != months[:-1], True)
    return np.flatnonzero(is_last_in_month)


def scheme_metrics(pdf: pd.DataFrame, risk_free_rate: float) -> pd.DataFrame:
    """Month-end (and latest) metrics for one scheme's NAV history.

    ``pdf`` has columns scheme_code, nav_date, nav for a single scheme.
    """
    pdf = pdf.sort_values("nav_date")
    index = pd.DatetimeIndex(pd.to_datetime(pdf["nav_date"]))
    dates = index.values.astype("datetime64[D]")
    navs = pdf["nav"].to_numpy(dtype="float64")
    scheme_code = int(pdf["scheme_code"].iloc[0])
    n = len(navs)

    log_returns = np.full(n, np.nan)
    if n > 1:
        log_returns[1:] = np.log(navs[1:] / navs[:-1])
        gaps = np.diff(dates) > np.timedelta64(MAX_RETURN_GAP_DAYS, "D")
        log_returns[1:][gaps] = np.nan

    rows = []
    for p in as_of_positions(index):
        t = index[p]
        nav_t = navs[p]

        def nav_back(offset: pd.DateOffset, as_of: pd.Timestamp = t) -> float | None:
            return _nav_on_or_before(dates, navs, np.datetime64((as_of - offset).date(), "D"))

        nav_1m = nav_back(pd.DateOffset(months=1))
        nav_1y = nav_back(pd.DateOffset(years=1))
        ret_1y = _growth(nav_t, nav_1y)

        window_start = np.searchsorted(dates, np.datetime64((t - pd.Timedelta(days=365)).date(), "D"), side="left")
        window_returns = log_returns[window_start + 1 : p + 1]
        obs = int(np.count_nonzero(~np.isnan(window_returns)))

        vol = max_dd = sharpe = None
        if obs >= MIN_OBSERVATIONS_1Y:
            vol = float(np.nanstd(window_returns, ddof=1) * math.sqrt(TRADING_DAYS_PER_YEAR))
            window_navs = navs[window_start : p + 1]
            max_dd = float(np.min(window_navs / np.maximum.accumulate(window_navs) - 1.0))
            if ret_1y is not None and vol >= MIN_VOLATILITY_FOR_SHARPE:
                sharpe = (ret_1y - risk_free_rate) / vol

        rows.append((
            scheme_code, t.date(), float(nav_t), p == n - 1,
            _growth(nav_t, nav_1m), ret_1y,
            _growth(nav_t, nav_back(pd.DateOffset(years=3)), 3.0),
            _growth(nav_t, nav_back(pd.DateOffset(years=5)), 5.0),
            vol, max_dd, sharpe, obs,
        ))

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
