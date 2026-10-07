"""Data-quality rules shared by the Spark jobs.

Rows that break a rule are never silently fixed or dropped: they are written
to the quarantine zone with the reason, and the run fails if too many rows
are quarantined.
"""
from datetime import date, timedelta

# Checked in this order; a row is tagged with the first rule it breaks.
# Written as SQL so this module imports without a running Spark session.
QUARANTINE_RULES: list[tuple[str, str]] = [
    ("malformed_line", "parse_error IS NOT NULL"),
    ("bad_scheme_code", "scheme_code_int IS NULL"),
    ("non_numeric_nav", "nav_value IS NULL"),
    ("zero_nav", "nav_value = 0"),
    ("negative_nav", "nav_value < 0"),
    ("bad_date", "nav_dt IS NULL"),
    ("date_outside_requested_window", "nav_dt < source_start OR nav_dt > source_end"),
]

# Quarantined, but expected, so they don't count towards the failure limit.
# AMFI publishes NAV = 0 every day for segregated portfolios: side pockets
# holding defaulted bonds that have been written down to nothing. They are
# about 1.2% of rows and growing, so counting them would eventually fail
# healthy runs.
EXPECTED_QUARANTINE_REASONS = {"zero_nav"}

# A daily NAV move larger than this is almost always a data error, a
# segregated portfolio, or a face-value change, so it is flagged for review.
SUSPICIOUS_DAILY_MOVE = 0.5


class DataQualityError(RuntimeError):
    pass


def quarantine_reason_sql() -> str:
    branches = " ".join(f"WHEN {condition} THEN '{reason}'" for reason, condition in QUARANTINE_RULES)
    return f"CASE {branches} END"


def check_quarantine_ratio(input_rows: int, quarantined_by_reason: dict[str, int], max_ratio: float) -> float:
    """Fail if unexpected quarantined rows exceed ``max_ratio`` of the input; return that ratio."""
    if input_rows == 0:
        return 0.0
    unexpected = sum(n for reason, n in quarantined_by_reason.items() if reason not in EXPECTED_QUARANTINE_REASONS)
    ratio = unexpected / input_rows
    if ratio > max_ratio:
        raise DataQualityError(
            f"{unexpected} of {input_rows} rows quarantined for unexpected reasons ({ratio:.2%}), "
            f"above the {max_ratio:.2%} limit; inspect the quarantine zone before re-running"
        )
    return ratio


def check_freshness(latest_nav_date: date | None, run_date: date, max_lag_days: int = 5) -> None:
    """NAVs are published every business day; a gap longer than a long weekend means the feed is stale."""
    if latest_nav_date is None:
        raise DataQualityError("no NAV data found in silver")
    if run_date - latest_nav_date > timedelta(days=max_lag_days):
        raise DataQualityError(
            f"latest NAV date {latest_nav_date} is more than {max_lag_days} days before {run_date}"
        )
