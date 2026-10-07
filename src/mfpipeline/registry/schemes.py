"""Build scheme registry documents from the gold ``scheme_versions`` table.

A scheme's metadata is naturally a nested document: a current view plus an
ordered list of every name / category / fund house it has carried. That
shape is why the registry lives in MongoDB rather than as a wide SQL table.
"""
from collections.abc import Iterable
from datetime import UTC, date, datetime

import pandas as pd

ATTRIBUTES = ["scheme_name", "scheme_type", "category", "fund_house", "isin_growth", "isin_reinvest"]


def _clean(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _as_datetime(value: date) -> datetime:
    # BSON has no date-only type.
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def build_scheme_documents(versions: pd.DataFrame, now: datetime | None = None) -> Iterable[dict]:
    now = now or datetime.now(UTC)
    for scheme_code, group in versions.groupby("scheme_code", sort=True):
        records = [{k: _clean(v) for k, v in row.items()} for row in group.to_dict("records")]
        records.sort(key=lambda r: (r["first_seen"], r["last_seen"]))
        current = max(records, key=lambda r: (r["last_seen"], r["first_seen"]))
        yield {
            "_id": int(scheme_code),
            "scheme_code": int(scheme_code),
            "current": {k: current[k] for k in ATTRIBUTES},
            "history": [
                {
                    **{k: r[k] for k in ATTRIBUTES},
                    "first_seen": _as_datetime(r["first_seen"]),
                    "last_seen": _as_datetime(r["last_seen"]),
                    "nav_rows": int(r["nav_rows"]),
                }
                for r in records
            ],
            "first_nav_date": _as_datetime(min(r["first_seen"] for r in records)),
            "last_nav_date": _as_datetime(max(r["last_seen"] for r in records)),
            "name_changes": len({r["scheme_name"] for r in records}) - 1,
            "updated_at": now,
        }


def to_warehouse_row(doc: dict) -> tuple:
    current = doc["current"]
    return (
        doc["scheme_code"], current["scheme_name"], current["scheme_type"], current["category"],
        current["fund_house"], current["isin_growth"], current["isin_reinvest"],
        doc["first_nav_date"].date(), doc["last_nav_date"].date(), doc["name_changes"],
    )
