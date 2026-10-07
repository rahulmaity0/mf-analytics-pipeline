"""Download NAV history from AMFI and land it, unchanged, in the bronze zone.

Bronze keeps the exact bytes AMFI returned, so a parsing bug can be fixed
and replayed later without downloading 20 years of history again.
"""
import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta

import requests

from mfpipeline.parse import HEADER_PREFIX
from mfpipeline.storage import LakeStorage

log = logging.getLogger(__name__)

AMFI_HISTORY_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
BRONZE_PREFIX = "bronze/amfi_nav"
# AMFI's history begins on 1 April 2006.
EARLIEST_DATE = date(2006, 4, 1)


class AmfiResponseError(RuntimeError):
    """AMFI answered, but not with a NAV file (usually an HTML error page)."""


@dataclass(frozen=True)
class LandedFile:
    key: str
    period: str
    start: date
    end: date
    size_bytes: int
    data_lines: int


def month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into windows that never cross a calendar month.

    Each bronze file then belongs to exactly one ``period=YYYY-MM`` folder,
    which is the unit the silver job reprocesses.
    """
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    windows = []
    cursor = start
    while cursor <= end:
        next_month = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1)
        window_end = min(end, next_month - timedelta(days=1))
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def bronze_key(start: date, end: date) -> str:
    # Deterministic key: re-running the same window overwrites the same object,
    # so ingestion is idempotent.
    return f"{BRONZE_PREFIX}/period={start:%Y-%m}/amfi_nav_{start:%Y%m%d}_{end:%Y%m%d}.txt"


def count_data_lines(body: bytes) -> int:
    return sum(1 for line in body.splitlines() if b";" in line) - 1  # minus the header


def fetch_nav_history(start: date, end: date, session: requests.Session, retries: int = 3) -> bytes:
    params = {"frmdt": start.strftime("%d-%b-%Y"), "todt": end.strftime("%d-%b-%Y")}
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(AMFI_HISTORY_URL, params=params, timeout=120)
            resp.raise_for_status()
            body = resp.content
            if not body.lstrip().startswith(HEADER_PREFIX.encode()):
                raise AmfiResponseError(f"unexpected response for {params}: {body[:120]!r}")
            return body
        except (requests.RequestException, AmfiResponseError) as exc:
            if attempt == retries:
                raise
            wait = 5 * attempt
            log.warning("AMFI fetch %s failed (%s), retrying in %ss", params, exc, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


def ingest_range(
    start: date,
    end: date,
    storage: LakeStorage,
    session: requests.Session | None = None,
    pause_seconds: float = 1.0,
) -> list[LandedFile]:
    """Download every month window in [start, end] into bronze."""
    start = max(start, EARLIEST_DATE)
    session = session or requests.Session()
    landed = []
    for window_start, window_end in month_windows(start, end):
        body = fetch_nav_history(window_start, window_end, session)
        key = bronze_key(window_start, window_end)
        storage.put_bytes(key, body)
        rows = count_data_lines(body)
        landed.append(LandedFile(key, f"{window_start:%Y-%m}", window_start, window_end, len(body), rows))
        log.info("landed %s (%d bytes, %d rows)", key, len(body), rows)
        time.sleep(pause_seconds)  # be polite to a free public endpoint
    return landed
