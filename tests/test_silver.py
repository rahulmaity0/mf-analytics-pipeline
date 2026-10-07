from datetime import date
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from mfpipeline.jobs.silver import build_silver  # noqa: E402
from mfpipeline.quality.checks import DataQualityError  # noqa: E402


@pytest.fixture
def built(spark, fixture_paths, tmp_path):
    silver_path = str(tmp_path / "silver")
    quarantine_path = str(tmp_path / "quarantine")
    stats = build_silver(spark, fixture_paths, silver_path, quarantine_path, max_quarantine_ratio=1.0)
    return stats, spark.read.parquet(silver_path), spark.read.parquet(quarantine_path)


def test_every_bad_row_is_quarantined_with_its_reason(built):
    stats, _, quarantine = built
    assert stats["input_rows"] == 15
    assert stats["quarantined_by_reason"] == {
        "non_numeric_nav": 1,
        "malformed_line": 1,
        "zero_nav": 1,
        "negative_nav": 1,
        "bad_date": 1,
        "date_outside_requested_window": 1,
    }
    assert quarantine.count() == 6
    # Zero NAVs (segregated portfolios) are kept out of silver but don't count as unexpected.
    assert stats["unexpected_quarantine_ratio"] == pytest.approx(5 / 15, abs=1e-6)


def test_duplicates_across_overlapping_windows_are_removed(built):
    stats, silver, _ = built
    assert stats["silver_rows"] == 7
    assert stats["duplicates_dropped"] == 2
    assert silver.groupBy("scheme_code", "nav_date").count().filter("count > 1").count() == 0


def test_conflicting_nav_resolves_to_the_newest_window(built):
    stats, silver, _ = built
    assert stats["conflicting_duplicates"] == 1
    row = silver.filter("scheme_code = 120465 AND nav_date = '2026-09-02'").first()
    assert row["nav"] == Decimal("65.490000")
    assert row["source_file"].endswith("amfi_nav_20260902_20260903.txt")


def test_silver_is_typed_and_partitioned_by_month(built):
    _, silver, _ = built
    row = silver.filter("scheme_code = 119018 AND nav_date = '2026-09-01'").first()
    assert row["nav_date"] == date(2026, 9, 1)
    assert row["fund_house"] == "HDFC Mutual Fund"
    assert row["period"] == "2026-09"


def test_run_fails_when_quarantine_ratio_exceeds_limit(spark, fixture_paths, tmp_path):
    silver_path = tmp_path / "silver"
    with pytest.raises(DataQualityError, match="rows quarantined"):
        build_silver(spark, fixture_paths, str(silver_path), str(tmp_path / "q"), max_quarantine_ratio=0.02)
    # Nothing reaches silver when the batch is rejected.
    assert not silver_path.exists()
