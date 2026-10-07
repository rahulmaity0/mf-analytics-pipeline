"""Bronze -> silver: parse raw AMFI files into typed, de-duplicated NAV rows.

Usage::

    python -m mfpipeline.jobs.silver --periods 2026-09,2026-10
"""
import argparse
import json
import logging
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from mfpipeline.config import Settings, get_settings
from mfpipeline.ingest.amfi import BRONZE_PREFIX
from mfpipeline.monitoring import push_stage_metrics
from mfpipeline.parse import RawNavRow, parse_amfi_text
from mfpipeline.quality.checks import EXPECTED_QUARANTINE_REASONS, check_quarantine_ratio, quarantine_reason_sql
from mfpipeline.spark import build_spark
from mfpipeline.storage import LakeStorage

log = logging.getLogger(__name__)

RAW_SCHEMA = T.StructType(
    [T.StructField("source_file", T.StringType())]
    + [
        T.StructField(name, T.IntegerType() if name == "line_no" else T.StringType())
        for name in RawNavRow._fields
    ]
)

SILVER_COLUMNS = [
    "scheme_code", "nav_date", "nav", "scheme_name", "isin_growth", "isin_reinvest",
    "scheme_type", "category", "fund_house", "source_file", "period",
]


def _explode_file(item: tuple[str, str]):
    path, text = item
    for row in parse_amfi_text(text):
        yield (path, *row)


def parse_files(spark: SparkSession, paths: list[str]) -> DataFrame:
    """One task per file: rows depend on header lines above them, so files can't be split."""
    rdd = spark.sparkContext.wholeTextFiles(",".join(paths), minPartitions=len(paths))
    return spark.createDataFrame(rdd.flatMap(_explode_file), schema=RAW_SCHEMA)


def type_and_validate(raw: DataFrame) -> DataFrame:
    window_dates = F.regexp_extract("source_file", r"amfi_nav_(\d{8})_(\d{8})", 0)
    typed = (
        raw.withColumn("source_start", F.to_date(F.regexp_extract(window_dates, r"_(\d{8})_", 1), "yyyyMMdd"))
        .withColumn("source_end", F.to_date(F.regexp_extract(window_dates, r"_(\d{8})$", 1), "yyyyMMdd"))
        .withColumn("scheme_code_int", F.expr("try_cast(scheme_code AS INT)"))
        .withColumn("nav_value", F.expr("try_cast(nav AS DECIMAL(20,6))"))
        .withColumn("nav_dt", F.to_date("nav_date", "dd-MMM-yyyy"))
    )
    return typed.withColumn("quarantine_reason", F.expr(quarantine_reason_sql()))


def deduplicate(valid: DataFrame) -> DataFrame:
    """Keep one row per (scheme, date): the one from the most recently requested window.

    Overlapping windows happen on purpose (the daily run re-fetches the last few
    days to pick up late NAVs), and AMFI occasionally corrects a NAV, so the
    newest window wins.
    """
    latest_first = Window.partitionBy("scheme_code_int", "nav_dt").orderBy(
        F.col("source_end").desc(), F.col("source_start").desc(), F.col("line_no").desc()
    )
    return valid.withColumn("_rank", F.row_number().over(latest_first)).filter("_rank = 1").drop("_rank")


def to_silver(deduped: DataFrame) -> DataFrame:
    return deduped.select(
        F.col("scheme_code_int").alias("scheme_code"),
        F.col("nav_dt").alias("nav_date"),
        F.col("nav_value").alias("nav"),
        "scheme_name", "isin_growth", "isin_reinvest", "scheme_type", "category", "fund_house",
        "source_file",
        F.date_format("nav_dt", "yyyy-MM").alias("period"),
    )


def build_silver(
    spark: SparkSession,
    paths: list[str],
    silver_path: str,
    quarantine_path: str,
    max_quarantine_ratio: float,
) -> dict:
    typed = type_and_validate(parse_files(spark, paths)).cache()

    reason_counts = {
        row["quarantine_reason"]: row["count"]
        for row in typed.groupBy("quarantine_reason").count().collect()
    }
    input_rows = sum(reason_counts.values())
    quarantined_by_reason = {reason: n for reason, n in reason_counts.items() if reason is not None}
    quarantined_rows = sum(quarantined_by_reason.values())

    if quarantined_rows:
        (
            typed.filter(F.col("quarantine_reason").isNotNull())
            .withColumn("period", F.regexp_extract("source_file", r"period=(\d{4}-\d{2})", 1))
            .select("source_file", "line_no", "scheme_code", "scheme_name", "nav", "nav_date",
                    "quarantine_reason", "period")
            .write.mode("overwrite").partitionBy("period").parquet(quarantine_path)
        )
    # Fail before anything reaches silver: bad batches must not leak downstream.
    ratio = check_quarantine_ratio(input_rows, quarantined_by_reason, max_quarantine_ratio)

    valid = typed.filter(F.col("quarantine_reason").isNull())
    conflicts = (
        valid.groupBy("scheme_code_int", "nav_dt")
        .agg(F.countDistinct("nav_value").alias("versions"))
        .filter("versions > 1")
        .count()
    )
    silver = to_silver(deduplicate(valid)).cache()
    silver_rows = silver.count()
    silver.write.mode("overwrite").partitionBy("period").parquet(silver_path)

    typed.unpersist()
    silver.unpersist()
    return {
        "input_rows": input_rows,
        "silver_rows": silver_rows,
        "duplicates_dropped": input_rows - quarantined_rows - silver_rows,
        "conflicting_duplicates": conflicts,
        "quarantined_rows": quarantined_rows,
        "unexpected_quarantine_ratio": round(ratio, 6),
        "quarantined_by_reason": quarantined_by_reason,
    }


def bronze_paths_for_periods(settings: Settings, periods: list[str]) -> list[str]:
    storage = LakeStorage(settings)
    keys = []
    for period in periods:
        keys.extend(storage.list_keys(f"{BRONZE_PREFIX}/period={period}/"))
    return [f"{settings.lake_root}/{key}" for key in sorted(keys)]


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--periods", required=True, help="comma-separated YYYY-MM periods to rebuild")
    args = parser.parse_args(argv)
    periods = sorted({p.strip() for p in args.periods.split(",") if p.strip()})

    settings = get_settings()
    started = time.time()
    paths = bronze_paths_for_periods(settings, periods)
    if not paths:
        raise SystemExit(f"no bronze files found for periods {periods}")

    spark = build_spark(settings, "mf-silver")
    try:
        stats = build_silver(
            spark,
            paths,
            silver_path=f"{settings.lake_root}/silver/nav_daily",
            quarantine_path=f"{settings.lake_root}/quarantine/amfi_nav",
            max_quarantine_ratio=settings.max_quarantine_ratio,
        )
    finally:
        spark.stop()

    stats.update(periods=periods, files=len(paths))
    push_stage_metrics(
        settings, "silver", time.time() - started,
        rows={"input": stats["input_rows"], "silver": stats["silver_rows"],
              "quarantined": stats["quarantined_rows"], "conflicting_duplicates": stats["conflicting_duplicates"],
              "quarantined_unexpected": sum(n for reason, n in stats["quarantined_by_reason"].items()
                                            if reason not in EXPECTED_QUARANTINE_REASONS),
              **{f"quarantined_{reason}": n for reason, n in stats["quarantined_by_reason"].items()}},
    )
    print(json.dumps(stats))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
