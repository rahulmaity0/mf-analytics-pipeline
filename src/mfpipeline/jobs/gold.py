"""Silver -> gold: per-scheme metrics, scheme versions, and NAV anomaly flags.

Usage::

    python -m mfpipeline.jobs.gold --run-date 2026-10-08
"""
import argparse
import json
import logging
import time
from datetime import date
from functools import partial

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from mfpipeline.analytics import OUTPUT_SCHEMA, scheme_metrics
from mfpipeline.config import get_settings
from mfpipeline.monitoring import push_stage_metrics
from mfpipeline.quality.checks import SUSPICIOUS_DAILY_MOVE, check_freshness
from mfpipeline.spark import build_spark

log = logging.getLogger(__name__)

SCHEME_ATTRIBUTES = ["scheme_name", "scheme_type", "category", "fund_house", "isin_growth", "isin_reinvest"]


def fund_metrics(silver: DataFrame, risk_free_rate: float) -> DataFrame:
    return (
        silver.select("scheme_code", "nav_date", F.col("nav").cast("double").alias("nav"))
        .groupBy("scheme_code")
        .applyInPandas(partial(scheme_metrics, risk_free_rate=risk_free_rate), schema=OUTPUT_SCHEMA)
    )


def scheme_versions(silver: DataFrame) -> DataFrame:
    """Each distinct set of attributes a scheme has carried, with the dates it was seen.

    Schemes get renamed and re-categorised (SEBI's 2018 recategorisation moved
    almost every fund), so the registry keeps the full history, not just the
    latest name.
    """
    return silver.groupBy("scheme_code", *SCHEME_ATTRIBUTES).agg(
        F.min("nav_date").alias("first_seen"),
        F.max("nav_date").alias("last_seen"),
        F.count("*").alias("nav_rows"),
    )


def nav_jumps(silver: DataFrame) -> DataFrame:
    by_date = Window.partitionBy("scheme_code").orderBy("nav_date")
    return (
        silver.select("scheme_code", "nav_date", "nav")
        .withColumn("prev_nav_date", F.lag("nav_date").over(by_date))
        .withColumn("prev_nav", F.lag("nav").over(by_date))
        .withColumn("change", F.col("nav") / F.col("prev_nav") - 1)
        .filter(F.abs("change") > SUSPICIOUS_DAILY_MOVE)
        .select("scheme_code", "prev_nav_date", "prev_nav", "nav_date", "nav",
                F.col("change").cast("double").alias("change"))
    )


def build_gold(spark: SparkSession, lake_root: str, risk_free_rate: float) -> dict:
    silver = spark.read.parquet(f"{lake_root}/silver/nav_daily")

    metrics = fund_metrics(silver, risk_free_rate)
    metrics.write.mode("overwrite").parquet(f"{lake_root}/gold/fund_metrics")

    versions = scheme_versions(silver)
    versions.write.mode("overwrite").parquet(f"{lake_root}/gold/scheme_versions")

    jumps = nav_jumps(silver)
    jumps.write.mode("overwrite").parquet(f"{lake_root}/gold/dq_nav_jumps")

    written = spark.read.parquet(f"{lake_root}/gold/fund_metrics")
    summary = silver.agg(F.max("nav_date").alias("latest"), F.countDistinct("scheme_code").alias("schemes")).first()
    return {
        "latest_nav_date": summary["latest"],
        "schemes": summary["schemes"],
        "metric_rows": written.count(),
        "latest_metric_rows": written.filter("is_latest").count(),
        "scheme_versions": spark.read.parquet(f"{lake_root}/gold/scheme_versions").count(),
        "suspicious_nav_jumps": spark.read.parquet(f"{lake_root}/gold/dq_nav_jumps").count(),
    }


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--skip-freshness-check", action="store_true",
                        help="for historical backfills, where the newest NAV is old by design")
    args = parser.parse_args(argv)

    settings = get_settings()
    started = time.time()
    spark = build_spark(settings, "mf-gold")
    try:
        stats = build_gold(spark, settings.lake_root, settings.risk_free_rate)
    finally:
        spark.stop()

    if not args.skip_freshness_check:
        check_freshness(stats["latest_nav_date"], args.run_date)

    push_stage_metrics(
        settings, "gold", time.time() - started,
        rows={"metrics": stats["metric_rows"], "schemes": stats["schemes"],
              "suspicious_nav_jumps": stats["suspicious_nav_jumps"]},
        gauges={"mf_data_latest_nav_date_timestamp_seconds":
                time.mktime(stats["latest_nav_date"].timetuple())},
    )
    stats["latest_nav_date"] = stats["latest_nav_date"].isoformat()
    print(json.dumps(stats))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
