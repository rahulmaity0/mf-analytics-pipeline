"""Gold -> Postgres: load metric tables into the warehouse's ``raw`` schema for dbt.

The full daily NAV history (tens of millions of rows) stays in the Parquet
lake; Postgres only gets the month-end metrics that analysts and the API
query.

Usage::

    python -m mfpipeline.jobs.load_warehouse
"""
import json
import logging
import time

from mfpipeline.config import get_settings
from mfpipeline.monitoring import push_stage_metrics
from mfpipeline.spark import build_spark

log = logging.getLogger(__name__)

TABLES = {
    "gold/fund_metrics": "raw.fund_metrics",
    "gold/dq_nav_jumps": "raw.dq_nav_jumps",
}


def main() -> dict:
    settings = get_settings()
    started = time.time()
    spark = build_spark(settings, "mf-load-warehouse")
    stats = {}
    try:
        for lake_path, table in TABLES.items():
            df = spark.read.parquet(f"{settings.lake_root}/{lake_path}")
            (
                df.write.format("jdbc")
                .option("url", settings.jdbc_url)
                .option("dbtable", table)
                .option("user", settings.warehouse_user)
                .option("password", settings.warehouse_password)
                .option("driver", "org.postgresql.Driver")
                # Keep the table (and dbt's dependent views) and only replace its rows.
                .option("truncate", "true")
                .option("batchsize", "10000")
                .mode("overwrite")
                .save()
            )
            stats[table] = df.count()
            log.info("loaded %s rows into %s", stats[table], table)
    finally:
        spark.stop()

    push_stage_metrics(settings, "load_warehouse", time.time() - started,
                       rows={table: n for table, n in stats.items()})
    print(json.dumps(stats))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
