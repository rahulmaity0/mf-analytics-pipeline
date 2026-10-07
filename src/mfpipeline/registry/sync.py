"""Sync the scheme registry: gold Parquet -> MongoDB -> Postgres ``raw.schemes``.

Plain pandas + PyArrow is enough here: the versions table is a few tens of
thousands of rows, so starting a Spark session would cost more than the work.

Usage::

    python -m mfpipeline.registry.sync
"""
import json
import logging
import time

import psycopg2
import pyarrow.parquet as pq
from psycopg2.extras import execute_values
from pyarrow import fs
from pymongo import ASCENDING, MongoClient, ReplaceOne

from mfpipeline.config import Settings, get_settings
from mfpipeline.monitoring import push_stage_metrics
from mfpipeline.registry.schemes import build_scheme_documents, to_warehouse_row

log = logging.getLogger(__name__)

SCHEMES_DDL = """
CREATE TABLE IF NOT EXISTS raw.schemes (
    scheme_code     INTEGER PRIMARY KEY,
    scheme_name     TEXT NOT NULL,
    scheme_type     TEXT,
    category        TEXT,
    fund_house      TEXT,
    isin_growth     TEXT,
    isin_reinvest   TEXT,
    first_nav_date  DATE NOT NULL,
    last_nav_date   DATE NOT NULL,
    name_changes    INTEGER NOT NULL
)
"""


def read_versions(settings: Settings):
    endpoint = settings.s3_endpoint.split("://", 1)
    s3 = fs.S3FileSystem(
        access_key=settings.s3_access_key, secret_key=settings.s3_secret_key,
        endpoint_override=endpoint[-1], scheme=endpoint[0] if len(endpoint) == 2 else "https",
    )
    return pq.read_table(f"{settings.lake_bucket}/gold/scheme_versions", filesystem=s3).to_pandas()


def write_mongo(settings: Settings, docs: list[dict]) -> int:
    client = MongoClient(settings.mongo_uri)
    try:
        schemes = client["mf_registry"]["schemes"]
        for start in range(0, len(docs), 5000):
            schemes.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs[start:start + 5000]],
                               ordered=False)
        schemes.create_index([("current.category", ASCENDING)])
        schemes.create_index([("current.fund_house", ASCENDING)])
        return schemes.count_documents({})
    finally:
        client.close()


def write_warehouse(settings: Settings, docs: list[dict]) -> None:
    conn = psycopg2.connect(
        host=settings.warehouse_host, port=settings.warehouse_port, dbname=settings.warehouse_db,
        user=settings.warehouse_user, password=settings.warehouse_password,
    )
    try:
        # One transaction: readers see either the old snapshot or the new one, never a half-loaded table.
        with conn, conn.cursor() as cur:
            cur.execute(SCHEMES_DDL)
            cur.execute("TRUNCATE raw.schemes")
            execute_values(cur, "INSERT INTO raw.schemes VALUES %s", [to_warehouse_row(d) for d in docs],
                           page_size=5000)
    finally:
        conn.close()


def main() -> dict:
    settings = get_settings()
    started = time.time()
    docs = list(build_scheme_documents(read_versions(settings)))
    mongo_count = write_mongo(settings, docs)
    write_warehouse(settings, docs)

    stats = {"schemes": len(docs), "mongo_documents": mongo_count,
             "renamed_schemes": sum(1 for d in docs if d["name_changes"] > 0)}
    push_stage_metrics(settings, "registry", time.time() - started,
                       rows={"schemes": stats["schemes"], "renamed_schemes": stats["renamed_schemes"]})
    print(json.dumps(stats))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
