"""Land AMFI NAV history for a date range in the bronze zone.

Usage::

    python -m mfpipeline.ingest.cli --start 2026-07-01 --end 2026-09-30
"""
import argparse
import json
import logging
import time
from datetime import date

from mfpipeline.config import get_settings
from mfpipeline.ingest.amfi import ingest_range
from mfpipeline.monitoring import push_stage_metrics
from mfpipeline.storage import LakeStorage


def run(start: date, end: date) -> dict:
    settings = get_settings()
    started = time.time()
    storage = LakeStorage(settings)
    storage.ensure_bucket()
    landed = ingest_range(start, end, storage)
    stats = {
        "files": len(landed),
        "bytes": sum(f.size_bytes for f in landed),
        "rows": sum(f.data_lines for f in landed),
        "periods": sorted({f.period for f in landed}),
    }
    push_stage_metrics(settings, "ingest", time.time() - started,
                       rows={"landed": stats["rows"]}, gauges={"mf_pipeline_ingested_bytes": stats["bytes"]})
    return stats


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    stats = run(args.start, args.end)
    print(json.dumps(stats))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
