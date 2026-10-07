"""Push batch-job metrics to the Prometheus Pushgateway.

Batch jobs finish before Prometheus could scrape them, so they push their
numbers instead. A monitoring outage must never fail a data run, so push
errors are logged and swallowed.
"""
import logging
import time

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from mfpipeline.config import Settings

log = logging.getLogger(__name__)


def push_stage_metrics(
    settings: Settings,
    stage: str,
    duration_seconds: float,
    rows: dict[str, float] | None = None,
    gauges: dict[str, float] | None = None,
) -> None:
    registry = CollectorRegistry()
    Gauge("mf_pipeline_last_success_timestamp_seconds", "Unix time of the last successful run",
          registry=registry).set(time.time())
    Gauge("mf_pipeline_duration_seconds", "Duration of the last successful run",
          registry=registry).set(duration_seconds)
    if rows:
        row_gauge = Gauge("mf_pipeline_rows", "Row counts from the last successful run", ["kind"],
                          registry=registry)
        for kind, value in rows.items():
            row_gauge.labels(kind=kind).set(value)
    for name, value in (gauges or {}).items():
        Gauge(name, name.replace("_", " "), registry=registry).set(value)

    try:
        push_to_gateway(settings.pushgateway_url, job="mf_pipeline", grouping_key={"stage": stage},
                        registry=registry, timeout=5)
    except Exception as exc:  # noqa: BLE001 - monitoring must not break the pipeline
        log.warning("could not push metrics for stage %s: %s", stage, exc)
