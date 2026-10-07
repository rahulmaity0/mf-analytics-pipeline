"""Mutual fund NAV pipeline DAGs.

mf_nav_daily     every morning: re-fetch the last few days from AMFI, rebuild
                 the affected silver months, then gold -> warehouse -> dbt.
mf_nav_backfill  on demand: load a historical date range, one year per
                 mapped task, then the same downstream steps.

Spark jobs run as separate processes (``python -m ...``) rather than inside
the Airflow worker, so each job's JVM is torn down when it finishes and the
same commands work from a terminal or CI.
"""
from datetime import date, datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

LOCAL_TZ = pendulum.timezone("Asia/Kolkata")
# AMFI sometimes publishes or corrects NAVs a day or two late, so the daily
# run re-fetches a short window; silver de-duplicates the overlap.
DAILY_LOOKBACK_DAYS = 5

DBT_BUILD = (
    "/opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt "
    "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
)

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@task
def refresh_api_cache() -> int:
    """Bump the cache version so the API stops serving responses built from old marts."""
    import redis

    from mfpipeline.config import get_settings

    return redis.Redis.from_url(get_settings().redis_url).incr("mf:cache_version")


def downstream_of_silver(gold_args: str):
    """gold -> (registry, warehouse load) -> dbt -> cache refresh, shared by both DAGs."""
    gold = BashOperator(task_id="build_gold", bash_command=f"python -m mfpipeline.jobs.gold {gold_args}")
    registry = BashOperator(task_id="sync_scheme_registry", bash_command="python -m mfpipeline.registry.sync")
    load = BashOperator(task_id="load_warehouse", bash_command="python -m mfpipeline.jobs.load_warehouse")
    dbt_build = BashOperator(task_id="dbt_build", bash_command=DBT_BUILD)
    gold >> [registry, load] >> dbt_build >> refresh_api_cache()
    return gold


@dag(
    dag_id="mf_nav_daily",
    schedule="0 7 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["mutual-funds", "daily"],
    doc_md=__doc__,
)
def mf_nav_daily():
    @task
    def ingest_recent(data_interval_end=None) -> list[str]:
        from mfpipeline.ingest.cli import run

        end = data_interval_end.in_timezone(LOCAL_TZ).date()
        stats = run(end - timedelta(days=DAILY_LOOKBACK_DAYS), end)
        return stats["periods"]

    periods = ingest_recent()
    silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            "python -m mfpipeline.jobs.silver "
            "--periods {{ ti.xcom_pull(task_ids='ingest_recent') | join(',') }}"
        ),
    )
    gold = downstream_of_silver(
        gold_args="--run-date {{ data_interval_end.in_timezone('Asia/Kolkata').to_date_string() }}"
    )
    periods >> silver >> gold


@dag(
    dag_id="mf_nav_backfill",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["mutual-funds", "backfill"],
    params={
        "start_date": Param("2021-10-01", type="string", format="date", description="first NAV date to load"),
        "end_date": Param(date.today().isoformat(), type="string", format="date", description="last NAV date to load"),
    },
    doc_md=__doc__,
)
def mf_nav_backfill():
    @task
    def plan_chunks(params=None) -> list[list[str]]:
        """Split the range into calendar-year chunks, one mapped task each."""
        start = date.fromisoformat(params["start_date"])
        end = date.fromisoformat(params["end_date"])
        if start > end:
            raise ValueError(f"start_date {start} is after end_date {end}")
        chunks = []
        for year in range(start.year, end.year + 1):
            chunks.append([max(start, date(year, 1, 1)).isoformat(), min(end, date(year, 12, 31)).isoformat()])
        return chunks

    # Two downloads at a time: faster than serial, still gentle on AMFI.
    @task(max_active_tis_per_dag=2)
    def ingest_chunk(chunk: list[str]) -> list[str]:
        from mfpipeline.ingest.cli import run

        return run(date.fromisoformat(chunk[0]), date.fromisoformat(chunk[1]))["periods"]

    landed = ingest_chunk.expand(chunk=plan_chunks())

    # One Spark job at a time: each takes several GB of memory.
    silver = BashOperator.partial(task_id="build_silver", max_active_tis_per_dag=1).expand(
        bash_command=landed.map(lambda periods: f"python -m mfpipeline.jobs.silver --periods {','.join(periods)}")
    )
    gold = downstream_of_silver(gold_args="--skip-freshness-check")
    silver >> gold


mf_nav_daily()
mf_nav_backfill()
