"""Runtime settings, read from environment variables.

Every value has a local-development default so the code runs inside the
docker-compose stack without extra setup.
"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    lake_bucket: str
    warehouse_host: str
    warehouse_port: int
    warehouse_db: str
    warehouse_user: str
    warehouse_password: str
    mongo_uri: str
    redis_url: str
    pushgateway_url: str
    spark_driver_memory: str
    spark_extra_jars: str
    # Annual risk-free rate used for the Sharpe ratio (roughly the 1y T-bill yield).
    risk_free_rate: float
    # Fail the silver build if more than this share of rows is quarantined.
    max_quarantine_ratio: float

    @property
    def lake_root(self) -> str:
        return f"s3a://{self.lake_bucket}"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.warehouse_host}:{self.warehouse_port}/{self.warehouse_db}"


def get_settings() -> Settings:
    env = os.environ.get
    return Settings(
        s3_endpoint=env("S3_ENDPOINT", "http://localhost:8333"),
        s3_access_key=env("S3_ACCESS_KEY", "lakeadmin"),
        s3_secret_key=env("S3_SECRET_KEY", "lakeadmin-secret"),
        lake_bucket=env("LAKE_BUCKET", "mf-lake"),
        warehouse_host=env("WAREHOUSE_HOST", "localhost"),
        warehouse_port=int(env("WAREHOUSE_PORT", "5432")),
        warehouse_db=env("WAREHOUSE_DB", "warehouse"),
        warehouse_user=env("WAREHOUSE_USER", "mf"),
        warehouse_password=env("WAREHOUSE_PASSWORD", "mf"),
        mongo_uri=env("MONGO_URI", "mongodb://localhost:27017"),
        redis_url=env("REDIS_URL", "redis://localhost:6379/0"),
        pushgateway_url=env("PUSHGATEWAY_URL", "localhost:9091"),
        spark_driver_memory=env("SPARK_DRIVER_MEMORY", "3g"),
        spark_extra_jars=env("SPARK_EXTRA_JARS", "/opt/spark-jars"),
        risk_free_rate=float(env("RISK_FREE_RATE", "0.065")),
        max_quarantine_ratio=float(env("MAX_QUARANTINE_RATIO", "0.02")),
    )
