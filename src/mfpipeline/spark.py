"""SparkSession factory shared by every job."""
import glob
import os

from pyspark.sql import SparkSession

from mfpipeline.config import Settings


def build_spark(settings: Settings, app_name: str, with_s3: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        # Overwrite only the partitions a job actually writes, never the whole table.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Unparseable dates become NULL (and get quarantined) instead of raising.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
    )

    jars = sorted(glob.glob(os.path.join(settings.spark_extra_jars, "*.jar")))
    if jars:
        builder = builder.config("spark.jars", ",".join(jars))

    if with_s3:
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings.s3_endpoint.startswith("https")).lower())
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
