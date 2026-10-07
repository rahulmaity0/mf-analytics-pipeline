FROM apache/airflow:2.10.5-python3.11

USER root
# Spark needs a JVM; curl is used to fetch the S3A / JDBC jars below.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Jars baked into the image so Spark never downloads dependencies at runtime.
# hadoop-aws must match the Hadoop version bundled with PySpark 3.5 (3.3.4).
ENV SPARK_EXTRA_JARS=/opt/spark-jars
RUN mkdir -p ${SPARK_EXTRA_JARS} && cd ${SPARK_EXTRA_JARS} \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

# dbt lives in its own virtualenv so its dependency pins never fight Airflow's.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.9.0" "dbt-postgres~=1.9.0"

COPY docker/airflow/start.sh /opt/airflow/start.sh
RUN chmod +x /opt/airflow/start.sh

USER airflow
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/airflow/src
