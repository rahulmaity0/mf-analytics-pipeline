#!/usr/bin/env bash
# Single-container Airflow for local development: migrate, ensure the admin
# user exists, then run the scheduler in the background and the webserver in
# the foreground.
set -euo pipefail

airflow db migrate
airflow users create \
    --username "${AIRFLOW_ADMIN_USER}" --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname Admin --lastname User --role Admin --email admin@example.com \
    || true

airflow scheduler &
exec airflow webserver --port 8080
