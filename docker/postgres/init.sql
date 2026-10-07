-- Airflow keeps its metadata in its own database, separate from the warehouse.
CREATE DATABASE airflow;

\connect warehouse
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
