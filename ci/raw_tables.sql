-- Empty versions of the tables the pipeline loads into the warehouse.
-- Used by CI to run `dbt build` without running Spark first.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.schemes (
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
);

CREATE TABLE raw.fund_metrics (
    scheme_code      INTEGER,
    as_of_date       DATE,
    nav              DOUBLE PRECISION,
    is_latest        BOOLEAN,
    ret_1m           DOUBLE PRECISION,
    ret_1y           DOUBLE PRECISION,
    cagr_3y          DOUBLE PRECISION,
    cagr_5y          DOUBLE PRECISION,
    vol_1y           DOUBLE PRECISION,
    max_drawdown_1y  DOUBLE PRECISION,
    sharpe_1y        DOUBLE PRECISION,
    obs_1y           INTEGER
);

CREATE TABLE raw.dq_nav_jumps (
    scheme_code    INTEGER,
    prev_nav_date  DATE,
    prev_nav       NUMERIC(20, 6),
    nav_date       DATE,
    nav            NUMERIC(20, 6),
    change         DOUBLE PRECISION
);
