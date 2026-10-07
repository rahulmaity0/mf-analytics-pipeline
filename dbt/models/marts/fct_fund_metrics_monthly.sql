-- One row per scheme per month, taken at the month's last NAV. The gold job
-- already emits exactly that grain; for the current month the row is
-- month-to-date (is_latest = true).
select
    scheme_code,
    date_trunc('month', as_of_date)::date as month,
    as_of_date,
    is_latest as is_month_to_date,
    nav,
    ret_1m,
    ret_1y,
    cagr_3y,
    cagr_5y,
    vol_1y,
    max_drawdown_1y,
    sharpe_1y
from {{ ref('stg_fund_metrics') }}
