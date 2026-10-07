select
    scheme_code,
    as_of_date,
    nav,
    is_latest,
    ret_1m,
    ret_1y,
    cagr_3y,
    cagr_5y,
    vol_1y,
    max_drawdown_1y,
    sharpe_1y,
    obs_1y
from {{ source('raw', 'fund_metrics') }}
