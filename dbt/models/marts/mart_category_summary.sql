-- How a category performed as a whole, and how widely its funds differed.
-- A wide spread between the 25th and 75th percentile means picking the
-- right fund matters more than picking the category.
select
    category,
    asset_class,
    plan,
    count(*) as funds,
    percentile_cont(0.25) within group (order by ret_1y) as p25_ret_1y,
    percentile_cont(0.5) within group (order by ret_1y) as median_ret_1y,
    percentile_cont(0.75) within group (order by ret_1y) as p75_ret_1y,
    percentile_cont(0.75) within group (order by ret_1y)
        - percentile_cont(0.25) within group (order by ret_1y) as ret_1y_spread,
    percentile_cont(0.5) within group (order by cagr_3y) as median_cagr_3y,
    percentile_cont(0.5) within group (order by cagr_5y) as median_cagr_5y,
    avg(vol_1y) as avg_vol_1y,
    avg(max_drawdown_1y) as avg_max_drawdown_1y,
    max(as_of_date) as as_of_date
from {{ ref('mart_fund_scorecard') }}
group by category, asset_class, plan
