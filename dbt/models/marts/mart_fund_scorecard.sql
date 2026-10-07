-- Latest metrics for every active Growth scheme, ranked within its category
-- and compared with the category median and the Nifty 50 benchmark.
-- Only Growth options are compared: IDCW NAVs drop on every payout, so their
-- NAV returns understate what investors actually earned.
with latest as (
    select m.*, s.scheme_name, s.fund_house, s.category, s.asset_class, s.plan
    from {{ ref('stg_fund_metrics') }} m
    join {{ ref('dim_scheme') }} s using (scheme_code)
    where m.is_latest
      and s.is_active
      and s.option_type = 'Growth'
),

benchmark as (
    select ret_1y as bench_ret_1y, cagr_3y as bench_cagr_3y, cagr_5y as bench_cagr_5y
    from {{ ref('stg_fund_metrics') }}
    where scheme_code = {{ var('benchmark_scheme_code') }}
      and is_latest
),

category_stats as (
    select
        category,
        plan,
        count(*) as funds_in_category,
        percentile_cont(0.5) within group (order by ret_1y) as category_median_ret_1y,
        percentile_cont(0.5) within group (order by cagr_3y) as category_median_cagr_3y
    from latest
    group by category, plan
)

select
    l.scheme_code,
    l.scheme_name,
    l.fund_house,
    l.category,
    l.asset_class,
    l.plan,
    l.as_of_date,
    l.nav,
    l.ret_1m,
    l.ret_1y,
    l.cagr_3y,
    l.cagr_5y,
    l.vol_1y,
    l.max_drawdown_1y,
    l.sharpe_1y,
    c.funds_in_category,
    c.category_median_ret_1y,
    c.category_median_cagr_3y,
    l.ret_1y - c.category_median_ret_1y as ret_1y_vs_category,
    l.ret_1y - b.bench_ret_1y as ret_1y_vs_benchmark,
    l.cagr_3y - b.bench_cagr_3y as cagr_3y_vs_benchmark,
    l.cagr_5y - b.bench_cagr_5y as cagr_5y_vs_benchmark,
    -- Rank 1 = best in category (same plan, so Direct funds aren't ranked against Regular).
    rank() over (partition by l.category, l.plan order by l.ret_1y desc nulls last) as rank_ret_1y,
    rank() over (partition by l.category, l.plan order by l.cagr_3y desc nulls last) as rank_cagr_3y,
    rank() over (partition by l.category, l.plan order by l.sharpe_1y desc nulls last) as rank_sharpe_1y
from latest l
join category_stats c using (category, plan)
left join benchmark b on true
