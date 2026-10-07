-- Fund houses by breadth and by how often their funds beat the category median.
select
    fund_house,
    count(*) as growth_schemes,
    count(*) filter (where plan = 'Direct') as direct_schemes,
    count(distinct category) as categories,
    avg(case when ret_1y_vs_category > 0 then 1.0 else 0.0 end)
        filter (where ret_1y is not null) as share_beating_category_1y,
    percentile_cont(0.5) within group (order by ret_1y_vs_category) as median_ret_1y_vs_category
from {{ ref('mart_fund_scorecard') }}
group by fund_house
