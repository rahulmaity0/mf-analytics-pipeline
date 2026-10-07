-- AMFI leaves the Plan/Option columns blank for most rows, so plan and option
-- are derived from the scheme name.
with source as (
    select * from {{ source('raw', 'schemes') }}
),

latest as (
    select max(last_nav_date) as max_nav_date from source
)

select
    scheme_code,
    scheme_name,
    scheme_type,
    category,
    -- "Equity Scheme - Large Cap Fund" -> asset class "Equity Scheme"
    nullif(trim(split_part(category, ' - ', 1)), '') as asset_class,
    fund_house,
    isin_growth,
    isin_reinvest,
    case
        when scheme_name ilike '%direct%' then 'Direct'
        else 'Regular'
    end as plan,
    case
        when scheme_name ilike '%idcw%' or scheme_name ilike '%dividend%' then 'IDCW'
        when scheme_name ilike '%bonus%' then 'Bonus'
        when scheme_name ilike '%growth%' then 'Growth'
        else 'Other'
    end as option_type,
    first_nav_date,
    last_nav_date,
    last_nav_date >= latest.max_nav_date - {{ var('active_within_days') }} as is_active,
    name_changes
from source
cross join latest
