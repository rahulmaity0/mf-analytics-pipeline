-- The gold job must emit exactly one "latest" metrics row per scheme.
select scheme_code, count(*) as latest_rows
from {{ ref('stg_fund_metrics') }}
where is_latest
group by scheme_code
having count(*) <> 1
