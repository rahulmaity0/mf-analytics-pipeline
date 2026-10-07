-- A 1y return below -90% or above +300% is usually a data error (NAV
-- restated, face value changed) rather than real performance. Warn, don't
-- fail: these need a human to look at mart_nav_anomalies.
{{ config(severity='warn') }}

select scheme_code, scheme_name, ret_1y
from {{ ref('mart_fund_scorecard') }}
where ret_1y < -0.9 or ret_1y > 3.0
