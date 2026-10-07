-- Suspicious NAV jumps with scheme context, for a human to review.
select
    j.scheme_code,
    s.scheme_name,
    s.fund_house,
    s.category,
    j.prev_nav_date,
    j.prev_nav,
    j.nav_date,
    j.nav,
    j.change
from {{ source('raw', 'dq_nav_jumps') }} j
left join {{ ref('dim_scheme') }} s using (scheme_code)
