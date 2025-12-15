with percentiles as (

    select
          percentile
        , paid_pmpm
        , risk_adj_pmpm
    from {{ ref('org_pmpm_percentiles') }}

)

, medians as (

    select
          max(case when percentile = 50 then paid_pmpm end) as paid_pmpm_median
        , max(case when percentile = 50 then risk_adj_pmpm end) as risk_adj_pmpm_median
    from percentiles

)

select
      p.percentile
    , p.paid_pmpm
    , p.risk_adj_pmpm
    , p.paid_pmpm / nullif(m.paid_pmpm_median, 0) as paid_pmpm_pct_of_median
    , p.risk_adj_pmpm / nullif(m.risk_adj_pmpm_median, 0) as risk_adj_pmpm_pct_of_median
from percentiles as p
cross join medians as m
order by p.percentile
