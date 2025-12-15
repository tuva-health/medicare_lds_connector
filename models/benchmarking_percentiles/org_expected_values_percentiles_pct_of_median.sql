with percentiles as (

    select
          metric_name
        , percentile
        , metric_value
    from {{ ref('org_expected_values_percentiles') }}

)

, medians as (

    select
          metric_name
        , max(case when percentile = 50 then metric_value end) as median_value
    from percentiles
    group by metric_name

)

select
      p.metric_name
    , p.percentile
    , p.metric_value
    , p.metric_value / nullif(m.median_value, 0) as pct_of_median
from percentiles as p
inner join medians as m
    on p.metric_name = m.metric_name
order by
      p.metric_name
    , p.percentile

