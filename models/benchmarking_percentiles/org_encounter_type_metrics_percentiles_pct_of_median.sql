with percentiles as (

    select
          metric_name
        , value_type
        , percentile
        , metric_value
    from {{ ref('org_encounter_type_metrics_percentiles') }}

)

, medians as (

    select
          metric_name
        , value_type
        , max(case when percentile = 50 then metric_value end) as median_value
    from percentiles
    group by
          metric_name
        , value_type

)

select
      p.metric_name
    , p.value_type
    , p.percentile
    , p.metric_value
    , p.metric_value / nullif(m.median_value, 0) as pct_of_median
from percentiles as p
inner join medians as m
    on p.metric_name = m.metric_name
    and p.value_type = m.value_type
order by
      p.metric_name
    , p.value_type
    , p.percentile

