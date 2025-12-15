with org_pmpm as (

    select
          paid_pmpm
        , risk_adj_pmpm
    from {{ ref('org_pmpm') }}
    where paid_pmpm is not null
      and risk_adj_pmpm is not null

)

select *
from (

    {% set percentiles = var('benchmarking_percentiles_percentiles', [10, 25, 50, 75, 90]) %}
    {% for p in percentiles %}
    select distinct
          {{ p }} as percentile
        , percentile_cont({{ p / 100 }}) within group (order by paid_pmpm) over () as paid_pmpm
        , percentile_cont({{ p / 100 }}) within group (order by risk_adj_pmpm) over () as risk_adj_pmpm
    from org_pmpm
    {% if not loop.last %} union all {% endif %}
    {% endfor %}

)
