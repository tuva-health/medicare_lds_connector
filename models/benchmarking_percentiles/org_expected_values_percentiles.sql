{%- set expected_columns = benchmarking_percentiles_expected_columns() -%}
{%- set ns = namespace(first=true) -%}
{%- set percentiles = var('benchmarking_percentiles_percentiles', [10, 25, 50, 75, 90]) -%}

with org_expected_values as (

    select
        {% for col in expected_columns %}
        {{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('org_expected_values') }}

)

select *
from (

    {% for col in expected_columns %}
    {% for p in percentiles %}
    {% if not ns.first %} union all {% endif %}
    select distinct
          '{{ col }}' as metric_name
        , {{ p }} as percentile
        , percentile_cont({{ p / 100 }}) within group (order by {{ col }}) over () as metric_value
    from org_expected_values
    where {{ col }} is not null
    {%- set ns.first = false -%}
    {% endfor %}
    {% endfor %}

)
