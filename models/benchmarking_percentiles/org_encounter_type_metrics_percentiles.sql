{%- set expected_cols = benchmarking_percentiles_expected_encounter_type_columns() -%}
{%- set ns = namespace(bases=[]) -%}
{%- for col in expected_cols -%}
    {%- do ns.bases.append(col | replace('expected_', '')) -%}
{%- endfor -%}
{%- set metric_bases = ns.bases -%}

{%- set percentiles = var('benchmarking_percentiles_percentiles', [10, 25, 50, 75, 90]) -%}

with org_metrics as (

    select *
    from {{ ref('org_encounter_type_metrics') }}

)

, metrics_long as (

    {%- set ns2 = namespace(first=true) -%}
    {% for base in metric_bases %}
    {% for value_type in ['actual', 'risk_adj'] %}
    {% if not ns2.first %} union all {% endif %}
    select
          '{{ base }}' as metric_name
        , '{{ value_type }}' as value_type
        {% if base.endswith('_paid_amount') %}
        , {{ value_type }}_{{ base }}_pmpm as metric_value
        {% elif base.endswith('_encounter_count') %}
        , {{ value_type }}_{{ base }}_per_mm as metric_value
        {% else %}
        , {{ value_type }}_{{ base }}_per_mm as metric_value
        {% endif %}
    from org_metrics
    where
        {% if base.endswith('_paid_amount') %}
        {{ value_type }}_{{ base }}_pmpm is not null
        {% elif base.endswith('_encounter_count') %}
        {{ value_type }}_{{ base }}_per_mm is not null
        {% else %}
        {{ value_type }}_{{ base }}_per_mm is not null
        {% endif %}
    {% set ns2.first = false %}
    {% endfor %}
    {% endfor %}

)

, percentiles_wide as (

    select
          metric_name
        , value_type
        {% for p in percentiles %}
        , percentile_cont({{ p / 100 }}) within group (order by metric_value) as p{{ p }}
        {% endfor %}
    from metrics_long
    group by
          metric_name
        , value_type

)

select *
from (

    {%- set ns3 = namespace(first=true) -%}
    {% for p in percentiles %}
    {% if not ns3.first %} union all {% endif %}
    select
          metric_name
        , value_type
        , {{ p }} as percentile
        , p{{ p }} as metric_value
    from percentiles_wide
    {% set ns3.first = false %}
    {% endfor %}

)
