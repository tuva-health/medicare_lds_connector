{%- set expected_cols = benchmarking_percentiles_expected_encounter_type_columns() -%}
{%- set ns = namespace(bases=[]) -%}
{%- for col in expected_cols -%}
    {%- do ns.bases.append(col | replace('expected_', '')) -%}
{%- endfor -%}
{%- set metric_bases = ns.bases -%}

with member_months as (

    select
          person_id
        , count(*) as member_months
    from {{ ref('core__member_months') }}
    where left(year_month, 4) = '2023'
    group by person_id

)

, actual_expected_by_person as (

    select
          person_id
        {% for expected_col in expected_cols %}
        , sum({{ expected_col | replace('expected_', 'actual_') }}) as total_{{ expected_col | replace('expected_', 'actual_') }}
        , sum({{ expected_col }}) as total_{{ expected_col }}
        {% endfor %}
    from {{ ref('benchmarks__predict_member_month') }}
    where left(cast(year_month as {{ dbt.type_string() }}), 4) = '2023'
    group by person_id

)

, person_metrics as (

    select
          mm.person_id
        , mm.member_months
        {% for expected_col in expected_cols %}
        , coalesce(ae.total_{{ expected_col | replace('expected_', 'actual_') }}, 0) as total_{{ expected_col | replace('expected_', 'actual_') }}
        , coalesce(ae.total_{{ expected_col }}, 0) as total_{{ expected_col }}
        {% endfor %}
    from member_months as mm
    left join actual_expected_by_person as ae
        on mm.person_id = ae.person_id

)

, member_orgs as (

    select
          person_id
        , assigned_hospital_npi
        , assigned_hospital_name
    from {{ ref('member_org_assignment') }}

)

, org_totals as (

    select
          mo.assigned_hospital_npi
        , mo.assigned_hospital_name
        , sum(pm.member_months) as total_member_months
        , sum(pm.member_months) / 12 as total_person_years
        {% for expected_col in expected_cols %}
        , sum(pm.total_{{ expected_col | replace('expected_', 'actual_') }}) as total_{{ expected_col | replace('expected_', 'actual_') }}
        , sum(pm.total_{{ expected_col }}) as total_{{ expected_col }}
        {% endfor %}
    from person_metrics as pm
    inner join member_orgs as mo
        on pm.person_id = mo.person_id
    group by
          mo.assigned_hospital_npi
        , mo.assigned_hospital_name

)

, eligible as (

    select
          *
        {% for base in metric_bases %}
        {%- if base.endswith('_paid_amount') -%}
        , total_actual_{{ base }} / nullif(total_member_months, 0) as actual_{{ base }}_pmpm
        , total_expected_{{ base }} / nullif(total_member_months, 0) as expected_{{ base }}_pmpm
        , (total_actual_{{ base }} / nullif(total_member_months, 0))
            / nullif((total_expected_{{ base }} / nullif(total_member_months, 0)), 0) as observed_to_expected_{{ base }}
        {%- elif base.endswith('_encounter_count') -%}
        , total_actual_{{ base }} / nullif(total_member_months, 0) as actual_{{ base }}_per_mm
        , total_expected_{{ base }} / nullif(total_member_months, 0) as expected_{{ base }}_per_mm
        , (total_actual_{{ base }} / nullif(total_member_months, 0))
            / nullif((total_expected_{{ base }} / nullif(total_member_months, 0)), 0) as observed_to_expected_{{ base }}
        {%- else -%}
        , total_actual_{{ base }} / nullif(total_member_months, 0) as actual_{{ base }}_per_mm
        , total_expected_{{ base }} / nullif(total_member_months, 0) as expected_{{ base }}_per_mm
        , (total_actual_{{ base }} / nullif(total_member_months, 0))
            / nullif((total_expected_{{ base }} / nullif(total_member_months, 0)), 0) as observed_to_expected_{{ base }}
        {%- endif -%}
        {% endfor %}
    from org_totals
    where total_person_years >= 1000

)

, population as (

    select
        {% for base in metric_bases %}
        {%- if base.endswith('_paid_amount') -%}
        sum(total_actual_{{ base }}) / nullif(sum(total_member_months), 0) as population_actual_{{ base }}_pmpm
        {%- elif base.endswith('_encounter_count') -%}
        sum(total_actual_{{ base }}) / nullif(sum(total_member_months), 0) as population_actual_{{ base }}_per_mm
        {%- else -%}
        sum(total_actual_{{ base }}) / nullif(sum(total_member_months), 0) as population_actual_{{ base }}_per_mm
        {%- endif -%}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    from eligible

)

select
      e.*
    {% for base in metric_bases %}
    {%- if base.endswith('_paid_amount') -%}
    , e.observed_to_expected_{{ base }} * p.population_actual_{{ base }}_pmpm as risk_adj_{{ base }}_pmpm
    {%- elif base.endswith('_encounter_count') -%}
    , e.observed_to_expected_{{ base }} * p.population_actual_{{ base }}_per_mm as risk_adj_{{ base }}_per_mm
    {%- else -%}
    , e.observed_to_expected_{{ base }} * p.population_actual_{{ base }}_per_mm as risk_adj_{{ base }}_per_mm
    {%- endif -%}
    {% endfor %}
from eligible as e
cross join population as p

