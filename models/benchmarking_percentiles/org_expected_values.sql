{%- set expected_columns = benchmarking_percentiles_expected_columns() -%}

with member_months as (

    select
          person_id
        , count(*) as member_months
    from {{ ref('core__member_months') }}
    where left(year_month, 4) = '2023'
    group by person_id

)

, expected_by_person as (

    select
          person_id
        {% for col in expected_columns %}
        , sum({{ col }}) as {{ col }}
        {% endfor %}
    from {{ ref('benchmarks__predict_member_month') }}
    where left(cast(year_month as {{ dbt.type_string() }}), 4) = '2023'
    group by person_id

)

, person_expected as (

    select
          mm.person_id
        , mm.member_months
        {% for col in expected_columns %}
        , coalesce(ebp.{{ col }}, 0) as {{ col }}
        {% endfor %}
    from member_months as mm
    left join expected_by_person as ebp
        on mm.person_id = ebp.person_id

)

, member_orgs as (

    select
          person_id
        , assigned_hospital_npi
        , assigned_hospital_name
    from {{ ref('member_org_assignment') }}

)

, final as (

    select
          mo.assigned_hospital_npi
        , mo.assigned_hospital_name
        , sum(pe.member_months) as total_member_months
        , sum(pe.member_months) / 12 as total_person_years
        {% for col in expected_columns %}
        , sum(pe.{{ col }}) / nullif(sum(pe.member_months), 0) as {{ col }}
        {% endfor %}
    from person_expected as pe
    inner join member_orgs as mo
        on pe.person_id = mo.person_id
    group by
          mo.assigned_hospital_npi
        , mo.assigned_hospital_name

)

select *
from final
where total_person_years >= 1000

