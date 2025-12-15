with member_months as (

    select
          person_id
        , count(*) as member_months
    from {{ ref('core__member_months') }}
    where left(year_month,4) = '2023'
    group by person_id

)

, paid_by_person as (

    select
          person_id
        , sum(paid_amount) as total_paid
    from {{ ref('core__medical_claim') }}
    where year(claim_end_date)=2023
    group by person_id

)

, expected_paid_by_person as (

    select
          person_id
        , sum(expected_paid_amount) as expected_paid_amount
    from {{ ref('benchmarks__predict_member_month') }}
    where left(cast(year_month as {{ dbt.type_string() }}), 4) = '2023'
    group by person_id

)

, person_financials as (

    select
          mm.person_id
        , coalesce(pb.total_paid, 0) as total_paid
        , coalesce(ep.expected_paid_amount, 0) as expected_paid_amount
        , mm.member_months
    from member_months as mm
        left join paid_by_person as pb
            on mm.person_id = pb.person_id
        left join expected_paid_by_person as ep
            on mm.person_id = ep.person_id

)

, member_orgs as (

    select
          person_id
        , assigned_hospital_npi
        , assigned_hospital_name
    from {{ ref('member_org_assignment') }}

)

,final as (
select
      mo.assigned_hospital_npi
    , mo.assigned_hospital_name
    , sum(pf.total_paid) as total_paid
    , sum(pf.expected_paid_amount) as expected_paid_amount
    , sum(pf.member_months) as total_member_months
    , sum(pf.member_months)/12 as total_person_years
    , case
        when sum(pf.member_months) = 0 then null
        else sum(pf.total_paid) / sum(pf.member_months)
      end as paid_pmpm
from person_financials as pf
    inner join member_orgs as mo
        on pf.person_id = mo.person_id
group by
      mo.assigned_hospital_npi
    , mo.assigned_hospital_name
)

, eligible as (

    select
          *
        , expected_paid_amount / nullif(total_member_months, 0) as expected_pmpm
        , paid_pmpm
            / nullif(
                expected_paid_amount / nullif(total_member_months, 0)
              , 0
              ) as observed_to_expected
    from final
    where total_person_years >= 1000

)

, population as (

    select
        sum(total_paid) / nullif(sum(total_member_months), 0) as population_paid_pmpm
    from eligible

)

select
      e.*
    , e.observed_to_expected * p.population_paid_pmpm as risk_adj_pmpm
from eligible as e
cross join population as p
