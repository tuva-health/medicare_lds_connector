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
        , sum(actual_paid_amount) as total_actual_paid_amount
        , sum(expected_paid_amount) as total_expected_paid_amount
        , sum(actual_inpatient_paid_amount) as total_actual_inpatient_paid_amount
        , sum(expected_inpatient_paid_amount) as total_expected_inpatient_paid_amount
        , sum(actual_outpatient_paid_amount) as total_actual_outpatient_paid_amount
        , sum(expected_outpatient_paid_amount) as total_expected_outpatient_paid_amount
        , sum(actual_office_based_paid_amount) as total_actual_office_based_paid_amount
        , sum(expected_office_based_paid_amount) as total_expected_office_based_paid_amount
        , sum(actual_other_paid_amount) as total_actual_other_paid_amount
        , sum(expected_other_paid_amount) as total_expected_other_paid_amount

        , sum(actual_inpatient_encounter_count) as total_actual_inpatient_encounter_count
        , sum(expected_inpatient_encounter_count) as total_expected_inpatient_encounter_count
        , sum(actual_outpatient_encounter_count) as total_actual_outpatient_encounter_count
        , sum(expected_outpatient_encounter_count) as total_expected_outpatient_encounter_count
        , sum(actual_office_based_encounter_count) as total_actual_office_based_encounter_count
        , sum(expected_office_based_encounter_count) as total_expected_office_based_encounter_count
        , sum(actual_other_encounter_count) as total_actual_other_encounter_count
        , sum(expected_other_encounter_count) as total_expected_other_encounter_count
    from {{ ref('benchmarks__predict_member_month') }}
    where left(cast(year_month as {{ dbt.type_string() }}), 4) = '2023'
    group by person_id

)

, person_financials as (

    select
          mm.person_id
        , mm.member_months
        , coalesce(ae.total_actual_paid_amount, 0) as total_actual_paid_amount
        , coalesce(ae.total_expected_paid_amount, 0) as total_expected_paid_amount
        , coalesce(ae.total_actual_inpatient_paid_amount, 0) as total_actual_inpatient_paid_amount
        , coalesce(ae.total_expected_inpatient_paid_amount, 0) as total_expected_inpatient_paid_amount
        , coalesce(ae.total_actual_outpatient_paid_amount, 0) as total_actual_outpatient_paid_amount
        , coalesce(ae.total_expected_outpatient_paid_amount, 0) as total_expected_outpatient_paid_amount
        , coalesce(ae.total_actual_office_based_paid_amount, 0) as total_actual_office_based_paid_amount
        , coalesce(ae.total_expected_office_based_paid_amount, 0) as total_expected_office_based_paid_amount
        , coalesce(ae.total_actual_other_paid_amount, 0) as total_actual_other_paid_amount
        , coalesce(ae.total_expected_other_paid_amount, 0) as total_expected_other_paid_amount

        , coalesce(ae.total_actual_inpatient_encounter_count, 0) as total_actual_inpatient_encounter_count
        , coalesce(ae.total_expected_inpatient_encounter_count, 0) as total_expected_inpatient_encounter_count
        , coalesce(ae.total_actual_outpatient_encounter_count, 0) as total_actual_outpatient_encounter_count
        , coalesce(ae.total_expected_outpatient_encounter_count, 0) as total_expected_outpatient_encounter_count
        , coalesce(ae.total_actual_office_based_encounter_count, 0) as total_actual_office_based_encounter_count
        , coalesce(ae.total_expected_office_based_encounter_count, 0) as total_expected_office_based_encounter_count
        , coalesce(ae.total_actual_other_encounter_count, 0) as total_actual_other_encounter_count
        , coalesce(ae.total_expected_other_encounter_count, 0) as total_expected_other_encounter_count
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
        , sum(pf.member_months) as total_member_months
        , sum(pf.member_months) / 12 as total_person_years

        , sum(pf.total_actual_paid_amount) as total_actual_paid_amount
        , sum(pf.total_expected_paid_amount) as total_expected_paid_amount
        , sum(pf.total_actual_inpatient_paid_amount) as total_actual_inpatient_paid_amount
        , sum(pf.total_expected_inpatient_paid_amount) as total_expected_inpatient_paid_amount
        , sum(pf.total_actual_outpatient_paid_amount) as total_actual_outpatient_paid_amount
        , sum(pf.total_expected_outpatient_paid_amount) as total_expected_outpatient_paid_amount
        , sum(pf.total_actual_office_based_paid_amount) as total_actual_office_based_paid_amount
        , sum(pf.total_expected_office_based_paid_amount) as total_expected_office_based_paid_amount
        , sum(pf.total_actual_other_paid_amount) as total_actual_other_paid_amount
        , sum(pf.total_expected_other_paid_amount) as total_expected_other_paid_amount

        , sum(pf.total_actual_inpatient_encounter_count) as total_actual_inpatient_encounter_count
        , sum(pf.total_expected_inpatient_encounter_count) as total_expected_inpatient_encounter_count
        , sum(pf.total_actual_outpatient_encounter_count) as total_actual_outpatient_encounter_count
        , sum(pf.total_expected_outpatient_encounter_count) as total_expected_outpatient_encounter_count
        , sum(pf.total_actual_office_based_encounter_count) as total_actual_office_based_encounter_count
        , sum(pf.total_expected_office_based_encounter_count) as total_expected_office_based_encounter_count
        , sum(pf.total_actual_other_encounter_count) as total_actual_other_encounter_count
        , sum(pf.total_expected_other_encounter_count) as total_expected_other_encounter_count
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
        , total_actual_paid_amount / nullif(total_member_months, 0) as actual_paid_amount_pmpm
        , total_expected_paid_amount / nullif(total_member_months, 0) as expected_paid_amount_pmpm
        , (total_actual_paid_amount / nullif(total_member_months, 0))
            / nullif((total_expected_paid_amount / nullif(total_member_months, 0)), 0) as observed_to_expected_paid_amount

        , total_actual_inpatient_paid_amount / nullif(total_member_months, 0) as actual_inpatient_paid_amount_pmpm
        , total_expected_inpatient_paid_amount / nullif(total_member_months, 0) as expected_inpatient_paid_amount_pmpm
        , (total_actual_inpatient_paid_amount / nullif(total_member_months, 0))
            / nullif((total_expected_inpatient_paid_amount / nullif(total_member_months, 0)), 0) as observed_to_expected_inpatient_paid_amount

        , total_actual_outpatient_paid_amount / nullif(total_member_months, 0) as actual_outpatient_paid_amount_pmpm
        , total_expected_outpatient_paid_amount / nullif(total_member_months, 0) as expected_outpatient_paid_amount_pmpm
        , (total_actual_outpatient_paid_amount / nullif(total_member_months, 0))
            / nullif((total_expected_outpatient_paid_amount / nullif(total_member_months, 0)), 0) as observed_to_expected_outpatient_paid_amount

        , total_actual_office_based_paid_amount / nullif(total_member_months, 0) as actual_office_based_paid_amount_pmpm
        , total_expected_office_based_paid_amount / nullif(total_member_months, 0) as expected_office_based_paid_amount_pmpm
        , (total_actual_office_based_paid_amount / nullif(total_member_months, 0))
            / nullif((total_expected_office_based_paid_amount / nullif(total_member_months, 0)), 0) as observed_to_expected_office_based_paid_amount

        , total_actual_other_paid_amount / nullif(total_member_months, 0) as actual_other_paid_amount_pmpm
        , total_expected_other_paid_amount / nullif(total_member_months, 0) as expected_other_paid_amount_pmpm
        , (total_actual_other_paid_amount / nullif(total_member_months, 0))
            / nullif((total_expected_other_paid_amount / nullif(total_member_months, 0)), 0) as observed_to_expected_other_paid_amount

        , total_actual_inpatient_encounter_count / nullif(total_member_months, 0) as actual_inpatient_encounter_count_per_mm
        , total_expected_inpatient_encounter_count / nullif(total_member_months, 0) as expected_inpatient_encounter_count_per_mm
        , (total_actual_inpatient_encounter_count / nullif(total_member_months, 0))
            / nullif((total_expected_inpatient_encounter_count / nullif(total_member_months, 0)), 0) as observed_to_expected_inpatient_encounter_count

        , total_actual_outpatient_encounter_count / nullif(total_member_months, 0) as actual_outpatient_encounter_count_per_mm
        , total_expected_outpatient_encounter_count / nullif(total_member_months, 0) as expected_outpatient_encounter_count_per_mm
        , (total_actual_outpatient_encounter_count / nullif(total_member_months, 0))
            / nullif((total_expected_outpatient_encounter_count / nullif(total_member_months, 0)), 0) as observed_to_expected_outpatient_encounter_count

        , total_actual_office_based_encounter_count / nullif(total_member_months, 0) as actual_office_based_encounter_count_per_mm
        , total_expected_office_based_encounter_count / nullif(total_member_months, 0) as expected_office_based_encounter_count_per_mm
        , (total_actual_office_based_encounter_count / nullif(total_member_months, 0))
            / nullif((total_expected_office_based_encounter_count / nullif(total_member_months, 0)), 0) as observed_to_expected_office_based_encounter_count

        , total_actual_other_encounter_count / nullif(total_member_months, 0) as actual_other_encounter_count_per_mm
        , total_expected_other_encounter_count / nullif(total_member_months, 0) as expected_other_encounter_count_per_mm
        , (total_actual_other_encounter_count / nullif(total_member_months, 0))
            / nullif((total_expected_other_encounter_count / nullif(total_member_months, 0)), 0) as observed_to_expected_other_encounter_count
    from org_totals
    where total_person_years >= 1000

)

, population as (

    select
          sum(total_actual_paid_amount) / nullif(sum(total_member_months), 0) as population_actual_paid_amount_pmpm
        , sum(total_actual_inpatient_paid_amount) / nullif(sum(total_member_months), 0) as population_actual_inpatient_paid_amount_pmpm
        , sum(total_actual_outpatient_paid_amount) / nullif(sum(total_member_months), 0) as population_actual_outpatient_paid_amount_pmpm
        , sum(total_actual_office_based_paid_amount) / nullif(sum(total_member_months), 0) as population_actual_office_based_paid_amount_pmpm
        , sum(total_actual_other_paid_amount) / nullif(sum(total_member_months), 0) as population_actual_other_paid_amount_pmpm

        , sum(total_actual_inpatient_encounter_count) / nullif(sum(total_member_months), 0) as population_actual_inpatient_encounter_count_per_mm
        , sum(total_actual_outpatient_encounter_count) / nullif(sum(total_member_months), 0) as population_actual_outpatient_encounter_count_per_mm
        , sum(total_actual_office_based_encounter_count) / nullif(sum(total_member_months), 0) as population_actual_office_based_encounter_count_per_mm
        , sum(total_actual_other_encounter_count) / nullif(sum(total_member_months), 0) as population_actual_other_encounter_count_per_mm
    from eligible

)

select
      e.*
    , e.observed_to_expected_paid_amount * p.population_actual_paid_amount_pmpm as risk_adj_paid_amount_pmpm
    , e.observed_to_expected_inpatient_paid_amount * p.population_actual_inpatient_paid_amount_pmpm as risk_adj_inpatient_paid_amount_pmpm
    , e.observed_to_expected_outpatient_paid_amount * p.population_actual_outpatient_paid_amount_pmpm as risk_adj_outpatient_paid_amount_pmpm
    , e.observed_to_expected_office_based_paid_amount * p.population_actual_office_based_paid_amount_pmpm as risk_adj_office_based_paid_amount_pmpm
    , e.observed_to_expected_other_paid_amount * p.population_actual_other_paid_amount_pmpm as risk_adj_other_paid_amount_pmpm

    , e.observed_to_expected_inpatient_encounter_count * p.population_actual_inpatient_encounter_count_per_mm as risk_adj_inpatient_encounter_count_per_mm
    , e.observed_to_expected_outpatient_encounter_count * p.population_actual_outpatient_encounter_count_per_mm as risk_adj_outpatient_encounter_count_per_mm
    , e.observed_to_expected_office_based_encounter_count * p.population_actual_office_based_encounter_count_per_mm as risk_adj_office_based_encounter_count_per_mm
    , e.observed_to_expected_other_encounter_count * p.population_actual_other_encounter_count_per_mm as risk_adj_other_encounter_count_per_mm
from eligible as e
cross join population as p

