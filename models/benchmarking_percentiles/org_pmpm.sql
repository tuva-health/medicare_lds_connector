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
    group by person_id

)

, person_financials as (

    select
          mm.person_id
        , coalesce(pb.total_paid, 0) as total_paid
        , mm.member_months
    from member_months as mm
        left join paid_by_person as pb
            on mm.person_id = pb.person_id

)

, member_orgs as (

    select
          person_id
        , assigned_hospital_npi
        , assigned_hospital_name
    from {{ ref('member_org_assignment') }}

)

select
      mo.assigned_hospital_npi
    , mo.assigned_hospital_name
    , sum(pf.total_paid) as total_paid
    , sum(pf.member_months) as total_member_months
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
