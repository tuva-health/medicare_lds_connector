with attributed as (

    select
          person_id
        , provider_id
        , state_abbrev
        , assigned_hospital_npi
        , assigned_hospital_name
        , assignment_source
    from {{ ref('attributed_member_org_assignment') }}

)

, unattributed as (

    select
          person_id
        , provider_id
        , state_abbrev
        , assigned_hospital_npi
        , assigned_hospital_name
        , assignment_source
    from {{ ref('unattributed_member_org_assignment') }}

)

select * from attributed

union all

select * from unattributed
