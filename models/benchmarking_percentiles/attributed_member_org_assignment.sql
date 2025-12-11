with assigned as (

    select
          person_id
        , provider_id
    from {{ ref('provider_attribution__assigned_beneficiaries_current') }}
    where provider_id <> '9999999999'

)

, state_lookup as (

    select distinct
          upper(trim(ansi_fips_state_name)) as state_key
        , ansi_fips_state_abbreviation as state_abbrev
    from {{ ref('reference_data__ansi_fips_state') }}

    union

    select distinct
          upper(trim(ansi_fips_state_abbreviation)) as state_key
        , ansi_fips_state_abbreviation as state_abbrev
    from {{ ref('reference_data__ansi_fips_state') }}

)

, assigned_with_state as (

    select
          a.person_id
        , a.provider_id
        , sl.state_abbrev
    from assigned as a
        left join {{ ref('core__patient') }} as p
            on a.person_id = p.person_id
        left join state_lookup as sl
            on upper(trim(p.state)) = sl.state_key

)

, provider_org_crosswalk as (

    select
          provider_id
        , attributed_hospital_npi
        , provider_organization_name
    from {{ ref('provider_org_crosswalk') }}

)

select
      a.person_id
    , a.provider_id
    , a.state_abbrev
    , poc.attributed_hospital_npi as assigned_hospital_npi
    , poc.provider_organization_name as assigned_hospital_name
    , 'attributed' as assignment_source
from assigned_with_state as a
    left join provider_org_crosswalk as poc
        on a.provider_id = poc.provider_id
