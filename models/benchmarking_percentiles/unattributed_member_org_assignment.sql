with unattributed as (

    select distinct
        person_id
    from {{ ref('provider_attribution__assigned_beneficiaries_current') }}
    where provider_id = '9999999999'

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

, unattributed_with_state as (

    select
          u.person_id
        , sl.state_abbrev
        , {{ dbt_utils.generate_surrogate_key(['u.person_id']) }} as person_sort_key
    from unattributed as u
        inner join {{ ref('core__patient') }} as p
            on u.person_id = p.person_id
        inner join state_lookup as sl
            on upper(trim(p.state)) = sl.state_key

)

, hospital_orgs as (

    select distinct
          tp.npi as hospital_npi
        , upper(trim(tp.practice_state)) as state_abbrev
    from {{ ref('terminology__provider') }} as tp
    where tp.entity_type_description = 'Organization'
      and tp.primary_specialty_description in (
          'General Acute Care Hospital'
        , 'Critical Access Hospital'
        , 'Children''s Hospital'
        , 'Long Term Care Hospital'
        , 'Rehabilitation Hospital'
        , 'Psychiatric Hospital'
        , 'Chronic Disease Hospital'
        , 'Military Hospital'
      )
      and tp.practice_state is not null

)

, org_ranked as (

    select
          state_abbrev
        , hospital_npi
        , row_number() over (
            partition by state_abbrev
            order by hospital_npi
          ) as org_rank
        , count(*) over (
            partition by state_abbrev
          ) as org_count
    from hospital_orgs

)

, members_ranked as (

    select
          person_id
        , state_abbrev
        , row_number() over (
            partition by state_abbrev
            order by person_sort_key
          ) as member_rank
    from unattributed_with_state

)

, member_slots as (

    select
          m.person_id
        , m.state_abbrev
        , ((m.member_rank - 1) % o.org_count) + 1 as assigned_org_rank
    from members_ranked as m
        inner join (
            select distinct
                  state_abbrev
                , org_count
            from org_ranked
        ) as o
            on o.state_abbrev = m.state_abbrev

)

select
      ms.person_id
    , cast(null as {{ dbt.type_string() }}) as provider_id
    , ms.state_abbrev
    , o.hospital_npi as assigned_hospital_npi
    , p_hosp.provider_organization_name as assigned_hospital_name
    , 'unattributed_no_claims' as assignment_source
from member_slots as ms
    inner join org_ranked as o
        on o.state_abbrev = ms.state_abbrev
        and o.org_rank = ms.assigned_org_rank
    left join {{ ref('terminology__provider') }} as p_hosp
        on p_hosp.npi = o.hospital_npi
