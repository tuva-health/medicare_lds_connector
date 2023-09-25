/*
  This model takes in eligibility data on the member month grain and converts
  it to enrollment date spans using row number, lag, and Medicare/Dual Status
  to account for continuous enrollment and gaps in coverage.
*/

with eligibility_unpivot as (

    select *
         , {{ to_date('year_month', 'YYYYMM') }} as enrollment_date
         , cast(year as integer) - cast(age as integer) as birth_year
    from {{ ref('eligibility_unpivot') }}

),

medicare_state_fips as (

    select * from {{ ref('terminology__ssa_fips_state') }}

),

add_row_num as (

    select
          desy_sort_key
        , enrollment_date
        , row_number() over (
            partition by desy_sort_key
            order by enrollment_date
          ) as row_num
        , case
            when medicare_status in (null, '00')
            and dual_status in (null, '00', '99', 'NA') then 1
            else 0
          end as disenrolled_flag
    from eligibility_unpivot

),

/*
    remove member months where the patient was not enrolled medicare
    and did not have dual medicaid status
*/
remove_disenrolled_months as (

     select
          desy_sort_key
        , enrollment_date
        , row_num
        , disenrolled_flag
    from add_row_num
    where disenrolled_flag = 0

),

add_lag_enrollment as (

    select
          desy_sort_key
        , enrollment_date
        , row_num
        , lag(enrollment_date) over (
            partition by desy_sort_key
            order by row_num
          ) as lag_enrollment
    from remove_disenrolled_months

),

calculate_lag_diff as (

    select
          desy_sort_key
        , enrollment_date
        , row_num
        , lag_enrollment
        , {{ datediff('lag_enrollment', 'enrollment_date', 'month') }} as lag_diff
    from add_lag_enrollment

),

calculate_gaps as (

     select
          desy_sort_key
        , enrollment_date
        , row_num
        , lag_enrollment
        , lag_diff
        , case
            when lag_diff > 1 then 1
            else 0
          end as gap_flag
    from calculate_lag_diff

),

calculate_groups as (

     select
          desy_sort_key
        , enrollment_date
        , row_num
        , gap_flag
        , sum(gap_flag) over (
            partition by desy_sort_key
            order by row_num
            rows between unbounded preceding and current row
          ) as row_group
    from calculate_gaps

),

enrollment_span as (

    select
          desy_sort_key
        , row_group
        , min(enrollment_date) as enrollment_start_date
        , max(enrollment_date) as enrollment_end_date_max
        , last_day(max(enrollment_date)) as enrollment_end_date_last
    from calculate_groups
    group by desy_sort_key, row_group

),

joined as (

    select
          cast(enrollment_span.desy_sort_key as {{ dbt.type_string() }} ) as patient_id
        , cast(NULL as {{ dbt.type_string() }} ) as member_id
        , case eligibility_unpivot.sex_code
               when '0' then 'unknown'
               when '1' then 'male'
               when '2' then 'female'
          end as gender
        , case eligibility_unpivot.race_code
               when '0' then 'unknown'
               when '1' then 'white'
               when '2' then 'black or african american'
               when '3' then 'other race'
               when '4' then 'asian'
               when '5' then 'other race'
               when '6' then 'american indian or alaska native'
          end as race
        , {{ to_date('eligibility_unpivot.birth_year', 'YYYY') }} as birth_date
        , {{ to_date('eligibility_unpivot.date_of_death', 'YYYYMMDD') }} as death_date
        , cast(case
               when eligibility_unpivot.date_of_death is null then 0
               else 1
          end as integer) as death_flag
        , enrollment_span.enrollment_start_date
        , enrollment_span.enrollment_end_date_last as enrollment_end_date
        , 'medicare' as payer
        , 'medicare' as payer_type
        , cast(eligibility_unpivot.orig_reason_for_entitlement as {{ dbt.type_string() }} ) as original_reason_entitlement_code
        , cast(eligibility_unpivot.dual_status as {{ dbt.type_string() }} ) as dual_status_code
        , cast(eligibility_unpivot.medicare_status as {{ dbt.type_string() }} ) as medicare_status_code
        , cast(NULL as {{ dbt.type_string() }} ) as first_name
        , cast(NULL as {{ dbt.type_string() }} ) as last_name
        , cast(NULL as {{ dbt.type_string() }} ) as address
        , cast(NULL as {{ dbt.type_string() }} ) as city
        , cast(medicare_state_fips.ssa_fips_state_name as {{ dbt.type_string() }} ) as state
        , cast(NULL as {{ dbt.type_string() }} ) as zip_code
        , cast(NULL as {{ dbt.type_string() }} ) as phone
        , 'medicare_lds' as data_source
    from enrollment_span
         left join eligibility_unpivot
            on enrollment_span.desy_sort_key = eligibility_unpivot.desy_sort_key
            and enrollment_span.enrollment_end_date_max = eligibility_unpivot.enrollment_date
         left join medicare_state_fips
            on eligibility_unpivot.state_code = medicare_state_fips.ssa_fips_state_code

)

select * from joined