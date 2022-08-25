-----------------------------------------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      August 2022
-- Purpose      Create eligibility file at the month grain.
-----------------------------------------------------------------------------------------------------------------
-- Modification History
--
-----------------------------------------------------------------------------------------------------------------

with dual_status as(
  select 
    desy_sort_key 
    ,right(month,2) as month
    ,reference_year as year

    ,dual_status as dual_status
  from {{ var('master_beneficiary_summary')}}
    unpivot(
            dual_status for month in (DUAL_STUS_CD_01
                                      ,DUAL_STUS_CD_02
                                      ,DUAL_STUS_CD_03
                                      ,DUAL_STUS_CD_04
                                      ,DUAL_STUS_CD_05
                                      ,DUAL_STUS_CD_06
                                      ,DUAL_STUS_CD_07
                                      ,DUAL_STUS_CD_08
                                      ,DUAL_STUS_CD_09
                                      ,DUAL_STUS_CD_10
                                      ,DUAL_STUS_CD_11
                                      ,DUAL_STUS_CD_12)
            )p1
)
, medicare_status as(
  select 
    desy_sort_key 
    ,right(month,2) as month
    ,REFERENCE_YEAR as year
    ,medicare_status
  from {{ var('master_beneficiary_summary')}}
    unpivot(
            medicare_status for month in (MDCR_STATUS_CODE_01
                                          ,MDCR_STATUS_CODE_02
                                          ,MDCR_STATUS_CODE_03
                                          ,MDCR_STATUS_CODE_04
                                          ,MDCR_STATUS_CODE_05
                                          ,MDCR_STATUS_CODE_06
                                          ,MDCR_STATUS_CODE_07
                                          ,MDCR_STATUS_CODE_08
                                          ,MDCR_STATUS_CODE_09
                                          ,MDCR_STATUS_CODE_10
                                          ,MDCR_STATUS_CODE_11
                                          ,MDCR_STATUS_CODE_12)
            )p1
)



select
  b.desy_sort_key as patient_id
  ,case when SEX_CODE = 0 then 'unknown'
         when SEX_CODE = 1 then 'male'
         when SEX_CODE = 2 then 'female'
  end as gender
  ,date(b.REFERENCE_YEAR - b.AGE) as birth_date
  ,case when RACE_CODE = 0 then 'unknown'
         when RACE_CODE = 1 then 'white'
         when RACE_CODE = 2 then 'black'
         when RACE_CODE = 3 then 'other'
         when RACE_CODE = 4 then 'asian'
         when RACE_CODE = 5 then 'hispanic'
         when RACE_CODE = 6 then 'north american native'
  end as race
  ,NULL as zip_code
  ,f.state as state
  ,case when DATE_OF_DEATH is not null then 1
       else 0
  end as deceased_flag
  ,date(DATE_OF_DEATH) as death_date
  ,'medicare' as payer
  ,'medicare' as payer_type
  ,d.dual_status as dual_status
  ,m.medicare_status as medicare_status
  ,cast(d.month as int) as month
  ,cast(d.year as int) as year
from {{ var('master_beneficiary_summary')}} b
inner join dual_status d
    on b.desy_sort_key = d.desy_sort_key
left join medicare_status m
    on d.desy_sort_key = m.desy_sort_key
    and d.month = m.month
    and d.year = m.year
left join {{ ref('medicare_state_fips')}} f
    on b.state_code = f.fips_code
