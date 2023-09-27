with demographics as (

    select * from {{ source('medicare_lds','master_beneficiary_summary') }}

),

unpivot_dual_status as (

    select
          desy_sort_key
        , right(month,2) as month
        , reference_year as year
        , dual_status as dual_status
    from demographics
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

),

unpivot_medicare_status as (

    select
          desy_sort_key
        , right(month,2) as month
        , reference_year as year
        , medicare_status
    from demographics
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
      demographics.desy_sort_key as desy_sort_key
    , demographics.age as age
    , demographics.sex_code as sex_code
    , demographics.race_code as race_code
    , demographics.state_code as state_code
    , demographics.date_of_death as date_of_death
    , demographics.hi_coverage as hi_coverage
    , demographics.smi_coverage as smi_coverage
    , demographics.hmo_coverage as hmo_coverage
    , demographics.orig_reason_for_entitlement as orig_reason_for_entitlement
    , unpivot_dual_status.dual_status as dual_status
    , unpivot_medicare_status.medicare_status as medicare_status
    , unpivot_dual_status.month as month
    , unpivot_dual_status.year as year
    , concat(
          unpivot_dual_status.year
        , unpivot_dual_status.month
      ) as year_month
from demographics
     inner join unpivot_dual_status
         on demographics.desy_sort_key = unpivot_dual_status.desy_sort_key
         and demographics.reference_year = unpivot_dual_status.year
     left join unpivot_medicare_status
         on unpivot_dual_status.desy_sort_key = unpivot_medicare_status.desy_sort_key
         and unpivot_dual_status.month = unpivot_medicare_status.month
         and unpivot_dual_status.year = unpivot_medicare_status.year