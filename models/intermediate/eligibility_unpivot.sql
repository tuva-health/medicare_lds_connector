with demographics as (

    select * from {{ ref('stg_master_beneficiary_summary') }}

)

, unpivot_dual_status as (

    select
          desy_sort_key
        , right(month,2) as month
        , reference_year as year
        , dual_status as dual_status
    from demographics
    unpivot(
            dual_status for month in (dual_stus_cd_01
                                      ,dual_stus_cd_02
                                      ,dual_stus_cd_03
                                      ,dual_stus_cd_04
                                      ,dual_stus_cd_05
                                      ,dual_stus_cd_06
                                      ,dual_stus_cd_07
                                      ,dual_stus_cd_08
                                      ,dual_stus_cd_09
                                      ,dual_stus_cd_10
                                      ,dual_stus_cd_11
                                      ,dual_stus_cd_12)
            )p1

)

, unpivot_medicare_status as (

    select
          desy_sort_key
        , right(month,2) as month
        , reference_year as year
        , medicare_status
    from demographics
    unpivot(
            medicare_status for month in (mdcr_status_code_01
                                          ,mdcr_status_code_02
                                          ,mdcr_status_code_03
                                          ,mdcr_status_code_04
                                          ,mdcr_status_code_05
                                          ,mdcr_status_code_06
                                          ,mdcr_status_code_07
                                          ,mdcr_status_code_08
                                          ,mdcr_status_code_09
                                          ,mdcr_status_code_10
                                          ,mdcr_status_code_11
                                          ,mdcr_status_code_12)
            )p1

)

, unpivot_hmo_status as (

    select
          desy_sort_key
        , case when length(month) = 14 then substring(month, 14, 1)  -- 'entitlement_buy_in_ind1' -> '1'
               when length(month) = 15 then substring(month, 14, 2)  -- 'entitlement_buy_in_ind10' -> '10'
               else null end as month
        , reference_year as year
        , hmo_status
    from demographics
    unpivot(
            hmo_status for month in (hmo_indicator1
                                     ,hmo_indicator2
                                     ,hmo_indicator3
                                     ,hmo_indicator4
                                     ,hmo_indicator5
                                     ,hmo_indicator6
                                     ,hmo_indicator7
                                     ,hmo_indicator8
                                     ,hmo_indicator9
                                     ,hmo_indicator10
                                     ,hmo_indicator11
                                     ,hmo_indicator12)
            )p1

)

, unpivot_entitlement as (

    select
          desy_sort_key
        , case when length(month) = 23 then substring(month, 23, 1)  -- 'entitlement_buy_in_ind1' -> '1'
               when length(month) = 24 then substring(month, 23, 2)  -- 'entitlement_buy_in_ind10' -> '10'
               else null end as month
        , reference_year as year
        , entitlement
    from demographics
    unpivot(
            entitlement for month in (entitlement_buy_in_ind1
                                      ,entitlement_buy_in_ind2
                                      ,entitlement_buy_in_ind3
                                      ,entitlement_buy_in_ind4
                                      ,entitlement_buy_in_ind5
                                      ,entitlement_buy_in_ind6
                                      ,entitlement_buy_in_ind7
                                      ,entitlement_buy_in_ind8
                                      ,entitlement_buy_in_ind9
                                      ,entitlement_buy_in_ind10
                                      ,entitlement_buy_in_ind11
                                      ,entitlement_buy_in_ind12)
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
    , unpivot_hmo_status.hmo_status
    , unpivot_entitlement.entitlement
    , demographics.file_name
    , demographics.ingest_datetime
from demographics
     inner join unpivot_dual_status
         on demographics.desy_sort_key = unpivot_dual_status.desy_sort_key
         and demographics.reference_year = unpivot_dual_status.year
     left join unpivot_medicare_status
         on unpivot_dual_status.desy_sort_key = unpivot_medicare_status.desy_sort_key
         and unpivot_dual_status.month = unpivot_medicare_status.month
         and unpivot_dual_status.year = unpivot_medicare_status.year
     left join unpivot_hmo_status
         on unpivot_dual_status.desy_sort_key = unpivot_hmo_status.desy_sort_key
         and cast(unpivot_dual_status.month as int) = cast(unpivot_hmo_status.month as int)
         and unpivot_dual_status.year = unpivot_hmo_status.year
     left join unpivot_entitlement
         on unpivot_dual_status.desy_sort_key = unpivot_entitlement.desy_sort_key
         and cast(unpivot_dual_status.month as int) = cast(unpivot_entitlement.month as int)
         and unpivot_dual_status.year = unpivot_entitlement.year