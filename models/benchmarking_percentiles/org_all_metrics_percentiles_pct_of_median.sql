with pmpm as (

    select
          'pmpm' as metric_group
        , 'paid_pmpm' as metric_name
        , 'actual' as value_type
        , percentile
        , paid_pmpm as metric_value
        , paid_pmpm_pct_of_median as pct_of_median
    from {{ ref('org_pmpm_percentiles_pct_of_median') }}

    union all

    select
          'pmpm' as metric_group
        , 'paid_pmpm' as metric_name
        , 'risk_adj' as value_type
        , percentile
        , risk_adj_pmpm as metric_value
        , risk_adj_pmpm_pct_of_median as pct_of_median
    from {{ ref('org_pmpm_percentiles_pct_of_median') }}

)

, encounter_groups as (

    select
          'encounter_group' as metric_group
        , metric_name
        , value_type
        , percentile
        , metric_value
        , pct_of_median
    from {{ ref('org_encounter_group_metrics_percentiles_pct_of_median') }}

)

, encounter_types as (

    select
          'encounter_type' as metric_group
        , metric_name
        , value_type
        , percentile
        , metric_value
        , pct_of_median
    from {{ ref('org_encounter_type_metrics_percentiles_pct_of_median') }}

)

select * from pmpm
union all
select * from encounter_groups
union all
select * from encounter_types
