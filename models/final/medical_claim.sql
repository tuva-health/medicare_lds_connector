{{ dbt_utils.union_relations(
    relations=[ref('carrier_claim'), ref('dme_claim'), ref('home_health_claim'), ref('hospice_claim')
        , ref('inpatient_claim'), ref('outpatient_claim'), ref('snf_claim')],
    exclude=["_DBT_SOURCE_RELATION"]
) }}
