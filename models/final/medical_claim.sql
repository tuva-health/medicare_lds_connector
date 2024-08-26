with unioned as (

    {{ dbt_utils.union_relations(relations=[
          ref('carrier_claim')
        , ref('dme_claim')
        , ref('home_health_claim')
        , ref('hospice_claim')
        , ref('inpatient_claim')
        , ref('outpatient_claim')
        , ref('snf_claim')
    ],
    exclude=["_DBT_SOURCE_RELATION"]
) }}

)

, month_start_and_end_dates as (

    select distinct
          year as year_nbr
        , month as month_nbr
        , first_day_of_month
        , last_day_of_month
    from {{ ref('reference_data__calendar') }}

)

, member_months as (

    select distinct
          a.patient_id
        , year_nbr
        , month_nbr
        , a.payer
        , a.plan
        , data_source
    from {{ ref('eligibility') }} as a
        inner join month_start_and_end_dates as b
            on a.enrollment_start_date <= b.last_day_of_month
            and a.enrollment_end_date >= b.first_day_of_month

)

-- limit claims to only enrolled members, removing claims for members that we only have partial claims data for.
select
      c.claim_id
    , c.claim_line_number
    , c.claim_type
    , c.patient_id
    , c.member_id
    , c.payer
    , c.plan
    , c.claim_start_date
    , c.claim_end_date
    , c.claim_line_start_date
    , c.claim_line_end_date
    , c.admission_date
    , c.discharge_date
    , c.admit_source_code
    , c.admit_type_code
    , c.discharge_disposition_code
    , c.place_of_service_code
    , c.bill_type_code
    , c.ms_drg_code
    , c.apr_drg_code
    , c.revenue_center_code
    , c.service_unit_quantity
    , c.hcpcs_code
    , c.hcpcs_modifier_1
    , c.hcpcs_modifier_2
    , c.hcpcs_modifier_3
    , c.hcpcs_modifier_4
    , c.hcpcs_modifier_5
    , c.rendering_npi
    , c.rendering_tin
    , c.billing_npi
    , c.billing_tin
    , c.facility_npi
    , c.paid_date
    , c.paid_amount
    , c.allowed_amount
    , c.charge_amount
    , c.coinsurance_amount
    , c.copayment_amount
    , c.deductible_amount
    , c.total_cost_amount
    , c.diagnosis_code_type
    , c.diagnosis_code_1
    , c.diagnosis_code_2
    , c.diagnosis_code_3
    , c.diagnosis_code_4
    , c.diagnosis_code_5
    , c.diagnosis_code_6
    , c.diagnosis_code_7
    , c.diagnosis_code_8
    , c.diagnosis_code_9
    , c.diagnosis_code_10
    , c.diagnosis_code_11
    , c.diagnosis_code_12
    , c.diagnosis_code_13
    , c.diagnosis_code_14
    , c.diagnosis_code_15
    , c.diagnosis_code_16
    , c.diagnosis_code_17
    , c.diagnosis_code_18
    , c.diagnosis_code_19
    , c.diagnosis_code_20
    , c.diagnosis_code_21
    , c.diagnosis_code_22
    , c.diagnosis_code_23
    , c.diagnosis_code_24
    , c.diagnosis_code_25
    , c.diagnosis_poa_1
    , c.diagnosis_poa_2
    , c.diagnosis_poa_3
    , c.diagnosis_poa_4
    , c.diagnosis_poa_5
    , c.diagnosis_poa_6
    , c.diagnosis_poa_7
    , c.diagnosis_poa_8
    , c.diagnosis_poa_9
    , c.diagnosis_poa_10
    , c.diagnosis_poa_11
    , c.diagnosis_poa_12
    , c.diagnosis_poa_13
    , c.diagnosis_poa_14
    , c.diagnosis_poa_15
    , c.diagnosis_poa_16
    , c.diagnosis_poa_17
    , c.diagnosis_poa_18
    , c.diagnosis_poa_19
    , c.diagnosis_poa_20
    , c.diagnosis_poa_21
    , c.diagnosis_poa_22
    , c.diagnosis_poa_23
    , c.diagnosis_poa_24
    , c.diagnosis_poa_25
    , c.procedure_code_type
    , c.procedure_code_1
    , c.procedure_code_2
    , c.procedure_code_3
    , c.procedure_code_4
    , c.procedure_code_5
    , c.procedure_code_6
    , c.procedure_code_7
    , c.procedure_code_8
    , c.procedure_code_9
    , c.procedure_code_10
    , c.procedure_code_11
    , c.procedure_code_12
    , c.procedure_code_13
    , c.procedure_code_14
    , c.procedure_code_15
    , c.procedure_code_16
    , c.procedure_code_17
    , c.procedure_code_18
    , c.procedure_code_19
    , c.procedure_code_20
    , c.procedure_code_21
    , c.procedure_code_22
    , c.procedure_code_23
    , c.procedure_code_24
    , c.procedure_code_25
    , c.procedure_date_1
    , c.procedure_date_2
    , c.procedure_date_3
    , c.procedure_date_4
    , c.procedure_date_5
    , c.procedure_date_6
    , c.procedure_date_7
    , c.procedure_date_8
    , c.procedure_date_9
    , c.procedure_date_10
    , c.procedure_date_11
    , c.procedure_date_12
    , c.procedure_date_13
    , c.procedure_date_14
    , c.procedure_date_15
    , c.procedure_date_16
    , c.procedure_date_17
    , c.procedure_date_18
    , c.procedure_date_19
    , c.procedure_date_20
    , c.procedure_date_21
    , c.procedure_date_22
    , c.procedure_date_23
    , c.procedure_date_24
    , c.procedure_date_25
    , c.in_network_flag
    , c.data_source
    , c.file_name
    , c.ingest_datetime
from unioned as c
    inner join member_months as mm
        on c.patient_id = mm.patient_id
        and extract(year from c.claim_start_date) = mm.year_nbr
        and extract(month from c.claim_start_date) = mm.month_nbr