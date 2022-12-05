with dme_base_claim as (

    select *
         , date_part(year, {{ try_to_cast_date('clm_thru_dt', 'YYYYMMDD') }} ) as clm_thru_dt_year
    from {{ var('dme_base_claim') }}

)

select
      /* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
      {{ cast_string_or_varchar('b.claim_no') }}
        || {{ cast_string_or_varchar('b.clm_thru_dt_year') }}
        || {{ cast_string_or_varchar('b.nch_clm_type_cd') }}
      as claim_id
    , cast(l.clm_line_num as integer) as claim_line_number
    , 'professional' as claim_type
    , {{ cast_string_or_varchar('b.desy_sort_key') }} as patient_id
    , {{ cast_string_or_varchar('NULL') }} as member_id
    , {{ try_to_cast_date('b.clm_thru_dt', 'YYYYMMDD') }} as claim_start_date
    , {{ try_to_cast_date('b.clm_thru_dt', 'YYYYMMDD') }} as claim_end_date
    , {{ try_to_cast_date('l.clm_thru_dt', 'YYYYMMDD') }} as claim_line_start_date
    , {{ try_to_cast_date('l.clm_thru_dt', 'YYYYMMDD') }} as claim_line_end_date
    , date(NULL) as admission_date
    , date(NULL) as discharge_date
    , {{ cast_string_or_varchar('NULL') }} as admit_source_code
    , {{ cast_string_or_varchar('NULL') }} as admit_type_code
    , {{ cast_string_or_varchar('NULL') }} as discharge_disposition_code
    , {{ cast_string_or_varchar('l.line_place_of_srvc_cd') }} as place_of_service_code
    , {{ cast_string_or_varchar('NULL') }} as bill_type_code
    , {{ cast_string_or_varchar('NULL') }} as ms_drg_code
    , {{ cast_string_or_varchar('NULL') }} as revenue_center_code
    , cast(regexp_substr(l.line_srvc_cnt,'.') as integer) as service_unit_quantity
    , {{ cast_string_or_varchar('l.hcpcs_cd') }} as hcpcs_code
    , {{ cast_string_or_varchar('l.hcpcs_1st_mdfr_cd') }} as hcpcs_modifier_1
    , {{ cast_string_or_varchar('l.hcpcs_2nd_mdfr_cd') }} as hcpcs_modifier_2
    , {{ cast_string_or_varchar('NULL') }} as hcpcs_modifier_3
    , {{ cast_string_or_varchar('NULL') }} as hcpcs_modifier_4
    , {{ cast_string_or_varchar('NULL') }} as hcpcs_modifier_5
    , {{ cast_string_or_varchar('l.prvdr_npi') }} as rendering_npi
    , {{ cast_string_or_varchar('NULL') }} as billing_npi
    , {{ cast_string_or_varchar('NULL') }} as facility_npi
    , date(NULL) as paid_date
    , {{ cast_numeric('l.line_nch_pmt_amt') }} as paid_amount
    , {{ cast_numeric('l.line_alowd_chrg_amt') }} as allowed_amount
    , {{ cast_numeric('l.line_alowd_chrg_amt') }} as charge_amount
    , case when b.prncpal_dgns_vrsn_cd = 0 then 'icd-10-cm'
           when b.prncpal_dgns_vrsn_cd = 9 then 'icd-9-cm'
           when b.prncpal_dgns_vrsn_cd is null then 'icd-9-cm'
      end as diagnosis_code_type
    , {{ cast_string_or_varchar('b.prncpal_dgns_cd') }} as diagnosis_code_1
    , {{ cast_string_or_varchar('b.icd_dgns_cd2') }} as diagnosis_code_2
    , {{ cast_string_or_varchar('b.icd_dgns_cd3') }} as diagnosis_code_3
    , {{ cast_string_or_varchar('b.icd_dgns_cd4') }} as diagnosis_code_4
    , {{ cast_string_or_varchar('b.icd_dgns_cd5') }} as diagnosis_code_5
    , {{ cast_string_or_varchar('b.icd_dgns_cd6') }} as diagnosis_code_6
    , {{ cast_string_or_varchar('b.icd_dgns_cd7') }} as diagnosis_code_7
    , {{ cast_string_or_varchar('b.icd_dgns_cd8') }} as diagnosis_code_8
    , {{ cast_string_or_varchar('b.icd_dgns_cd9') }} as diagnosis_code_9
    , {{ cast_string_or_varchar('b.icd_dgns_cd10') }} as diagnosis_code_10
    , {{ cast_string_or_varchar('b.icd_dgns_cd11') }} as diagnosis_code_11
    , {{ cast_string_or_varchar('b.icd_dgns_cd12') }} as diagnosis_code_12
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_13
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_14
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_15
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_16
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_17
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_18
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_19
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_20
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_21
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_22
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_23
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_24
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_code_25
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_1
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_2
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_3
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_4
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_5
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_6
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_7
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_8
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_9
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_10
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_11
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_12
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_13
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_14
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_15
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_16
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_17
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_18
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_19
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_20
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_21
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_22
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_23
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_24
    , {{ cast_string_or_varchar('NULL') }} as diagnosis_poa_25
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_type
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_1
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_2
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_3
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_4
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_5
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_6
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_7
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_8
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_9
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_10
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_11
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_12
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_13
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_14
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_15
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_16
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_17
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_18
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_19
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_20
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_21
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_22
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_23
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_24
    , {{ cast_string_or_varchar('NULL') }} as procedure_code_25
    , date(NULL) as procedure_date_1
    , date(NULL) as procedure_date_2
    , date(NULL) as procedure_date_3
    , date(NULL) as procedure_date_4
    , date(NULL) as procedure_date_5
    , date(NULL) as procedure_date_6
    , date(NULL) as procedure_date_7
    , date(NULL) as procedure_date_8
    , date(NULL) as procedure_date_9
    , date(NULL) as procedure_date_10
    , date(NULL) as procedure_date_11
    , date(NULL) as procedure_date_12
    , date(NULL) as procedure_date_13
    , date(NULL) as procedure_date_14
    , date(NULL) as procedure_date_15
    , date(NULL) as procedure_date_16
    , date(NULL) as procedure_date_17
    , date(NULL) as procedure_date_18
    , date(NULL) as procedure_date_19
    , date(NULL) as procedure_date_20
    , date(NULL) as procedure_date_21
    , date(NULL) as procedure_date_22
    , date(NULL) as procedure_date_23
    , date(NULL) as procedure_date_24
    , date(NULL) as procedure_date_25
    , 'saf' as data_source
from dme_base_claim as b
inner join {{ var('dme_claim_line') }} as l
    on b.claim_no = l.claim_no
