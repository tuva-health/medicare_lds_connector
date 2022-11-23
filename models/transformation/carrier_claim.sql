select
    /* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
    cast(b.claim_no || date_part(year,{{ try_to_cast_date('b.clm_thru_dt', 'YYYYMMDD') }}) || b.nch_clm_type_cd as varchar) as claim_id
    ,cast(l.line_num as int) as claim_line_number
    ,cast('professional' as varchar) as claim_type
    ,cast(b.desy_sort_key as varchar) as patient_id
    ,cast(NULL as varchar) as member_id
    ,{{ try_to_cast_date('b.clm_thru_dt', 'YYYYMMDD') }} as claim_start_date
    ,{{ try_to_cast_date('b.clm_thru_dt', 'YYYYMMDD') }} as claim_end_date
    ,{{ try_to_cast_date('l.clm_thru_dt', 'YYYYMMDD') }} as claim_line_start_date
    ,{{ try_to_cast_date('l.clm_thru_dt', 'YYYYMMDD') }} as claim_line_end_date
    ,date(NULL) as admission_date
    ,date(NULL) as discharge_date
    ,cast(NULL as varchar) as admit_source_code
    ,cast(NULL as varchar) as admit_type_code
    ,cast(NULL as varchar) as discharge_disposition_code
    ,cast(l.line_place_of_srvc_cd as varchar) as place_of_service_code
    ,cast(NULL as varchar) as bill_type_code
    ,cast(NULL as varchar) as ms_drg_code
    ,cast(NULL as varchar) as revenue_center_code
    ,cast(l.line_srvc_cnt as int) as service_unit_quantity
    ,cast(l.hcpcs_cd as varchar) as hcpcs_code
    ,cast(l.hcpcs_1st_mdfr_cd as varchar) as hcpcs_modifier_1
    ,cast(l.hcpcs_2nd_mdfr_cd as varchar) as hcpcs_modifier_2
    ,cast(NULL as varchar) as hcpcs_modifier_3
    ,cast(NULL as varchar) as hcpcs_modifier_4
    ,cast(NULL as varchar) as hcpcs_modifier_5
    ,cast(l.prf_physn_npi as varchar) as rendering_npi
    ,cast(NULL as varchar) as billing_npi
    ,cast(l.org_npi_num as varchar) as facility_npi
    ,cast(NULL as varchar) as paid_date
    ,cast(l.line_nch_pmt_amt as numeric(38,2)) as paid_amount
    ,cast(l.line_alowd_chrg_amt as numeric(38,2)) as allowed_amount
    ,cast(l.line_alowd_chrg_amt as numeric(38,2)) as charge_amount
    ,cast(case when b.prncpal_dgns_vrsn_cd = 0 then 'icd-10-cm'
               when b.prncpal_dgns_vrsn_cd = 0 then 'icd-9-cm'
     end as varchar) as diagnosis_code_type
    ,cast(b.prncpal_dgns_cd as varchar) as diagnosis_code_1
    ,cast(b.icd_dgns_cd2 as varchar) as diagnosis_code_2
    ,cast(b.icd_dgns_cd3 as varchar) as diagnosis_code_3
    ,cast(b.icd_dgns_cd4 as varchar) as diagnosis_code_4
    ,cast(b.icd_dgns_cd5 as varchar) as diagnosis_code_5
    ,cast(b.icd_dgns_cd6 as varchar) as diagnosis_code_6
    ,cast(b.icd_dgns_cd7 as varchar) as diagnosis_code_7
    ,cast(b.icd_dgns_cd8 as varchar) as diagnosis_code_8
    ,cast(b.icd_dgns_cd9 as varchar) as diagnosis_code_9
    ,cast(b.icd_dgns_cd10 as varchar) as diagnosis_code_10
    ,cast(b.icd_dgns_cd11 as varchar) as diagnosis_code_11
    ,cast(b.icd_dgns_cd12 as varchar) as diagnosis_code_12
    ,cast(NULL as varchar) as diagnosis_code_13
    ,cast(NULL as varchar) as diagnosis_code_14
    ,cast(NULL as varchar) as diagnosis_code_15
    ,cast(NULL as varchar) as diagnosis_code_16
    ,cast(NULL as varchar) as diagnosis_code_17
    ,cast(NULL as varchar) as diagnosis_code_18
    ,cast(NULL as varchar) as diagnosis_code_19
    ,cast(NULL as varchar) as diagnosis_code_20
    ,cast(NULL as varchar) as diagnosis_code_21
    ,cast(NULL as varchar) as diagnosis_code_22
    ,cast(NULL as varchar) as diagnosis_code_23
    ,cast(NULL as varchar) as diagnosis_code_24
    ,cast(NULL as varchar) as diagnosis_code_25
    ,cast(NULL as varchar) as diagnosis_poa_1
    ,cast(NULL as varchar) as diagnosis_poa_2
    ,cast(NULL as varchar) as diagnosis_poa_3
    ,cast(NULL as varchar) as diagnosis_poa_4
    ,cast(NULL as varchar) as diagnosis_poa_5
    ,cast(NULL as varchar) as diagnosis_poa_6
    ,cast(NULL as varchar) as diagnosis_poa_7
    ,cast(NULL as varchar) as diagnosis_poa_8
    ,cast(NULL as varchar) as diagnosis_poa_9
    ,cast(NULL as varchar) as diagnosis_poa_10
    ,cast(NULL as varchar) as diagnosis_poa_11
    ,cast(NULL as varchar) as diagnosis_poa_12
    ,cast(NULL as varchar) as diagnosis_poa_13
    ,cast(NULL as varchar) as diagnosis_poa_14
    ,cast(NULL as varchar) as diagnosis_poa_15
    ,cast(NULL as varchar) as diagnosis_poa_16
    ,cast(NULL as varchar) as diagnosis_poa_17
    ,cast(NULL as varchar) as diagnosis_poa_18
    ,cast(NULL as varchar) as diagnosis_poa_19
    ,cast(NULL as varchar) as diagnosis_poa_20
    ,cast(NULL as varchar) as diagnosis_poa_21
    ,cast(NULL as varchar) as diagnosis_poa_22
    ,cast(NULL as varchar) as diagnosis_poa_23
    ,cast(NULL as varchar) as diagnosis_poa_24
    ,cast(NULL as varchar) as diagnosis_poa_25
    ,cast(NULL as varchar) as procedure_code_type
    ,cast(NULL as varchar) as procedure_code_1
    ,cast(NULL as varchar) as procedure_code_2
    ,cast(NULL as varchar) as procedure_code_3
    ,cast(NULL as varchar) as procedure_code_4
    ,cast(NULL as varchar) as procedure_code_5
    ,cast(NULL as varchar) as procedure_code_6
    ,cast(NULL as varchar) as procedure_code_7
    ,cast(NULL as varchar) as procedure_code_8
    ,cast(NULL as varchar) as procedure_code_9
    ,cast(NULL as varchar) as procedure_code_10
    ,cast(NULL as varchar) as procedure_code_11
    ,cast(NULL as varchar) as procedure_code_12
    ,cast(NULL as varchar) as procedure_code_13
    ,cast(NULL as varchar) as procedure_code_14
    ,cast(NULL as varchar) as procedure_code_15
    ,cast(NULL as varchar) as procedure_code_16
    ,cast(NULL as varchar) as procedure_code_17
    ,cast(NULL as varchar) as procedure_code_18
    ,cast(NULL as varchar) as procedure_code_19
    ,cast(NULL as varchar) as procedure_code_20
    ,cast(NULL as varchar) as procedure_code_21
    ,cast(NULL as varchar) as procedure_code_22
    ,cast(NULL as varchar) as procedure_code_23
    ,cast(NULL as varchar) as procedure_code_24
    ,cast(NULL as varchar) as procedure_code_25
    ,date(NULL) as procedure_date_1
    ,date(NULL) as procedure_date_2
    ,date(NULL) as procedure_date_3
    ,date(NULL) as procedure_date_4
    ,date(NULL) as procedure_date_5
    ,date(NULL) as procedure_date_6
    ,date(NULL) as procedure_date_7
    ,date(NULL) as procedure_date_8
    ,date(NULL) as procedure_date_9
    ,date(NULL) as procedure_date_10
    ,date(NULL) as procedure_date_11
    ,date(NULL) as procedure_date_12
    ,date(NULL) as procedure_date_13
    ,date(NULL) as procedure_date_14
    ,date(NULL) as procedure_date_15
    ,date(NULL) as procedure_date_16
    ,date(NULL) as procedure_date_17
    ,date(NULL) as procedure_date_18
    ,date(NULL) as procedure_date_19
    ,date(NULL) as procedure_date_20
    ,date(NULL) as procedure_date_21
    ,date(NULL) as procedure_date_22
    ,date(NULL) as procedure_date_23
    ,date(NULL) as procedure_date_24
    ,date(NULL) as procedure_date_25
    ,cast('saf' as varchar) as data_source
from {{ var('carrier_base_claim')}} b
inner join {{ var('carrier_claim_line')}} l
    on b.claim_no = l.claim_no
