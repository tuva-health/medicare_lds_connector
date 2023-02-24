with outpatient_base_claim as (

    select *
         , left(clm_thru_dt,4) as clm_thru_dt_year
    from {{ var('outpatient_base_claim') }}
)

select
      /* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
      {{ cast_string_or_varchar('b.claim_no') }}
        || {{ cast_string_or_varchar('b.clm_thru_dt_year') }}
        || {{ cast_string_or_varchar('b.nch_clm_type_cd') }}
      as claim_id
    , cast(l.clm_line_num as integer) as claim_line_number
    , 'institutional' as claim_type
    , {{ cast_string_or_varchar('b.desy_sort_key') }} as patient_id
    , {{ cast_string_or_varchar('NULL') }} as member_id
    , date(NULL) as claim_start_date
    , {{ try_to_cast_date('b.clm_thru_dt', 'YYYYMMDD') }} as claim_end_date
    , {{ try_to_cast_date('l.rev_cntr_dt', 'YYYYMMDD') }} as claim_line_start_date
    , {{ try_to_cast_date('l.rev_cntr_dt', 'YYYYMMDD') }} as claim_line_end_date
    , date(NULL) as admission_date
    , date(NULL) as discharge_date
    , {{ cast_string_or_varchar('NULL') }} as admit_source_code
    , {{ cast_string_or_varchar('NULL') }} as admit_type_code
    , {{ cast_string_or_varchar('b.ptnt_dschrg_stus_cd') }} as discharge_disposition_code
    , {{ cast_string_or_varchar('NULL') }} as place_of_service_code
    , {{ cast_string_or_varchar('b.clm_fac_type_cd') }}
        || {{ cast_string_or_varchar('b.clm_srvc_clsfctn_type_cd') }}
        || {{ cast_string_or_varchar('b.clm_freq_cd') }}
      as bill_type_code
    , {{ cast_string_or_varchar('NULL') }} as ms_drg_code
    , {{ cast_string_or_varchar('NULL') }} as apr_drg_code
    , {{ cast_string_or_varchar('l.rev_cntr') }} as revenue_center_code
    , cast(regexp_substr(l.rev_cntr_unit_cnt, '.') as integer) as service_unit_quantity
    , {{ cast_string_or_varchar('l.hcpcs_cd') }} as hcpcs_code
    , {{ cast_string_or_varchar('l.hcpcs_1st_mdfr_cd') }} as hcpcs_modifier_1
    , {{ cast_string_or_varchar('l.hcpcs_2nd_mdfr_cd') }} as hcpcs_modifier_2
    , {{ cast_string_or_varchar('l.hcpcs_3rd_mdfr_cd') }} as hcpcs_modifier_3
    , {{ cast_string_or_varchar('l.hcpcs_4th_mdfr_cd') }} as hcpcs_modifier_4
    , {{ cast_string_or_varchar('NULL') }} as hcpcs_modifier_5
    , {{ cast_string_or_varchar('b.rndrng_physn_npi') }} as rendering_npi
    , {{ cast_string_or_varchar('NULL') }} as billing_npi
    , {{ cast_string_or_varchar('b.org_npi_num') }} as facility_npi
    , date(NULL) as paid_date
    , {{ cast_numeric('l.rev_cntr_pmt_amt_amt') }} as paid_amount
    , {{ cast_numeric('NULL') }} as allowed_amount
    , {{ cast_numeric('l.rev_cntr_tot_chrg_amt') }} as charge_amount
    , 'icd-10-cm' as diagnosis_code_type
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
    , {{ cast_string_or_varchar('b.icd_dgns_cd13') }} as diagnosis_code_13
    , {{ cast_string_or_varchar('b.icd_dgns_cd14') }} as diagnosis_code_14
    , {{ cast_string_or_varchar('b.icd_dgns_cd15') }} as diagnosis_code_15
    , {{ cast_string_or_varchar('b.icd_dgns_cd16') }} as diagnosis_code_16
    , {{ cast_string_or_varchar('b.icd_dgns_cd17') }} as diagnosis_code_17
    , {{ cast_string_or_varchar('b.icd_dgns_cd18') }} as diagnosis_code_18
    , {{ cast_string_or_varchar('b.icd_dgns_cd19') }} as diagnosis_code_19
    , {{ cast_string_or_varchar('b.icd_dgns_cd20') }} as diagnosis_code_20
    , {{ cast_string_or_varchar('b.icd_dgns_cd21') }} as diagnosis_code_21
    , {{ cast_string_or_varchar('b.icd_dgns_cd22') }} as diagnosis_code_22
    , {{ cast_string_or_varchar('b.icd_dgns_cd23') }} as diagnosis_code_23
    , {{ cast_string_or_varchar('b.icd_dgns_cd24') }} as diagnosis_code_24
    , {{ cast_string_or_varchar('b.icd_dgns_cd25') }} as diagnosis_code_25
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
    , 'icd-10-pcs' as procedure_code_type
    , {{ cast_string_or_varchar('b.icd_prcdr_cd1') }} as procedure_code_1
    , {{ cast_string_or_varchar('b.icd_prcdr_cd2') }} as procedure_code_2
    , {{ cast_string_or_varchar('b.icd_prcdr_cd3') }} as procedure_code_3
    , {{ cast_string_or_varchar('b.icd_prcdr_cd4') }} as procedure_code_4
    , {{ cast_string_or_varchar('b.icd_prcdr_cd5') }} as procedure_code_5
    , {{ cast_string_or_varchar('b.icd_prcdr_cd6') }} as procedure_code_6
    , {{ cast_string_or_varchar('b.icd_prcdr_cd7') }} as procedure_code_7
    , {{ cast_string_or_varchar('b.icd_prcdr_cd8') }} as procedure_code_8
    , {{ cast_string_or_varchar('b.icd_prcdr_cd9') }} as procedure_code_9
    , {{ cast_string_or_varchar('b.icd_prcdr_cd10') }} as procedure_code_10
    , {{ cast_string_or_varchar('b.icd_prcdr_cd11') }} as procedure_code_11
    , {{ cast_string_or_varchar('b.icd_prcdr_cd12') }} as procedure_code_12
    , {{ cast_string_or_varchar('b.icd_prcdr_cd13') }} as procedure_code_13
    , {{ cast_string_or_varchar('b.icd_prcdr_cd14') }} as procedure_code_14
    , {{ cast_string_or_varchar('b.icd_prcdr_cd15') }} as procedure_code_15
    , {{ cast_string_or_varchar('b.icd_prcdr_cd16') }} as procedure_code_16
    , {{ cast_string_or_varchar('b.icd_prcdr_cd17') }} as procedure_code_17
    , {{ cast_string_or_varchar('b.icd_prcdr_cd18') }} as procedure_code_18
    , {{ cast_string_or_varchar('b.icd_prcdr_cd19') }} as procedure_code_19
    , {{ cast_string_or_varchar('b.icd_prcdr_cd20') }} as procedure_code_20
    , {{ cast_string_or_varchar('b.icd_prcdr_cd21') }} as procedure_code_21
    , {{ cast_string_or_varchar('b.icd_prcdr_cd22') }} as procedure_code_22
    , {{ cast_string_or_varchar('b.icd_prcdr_cd23') }} as procedure_code_23
    , {{ cast_string_or_varchar('b.icd_prcdr_cd24') }} as procedure_code_24
    , {{ cast_string_or_varchar('b.icd_prcdr_cd25') }} as procedure_code_25
    , {{ try_to_cast_date('b.prcdr_dt1', 'YYYYMMDD') }} as procedure_date_1
    , {{ try_to_cast_date('b.prcdr_dt2', 'YYYYMMDD') }} as procedure_date_2
    , {{ try_to_cast_date('b.prcdr_dt3', 'YYYYMMDD') }} as procedure_date_3
    , {{ try_to_cast_date('b.prcdr_dt4', 'YYYYMMDD') }} as procedure_date_4
    , {{ try_to_cast_date('b.prcdr_dt5', 'YYYYMMDD') }} as procedure_date_5
    , {{ try_to_cast_date('b.prcdr_dt6', 'YYYYMMDD') }} as procedure_date_6
    , {{ try_to_cast_date('b.prcdr_dt7', 'YYYYMMDD') }} as procedure_date_7
    , {{ try_to_cast_date('b.prcdr_dt8', 'YYYYMMDD') }} as procedure_date_8
    , {{ try_to_cast_date('b.prcdr_dt9', 'YYYYMMDD') }} as procedure_date_9
    , {{ try_to_cast_date('b.prcdr_dt10', 'YYYYMMDD') }} as procedure_date_10
    , {{ try_to_cast_date('b.prcdr_dt11', 'YYYYMMDD') }} as procedure_date_11
    , {{ try_to_cast_date('b.prcdr_dt12', 'YYYYMMDD') }} as procedure_date_12
    , {{ try_to_cast_date('b.prcdr_dt13', 'YYYYMMDD') }} as procedure_date_13
    , {{ try_to_cast_date('b.prcdr_dt14', 'YYYYMMDD') }} as procedure_date_14
    , {{ try_to_cast_date('b.prcdr_dt15', 'YYYYMMDD') }} as procedure_date_15
    , {{ try_to_cast_date('b.prcdr_dt16', 'YYYYMMDD') }} as procedure_date_16
    , {{ try_to_cast_date('b.prcdr_dt17', 'YYYYMMDD') }} as procedure_date_17
    , {{ try_to_cast_date('b.prcdr_dt18', 'YYYYMMDD') }} as procedure_date_18
    , {{ try_to_cast_date('b.prcdr_dt19', 'YYYYMMDD') }} as procedure_date_19
    , {{ try_to_cast_date('b.prcdr_dt20', 'YYYYMMDD') }} as procedure_date_20
    , {{ try_to_cast_date('b.prcdr_dt21', 'YYYYMMDD') }} as procedure_date_21
    , {{ try_to_cast_date('b.prcdr_dt22', 'YYYYMMDD') }} as procedure_date_22
    , {{ try_to_cast_date('b.prcdr_dt23', 'YYYYMMDD') }} as procedure_date_23
    , {{ try_to_cast_date('b.prcdr_dt24', 'YYYYMMDD') }} as procedure_date_24
    , {{ try_to_cast_date('b.prcdr_dt25', 'YYYYMMDD') }} as procedure_date_25
    , 'saf' as data_source
from outpatient_base_claim as b
inner join {{ var('outpatient_revenue_center') }} as l
    on b.claim_no = l.claim_no