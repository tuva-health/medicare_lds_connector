with snf_base_claim as (

    select *
         , left(clm_thru_dt,4) as clm_thru_dt_year
    from {{ source('medicare_lds','snf_base_claim') }}
    where clm_mdcr_non_pmt_rsn_cd is null
    /** filter out denied claims **/
),

/* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
add_claim_id as (

    select
          cast(claim_no as {{ dbt.type_string() }} )
            || cast(clm_thru_dt_year as {{ dbt.type_string() }} )
            || cast(nch_clm_type_cd as {{ dbt.type_string() }} )
          as claim_id
        , *
    from snf_base_claim

),

header_payment as (

    select
          claim_id
        , cast(clm_pmt_amt as {{ dbt.type_numeric() }}) as paid_amount
        , /** Medicare payment **/
          cast(clm_pmt_amt as {{ dbt.type_numeric() }})
          /** benficiary payment **/
          + cast(nch_bene_ip_ddctbl_amt as {{ dbt.type_numeric() }}) + cast(nch_bene_pta_coinsrnc_lblty_am as {{ dbt.type_numeric() }}) + cast(nch_bene_blood_ddctbl_lblty_am as {{ dbt.type_numeric() }})
          /** primary payer payment **/
          + cast(nch_prmry_pyr_clm_pd_amt as {{ dbt.type_numeric() }})
        as total_cost_amount
        , cast(clm_tot_chrg_amt as {{ dbt.type_numeric() }}) as charge_amount
    from add_claim_id

)

select
      b.claim_id
    , cast(l.clm_line_num as integer) as claim_line_number
    , 'institutional' as claim_type
    , cast(b.desy_sort_key as {{ dbt.type_string() }}) as patient_id
    , cast(b.desy_sort_key as {{ dbt.type_string() }}) as member_id
    , date(NULL) as claim_start_date
    , {{ try_to_cast_date('b.clm_thru_dt', 'YYYYMMDD') }} as claim_end_date
     , date(NULL) as claim_line_start_date
    , {{ try_to_cast_date('l.clm_thru_dt', 'YYYYMMDD') }} as claim_line_end_date
    , {{ try_to_cast_date('b.clm_admsn_dt','YYYYMMDD') }} as admission_date
    , {{ try_to_cast_date('b.nch_bene_dschrg_dt','YYYYMMDD') }} as discharge_date
    , cast(NULL as {{ dbt.type_string() }}) as admit_source_code
    , cast(NULL as {{ dbt.type_string() }}) as admit_type_code
    , cast(b.ptnt_dschrg_stus_cd as {{ dbt.type_string() }}) as discharge_disposition_code
    , cast(NULL as {{ dbt.type_string() }}) as place_of_service_code
    , cast(b.clm_fac_type_cd as {{ dbt.type_string() }})
        || cast(b.clm_srvc_clsfctn_type_cd as {{ dbt.type_string() }})
        || cast(b.clm_freq_cd as {{ dbt.type_string() }})
      as bill_type_code
    , cast(b.clm_drg_cd as {{ dbt.type_string() }}) as ms_drg_code
    , cast(NULL as {{ dbt.type_string() }}) as apr_drg_code
    , cast(l.rev_cntr as {{ dbt.type_string() }}) as revenue_center_code
    , cast(regexp_substr(l.rev_cntr_unit_cnt, '.') as integer) as service_unit_quantity
    , cast(l.hcpcs_cd as {{ dbt.type_string() }}) as hcpcs_code
    , cast(l.hcpcs_1st_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_modifier_1
    , cast(l.hcpcs_2nd_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_modifier_2
    , cast(l.hcpcs_3rd_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_modifier_3
    , cast(NULL as {{ dbt.type_string() }}) as hcpcs_modifier_4
    , cast(NULL as {{ dbt.type_string() }}) as hcpcs_modifier_5
    , cast(b.rndrng_physn_npi as {{ dbt.type_string() }}) as rendering_npi
    , cast(NULL as {{ dbt.type_string() }}) as billing_npi
    , cast(b.org_npi_num as {{ dbt.type_string() }}) as facility_npi
    , date(NULL) as paid_date
    , coalesce(
            p.paid_amount
            , cast(0 as {{ dbt.type_numeric() }})
      ) as paid_amount
    , p.total_cost_amount as total_cost_amount
    , cast(NULL as {{ dbt.type_numeric() }}) as allowed_amount
    , p.charge_amount as charge_amount
    , 'icd-10-cm' as diagnosis_code_type
    , cast(b.prncpal_dgns_cd as {{ dbt.type_string() }}) as diagnosis_code_1
    , cast(b.icd_dgns_cd2 as {{ dbt.type_string() }}) as diagnosis_code_2
    , cast(b.icd_dgns_cd3 as {{ dbt.type_string() }}) as diagnosis_code_3
    , cast(b.icd_dgns_cd4 as {{ dbt.type_string() }}) as diagnosis_code_4
    , cast(b.icd_dgns_cd5 as {{ dbt.type_string() }}) as diagnosis_code_5
    , cast(b.icd_dgns_cd6 as {{ dbt.type_string() }}) as diagnosis_code_6
    , cast(b.icd_dgns_cd7 as {{ dbt.type_string() }}) as diagnosis_code_7
    , cast(b.icd_dgns_cd8 as {{ dbt.type_string() }}) as diagnosis_code_8
    , cast(b.icd_dgns_cd9 as {{ dbt.type_string() }}) as diagnosis_code_9
    , cast(b.icd_dgns_cd10 as {{ dbt.type_string() }}) as diagnosis_code_10
    , cast(b.icd_dgns_cd11 as {{ dbt.type_string() }}) as diagnosis_code_11
    , cast(b.icd_dgns_cd12 as {{ dbt.type_string() }}) as diagnosis_code_12
    , cast(b.icd_dgns_cd13 as {{ dbt.type_string() }}) as diagnosis_code_13
    , cast(b.icd_dgns_cd14 as {{ dbt.type_string() }}) as diagnosis_code_14
    , cast(b.icd_dgns_cd15 as {{ dbt.type_string() }}) as diagnosis_code_15
    , cast(b.icd_dgns_cd16 as {{ dbt.type_string() }}) as diagnosis_code_16
    , cast(b.icd_dgns_cd17 as {{ dbt.type_string() }}) as diagnosis_code_17
    , cast(b.icd_dgns_cd18 as {{ dbt.type_string() }}) as diagnosis_code_18
    , cast(b.icd_dgns_cd19 as {{ dbt.type_string() }}) as diagnosis_code_19
    , cast(b.icd_dgns_cd20 as {{ dbt.type_string() }}) as diagnosis_code_20
    , cast(b.icd_dgns_cd21 as {{ dbt.type_string() }}) as diagnosis_code_21
    , cast(b.icd_dgns_cd22 as {{ dbt.type_string() }}) as diagnosis_code_22
    , cast(b.icd_dgns_cd23 as {{ dbt.type_string() }}) as diagnosis_code_23
    , cast(b.icd_dgns_cd24 as {{ dbt.type_string() }}) as diagnosis_code_24
    , cast(b.icd_dgns_cd25 as {{ dbt.type_string() }}) as diagnosis_code_25
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_1
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_2
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_3
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_4
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_5
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_6
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_7
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_8
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_9
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_10
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_11
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_12
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_13
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_14
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_15
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_16
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_17
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_18
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_19
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_20
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_21
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_22
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_23
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_24
    , cast(NULL as {{ dbt.type_string() }}) as diagnosis_poa_25
    , 'icd-10-pcs' as procedure_code_type
    , cast(b.icd_prcdr_cd1 as {{ dbt.type_string() }}) as procedure_code_1
    , cast(b.icd_prcdr_cd2 as {{ dbt.type_string() }}) as procedure_code_2
    , cast(b.icd_prcdr_cd3 as {{ dbt.type_string() }}) as procedure_code_3
    , cast(b.icd_prcdr_cd4 as {{ dbt.type_string() }}) as procedure_code_4
    , cast(b.icd_prcdr_cd5 as {{ dbt.type_string() }}) as procedure_code_5
    , cast(b.icd_prcdr_cd6 as {{ dbt.type_string() }}) as procedure_code_6
    , cast(b.icd_prcdr_cd7 as {{ dbt.type_string() }}) as procedure_code_7
    , cast(b.icd_prcdr_cd8 as {{ dbt.type_string() }}) as procedure_code_8
    , cast(b.icd_prcdr_cd9 as {{ dbt.type_string() }}) as procedure_code_9
    , cast(b.icd_prcdr_cd10 as {{ dbt.type_string() }}) as procedure_code_10
    , cast(b.icd_prcdr_cd11 as {{ dbt.type_string() }}) as procedure_code_11
    , cast(b.icd_prcdr_cd12 as {{ dbt.type_string() }}) as procedure_code_12
    , cast(b.icd_prcdr_cd13 as {{ dbt.type_string() }}) as procedure_code_13
    , cast(b.icd_prcdr_cd14 as {{ dbt.type_string() }}) as procedure_code_14
    , cast(b.icd_prcdr_cd15 as {{ dbt.type_string() }}) as procedure_code_15
    , cast(b.icd_prcdr_cd16 as {{ dbt.type_string() }}) as procedure_code_16
    , cast(b.icd_prcdr_cd17 as {{ dbt.type_string() }}) as procedure_code_17
    , cast(b.icd_prcdr_cd18 as {{ dbt.type_string() }}) as procedure_code_18
    , cast(b.icd_prcdr_cd19 as {{ dbt.type_string() }}) as procedure_code_19
    , cast(b.icd_prcdr_cd20 as {{ dbt.type_string() }}) as procedure_code_20
    , cast(b.icd_prcdr_cd21 as {{ dbt.type_string() }}) as procedure_code_21
    , cast(b.icd_prcdr_cd22 as {{ dbt.type_string() }}) as procedure_code_22
    , cast(b.icd_prcdr_cd23 as {{ dbt.type_string() }}) as procedure_code_23
    , cast(b.icd_prcdr_cd24 as {{ dbt.type_string() }}) as procedure_code_24
    , cast(b.icd_prcdr_cd25 as {{ dbt.type_string() }}) as procedure_code_25
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
    , 'medicare_lds' as data_source
from add_claim_id as b
inner join {{ source('medicare_lds','snf_revenue_center') }} as l
    on b.claim_no = l.claim_no
/* Payment is provided at the header level only.  Populating on revenu center 001 to avoid duplication. */
left join header_payment p
    on b.claim_id = p.claim_id
    and l.rev_cntr = '0001'