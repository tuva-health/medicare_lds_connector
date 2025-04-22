with carrier_base_claim as (

    select *
         , left(cast(clm_thru_dt as {{ dbt.type_string() }} ) ,4) as clm_thru_dt_year
    from {{ ref('stg_carrier_base_claim') }}
    where carr_clm_pmt_dnl_cd <> '0'
    /** filter out denied claims **/

)

, claim_start_date as (

    select
          claim_no
        , min(line_last_expns_dt) as claim_start_date
    from {{ ref('stg_carrier_claim_line') }}
    group by claim_no

)

select
      /* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
      b.claim_no
        || b.clm_thru_dt_year
        || b.nch_clm_type_cd
      as claim_id
    , l.line_num as claim_line_number
    , cast('professional' as {{ dbt.type_string() }} ) as claim_type
    , b.desy_sort_key as person_id
    , b.desy_sort_key as member_id
    , cast('medicare' as {{ dbt.type_string() }} ) as payer
    , cast('medicare' as {{ dbt.type_string() }} ) as plan
    , claim_start_date
    , b.clm_thru_dt as claim_end_date
    , line_last_expns_dt as claim_line_start_date
    , line_last_expns_dt as claim_line_end_date
    , date(NULL) as admission_date
    , date(NULL) as discharge_date
    , cast(NULL as {{ dbt.type_string() }} ) as admit_source_code
    , cast(NULL as {{ dbt.type_string() }} ) as admit_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as discharge_disposition_code
    , l.line_place_of_srvc_cd as place_of_service_code
    , cast(NULL as {{ dbt.type_string() }} ) as bill_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as revenue_center_code
    , l.line_srvc_cnt as service_unit_quantity
    , l.hcpcs_cd as hcpcs_code
    , l.hcpcs_1st_mdfr_cd as hcpcs_modifier_1
    , l.hcpcs_2nd_mdfr_cd as hcpcs_modifier_2
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_5
    , l.prf_physn_npi as rendering_npi
    , cast(NULL as {{ dbt.type_string() }} ) as rendering_tin
    , b.carr_clm_blg_npi_num as billing_npi
    , cast(NULL as {{ dbt.type_string() }} ) as billing_tin
    , cast(NULL as {{ dbt.type_string() }} ) as facility_npi
    , date(NULL) as paid_date
    , l.line_nch_pmt_amt as paid_amount
    , cast(NULL as {{ dbt.type_numeric() }} ) as allowed_amount
    , l.line_alowd_chrg_amt as charge_amount
    , cast(NULL as {{ dbt.type_numeric() }} ) as coinsurance_amount
    , cast(NULL as {{ dbt.type_numeric() }} ) as copayment_amount
    , cast(NULL as {{ dbt.type_numeric() }} ) as deductible_amount
    , /** medicare payment **/
      line_nch_pmt_amt 
      /** beneficiary payment **/
      + line_bene_ptb_ddctbl_amt 
      /** primary payer payment **/
      + line_bene_prmry_pyr_pd_amt
    as total_cost_amount
    , case when b.prncpal_dgns_vrsn_cd = '0' then 'icd-10-cm'
           when b.prncpal_dgns_vrsn_cd = '9' then 'icd-9-cm'
           when b.prncpal_dgns_vrsn_cd is null then 'icd-9-cm'
      end as diagnosis_code_type
    , b.prncpal_dgns_cd as diagnosis_code_1
    , b.icd_dgns_cd2 as diagnosis_code_2
    , b.icd_dgns_cd3 as diagnosis_code_3
    , b.icd_dgns_cd4 as diagnosis_code_4
    , b.icd_dgns_cd5 as diagnosis_code_5
    , b.icd_dgns_cd6 as diagnosis_code_6
    , b.icd_dgns_cd7 as diagnosis_code_7
    , b.icd_dgns_cd8 as diagnosis_code_8
    , b.icd_dgns_cd9 as diagnosis_code_9
    , b.icd_dgns_cd10 as diagnosis_code_10
    , b.icd_dgns_cd11 as diagnosis_code_11
    , b.icd_dgns_cd12 as diagnosis_code_12
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_13
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_14
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_15
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_16
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_17
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_18
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_19
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_20
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_21
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_22
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_23
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_24
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_25
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_1
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_2
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_3
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_4
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_5
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_6
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_7
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_8
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_9
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_10
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_11
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_12
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_13
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_14
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_15
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_16
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_17
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_18
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_19
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_20
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_21
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_22
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_23
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_24
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_25
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_type
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_1
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_2
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_3
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_4
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_5
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_6
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_7
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_8
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_9
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_10
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_11
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_12
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_13
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_14
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_15
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_16
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_17
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_18
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_19
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_20
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_21
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_22
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_23
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_24
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_25
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
    , cast(1 as int) as in_network_flag
    , cast('medicare_lds' as {{ dbt.type_string() }} ) as data_source
    , b.file_name
    , b.ingest_datetime
from carrier_base_claim as b
    inner join {{ ref('stg_carrier_claim_line') }} as l
        on b.claim_no = l.claim_no
    inner join claim_start_date as c
        on b.claim_no = c.claim_no