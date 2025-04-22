select
      cast(desy_sort_key as {{ dbt.type_string() }} ) as desy_sort_key
    , cast(claim_no as {{ dbt.type_string() }} ) as claim_no
    , {{ try_to_cast_date('clm_thru_dt', 'YYYYMMDD') }} as clm_thru_dt
    , cast(nch_near_line_rec_ident_cd as {{ dbt.type_string() }} ) as nch_near_line_rec_ident_cd
    , cast(nch_clm_type_cd as {{ dbt.type_string() }} ) as nch_clm_type_cd
    , cast(clm_disp_cd as {{ dbt.type_string() }} ) as clm_disp_cd
    , cast(carr_num as {{ dbt.type_string() }} ) as carr_num
    , cast(carr_clm_pmt_dnl_cd as {{ dbt.type_string() }} ) as carr_clm_pmt_dnl_cd
    , cast(clm_pmt_amt as {{ dbt.type_numeric() }} ) as clm_pmt_amt
    , cast(carr_clm_prmry_pyr_pd_amt as {{ dbt.type_numeric() }} ) as carr_clm_prmry_pyr_pd_amt
    , cast(carr_clm_prvdr_asgnmt_ind_sw as {{ dbt.type_string() }} ) as carr_clm_prvdr_asgnmt_ind_sw
    , cast(nch_clm_prvdr_pmt_amt as {{ dbt.type_numeric() }} ) as nch_clm_prvdr_pmt_amt
    , cast(nch_clm_bene_pmt_amt as {{ dbt.type_numeric() }} ) as nch_clm_bene_pmt_amt
    , cast(nch_carr_clm_sbmtd_chrg_amt as {{ dbt.type_numeric() }} ) as nch_carr_clm_sbmtd_chrg_amt
    , cast(nch_carr_clm_alowd_amt as {{ dbt.type_numeric() }} ) as nch_carr_clm_alowd_amt
    , cast(carr_clm_cash_ddctbl_apld_amt as {{ dbt.type_numeric() }} ) as carr_clm_cash_ddctbl_apld_amt
    , cast(carr_clm_hcpcs_yr_cd as {{ dbt.type_string() }} ) as carr_clm_hcpcs_yr_cd
    , cast(prncpal_dgns_cd as {{ dbt.type_string() }} ) as prncpal_dgns_cd
    , cast(prncpal_dgns_vrsn_cd as {{ dbt.type_string() }} ) as prncpal_dgns_vrsn_cd
    , cast(icd_dgns_cd1 as {{ dbt.type_string() }} ) as icd_dgns_cd1
    , cast(icd_dgns_vrsn_cd1 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd1
    , cast(icd_dgns_cd2 as {{ dbt.type_string() }} ) as icd_dgns_cd2
    , cast(icd_dgns_vrsn_cd2 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd2
    , cast(icd_dgns_cd3 as {{ dbt.type_string() }} ) as icd_dgns_cd3
    , cast(icd_dgns_vrsn_cd3 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd3
    , cast(icd_dgns_cd4 as {{ dbt.type_string() }} ) as icd_dgns_cd4
    , cast(icd_dgns_vrsn_cd4 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd4
    , cast(icd_dgns_cd5 as {{ dbt.type_string() }} ) as icd_dgns_cd5
    , cast(icd_dgns_vrsn_cd5 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd5
    , cast(icd_dgns_cd6 as {{ dbt.type_string() }} ) as icd_dgns_cd6
    , cast(icd_dgns_vrsn_cd6 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd6
    , cast(icd_dgns_cd7 as {{ dbt.type_string() }} ) as icd_dgns_cd7
    , cast(icd_dgns_vrsn_cd7 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd7
    , cast(icd_dgns_cd8 as {{ dbt.type_string() }} ) as icd_dgns_cd8
    , cast(icd_dgns_vrsn_cd8 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd8
    , cast(icd_dgns_cd9 as {{ dbt.type_string() }} ) as icd_dgns_cd9
    , cast(icd_dgns_vrsn_cd9 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd9
    , cast(icd_dgns_cd10 as {{ dbt.type_string() }} ) as icd_dgns_cd10
    , cast(icd_dgns_vrsn_cd10 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd10
    , cast(icd_dgns_cd11 as {{ dbt.type_string() }} ) as icd_dgns_cd11
    , cast(icd_dgns_vrsn_cd11 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd11
    , cast(icd_dgns_cd12 as {{ dbt.type_string() }} ) as icd_dgns_cd12
    , cast(icd_dgns_vrsn_cd12 as {{ dbt.type_string() }} ) as icd_dgns_vrsn_cd12
    , cast(rfr_physn_upin as {{ dbt.type_string() }} ) as rfr_physn_upin
    , cast(rfr_physn_npi as {{ dbt.type_string() }} ) as rfr_physn_npi
    , {{ try_to_cast_date('dob_dt', 'YYYYMMDD') }} as dob_dt
    , cast(gndr_cd as {{ dbt.type_string() }} ) as gndr_cd
    , cast(bene_race_cd as {{ dbt.type_string() }} ) as bene_race_cd
    , cast(bene_cnty_cd as {{ dbt.type_string() }} ) as bene_cnty_cd
    , cast(bene_state_cd as {{ dbt.type_string() }} ) as bene_state_cd
    , cast(cwf_bene_mdcr_stus_cd as {{ dbt.type_string() }} ) as cwf_bene_mdcr_stus_cd
    , cast(clm_bene_pd_amt as {{ dbt.type_numeric() }} ) as clm_bene_pd_amt
    , cast(aco_id_num as {{ dbt.type_string() }} ) as aco_id_num
    , cast(file_name as {{ dbt.type_string() }} ) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }} ) as ingest_datetime 
from {{ source('medicare_lds','dme_base_claim') }}