select
      cast(desy_sort_key as {{ dbt.type_string() }} ) as desy_sort_key
    , cast(claim_no as {{ dbt.type_string() }} ) as claim_no
    , cast(line_num as integer) as line_num
    , {{ try_to_cast_date('clm_thru_dt', 'YYYYMMDD') }} as clm_thru_dt
    , cast(nch_clm_type_cd as {{ dbt.type_string() }} ) as nch_clm_type_cd
    , cast(carr_prfrng_pin_num as {{ dbt.type_string() }} ) as carr_prfrng_pin_num
    , cast(prf_physn_upin as {{ dbt.type_string() }} ) as prf_physn_upin
    , cast(prf_physn_npi as {{ dbt.type_string() }} ) as prf_physn_npi
    , cast(org_npi_num as {{ dbt.type_string() }} ) as org_npi_num
    , cast(carr_line_prvdr_type_cd as {{ dbt.type_string() }} ) as carr_line_prvdr_type_cd
    , cast(prvdr_state_cd as {{ dbt.type_string() }} ) as prvdr_state_cd
    , cast(prvdr_spclty as {{ dbt.type_string() }} ) as prvdr_spclty
    , cast(prtcptng_ind_cd as {{ dbt.type_string() }} ) as prtcptng_ind_cd
    , cast(carr_line_rdcd_pmt_phys_astn_c as {{ dbt.type_string() }} ) as carr_line_rdcd_pmt_phys_astn_c
    , cast(regexp_substr(line_srvc_cnt,'.') as integer) as line_srvc_cnt
    , cast(line_cms_type_srvc_cd as {{ dbt.type_string() }} ) as line_cms_type_srvc_cd
    , cast(line_place_of_srvc_cd as {{ dbt.type_string() }} ) as line_place_of_srvc_cd
    , cast(carr_line_prcng_lclty_cd as {{ dbt.type_string() }} ) as carr_line_prcng_lclty_cd
    , {{ try_to_cast_date('line_last_expns_dt', 'YYYYMMDD') }} as line_last_expns_dt
    , cast(hcpcs_cd as {{ dbt.type_string() }} ) as hcpcs_cd
    , cast(hcpcs_1st_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_1st_mdfr_cd
    , cast(hcpcs_2nd_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_2nd_mdfr_cd
    , cast(betos_cd as {{ dbt.type_string() }} ) as betos_cd
    , cast(line_nch_pmt_amt as {{ dbt.type_numeric() }} ) as line_nch_pmt_amt
    , cast(line_bene_pmt_amt as {{ dbt.type_numeric() }} ) as line_bene_pmt_amt
    , cast(line_prvdr_pmt_amt as {{ dbt.type_numeric() }} ) as line_prvdr_pmt_amt
    , cast(line_bene_ptb_ddctbl_amt as {{ dbt.type_numeric() }} ) as line_bene_ptb_ddctbl_amt
    , cast(line_bene_prmry_pyr_cd as {{ dbt.type_string() }} ) as line_bene_prmry_pyr_cd
    , cast(line_bene_prmry_pyr_pd_amt as {{ dbt.type_numeric() }} ) as line_bene_prmry_pyr_pd_amt
    , cast(line_coinsrnc_amt as {{ dbt.type_numeric() }} ) as line_coinsrnc_amt
    , cast(line_sbmtd_chrg_amt as {{ dbt.type_numeric() }} ) as line_sbmtd_chrg_amt
    , cast(line_alowd_chrg_amt as {{ dbt.type_numeric() }} ) as line_alowd_chrg_amt
    , cast(line_prcsg_ind_cd as {{ dbt.type_string() }} ) as line_prcsg_ind_cd
    , cast(line_pmt_80_100_cd as {{ dbt.type_string() }} ) as line_pmt_80_100_cd
    , cast(line_service_deductible as {{ dbt.type_numeric() }} ) as line_service_deductible
    , cast(carr_line_mtus_cnt as {{ dbt.type_numeric() }} ) as carr_line_mtus_cnt
    , cast(carr_line_mtus_cd as {{ dbt.type_string() }} ) as carr_line_mtus_cd
    , cast(line_icd_dgns_cd as {{ dbt.type_string() }} ) as line_icd_dgns_cd
    , cast(line_icd_dgns_vrsn_cd as {{ dbt.type_string() }} ) as line_icd_dgns_vrsn_cd
    , cast(line_hct_hgb_rslt_num as {{ dbt.type_string() }} ) as line_hct_hgb_rslt_num
    , cast(line_hct_hgb_type_cd as {{ dbt.type_string() }} ) as line_hct_hgb_type_cd
    , cast(line_ndc_cd as {{ dbt.type_string() }} ) as line_ndc_cd
    , cast(carr_line_clia_lab_num as {{ dbt.type_string() }} ) as carr_line_clia_lab_num
    , cast(carr_line_ansthsa_unit_cnt as {{ dbt.type_numeric() }} ) as carr_line_ansthsa_unit_cnt
    , cast(carr_line_cl_chrg_amt as {{ dbt.type_numeric() }} ) as carr_line_cl_chrg_amt
    , cast(line_othr_apld_ind_cd1 as {{ dbt.type_string() }} ) as line_othr_apld_ind_cd1
    , cast(line_othr_apld_ind_cd2 as {{ dbt.type_string() }} ) as line_othr_apld_ind_cd2
    , cast(line_othr_apld_ind_cd3 as {{ dbt.type_string() }} ) as line_othr_apld_ind_cd3
    , cast(line_othr_apld_ind_cd4 as {{ dbt.type_string() }} ) as line_othr_apld_ind_cd4
    , cast(line_othr_apld_ind_cd5 as {{ dbt.type_string() }} ) as line_othr_apld_ind_cd5
    , cast(line_othr_apld_ind_cd6 as {{ dbt.type_string() }} ) as line_othr_apld_ind_cd6
    , cast(line_othr_apld_ind_cd7 as {{ dbt.type_string() }} ) as line_othr_apld_ind_cd7
    , cast(line_othr_apld_amt1 as {{ dbt.type_numeric() }} ) as line_othr_apld_amt1
    , cast(line_othr_apld_amt2 as {{ dbt.type_numeric() }} ) as line_othr_apld_amt2
    , cast(line_othr_apld_amt3 as {{ dbt.type_numeric() }} ) as line_othr_apld_amt3
    , cast(line_othr_apld_amt4 as {{ dbt.type_numeric() }} ) as line_othr_apld_amt4
    , cast(line_othr_apld_amt5 as {{ dbt.type_numeric() }} ) as line_othr_apld_amt5
    , cast(line_othr_apld_amt6 as {{ dbt.type_numeric() }} ) as line_othr_apld_amt6
    , cast(line_othr_apld_amt7 as {{ dbt.type_numeric() }} ) as line_othr_apld_amt7
    , cast(thrpy_cap_ind_cd1 as {{ dbt.type_string() }} ) as thrpy_cap_ind_cd1
    , cast(thrpy_cap_ind_cd2 as {{ dbt.type_string() }} ) as thrpy_cap_ind_cd2
    , cast(thrpy_cap_ind_cd3 as {{ dbt.type_string() }} ) as thrpy_cap_ind_cd3
    , cast(thrpy_cap_ind_cd4 as {{ dbt.type_string() }} ) as thrpy_cap_ind_cd4
    , cast(thrpy_cap_ind_cd5 as {{ dbt.type_string() }} ) as thrpy_cap_ind_cd5
    , cast(clm_next_gnrtn_aco_ind_cd1 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd1
    , cast(clm_next_gnrtn_aco_ind_cd2 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd2
    , cast(clm_next_gnrtn_aco_ind_cd3 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd3
    , cast(clm_next_gnrtn_aco_ind_cd4 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd4
    , cast(clm_next_gnrtn_aco_ind_cd5 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd5
    , cast(file_name as {{ dbt.type_string() }} ) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }} ) as ingest_datetime 
from {{ source('medicare_lds','carrier_claim_line') }}