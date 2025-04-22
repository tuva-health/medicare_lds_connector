select
      cast(desy_sort_key as {{ dbt.type_string() }} ) as desy_sort_key
    , cast(claim_no as {{ dbt.type_string() }} ) as claim_no
    , cast(clm_line_num as {{ dbt.type_string() }} ) as clm_line_num
    , {{ try_to_cast_date('clm_thru_dt', 'YYYYMMDD') }} as clm_thru_dt
    , cast(nch_clm_type_cd as {{ dbt.type_string() }} ) as nch_clm_type_cd
    , cast(prvdr_spclty as {{ dbt.type_string() }} ) as prvdr_spclty
    , cast(prtcptng_ind_cd as {{ dbt.type_string() }} ) as prtcptng_ind_cd
    , cast(regexp_substr(line_srvc_cnt,'.') as integer) as line_srvc_cnt
    , cast(line_cms_type_srvc_cd as {{ dbt.type_string() }} ) as line_cms_type_srvc_cd
    , cast(line_place_of_srvc_cd as {{ dbt.type_string() }} ) as line_place_of_srvc_cd
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
    , cast(line_prmry_alowd_chrg_amt as {{ dbt.type_numeric() }} ) as line_prmry_alowd_chrg_amt
    , cast(line_sbmtd_chrg_amt as {{ dbt.type_numeric() }} ) as line_sbmtd_chrg_amt
    , cast(line_alowd_chrg_amt as {{ dbt.type_numeric() }} ) as line_alowd_chrg_amt
    , cast(line_prcsg_ind_cd as {{ dbt.type_string() }} ) as line_prcsg_ind_cd
    , cast(line_pmt_80_100_cd as {{ dbt.type_string() }} ) as line_pmt_80_100_cd
    , cast(line_service_deductible as {{ dbt.type_string() }} ) as line_service_deductible
    , cast(line_icd_dgns_cd as {{ dbt.type_string() }} ) as line_icd_dgns_cd
    , cast(line_icd_dgns_vrsn_cd as {{ dbt.type_string() }} ) as line_icd_dgns_vrsn_cd
    , cast(line_dme_prchs_price_amt as {{ dbt.type_numeric() }} ) as line_dme_prchs_price_amt
    , cast(prvdr_num as {{ dbt.type_string() }} ) as prvdr_num
    , cast(prvdr_npi as {{ dbt.type_string() }} ) as prvdr_npi
    , cast(dmerc_line_prcng_state_cd as {{ dbt.type_string() }} ) as dmerc_line_prcng_state_cd
    , cast(prvdr_state_cd as {{ dbt.type_string() }} ) as prvdr_state_cd
    , cast(hcpcs_3rd_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_3rd_mdfr_cd
    , cast(hcpcs_4th_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_4th_mdfr_cd
    , cast(dmerc_line_scrn_svgs_amt as {{ dbt.type_numeric() }} ) as dmerc_line_scrn_svgs_amt
    , cast(dmerc_line_mtus_cnt as {{ dbt.type_numeric() }} ) as dmerc_line_mtus_cnt
    , cast(dmerc_line_mtus_cd as {{ dbt.type_string() }} ) as dmerc_line_mtus_cd
    , cast(line_hct_hgb_rslt_num as {{ dbt.type_numeric() }} ) as line_hct_hgb_rslt_num
    , cast(line_hct_hgb_type_cd as {{ dbt.type_string() }} ) as line_hct_hgb_type_cd
    , cast(line_ndc_cd as {{ dbt.type_string() }} ) as line_ndc_cd
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
    , cast(file_name as {{ dbt.type_string() }} ) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }} ) as ingest_datetime 
from {{ source('medicare_lds','dme_claim_line') }}