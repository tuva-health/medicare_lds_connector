select
      desy_sort_key
    , claim_no
    , clm_line_num
    , clm_thru_dt
    , nch_clm_type_cd
    , prvdr_spclty
    , prtcptng_ind_cd
    , line_srvc_cnt
    , line_cms_type_srvc_cd
    , line_place_of_srvc_cd
    , line_last_expns_dt
    , hcpcs_cd
    , hcpcs_1st_mdfr_cd
    , hcpcs_2nd_mdfr_cd
    , betos_cd
    , line_nch_pmt_amt
    , line_bene_pmt_amt
    , line_prvdr_pmt_amt
    , line_bene_ptb_ddctbl_amt
    , line_bene_prmry_pyr_cd
    , line_bene_prmry_pyr_pd_amt
    , line_coinsrnc_amt
    , line_prmry_alowd_chrg_amt
    , line_sbmtd_chrg_amt
    , line_alowd_chrg_amt
    , line_prcsg_ind_cd
    , line_pmt_80_100_cd
    , line_service_deductible
    , line_icd_dgns_cd
    , line_icd_dgns_vrsn_cd
    , line_dme_prchs_price_amt
    , prvdr_num
    , prvdr_npi
    , dmerc_line_prcng_state_cd
    , prvdr_state_cd
    , hcpcs_3rd_mdfr_cd
    , hcpcs_4th_mdfr_cd
    , dmerc_line_scrn_svgs_amt
    , dmerc_line_mtus_cnt
    , dmerc_line_mtus_cd
    , line_hct_hgb_rslt_num
    , line_hct_hgb_type_cd
    , line_ndc_cd
    , line_othr_apld_ind_cd1
    , line_othr_apld_ind_cd2
    , line_othr_apld_ind_cd3
    , line_othr_apld_ind_cd4
    , line_othr_apld_ind_cd5
    , line_othr_apld_ind_cd6
    , line_othr_apld_ind_cd7
    , line_othr_apld_amt1
    , line_othr_apld_amt2
    , line_othr_apld_amt3
    , line_othr_apld_amt4
    , line_othr_apld_amt5
    , line_othr_apld_amt6
    , line_othr_apld_amt7
    , file_name
    , ingest_datetime
from {{ source('medicare_lds','dme_claim_line') }}