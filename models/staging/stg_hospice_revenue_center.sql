select
      desy_sort_key
    , claim_no
    , clm_line_num
    , clm_thru_dt
    , nch_clm_type_cd
    , rev_cntr
    , rev_cntr_dt
    , hcpcs_cd
    , hcpcs_1st_mdfr_cd
    , hcpcs_2nd_mdfr_cd
    , hcpcs_3rd_mdfr_cd
    , rev_cntr_unit_cnt
    , rev_cntr_rate_amt
    , rev_cntr_prvdr_pmt_amt
    , rev_cntr_bene_pmt_amt
    , rev_cntr_pmt_amt_amt
    , rev_cntr_tot_chrg_amt
    , rev_cntr_ncvrd_chrg_amt
    , rev_cntr_ddctbl_coinsrnc_cd
    , rev_cntr_rndrng_physn_upin
    , rev_cntr_rndrng_physn_npi
    , rev_cntr_rndrng_physn_spclty_cd
    , rev_cntr_ide_ndc_upc_num
    , rev_cntr_stus_ind_cd
    , rev_cntr_prcng_ind_cd
    , thrpy_cap_ind_cd1
    , thrpy_cap_ind_cd2
    , file_name
    , ingest_datetime
from {{ source('medicare_lds','hospice_revenue_center') }}