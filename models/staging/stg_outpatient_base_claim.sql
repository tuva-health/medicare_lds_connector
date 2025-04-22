select
      cast(desy_sort_key as {{ dbt.type_string() }} ) as desy_sort_key
    , cast(claim_no as {{ dbt.type_string() }} ) as claim_no
    , cast(prvdr_num as {{ dbt.type_string() }} ) as prvdr_num
    , {{ try_to_cast_date('clm_thru_dt', 'YYYYMMDD') }} as clm_thru_dt
    , cast(nch_near_line_rec_ident_cd as {{ dbt.type_string() }} ) as nch_near_line_rec_ident_cd
    , cast(nch_clm_type_cd as {{ dbt.type_string() }} ) as nch_clm_type_cd
    , cast(claim_query_code as {{ dbt.type_string() }} ) as claim_query_code
    , cast(clm_fac_type_cd as {{ dbt.type_string() }} ) as clm_fac_type_cd
    , cast(clm_srvc_clsfctn_type_cd as {{ dbt.type_string() }} ) as clm_srvc_clsfctn_type_cd
    , cast(clm_freq_cd as {{ dbt.type_string() }} ) as clm_freq_cd
    , cast(fi_num as {{ dbt.type_string() }} ) as fi_num
    , cast(clm_mdcr_non_pmt_rsn_cd as {{ dbt.type_string() }} ) as clm_mdcr_non_pmt_rsn_cd
    , cast(clm_pmt_amt as {{ dbt.type_numeric() }} ) as clm_pmt_amt
    , cast(nch_prmry_pyr_clm_pd_amt as {{ dbt.type_numeric() }} ) as nch_prmry_pyr_clm_pd_amt
    , cast(nch_prmry_pyr_cd as {{ dbt.type_string() }} ) as nch_prmry_pyr_cd
    , cast(prvdr_state_cd as {{ dbt.type_string() }} ) as prvdr_state_cd
    , cast(org_npi_num as {{ dbt.type_string() }} ) as org_npi_num
    , cast(srvc_loc_npi_num as {{ dbt.type_string() }} ) as srvc_loc_npi_num
    , cast(at_physn_upin as {{ dbt.type_string() }} ) as at_physn_upin
    , cast(at_physn_npi as {{ dbt.type_string() }} ) as at_physn_npi
    , cast(at_physn_spclty_cd as {{ dbt.type_string() }} ) as at_physn_spclty_cd
    , cast(op_physn_upin as {{ dbt.type_string() }} ) as op_physn_upin
    , cast(op_physn_npi as {{ dbt.type_string() }} ) as op_physn_npi
    , cast(op_physn_spclty_cd as {{ dbt.type_string() }} ) as op_physn_spclty_cd
    , cast(ot_physn_upin as {{ dbt.type_string() }} ) as ot_physn_upin
    , cast(ot_physn_npi as {{ dbt.type_string() }} ) as ot_physn_npi
    , cast(ot_physn_spclty_cd as {{ dbt.type_string() }} ) as ot_physn_spclty_cd
    , cast(rndrng_physn_npi as {{ dbt.type_string() }} ) as rndrng_physn_npi
    , cast(rndrng_physn_spclty_cd as {{ dbt.type_string() }} ) as rndrng_physn_spclty_cd
    , cast(rfr_physn_npi as {{ dbt.type_string() }} ) as rfr_physn_npi
    , cast(rfr_physn_spclty_cd as {{ dbt.type_string() }} ) as rfr_physn_spclty_cd
    , cast(clm_mco_pd_sw as {{ dbt.type_string() }} ) as clm_mco_pd_sw
    , cast(ptnt_dschrg_stus_cd as {{ dbt.type_string() }} ) as ptnt_dschrg_stus_cd
    , cast(clm_tot_chrg_amt as {{ dbt.type_numeric() }} ) as clm_tot_chrg_amt
    , cast(nch_bene_blood_ddctbl_lblty_am as {{ dbt.type_numeric() }} ) as nch_bene_blood_ddctbl_lblty_am
    , cast(nch_profnl_cmpnt_chrg_amt as {{ dbt.type_numeric() }} ) as nch_profnl_cmpnt_chrg_amt
    , cast(prncpal_dgns_cd as {{ dbt.type_string() }} ) as prncpal_dgns_cd
    , cast(icd_dgns_cd1 as {{ dbt.type_string() }} ) as icd_dgns_cd1
    , cast(icd_dgns_cd2 as {{ dbt.type_string() }} ) as icd_dgns_cd2
    , cast(icd_dgns_cd3 as {{ dbt.type_string() }} ) as icd_dgns_cd3
    , cast(icd_dgns_cd4 as {{ dbt.type_string() }} ) as icd_dgns_cd4
    , cast(icd_dgns_cd5 as {{ dbt.type_string() }} ) as icd_dgns_cd5
    , cast(icd_dgns_cd6 as {{ dbt.type_string() }} ) as icd_dgns_cd6
    , cast(icd_dgns_cd7 as {{ dbt.type_string() }} ) as icd_dgns_cd7
    , cast(icd_dgns_cd8 as {{ dbt.type_string() }} ) as icd_dgns_cd8
    , cast(icd_dgns_cd9 as {{ dbt.type_string() }} ) as icd_dgns_cd9
    , cast(icd_dgns_cd10 as {{ dbt.type_string() }} ) as icd_dgns_cd10
    , cast(icd_dgns_cd11 as {{ dbt.type_string() }} ) as icd_dgns_cd11
    , cast(icd_dgns_cd12 as {{ dbt.type_string() }} ) as icd_dgns_cd12
    , cast(icd_dgns_cd13 as {{ dbt.type_string() }} ) as icd_dgns_cd13
    , cast(icd_dgns_cd14 as {{ dbt.type_string() }} ) as icd_dgns_cd14
    , cast(icd_dgns_cd15 as {{ dbt.type_string() }} ) as icd_dgns_cd15
    , cast(icd_dgns_cd16 as {{ dbt.type_string() }} ) as icd_dgns_cd16
    , cast(icd_dgns_cd17 as {{ dbt.type_string() }} ) as icd_dgns_cd17
    , cast(icd_dgns_cd18 as {{ dbt.type_string() }} ) as icd_dgns_cd18
    , cast(icd_dgns_cd19 as {{ dbt.type_string() }} ) as icd_dgns_cd19
    , cast(icd_dgns_cd20 as {{ dbt.type_string() }} ) as icd_dgns_cd20
    , cast(icd_dgns_cd21 as {{ dbt.type_string() }} ) as icd_dgns_cd21
    , cast(icd_dgns_cd22 as {{ dbt.type_string() }} ) as icd_dgns_cd22
    , cast(icd_dgns_cd23 as {{ dbt.type_string() }} ) as icd_dgns_cd23
    , cast(icd_dgns_cd24 as {{ dbt.type_string() }} ) as icd_dgns_cd24
    , cast(icd_dgns_cd25 as {{ dbt.type_string() }} ) as icd_dgns_cd25
    , cast(fst_dgns_e_cd as {{ dbt.type_string() }} ) as fst_dgns_e_cd
    , cast(icd_dgns_e_cd1 as {{ dbt.type_string() }} ) as icd_dgns_e_cd1
    , cast(icd_dgns_e_cd2 as {{ dbt.type_string() }} ) as icd_dgns_e_cd2
    , cast(icd_dgns_e_cd3 as {{ dbt.type_string() }} ) as icd_dgns_e_cd3
    , cast(icd_dgns_e_cd4 as {{ dbt.type_string() }} ) as icd_dgns_e_cd4
    , cast(icd_dgns_e_cd5 as {{ dbt.type_string() }} ) as icd_dgns_e_cd5
    , cast(icd_dgns_e_cd6 as {{ dbt.type_string() }} ) as icd_dgns_e_cd6
    , cast(icd_dgns_e_cd7 as {{ dbt.type_string() }} ) as icd_dgns_e_cd7
    , cast(icd_dgns_e_cd8 as {{ dbt.type_string() }} ) as icd_dgns_e_cd8
    , cast(icd_dgns_e_cd9 as {{ dbt.type_string() }} ) as icd_dgns_e_cd9
    , cast(icd_dgns_e_cd10 as {{ dbt.type_string() }} ) as icd_dgns_e_cd10
    , cast(icd_dgns_e_cd11 as {{ dbt.type_string() }} ) as icd_dgns_e_cd11
    , cast(icd_dgns_e_cd12 as {{ dbt.type_string() }} ) as icd_dgns_e_cd12
    , cast(icd_prcdr_cd1 as {{ dbt.type_string() }} ) as icd_prcdr_cd1
    , {{ try_to_cast_date('prcdr_dt1', 'YYYYMMDD') }} as prcdr_dt1
    , cast(icd_prcdr_cd2 as {{ dbt.type_string() }} ) as icd_prcdr_cd2
    , {{ try_to_cast_date('prcdr_dt2', 'YYYYMMDD') }} as prcdr_dt2
    , cast(icd_prcdr_cd3 as {{ dbt.type_string() }} ) as icd_prcdr_cd3
    , {{ try_to_cast_date('prcdr_dt3', 'YYYYMMDD') }} as prcdr_dt3
    , cast(icd_prcdr_cd4 as {{ dbt.type_string() }} ) as icd_prcdr_cd4
    , {{ try_to_cast_date('prcdr_dt4', 'YYYYMMDD') }} as prcdr_dt4
    , cast(icd_prcdr_cd5 as {{ dbt.type_string() }} ) as icd_prcdr_cd5
    , {{ try_to_cast_date('prcdr_dt5', 'YYYYMMDD') }} as prcdr_dt5
    , cast(icd_prcdr_cd6 as {{ dbt.type_string() }} ) as icd_prcdr_cd6
    , {{ try_to_cast_date('prcdr_dt6', 'YYYYMMDD') }} as prcdr_dt6
    , cast(icd_prcdr_cd7 as {{ dbt.type_string() }} ) as icd_prcdr_cd7
    , {{ try_to_cast_date('prcdr_dt7', 'YYYYMMDD') }} as prcdr_dt7
    , cast(icd_prcdr_cd8 as {{ dbt.type_string() }} ) as icd_prcdr_cd8
    , {{ try_to_cast_date('prcdr_dt8', 'YYYYMMDD') }} as prcdr_dt8
    , cast(icd_prcdr_cd9 as {{ dbt.type_string() }} ) as icd_prcdr_cd9
    , {{ try_to_cast_date('prcdr_dt9', 'YYYYMMDD') }} as prcdr_dt9
    , cast(icd_prcdr_cd10 as {{ dbt.type_string() }} ) as icd_prcdr_cd10
    , {{ try_to_cast_date('prcdr_dt10', 'YYYYMMDD') }} as prcdr_dt10
    , cast(icd_prcdr_cd11 as {{ dbt.type_string() }} ) as icd_prcdr_cd11
    , {{ try_to_cast_date('prcdr_dt11', 'YYYYMMDD') }} as prcdr_dt11
    , cast(icd_prcdr_cd12 as {{ dbt.type_string() }} ) as icd_prcdr_cd12
    , {{ try_to_cast_date('prcdr_dt12', 'YYYYMMDD') }} as prcdr_dt12
    , cast(icd_prcdr_cd13 as {{ dbt.type_string() }} ) as icd_prcdr_cd13
    , {{ try_to_cast_date('prcdr_dt13', 'YYYYMMDD') }} as prcdr_dt13
    , cast(icd_prcdr_cd14 as {{ dbt.type_string() }} ) as icd_prcdr_cd14
    , {{ try_to_cast_date('prcdr_dt14', 'YYYYMMDD') }} as prcdr_dt14
    , cast(icd_prcdr_cd15 as {{ dbt.type_string() }} ) as icd_prcdr_cd15
    , {{ try_to_cast_date('prcdr_dt15', 'YYYYMMDD') }} as prcdr_dt15
    , cast(icd_prcdr_cd16 as {{ dbt.type_string() }} ) as icd_prcdr_cd16
    , {{ try_to_cast_date('prcdr_dt16', 'YYYYMMDD') }} as prcdr_dt16
    , cast(icd_prcdr_cd17 as {{ dbt.type_string() }} ) as icd_prcdr_cd17
    , {{ try_to_cast_date('prcdr_dt17', 'YYYYMMDD') }} as prcdr_dt17
    , cast(icd_prcdr_cd18 as {{ dbt.type_string() }} ) as icd_prcdr_cd18
    , {{ try_to_cast_date('prcdr_dt18', 'YYYYMMDD') }} as prcdr_dt18
    , cast(icd_prcdr_cd19 as {{ dbt.type_string() }} ) as icd_prcdr_cd19
    , {{ try_to_cast_date('prcdr_dt19', 'YYYYMMDD') }} as prcdr_dt19
    , cast(icd_prcdr_cd20 as {{ dbt.type_string() }} ) as icd_prcdr_cd20
    , {{ try_to_cast_date('prcdr_dt20', 'YYYYMMDD') }} as prcdr_dt20
    , cast(icd_prcdr_cd21 as {{ dbt.type_string() }} ) as icd_prcdr_cd21
    , {{ try_to_cast_date('prcdr_dt21', 'YYYYMMDD') }} as prcdr_dt21
    , cast(icd_prcdr_cd22 as {{ dbt.type_string() }} ) as icd_prcdr_cd22
    , {{ try_to_cast_date('prcdr_dt22', 'YYYYMMDD') }} as prcdr_dt22
    , cast(icd_prcdr_cd23 as {{ dbt.type_string() }} ) as icd_prcdr_cd23
    , {{ try_to_cast_date('prcdr_dt23', 'YYYYMMDD') }} as prcdr_dt23
    , cast(icd_prcdr_cd24 as {{ dbt.type_string() }} ) as icd_prcdr_cd24
    , {{ try_to_cast_date('prcdr_dt24', 'YYYYMMDD') }} as prcdr_dt24
    , cast(icd_prcdr_cd25 as {{ dbt.type_string() }} ) as icd_prcdr_cd25
    , {{ try_to_cast_date('prcdr_dt25', 'YYYYMMDD') }} as prcdr_dt25
    , cast(rsn_visit_cd1 as {{ dbt.type_string() }} ) as rsn_visit_cd1
    , cast(rsn_visit_cd2 as {{ dbt.type_string() }} ) as rsn_visit_cd2
    , cast(rsn_visit_cd3 as {{ dbt.type_string() }} ) as rsn_visit_cd3
    , cast(nch_bene_ptb_ddctbl_amt as {{ dbt.type_numeric() }} ) as nch_bene_ptb_ddctbl_amt
    , cast(nch_bene_ptb_coinsrnc_amt as {{ dbt.type_numeric() }} ) as nch_bene_ptb_coinsrnc_amt
    , cast(clm_op_prvdr_pmt_amt as {{ dbt.type_numeric() }} ) as clm_op_prvdr_pmt_amt
    , cast(clm_op_bene_pmt_amt as {{ dbt.type_numeric() }} ) as clm_op_bene_pmt_amt
    , {{ try_to_cast_date('dob_dt', 'YYYYMMDD') }} as dob_dt
    , cast(gndr_cd as {{ dbt.type_string() }} ) as gndr_cd
    , cast(bene_race_cd as {{ dbt.type_string() }} ) as bene_race_cd
    , cast(bene_cnty_cd as {{ dbt.type_string() }} ) as bene_cnty_cd
    , cast(bene_state_cd as {{ dbt.type_string() }} ) as bene_state_cd
    , cast(cwf_bene_mdcr_stus_cd as {{ dbt.type_string() }} ) as cwf_bene_mdcr_stus_cd
    , cast(fi_clm_actn_cd as {{ dbt.type_string() }} ) as fi_clm_actn_cd
    , cast(nch_blood_pnts_frnshd_qty as integer) as nch_blood_pnts_frnshd_qty
    , cast(clm_trtmt_authrztn_num as {{ dbt.type_string() }} ) as clm_trtmt_authrztn_num
    , cast(clm_prcr_rtrn_cd as {{ dbt.type_string() }} ) as clm_prcr_rtrn_cd
    , cast(clm_op_trans_type_cd as {{ dbt.type_string() }} ) as clm_op_trans_type_cd
    , cast(clm_op_esrd_mthd_cd as {{ dbt.type_string() }} ) as clm_op_esrd_mthd_cd
    , cast(clm_next_gnrtn_aco_ind_cd1 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd1
    , cast(clm_next_gnrtn_aco_ind_cd2 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd2
    , cast(clm_next_gnrtn_aco_ind_cd3 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd3
    , cast(clm_next_gnrtn_aco_ind_cd4 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd4
    , cast(clm_next_gnrtn_aco_ind_cd5 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd5
    , cast(aco_id_num as {{ dbt.type_string() }} ) as aco_id_num
    , cast(file_name as {{ dbt.type_string() }} ) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }} ) as ingest_datetime 
from {{ source('medicare_lds','outpatient_base_claim') }}