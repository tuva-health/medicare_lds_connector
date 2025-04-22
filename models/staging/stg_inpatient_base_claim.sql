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
    , cast(fi_clm_actn_cd as {{ dbt.type_string() }} ) as fi_clm_actn_cd
    , cast(prvdr_state_cd as {{ dbt.type_string() }} ) as prvdr_state_cd
    , cast(org_npi_num as {{ dbt.type_string() }} ) as org_npi_num
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
    , cast(clm_mco_pd_sw as {{ dbt.type_string() }} ) as clm_mco_pd_sw
    , cast(ptnt_dschrg_stus_cd as {{ dbt.type_string() }} ) as ptnt_dschrg_stus_cd
    , cast(clm_pps_ind_cd as {{ dbt.type_string() }} ) as clm_pps_ind_cd
    , cast(clm_tot_chrg_amt as {{ dbt.type_numeric() }} ) as clm_tot_chrg_amt
    , {{ try_to_cast_date('clm_admsn_dt', 'YYYYMMDD') }} as clm_admsn_dt
    , cast(clm_ip_admsn_type_cd as {{ dbt.type_string() }} ) as clm_ip_admsn_type_cd
    , cast(clm_src_ip_admsn_cd as {{ dbt.type_string() }} ) as clm_src_ip_admsn_cd
    , cast(nch_ptnt_status_ind_cd as {{ dbt.type_string() }} ) as nch_ptnt_status_ind_cd
    , cast(clm_pass_thru_per_diem_amt as {{ dbt.type_numeric() }} ) as clm_pass_thru_per_diem_amt
    , cast(nch_bene_ip_ddctbl_amt as {{ dbt.type_numeric() }} ) as nch_bene_ip_ddctbl_amt
    , cast(nch_bene_pta_coinsrnc_lblty_am as {{ dbt.type_numeric() }} ) as nch_bene_pta_coinsrnc_lblty_am
    , cast(nch_bene_blood_ddctbl_lblty_am as {{ dbt.type_numeric() }} ) as nch_bene_blood_ddctbl_lblty_am
    , cast(nch_profnl_cmpnt_chrg_amt as {{ dbt.type_numeric() }} ) as nch_profnl_cmpnt_chrg_amt
    , cast(nch_ip_ncvrd_chrg_amt as {{ dbt.type_numeric() }} ) as nch_ip_ncvrd_chrg_amt
    , cast(clm_tot_pps_cptl_amt as {{ dbt.type_numeric() }} ) as clm_tot_pps_cptl_amt
    , cast(clm_pps_cptl_fsp_amt as {{ dbt.type_numeric() }} ) as clm_pps_cptl_fsp_amt
    , cast(clm_pps_cptl_outlier_amt as {{ dbt.type_numeric() }} ) as clm_pps_cptl_outlier_amt
    , cast(clm_pps_cptl_dsprprtnt_shr_amt as {{ dbt.type_numeric() }} ) as clm_pps_cptl_dsprprtnt_shr_amt
    , cast(clm_pps_cptl_ime_amt as {{ dbt.type_numeric() }} ) as clm_pps_cptl_ime_amt
    , cast(clm_pps_cptl_excptn_amt as {{ dbt.type_numeric() }} ) as clm_pps_cptl_excptn_amt
    , cast(clm_pps_old_cptl_hld_hrmls_amt as {{ dbt.type_numeric() }} ) as clm_pps_old_cptl_hld_hrmls_amt
    , cast(clm_pps_cptl_drg_wt_num as {{ dbt.type_string() }} ) as clm_pps_cptl_drg_wt_num
    , cast(clm_utlztn_day_cnt as integer) as clm_utlztn_day_cnt
    , cast(bene_tot_coinsrnc_days_cnt as integer) as bene_tot_coinsrnc_days_cnt
    , cast(bene_lrd_used_cnt as integer) as bene_lrd_used_cnt
    , cast(clm_non_utlztn_days_cnt as integer) as clm_non_utlztn_days_cnt
    , cast(nch_blood_pnts_frnshd_qty as integer) as nch_blood_pnts_frnshd_qty
    , {{ try_to_cast_date('nch_vrfd_ncvrd_stay_from_dt', 'YYYYMMDD') }} as nch_vrfd_ncvrd_stay_from_dt
    , {{ try_to_cast_date('nch_vrfd_ncvrd_stay_thru_dt', 'YYYYMMDD') }} as nch_vrfd_ncvrd_stay_thru_dt
    , {{ try_to_cast_date('nch_bene_mdcr_bnfts_exhtd_dt_i', 'YYYYMMDD') }} as nch_bene_mdcr_bnfts_exhtd_dt_i
    , {{ try_to_cast_date('nch_bene_dschrg_dt', 'YYYYMMDD') }} as nch_bene_dschrg_dt
    , cast(clm_drg_cd as {{ dbt.type_string() }} ) as clm_drg_cd
    , cast(clm_drg_outlier_stay_cd as {{ dbt.type_string() }} ) as clm_drg_outlier_stay_cd
    , cast(nch_drg_outlier_aprvd_pmt_amt as {{ dbt.type_numeric() }} ) as nch_drg_outlier_aprvd_pmt_amt
    , cast(admtg_dgns_cd as {{ dbt.type_string() }} ) as admtg_dgns_cd
    , cast(prncpal_dgns_cd as {{ dbt.type_string() }} ) as prncpal_dgns_cd
    , cast(icd_dgns_cd1 as {{ dbt.type_string() }} ) as icd_dgns_cd1
    , cast(clm_poa_ind_sw1 as {{ dbt.type_string() }} ) as clm_poa_ind_sw1
    , cast(icd_dgns_cd2 as {{ dbt.type_string() }} ) as icd_dgns_cd2
    , cast(clm_poa_ind_sw2 as {{ dbt.type_string() }} ) as clm_poa_ind_sw2
    , cast(icd_dgns_cd3 as {{ dbt.type_string() }} ) as icd_dgns_cd3
    , cast(clm_poa_ind_sw3 as {{ dbt.type_string() }} ) as clm_poa_ind_sw3
    , cast(icd_dgns_cd4 as {{ dbt.type_string() }} ) as icd_dgns_cd4
    , cast(clm_poa_ind_sw4 as {{ dbt.type_string() }} ) as clm_poa_ind_sw4
    , cast(icd_dgns_cd5 as {{ dbt.type_string() }} ) as icd_dgns_cd5
    , cast(clm_poa_ind_sw5 as {{ dbt.type_string() }} ) as clm_poa_ind_sw5
    , cast(icd_dgns_cd6 as {{ dbt.type_string() }} ) as icd_dgns_cd6
    , cast(clm_poa_ind_sw6 as {{ dbt.type_string() }} ) as clm_poa_ind_sw6
    , cast(icd_dgns_cd7 as {{ dbt.type_string() }} ) as icd_dgns_cd7
    , cast(clm_poa_ind_sw7 as {{ dbt.type_string() }} ) as clm_poa_ind_sw7
    , cast(icd_dgns_cd8 as {{ dbt.type_string() }} ) as icd_dgns_cd8
    , cast(clm_poa_ind_sw8 as {{ dbt.type_string() }} ) as clm_poa_ind_sw8
    , cast(icd_dgns_cd9 as {{ dbt.type_string() }} ) as icd_dgns_cd9
    , cast(clm_poa_ind_sw9 as {{ dbt.type_string() }} ) as clm_poa_ind_sw9
    , cast(icd_dgns_cd10 as {{ dbt.type_string() }} ) as icd_dgns_cd10
    , cast(clm_poa_ind_sw10 as {{ dbt.type_string() }} ) as clm_poa_ind_sw10
    , cast(icd_dgns_cd11 as {{ dbt.type_string() }} ) as icd_dgns_cd11
    , cast(clm_poa_ind_sw11 as {{ dbt.type_string() }} ) as clm_poa_ind_sw11
    , cast(icd_dgns_cd12 as {{ dbt.type_string() }} ) as icd_dgns_cd12
    , cast(clm_poa_ind_sw12 as {{ dbt.type_string() }} ) as clm_poa_ind_sw12
    , cast(icd_dgns_cd13 as {{ dbt.type_string() }} ) as icd_dgns_cd13
    , cast(clm_poa_ind_sw13 as {{ dbt.type_string() }} ) as clm_poa_ind_sw13
    , cast(icd_dgns_cd14 as {{ dbt.type_string() }} ) as icd_dgns_cd14
    , cast(clm_poa_ind_sw14 as {{ dbt.type_string() }} ) as clm_poa_ind_sw14
    , cast(icd_dgns_cd15 as {{ dbt.type_string() }} ) as icd_dgns_cd15
    , cast(clm_poa_ind_sw15 as {{ dbt.type_string() }} ) as clm_poa_ind_sw15
    , cast(icd_dgns_cd16 as {{ dbt.type_string() }} ) as icd_dgns_cd16
    , cast(clm_poa_ind_sw16 as {{ dbt.type_string() }} ) as clm_poa_ind_sw16
    , cast(icd_dgns_cd17 as {{ dbt.type_string() }} ) as icd_dgns_cd17
    , cast(clm_poa_ind_sw17 as {{ dbt.type_string() }} ) as clm_poa_ind_sw17
    , cast(icd_dgns_cd18 as {{ dbt.type_string() }} ) as icd_dgns_cd18
    , cast(clm_poa_ind_sw18 as {{ dbt.type_string() }} ) as clm_poa_ind_sw18
    , cast(icd_dgns_cd19 as {{ dbt.type_string() }} ) as icd_dgns_cd19
    , cast(clm_poa_ind_sw19 as {{ dbt.type_string() }} ) as clm_poa_ind_sw19
    , cast(icd_dgns_cd20 as {{ dbt.type_string() }} ) as icd_dgns_cd20
    , cast(clm_poa_ind_sw20 as {{ dbt.type_string() }} ) as clm_poa_ind_sw20
    , cast(icd_dgns_cd21 as {{ dbt.type_string() }} ) as icd_dgns_cd21
    , cast(clm_poa_ind_sw21 as {{ dbt.type_string() }} ) as clm_poa_ind_sw21
    , cast(icd_dgns_cd22 as {{ dbt.type_string() }} ) as icd_dgns_cd22
    , cast(clm_poa_ind_sw22 as {{ dbt.type_string() }} ) as clm_poa_ind_sw22
    , cast(icd_dgns_cd23 as {{ dbt.type_string() }} ) as icd_dgns_cd23
    , cast(clm_poa_ind_sw23 as {{ dbt.type_string() }} ) as clm_poa_ind_sw23
    , cast(icd_dgns_cd24 as {{ dbt.type_string() }} ) as icd_dgns_cd24
    , cast(clm_poa_ind_sw24 as {{ dbt.type_string() }} ) as clm_poa_ind_sw24
    , cast(icd_dgns_cd25 as {{ dbt.type_string() }} ) as icd_dgns_cd25
    , cast(clm_poa_ind_sw25 as {{ dbt.type_string() }} ) as clm_poa_ind_sw25
    , cast(fst_dgns_e_cd as {{ dbt.type_string() }} ) as fst_dgns_e_cd
    , cast(icd_dgns_e_cd1 as {{ dbt.type_string() }} ) as icd_dgns_e_cd1
    , cast(clm_e_poa_ind_sw1 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw1
    , cast(icd_dgns_e_cd2 as {{ dbt.type_string() }} ) as icd_dgns_e_cd2
    , cast(clm_e_poa_ind_sw2 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw2
    , cast(icd_dgns_e_cd3 as {{ dbt.type_string() }} ) as icd_dgns_e_cd3
    , cast(clm_e_poa_ind_sw3 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw3
    , cast(icd_dgns_e_cd4 as {{ dbt.type_string() }} ) as icd_dgns_e_cd4
    , cast(clm_e_poa_ind_sw4 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw4
    , cast(icd_dgns_e_cd5 as {{ dbt.type_string() }} ) as icd_dgns_e_cd5
    , cast(clm_e_poa_ind_sw5 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw5
    , cast(icd_dgns_e_cd6 as {{ dbt.type_string() }} ) as icd_dgns_e_cd6
    , cast(clm_e_poa_ind_sw6 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw6
    , cast(icd_dgns_e_cd7 as {{ dbt.type_string() }} ) as icd_dgns_e_cd7
    , cast(clm_e_poa_ind_sw7 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw7
    , cast(icd_dgns_e_cd8 as {{ dbt.type_string() }} ) as icd_dgns_e_cd8
    , cast(clm_e_poa_ind_sw8 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw8
    , cast(icd_dgns_e_cd9 as {{ dbt.type_string() }} ) as icd_dgns_e_cd9
    , cast(clm_e_poa_ind_sw9 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw9
    , cast(icd_dgns_e_cd10 as {{ dbt.type_string() }} ) as icd_dgns_e_cd10
    , cast(clm_e_poa_ind_sw10 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw10
    , cast(icd_dgns_e_cd11 as {{ dbt.type_string() }} ) as icd_dgns_e_cd11
    , cast(clm_e_poa_ind_sw11 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw11
    , cast(icd_dgns_e_cd12 as {{ dbt.type_string() }} ) as icd_dgns_e_cd12
    , cast(clm_e_poa_ind_sw12 as {{ dbt.type_string() }} ) as clm_e_poa_ind_sw12
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
    , {{ try_to_cast_date('dob_dt', 'YYYYMMDD') }} as dob_dt
    , cast(gndr_cd as {{ dbt.type_string() }} ) as gndr_cd
    , cast(bene_race_cd as {{ dbt.type_string() }} ) as bene_race_cd
    , cast(bene_cnty_cd as {{ dbt.type_string() }} ) as bene_cnty_cd
    , cast(bene_state_cd as {{ dbt.type_string() }} ) as bene_state_cd
    , cast(cwf_bene_mdcr_stus_cd as {{ dbt.type_string() }} ) as cwf_bene_mdcr_stus_cd
    , cast(clm_trtmt_authrztn_num as {{ dbt.type_string() }} ) as clm_trtmt_authrztn_num
    , cast(clm_prcr_rtrn_cd as {{ dbt.type_string() }} ) as clm_prcr_rtrn_cd
    , cast(clm_ip_low_vol_pmt_amt as {{ dbt.type_numeric() }} ) as clm_ip_low_vol_pmt_amt
    , cast(clm_care_imprvmt_model_cd1 as {{ dbt.type_string() }} ) as clm_care_imprvmt_model_cd1
    , cast(clm_care_imprvmt_model_cd2 as {{ dbt.type_string() }} ) as clm_care_imprvmt_model_cd2
    , cast(clm_care_imprvmt_model_cd3 as {{ dbt.type_string() }} ) as clm_care_imprvmt_model_cd3
    , cast(clm_care_imprvmt_model_cd4 as {{ dbt.type_string() }} ) as clm_care_imprvmt_model_cd4
    , cast(clm_bndld_model_1_dscnt_pct as {{ dbt.type_string() }} ) as clm_bndld_model_1_dscnt_pct
    , cast(clm_base_oprtg_drg_amt as {{ dbt.type_numeric() }} ) as clm_base_oprtg_drg_amt
    , cast(clm_vbp_prtcpnt_ind_cd as {{ dbt.type_string() }} ) as clm_vbp_prtcpnt_ind_cd
    , cast(clm_vbp_adjstmt_pct as {{ dbt.type_string() }} ) as clm_vbp_adjstmt_pct
    , cast(clm_hrr_prtcpnt_ind_cd as {{ dbt.type_string() }} ) as clm_hrr_prtcpnt_ind_cd
    , cast(clm_hrr_adjstmt_pct as {{ dbt.type_string() }} ) as clm_hrr_adjstmt_pct
    , cast(clm_model_4_readmsn_ind_cd as {{ dbt.type_string() }} ) as clm_model_4_readmsn_ind_cd
    , cast(clm_uncompd_care_pmt_amt as {{ dbt.type_numeric() }} ) as clm_uncompd_care_pmt_amt
    , cast(clm_bndld_adjstmt_pmt_amt as {{ dbt.type_numeric() }} ) as clm_bndld_adjstmt_pmt_amt
    , cast(clm_vbp_adjstmt_pmt_amt as {{ dbt.type_numeric() }} ) as clm_vbp_adjstmt_pmt_amt
    , cast(clm_hrr_adjstmt_pmt_amt as {{ dbt.type_numeric() }} ) as clm_hrr_adjstmt_pmt_amt
    , cast(ehr_pymt_adjstmt_amt as {{ dbt.type_numeric() }} ) as ehr_pymt_adjstmt_amt
    , cast(pps_std_val_pymt_amt as {{ dbt.type_numeric() }} ) as pps_std_val_pymt_amt
    , cast(finl_std_amt as {{ dbt.type_numeric() }} ) as finl_std_amt
    , cast(hac_pgm_rdctn_ind_sw as {{ dbt.type_string() }} ) as hac_pgm_rdctn_ind_sw
    , cast(ehr_pgm_rdctn_ind_sw as {{ dbt.type_string() }} ) as ehr_pgm_rdctn_ind_sw
    , cast(clm_site_ntrl_pymt_cst_amt as {{ dbt.type_numeric() }} ) as clm_site_ntrl_pymt_cst_amt
    , cast(clm_site_ntrl_pymt_ipps_amt as {{ dbt.type_numeric() }} ) as clm_site_ntrl_pymt_ipps_amt
    , cast(clm_full_std_pymt_amt as {{ dbt.type_numeric() }} ) as clm_full_std_pymt_amt
    , cast(clm_ss_outlier_std_pymt_amt as {{ dbt.type_numeric() }} ) as clm_ss_outlier_std_pymt_amt
    , cast(clm_next_gnrtn_aco_ind_cd1 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd1
    , cast(clm_next_gnrtn_aco_ind_cd2 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd2
    , cast(clm_next_gnrtn_aco_ind_cd3 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd3
    , cast(clm_next_gnrtn_aco_ind_cd4 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd4
    , cast(clm_next_gnrtn_aco_ind_cd5 as {{ dbt.type_string() }} ) as clm_next_gnrtn_aco_ind_cd5
    , cast(aco_id_num as {{ dbt.type_string() }} ) as aco_id_num
    , cast(file_name as {{ dbt.type_string() }} ) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }} ) as ingest_datetime 
from {{ source('medicare_lds','inpatient_base_claim') }}