with header_payment as(
    select
        cast(claim_no || date_part(year,date(clm_thru_dt,'yyyymmdd')) || nch_clm_type_cd as varchar) as claim_id
        ,clm_pmt_amt
    from {{ var('snf_base_claim')}}
) 

select
    /* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
    cast(b.claim_no || date_part(year,date(b.clm_thru_dt,'yyyymmdd')) || b.nch_clm_type_cd as varchar) as claim_id
    ,cast(l.clm_line_num as varchar) as claim_line_number
    ,cast('institutional' as varchar) as claim_type
    ,cast(b.desy_sort_key as varchar) as patient_id
    ,cast(NULL as varchar) as member_id
    ,try_to_date(b.clm_thru_dt,'yyyymmdd') as claim_start_date
    ,try_to_date(b.clm_thru_dt,'yyyymmdd') as claim_end_date
    ,try_to_date(l.clm_thru_dt,'yyyymmdd') as claim_line_start_date
    ,try_to_date(l.clm_thru_dt,'yyyymmdd') as claim_line_end_date
    ,try_to_date(b.clm_admsn_dt,'yyyymmdd') as admission_date
    ,try_to_date(b.nch_bene_dschrg_dt,'yyyymmdd') as discharge_date
    ,cast(NULL as varchar) as admit_source_code
    ,cast(NULL as varchar) as admit_type_code
    ,cast(b.ptnt_dschrg_stus_cd as varchar) as discharge_disposition_code
    ,cast(NULL as varchar) as place_of_service_code
    ,cast(b.clm_fac_type_cd || b.clm_srvc_clsfctn_type_cd || b.clm_freq_cd as varchar) as bill_type_code
    ,cast(b.clm_drg_cd as varchar) as ms_drg
    ,cast(l.rev_cntr as varchar) as revenue_center_code
    ,cast(l.rev_cntr_unit_cnt as int) as service_unit_quantity
    ,cast(l.hcpcs_cd as varchar) as hcpcs_code
    ,cast(l.hcpcs_1st_mdfr_cd as varchar) as hcpcs_modifier_1
    ,cast(l.hcpcs_2nd_mdfr_cd as varchar) as hcpcs_modifier_2
    ,cast(l.hcpcs_3rd_mdfr_cd as varchar) as hcpcs_modifier_3
    ,cast(NULL as varchar) as hcpcs_modifier_4
    ,cast(NULL as varchar) as hcpcs_modifier_5
    ,cast(b.rndrng_physn_npi as varchar) as rendering_npi
    ,cast(NULL as varchar) as billing_npi
    ,cast(b.org_npi_num as varchar) as facility_npi
    ,cast(NULL as varchar) as paid_date
    ,cast(coalesce(p.clm_pmt_amt,0) as numeric(38,2)) as paid_amount
    ,cast(NULL as float) as allowed_amount
    ,cast(l.rev_cntr_tot_chrg_amt as numeric(38,2)) as charge_amount
    ,cast('icd-10-cm' as varchar) as diagnosis_code_type
    ,cast(b.prncpal_dgns_cd as varchar) as diagnosis_code_1
    ,cast(b.icd_dgns_cd2 as varchar) as diagnosis_code_2
    ,cast(b.icd_dgns_cd3 as varchar) as diagnosis_code_3
    ,cast(b.icd_dgns_cd4 as varchar) as diagnosis_code_4
    ,cast(b.icd_dgns_cd5 as varchar) as diagnosis_code_5
    ,cast(b.icd_dgns_cd6 as varchar) as diagnosis_code_6
    ,cast(b.icd_dgns_cd7 as varchar) as diagnosis_code_7
    ,cast(b.icd_dgns_cd8 as varchar) as diagnosis_code_8
    ,cast(b.icd_dgns_cd9 as varchar) as diagnosis_code_9
    ,cast(b.icd_dgns_cd10 as varchar) as diagnosis_code_10
    ,cast(b.icd_dgns_cd11 as varchar) as diagnosis_code_11
    ,cast(b.icd_dgns_cd12 as varchar) as diagnosis_code_12
    ,cast(b.icd_dgns_cd13 as varchar) as diagnosis_code_13
    ,cast(b.icd_dgns_cd14 as varchar) as diagnosis_code_14
    ,cast(b.icd_dgns_cd15 as varchar) as diagnosis_code_15
    ,cast(b.icd_dgns_cd16 as varchar) as diagnosis_code_16
    ,cast(b.icd_dgns_cd17 as varchar) as diagnosis_code_17
    ,cast(b.icd_dgns_cd18 as varchar) as diagnosis_code_18
    ,cast(b.icd_dgns_cd19 as varchar) as diagnosis_code_19
    ,cast(b.icd_dgns_cd20 as varchar) as diagnosis_code_20
    ,cast(b.icd_dgns_cd21 as varchar) as diagnosis_code_21
    ,cast(b.icd_dgns_cd22 as varchar) as diagnosis_code_22
    ,cast(b.icd_dgns_cd23 as varchar) as diagnosis_code_23
    ,cast(b.icd_dgns_cd24 as varchar) as diagnosis_code_24
    ,cast(b.icd_dgns_cd25 as varchar) as diagnosis_code_25
    ,cast(NULL as varchar) as diagnosis_poa_1
    ,cast(NULL as varchar) as diagnosis_poa_2
    ,cast(NULL as varchar) as diagnosis_poa_3
    ,cast(NULL as varchar) as diagnosis_poa_4
    ,cast(NULL as varchar) as diagnosis_poa_5
    ,cast(NULL as varchar) as diagnosis_poa_6
    ,cast(NULL as varchar) as diagnosis_poa_7
    ,cast(NULL as varchar) as diagnosis_poa_8
    ,cast(NULL as varchar) as diagnosis_poa_9
    ,cast(NULL as varchar) as diagnosis_poa_10
    ,cast(NULL as varchar) as diagnosis_poa_11
    ,cast(NULL as varchar) as diagnosis_poa_12
    ,cast(NULL as varchar) as diagnosis_poa_13
    ,cast(NULL as varchar) as diagnosis_poa_14
    ,cast(NULL as varchar) as diagnosis_poa_15
    ,cast(NULL as varchar) as diagnosis_poa_16
    ,cast(NULL as varchar) as diagnosis_poa_17
    ,cast(NULL as varchar) as diagnosis_poa_18
    ,cast(NULL as varchar) as diagnosis_poa_19
    ,cast(NULL as varchar) as diagnosis_poa_20
    ,cast(NULL as varchar) as diagnosis_poa_21
    ,cast(NULL as varchar) as diagnosis_poa_22
    ,cast(NULL as varchar) as diagnosis_poa_23
    ,cast(NULL as varchar) as diagnosis_poa_24
    ,cast(NULL as varchar) as diagnosis_poa_25
    ,cast('icd-10-pcs' as varchar) as procedure_code_type
    ,cast(b.icd_prcdr_cd1 as varchar) as procedure_code_1
    ,cast(b.icd_prcdr_cd2 as varchar) as procedure_code_2
    ,cast(b.icd_prcdr_cd3 as varchar) as procedure_code_3
    ,cast(b.icd_prcdr_cd4 as varchar) as procedure_code_4
    ,cast(b.icd_prcdr_cd5 as varchar) as procedure_code_5
    ,cast(b.icd_prcdr_cd6 as varchar) as procedure_code_6
    ,cast(b.icd_prcdr_cd7 as varchar) as procedure_code_7
    ,cast(b.icd_prcdr_cd8 as varchar) as procedure_code_8
    ,cast(b.icd_prcdr_cd9 as varchar) as procedure_code_9
    ,cast(b.icd_prcdr_cd10 as varchar) as procedure_code_10
    ,cast(b.icd_prcdr_cd11 as varchar) as procedure_code_11
    ,cast(b.icd_prcdr_cd12 as varchar) as procedure_code_12
    ,cast(b.icd_prcdr_cd13 as varchar) as procedure_code_13
    ,cast(b.icd_prcdr_cd14 as varchar) as procedure_code_14
    ,cast(b.icd_prcdr_cd15 as varchar) as procedure_code_15
    ,cast(b.icd_prcdr_cd16 as varchar) as procedure_code_16
    ,cast(b.icd_prcdr_cd17 as varchar) as procedure_code_17
    ,cast(b.icd_prcdr_cd18 as varchar) as procedure_code_18
    ,cast(b.icd_prcdr_cd19 as varchar) as procedure_code_19
    ,cast(b.icd_prcdr_cd20 as varchar) as procedure_code_20
    ,cast(b.icd_prcdr_cd21 as varchar) as procedure_code_21
    ,cast(b.icd_prcdr_cd22 as varchar) as procedure_code_22
    ,cast(b.icd_prcdr_cd23 as varchar) as procedure_code_23
    ,cast(b.icd_prcdr_cd24 as varchar) as procedure_code_24
    ,cast(b.icd_prcdr_cd25 as varchar) as procedure_code_25
    ,try_to_date(b.prcdr_dt1,'yyyymmdd') as procedure_date_1
    ,try_to_date(b.prcdr_dt2,'yyyymmdd') as procedure_date_2
    ,try_to_date(b.prcdr_dt3,'yyyymmdd') as procedure_date_3
    ,try_to_date(b.prcdr_dt4,'yyyymmdd') as procedure_date_4
    ,try_to_date(b.prcdr_dt5,'yyyymmdd') as procedure_date_5
    ,try_to_date(b.prcdr_dt6,'yyyymmdd') as procedure_date_6
    ,try_to_date(b.prcdr_dt7,'yyyymmdd') as procedure_date_7
    ,try_to_date(b.prcdr_dt8,'yyyymmdd') as procedure_date_8
    ,try_to_date(b.prcdr_dt9,'yyyymmdd') as procedure_date_9
    ,try_to_date(b.prcdr_dt10,'yyyymmdd') as procedure_date_10
    ,try_to_date(b.prcdr_dt11,'yyyymmdd') as procedure_date_11
    ,try_to_date(b.prcdr_dt12,'yyyymmdd') as procedure_date_12
    ,try_to_date(b.prcdr_dt13,'yyyymmdd') as procedure_date_13
    ,try_to_date(b.prcdr_dt14,'yyyymmdd') as procedure_date_14
    ,try_to_date(b.prcdr_dt15,'yyyymmdd') as procedure_date_15
    ,try_to_date(b.prcdr_dt16,'yyyymmdd') as procedure_date_16
    ,try_to_date(b.prcdr_dt17,'yyyymmdd') as procedure_date_17
    ,try_to_date(b.prcdr_dt18,'yyyymmdd') as procedure_date_18
    ,try_to_date(b.prcdr_dt19,'yyyymmdd') as procedure_date_19
    ,try_to_date(b.prcdr_dt20,'yyyymmdd') as procedure_date_20
    ,try_to_date(b.prcdr_dt21,'yyyymmdd') as procedure_date_21
    ,try_to_date(b.prcdr_dt22,'yyyymmdd') as procedure_date_22
    ,try_to_date(b.prcdr_dt23,'yyyymmdd') as procedure_date_23
    ,try_to_date(b.prcdr_dt24,'yyyymmdd') as procedure_date_24
    ,try_to_date(b.prcdr_dt25,'yyyymmdd') as procedure_date_25
    ,cast('saf' as varchar) as data_source
from {{ var('snf_base_claim')}} b
inner join {{ var('snf_revenue_center')}} l
    on b.claim_no = l.claim_no
/* Payment is provided at the header level only.  Populating on line number 1 to avoid duplication. */
left join header_payment p
    on cast(b.claim_no || date_part(year,date(b.clm_thru_dt,'yyyymmdd')) || b.nch_clm_type_cd as varchar) = p.claim_id
    and l.clm_line_num = 1