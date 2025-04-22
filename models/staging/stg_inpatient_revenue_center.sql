select
      cast(desy_sort_key as {{ dbt.type_string() }} ) as desy_sort_key
    , cast(claim_no as {{ dbt.type_string() }} ) as claim_no
    , cast(clm_line_num as {{ dbt.type_string() }} ) as clm_line_num
    , {{ try_to_cast_date('clm_thru_dt', 'YYYYMMDD') }} as clm_thru_dt
    , cast(nch_clm_type_cd as {{ dbt.type_string() }} ) as nch_clm_type_cd
    , cast(rev_cntr as {{ dbt.type_string() }} ) as rev_cntr
    , cast(hcpcs_cd as {{ dbt.type_string() }} ) as hcpcs_cd
    , cast(hcpcs_1st_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_1st_mdfr_cd
    , cast(hcpcs_2nd_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_2nd_mdfr_cd
    , cast(hcpcs_3rd_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_3rd_mdfr_cd
    , cast(regexp_substr(rev_cntr_unit_cnt, '.') as integer) as rev_cntr_unit_cnt
    , cast(rev_cntr_rate_amt as {{ dbt.type_numeric() }} ) as rev_cntr_rate_amt
    , cast(rev_cntr_tot_chrg_amt as {{ dbt.type_numeric() }} ) as rev_cntr_tot_chrg_amt
    , cast(rev_cntr_ncvrd_chrg_amt as {{ dbt.type_numeric() }} ) as rev_cntr_ncvrd_chrg_amt
    , cast(rev_cntr_ddctbl_coinsrnc_cd as {{ dbt.type_string() }} ) as rev_cntr_ddctbl_coinsrnc_cd
    , cast(rev_cntr_apc_hipps_cd as {{ dbt.type_string() }} ) as rev_cntr_apc_hipps_cd
    , cast(rev_cntr_rndrng_physn_upin as {{ dbt.type_string() }} ) as rev_cntr_rndrng_physn_upin
    , cast(rev_cntr_rndrng_physn_npi as {{ dbt.type_string() }} ) as rev_cntr_rndrng_physn_npi
    , cast(rev_cntr_rndrng_physn_spclty_cd as {{ dbt.type_string() }} ) as rev_cntr_rndrng_physn_spclty_cd
    , cast(rev_cntr_ide_ndc_upc_num as {{ dbt.type_string() }} ) as rev_cntr_ide_ndc_upc_num
    , cast(rev_cntr_prcng_ind_cd as {{ dbt.type_string() }} ) as rev_cntr_prcng_ind_cd
    , cast(thrpy_cap_ind_cd1 as {{ dbt.type_string() }} ) as thrpy_cap_ind_cd1
    , cast(thrpy_cap_ind_cd2 as {{ dbt.type_string() }} ) as thrpy_cap_ind_cd2
    , cast(file_name as {{ dbt.type_string() }} ) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }} ) as ingest_datetime 
from {{ source('medicare_lds','inpatient_revenue_center') }}