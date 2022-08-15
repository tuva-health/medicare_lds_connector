-----------------------------------------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      August 2022
-- Purpose      Union of all claim line types
-- Notes
-----------------------------------------------------------------------------------------------------------------
-- Modification History
--
-----------------------------------------------------------------------------------------------------------------

select * from {{ ref('carrier_claim')}}
union all
select * from {{ ref('dme_claim')}}
union all
select * from {{ ref('home_health_claim')}}
union all
select * from {{ ref('hospice_claim')}}
union all
select * from {{ ref('inpatient_claim')}}
union all
select * from {{ ref('outpatient_claim')}}
union all
select * from {{ ref('snf_claim')}}