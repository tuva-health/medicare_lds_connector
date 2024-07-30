
with month_start_and_end_dates as (
  select distinct 
      year as year_nbr
      ,month as month_nbr
      ,first_day_of_month
      ,last_day_of_month
  from {{ ref('reference_data__calendar')}}
)

,member_months as (
select distinct
    a.patient_id
  , year_nbr
  , month_nbr
  , a.payer
  , a.plan
  , data_source
from {{ ref('eligibility') }} a
inner join month_start_and_end_dates b
  on a.enrollment_start_date <= b.last_day_of_month
  and a.enrollment_end_date >= b.first_day_of_month
)

-- limit claims to only enrolled members, removing claims for members that we only have partial claims data for.
select c.*
,cast(null as {{ dbt.type_string() }} ) as rendering_tin
,cast(null as {{ dbt.type_string() }} ) as billing_tin
from {{ ref('_int_medical_claim') }} c
inner join member_months mm on c.patient_id = mm.patient_id
and
extract(year from c.claim_start_date) = mm.year_nbr
and
extract(month from c.claim_start_date) = mm.month_nbr