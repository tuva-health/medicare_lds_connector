{% set ns = namespace(first=true) %}
{% set percentiles = var('benchmarking_percentiles_percentiles', [10, 25, 50, 75, 90]) %}

with org_metrics as (

    select
          actual_paid_amount_pmpm
        , risk_adj_paid_amount_pmpm
        , actual_inpatient_paid_amount_pmpm
        , risk_adj_inpatient_paid_amount_pmpm
        , actual_outpatient_paid_amount_pmpm
        , risk_adj_outpatient_paid_amount_pmpm
        , actual_office_based_paid_amount_pmpm
        , risk_adj_office_based_paid_amount_pmpm
        , actual_other_paid_amount_pmpm
        , risk_adj_other_paid_amount_pmpm

        , actual_inpatient_encounter_count_per_mm
        , risk_adj_inpatient_encounter_count_per_mm
        , actual_outpatient_encounter_count_per_mm
        , risk_adj_outpatient_encounter_count_per_mm
        , actual_office_based_encounter_count_per_mm
        , risk_adj_office_based_encounter_count_per_mm
        , actual_other_encounter_count_per_mm
        , risk_adj_other_encounter_count_per_mm
    from {{ ref('org_encounter_group_metrics') }}

)

select *
from (

    {% set metric_pairs = [
        ("paid_amount_pmpm", "actual_paid_amount_pmpm", "risk_adj_paid_amount_pmpm"),
        ("inpatient_paid_amount_pmpm", "actual_inpatient_paid_amount_pmpm", "risk_adj_inpatient_paid_amount_pmpm"),
        ("outpatient_paid_amount_pmpm", "actual_outpatient_paid_amount_pmpm", "risk_adj_outpatient_paid_amount_pmpm"),
        ("office_based_paid_amount_pmpm", "actual_office_based_paid_amount_pmpm", "risk_adj_office_based_paid_amount_pmpm"),
        ("other_paid_amount_pmpm", "actual_other_paid_amount_pmpm", "risk_adj_other_paid_amount_pmpm"),

        ("inpatient_encounter_count_per_mm", "actual_inpatient_encounter_count_per_mm", "risk_adj_inpatient_encounter_count_per_mm"),
        ("outpatient_encounter_count_per_mm", "actual_outpatient_encounter_count_per_mm", "risk_adj_outpatient_encounter_count_per_mm"),
        ("office_based_encounter_count_per_mm", "actual_office_based_encounter_count_per_mm", "risk_adj_office_based_encounter_count_per_mm"),
        ("other_encounter_count_per_mm", "actual_other_encounter_count_per_mm", "risk_adj_other_encounter_count_per_mm"),
    ] %}

    {% for metric_name, actual_col, risk_adj_col in metric_pairs %}
    {% for p in percentiles %}
    {% for value_type, metric_col in [('actual', actual_col), ('risk_adj', risk_adj_col)] %}
    {% if not ns.first %} union all {% endif %}
    select distinct
          '{{ metric_name }}' as metric_name
        , '{{ value_type }}' as value_type
        , {{ p }} as percentile
        , percentile_cont({{ p / 100 }}) within group (order by {{ metric_col }}) over () as metric_value
    from org_metrics
    where {{ metric_col }} is not null
    {% set ns.first = false %}
    {% endfor %}
    {% endfor %}
    {% endfor %}

)
