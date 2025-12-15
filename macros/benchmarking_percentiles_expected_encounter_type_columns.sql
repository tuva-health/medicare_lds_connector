{% macro benchmarking_percentiles_expected_encounter_type_columns() %}
    {%- set excluded = [
        "expected_paid_amount",
        "expected_inpatient_paid_amount",
        "expected_outpatient_paid_amount",
        "expected_office_based_paid_amount",
        "expected_other_paid_amount",
        "expected_inpatient_encounter_count",
        "expected_outpatient_encounter_count",
        "expected_office_based_encounter_count",
        "expected_other_encounter_count",
    ] -%}

    {%- set ns = namespace(cols=[]) -%}
    {%- for col in benchmarking_percentiles_expected_columns() -%}
        {%- if col not in excluded -%}
            {%- do ns.cols.append(col) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(ns.cols) }}
{% endmacro %}

