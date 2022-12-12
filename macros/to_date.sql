{#-
    Formats a date expression into "YYYY-MM-DD" based on a given input format.
    Currently supports input formats of "YYYYMMDD", "YYYYMM", and "YYYY".
-#}

{%- macro to_date(column_name, date_format) -%}

    {{ return(adapter.dispatch('to_date')(column_name, date_format)) }}

{%- endmacro -%}

{%- macro bigquery__to_date(column_name, date_format) -%}

    {%- if date_format == 'YYYYMMDD' -%}
    parse_date( '%Y%m%d', {{ cast_string_or_varchar(column_name) }} )
    {%- elif date_format == 'YYYYMM' -%}
    parse_date( '%Y%m', {{ cast_string_or_varchar(column_name) }} )
    {%- elif date_format == 'YYYY' -%}
    parse_date( '%Y', {{ cast_string_or_varchar(column_name) }} )
    {%- endif -%}

{%- endmacro -%}

{%- macro default__to_date(column_name, date_format) %}

    to_date( {{ cast_string_or_varchar(column_name) }}, {{ date_format }} )

{%- endmacro -%}

{%- macro redshift__to_date(column_name, date_format) -%}

    to_date( {{ cast_string_or_varchar(column_name) }}, {{ date_format }} )

{%- endmacro -%}

{%- macro snowflake__to_date(column_name, date_format) %}

    to_date( {{ cast_string_or_varchar(column_name) }}, {{ date_format }} )

{%- endmacro -%}