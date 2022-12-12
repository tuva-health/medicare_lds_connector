{#-
    Returns the value corresponding to the specified date part.
-#}

{%- macro date_trunc(column_name, date_format, date_part) -%}

    {{ return(adapter.dispatch('date_trunc')(column_name, date_format, date_part)) }}

{%- endmacro -%}

{%- macro bigquery__date_trunc(column_name, date_format, date_part) -%}

    date_trunc( {{ try_to_cast_date(column_name, date_format) }}, {{ date_part }} )

{%- endmacro -%}

{%- macro default__date_trunc(column_name, date_format, date_part) %}

    date_trunc( {{ date_part }}, {{ try_to_cast_date(column_name, date_format) }} )

{%- endmacro -%}

{%- macro redshift__date_trunc(column_name, date_format, date_part) -%}

    date_trunc( {{ date_part }}, {{ try_to_cast_date(column_name, date_format) }} )

{%- endmacro -%}

{%- macro snowflake__date_trunc(column_name, date_format, date_part) %}

    date_trunc( {{ date_part }}, {{ try_to_cast_date(column_name, date_format) }} )

{%- endmacro -%}