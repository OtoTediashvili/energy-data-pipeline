{#
    By default dbt builds schemas as "<target_schema>_<custom_schema>", which
    gives you main_raw, main_staging, main_marts. That prefixing exists to keep
    developers from colliding in a shared warehouse.

    Here each developer has their own DuckDB file, so the prefix buys nothing
    and makes source() references awkward. This override uses the custom schema
    verbatim: raw, staging, marts.

    If you later move to a shared Snowflake or BigQuery instance, revert this
    or gate it on target.name so dev runs stay isolated.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
