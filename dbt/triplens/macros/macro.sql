{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {# If no custom schema is defined, fall back to the default profile schema #}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {# Enforce the exact custom schema name #}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}