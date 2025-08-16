{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}

    {%- if custom_database_name is none -%}

        {%- set database = default_database -%}

    {%- else -%}

        {%- set database = custom_database_name | trim -%}

    {%- endif -%}

    {{ return(database) }}

{%- endmacro %}