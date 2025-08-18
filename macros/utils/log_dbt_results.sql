{% macro log_dbt_results(results) %}

    {%- if execute -%}

        {%- set parsed_results = parse_dbt_results(results) -%}

        {% if parsed_results | length > 0 %}
            {% set insert_dbt_results_query %}
                insert into {{ target.database }}.internals.dbt_job_status (
                    invocation_id
                    , query_id
                    , unique_id
                    , run_type
                    , database_name
                    , schema_name
                    , table_name
                    , tags
                    , started_at
                    , completed_at
                    , status
                    , message
                    , execution_time
                    , rows_affected
                    , dbt_vars
                    , loading_time
                ) values
                {% for r in parsed_results %}
                 (
                    '{{ r.invocation_id }}'
                    , '{{ r.query_id }}'
                    , '{{ r.unique_id }}'
                    , '{{ r.run_type }}'
                    , '{{ r.database_name }}'
                    , '{{ r.schema_name }}'
                    , '{{ r.table_name }}'
                    , '{{ r.tags }}'
                    , {{ "'" ~ r.started_at ~ "'" if r.started_at else "null" }}
                    , {{ "'" ~ r.completed_at ~ "'" if r.completed_at else "null" }}
                    , '{{ r.status }}'
                    , {{ "'" ~ r.message ~ "'" if r.message else "null" }}
                    , {{ r.execution_time }}
                    , {{ r.rows_affected }}
                    , '{{ r.dbt_vars }}'
                    , CURRENT_TIMESTAMP()
                )
                {% if not loop.last %},{% endif %}
                {% endfor %}
            {% endset %}
            {%- do run_query(insert_dbt_results_query) -%}
        {% endif %}
    {% endif %}
    -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
    {{ log("Here you can find current run's invocation_id: "  ~ invocation_id ~ '.', True)}}
    {{ return ('') }}
{% endmacro %}