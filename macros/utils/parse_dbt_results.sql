{% macro parse_dbt_results(results) %}

    {%- set parsed_results = [] %}

    {% for run_result in results %}
        -- Convert the run result object to a simple dictionary
        {% set run_result_dict = run_result.to_dict() %}

        -- Get the underlying dbt graph node that was executed
        {% set node = run_result_dict.get('node') %}

        -- Extract desired parameters
        {% set run_type = flags.WHICH %} -- 'run', 'test', etc.

        {% if run_type in ['run','snapshot'] %}
            {% set rows_affected = run_result_dict.get('adapter_response', {}).get('rows_affected', 0) %}
        {% elif run_type == 'test' %}
            {% set rows_affected = run_result_dict.get('failures', 0) %}
        {%- endif -%}

        {%- if not rows_affected -%}
            {% set rows_affected = 0 %}
        {%- endif -%}

        {%- set timing_length = run_result_dict.get('timing') | length -%} -- We don't want to check non-existing indexes
        {%- set started_at = namespace(value=0) -%} -- We need to use a namespace to set the value inside a loop
        {%- set completed_at = namespace(value=0) -%}

        {% for list_index in range(timing_length) %}
            {% if run_result_dict.get('timing')[list_index].get('name') == 'execute' %}
                {%- set started_at.value = run_result_dict.get('timing')[list_index].get('started_at') -%}
                {%- set completed_at.value = run_result_dict.get('timing')[list_index].get('completed_at') -%}
            {% endif %}
        {% endfor %}

        {% if invocation_args_dict and invocation_args_dict.vars %}
            {% if invocation_args_dict.vars is mapping %}
                {% set invocation_vars = invocation_args_dict.vars %}
            {% else %}
                {% set invocation_vars = fromyaml(invocation_args_dict.vars) %}
            {% endif %}
            {% set invocation_vars = tojson(invocation_vars) %}
        {% else %}
            {% set invocation_vars = {} %}
        {% endif %}

        {% set parsed_result_dict = {
                'invocation_id': invocation_id,
                'query_id': run_result_dict.get('adapter_response', {}).get('query_id', null),
                'unique_id': node.get('unique_id'),
                'run_type': run_type,
                'database_name': node.get('database'),
                'schema_name': node.get('schema'),
                'table_name': node.get('name'),
                'tags': node.get('tags')|join(', '),
                'started_at': started_at.value,
                'completed_at': completed_at.value,
                'status': run_result_dict.get('status'),
                'message': run_result_dict.get('message'),
                'execution_time': run_result_dict.get('execution_time'),
                'rows_affected': rows_affected,
                'dbt_vars': invocation_vars
        }%}

        {% do parsed_results.append(parsed_result_dict) %}

    {% endfor %}
    {{ return(parsed_results) }}
{% endmacro %}