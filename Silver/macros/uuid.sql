{% macro validate_uuid(column_name) %}

    case 
        when {{ column_name }} is null then null
        when regexp_like(
            {{ column_name }},
            '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        )
        then {{ column_name }}
        else null
    end

{% endmacro %}
