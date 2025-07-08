{%- macro table_exists_by_stage_view(database, table, stage_view) -%}
    {% set sql_statement_database %}
        SELECT 
            SPLIT_PART('{{ source(database, table) }}', '.', 1)
    {% endset %}
    {% set table_database = dbt_utils.get_single_value(sql_statement_database) %}

    {% set sql_statement_schema %}
        SELECT 
            SPLIT_PART('{{ source(database, table) }}', '.', 2)
    {% endset %}
    {% set table_schema = dbt_utils.get_single_value(sql_statement_schema) %}

    {% set sql_statement_table %}
        SELECT 
            SPLIT_PART('{{ source(database, table) }}', '.', 3)
    {% endset %}
    {% set table_name = dbt_utils.get_single_value(sql_statement_table) %}

    {% set sql_statement %}
        
        SELECT 
            EXISTS(
                SELECT 
                    1 
                FROM 
                    {{ table_database }}.information_schema.tables
                WHERE 
                    table_name ILIKE '{{ table_name }}' AND
                    table_schema ILIKE '{{ table_schema }}'
            )
    
    {% endset %}

    {%- set tables_exists = dbt_utils.get_single_value(sql_statement) -%}

    {% if tables_exists %}
    {{ table_database }}.{{ table_schema }}.{{ table_name }}
    {% else %}
    (
        SELECT TOP 1
            *
        FROM
            {{ ref(stage_view) }}
        WHERE 
            1 = 2
    )
    {% endif %}
  
{%- endmacro -%}