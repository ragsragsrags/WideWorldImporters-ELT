{%- macro test_existing_legacy(legacy_table, existing_table, primary_key) -%}
    {% set old_etl_relation=ref(legacy_table) %} 
    {% set dbt_relation=ref(existing_table) %}  

    WITH compare AS (

        {{ 
            audit_helper.compare_relations(
                a_relation=old_etl_relation,
                b_relation=dbt_relation,
                primary_key=primary_key) 
        }}

    ),

    legacy_cities_count AS (

        SELECT
            COUNT(*) AS cnt
        FROM
            {{ ref(legacy_table) }}

    ),

    cities_count AS (

        SELECT
            COUNT(*) AS cnt
        FROM
            {{ ref(existing_table) }}

    ),

    final AS (

        SELECT
            (
                SELECT
                    COUNT(*)
                FROM
                    compare
                WHERE
                    in_a = true AND
                    in_b = false
            ) > 0 AS has_data_in_a_but_not_in_b,
            (
                SELECT
                    COUNT(*)
                FROM
                    compare
                WHERE
                    in_a = false AND
                    in_b = true
            ) > 0 AS has_data_in_b_but_not_in_a,
            (
                SELECT 
                    SUM(cnt) 
                FROM 
                    legacy_cities_count
            ) <> 
            (
                SELECT
                    SUM(cnt)
                FROM 
                    cities_count
            ) AS counts_with_legacy_does_not_match 

    )

    SELECT
        *
    FROM
        final
    WHERE
        has_data_in_a_but_not_in_b OR
        has_data_in_b_but_not_in_a OR
        counts_with_legacy_does_not_match
  
{%- endmacro -%}