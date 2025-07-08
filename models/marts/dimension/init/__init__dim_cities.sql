WITH changed_cities AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__dim_cities') }}

),

init_cities AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__dim_cities", 
            "stg_application__cities") }}

),

final AS (

    SELECT
        *
    FROM
        changed_cities

    UNION ALL

    SELECT
        *
    FROM
        init_cities 
    WHERE
        init_cities.city_id NOT IN  (
            SELECT
                changed_cities.city_id
            FROM
                changed_cities
        ) 

)

SELECT 
    * 
FROM 
    final