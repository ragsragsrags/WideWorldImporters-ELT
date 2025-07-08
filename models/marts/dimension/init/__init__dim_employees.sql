WITH changed_employees AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__dim_employees') }}

),

init_employees AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__dim_employees", 
            "stg_application__people") }}

),

final AS (

    SELECT
        *
    FROM
        changed_employees

    UNION ALL

    SELECT
        *
    FROM
        init_employees
    WHERE
        init_employees.people_id NOT IN  (
            SELECT
                changed_employees.people_id
            FROM
                changed_employees
        ) 

)

SELECT 
    * 
FROM 
    final