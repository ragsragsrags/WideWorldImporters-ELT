WITH changed_customers AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__dim_customers') }}

),

init_customers AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__dim_customers", 
            "stg_sales__customers") }}

),

final AS (

    SELECT
        *
    FROM
        changed_customers

    UNION ALL

    SELECT
        *
    FROM
        init_customers
    WHERE
        init_customers.customer_id NOT IN  (
            SELECT
                changed_customers.customer_id
            FROM
                changed_customers
        ) 

)

SELECT 
    * 
FROM 
    final