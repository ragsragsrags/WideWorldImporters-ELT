WITH changed_orders AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__fct_orders') }}

),

init_orders AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__fct_orders", 
            "stg_sales__orders") }}

),

final AS (

    SELECT
        *
    FROM
        changed_orders

    UNION ALL

    SELECT
        *
    FROM
        init_orders
    WHERE
        init_orders.order_id NOT IN  (
            SELECT
                changed_orders.order_id
            FROM
                changed_orders
        ) 

)

SELECT 
    * 
FROM 
    final