WITH changed_purchases AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__fct_purchases') }}

),

init_purchases AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__fct_purchases", 
            "stg_purchasing__purchase_orders") }}

),

final AS (

    SELECT
        *
    FROM
        changed_purchases

    UNION ALL

    SELECT
        *
    FROM
        init_purchases
    WHERE
        init_purchases.purchase_order_id NOT IN  (
            SELECT
                changed_purchases.purchase_order_id
            FROM
                changed_purchases
        ) 

)

SELECT 
    * 
FROM 
    final