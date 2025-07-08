WITH changed_movements AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__fct_movements') }}

),

init_movements AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__fct_movements", 
            "stg_warehouse__stock_item_transactions") }}

),

final AS (

    SELECT
        *
    FROM
        changed_movements

    UNION ALL

    SELECT
        *
    FROM
        init_movements
    WHERE
        init_movements.stock_item_transaction_id NOT IN  (
            SELECT
                changed_movements.stock_item_transaction_id
            FROM
                changed_movements
        ) 

)

SELECT 
    * 
FROM 
    final