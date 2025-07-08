WITH changed_stock_items AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__dim_stock_items') }}

),

init_stock_items AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__dim_stock_items", 
            "stg_warehouse__stock_items") }}

),

final AS (

    SELECT
        *
    FROM
        changed_stock_items

    UNION ALL

    SELECT
        *
    FROM
        init_stock_items
    WHERE
        init_stock_items.stock_item_id NOT IN  (
            SELECT
                changed_stock_items.stock_item_id
            FROM
                changed_stock_items
        ) 

)

SELECT 
    * 
FROM 
    final