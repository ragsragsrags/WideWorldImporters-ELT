WITH changed_stock_holdings AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__fct_stock_holdings') }}

),

init_stock_holdings AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__fct_stock_holdings", 
            "stg_warehouse__stock_item_holdings") }}

),

final AS (

    SELECT
        *
    FROM
        changed_stock_holdings

    UNION ALL

    SELECT
        *
    FROM
        init_stock_holdings
    WHERE
        init_stock_holdings.stock_item_id NOT IN  (
            SELECT
                changed_stock_holdings.stock_item_id
            FROM
                changed_stock_holdings
        ) 

)

SELECT 
    * 
FROM 
    final