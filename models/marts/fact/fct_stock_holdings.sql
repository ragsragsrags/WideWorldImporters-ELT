WITH stock_holdings AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__fct_stock_holdings') }}      

),

stock_items AS (
    
    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY stock_holdings.stock_item_id) AS stock_holding_key,
        stock_items.stock_item_key,
        stock_items.wwi_stock_item_id,
        stock_holdings.stock_item_holding_quantity_on_hand AS quantity_on_hand,
        stock_holdings.stock_item_holding_bin_location AS bin_location,
        stock_holdings.stock_item_holding_last_stocktake_quantity AS last_stocktake_quantity,
        stock_holdings.stock_item_holding_last_cost_price AS last_cost_price,
        stock_holdings.stock_item_holding_reorder_level AS reorder_level,
        stock_holdings.stock_item_holding_target_stock_level AS target_stock_level
    FROM
        stock_holdings JOIN
        stock_items ON
            stock_holdings.stock_item_id = stock_items.wwi_stock_item_id 
                
)

SELECT
    *
FROM
    final