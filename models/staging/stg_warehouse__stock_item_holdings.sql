WITH source AS (

    SELECT
        *
    FROM
        {{ source('base_warehouse', 'stockitemholdings') }}

),

transformed AS (

    SELECT
        StockItemID AS stock_item_id,
        QuantityOnHand AS stock_item_holding_quantity_on_hand,
        BinLocation AS stock_item_holding_bin_location,
        LastStocktakeQuantity AS stock_item_holding_last_stocktake_quantity,
        LastCostPrice AS stock_item_holding_last_cost_price,
        ReorderLevel AS stock_item_holding_reorder_level,
        TargetStockLevel AS stock_item_holding_target_stock_level,
        LastEditedBy AS stock_item_holding_last_edited_by,
        LastEditedWhen AS stock_item_holding_last_edited_when
    FROM
        source

)

SELECT
    *
FROM
    transformed