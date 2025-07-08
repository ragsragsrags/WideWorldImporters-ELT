WITH cutoff AS (

    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}

),

stock_holdings AS (

    SELECT 
        SI.StockItemID AS wwi_stock_item_id,
        SIH.QuantityOnHand AS quantity_on_hand,
        SIH.BinLocation AS bin_location,
        SIH.LastStocktakeQuantity AS last_stocktake_quantity,
        SIH.LastCostPrice AS last_cost_price,
        SIH.ReorderLevel AS reorder_level,
        SIH.TargetStockLevel AS target_stock_level
    FROM
        WideWorldImporters.Warehouse.StockItemHoldings SIH JOIN
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName
            FROM
                WideWorldImporters.Warehouse.StockItems SI
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SI.ValidFrom AND SI.ValidTo

            UNION

            SELECT
                SIA.StockItemID,
                SIA.StockItemName
            FROM
                WideWorldImporters.Warehouse.StockItems_Archive SIA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI ON
            SI.StockItemID = SIH.StockItemID

)

SELECT
    *
FROM
    stock_holdings