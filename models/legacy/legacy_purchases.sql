WITH cutoff AS (
    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}
),

purchases AS (

    SELECT
        PO.OrderDate AS date_key,
        PO.PurchaseOrderID AS wwi_purchase_order_id,
        POL.PurchaseOrderLineID AS wwi_purchase_order_line_id,
        S.SupplierID AS wwi_supplier_id,
        SI.StockItemID AS wwi_stock_item_id,
        POL.OrderedOuters AS ordered_outers,
        POL.OrderedOuters * SI.QuantityPerOuter AS ordered_quantity,
        POL.ReceivedOuters AS received_outers,
        PT.PackageTypeName AS package,
        PO.IsOrderFinalized AS is_order_finalized
    FROM
        WideWorldImporters.Purchasing.PurchaseOrders PO LEFT JOIN
        WideWorldImporters.Purchasing.PurchaseOrderLines POL ON
            POL.PurchaseOrderID = PO.PurchaseOrderID LEFT JOIN
        (
            SELECT 
                S.SupplierID,
                S.SupplierName
            FROM
                WideWorldImporters.Purchasing.Suppliers S
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN S.ValidFrom AND S.ValidTo

            UNION

            SELECT 
                SA.SupplierID,
                SA.SupplierName
            FROM
                WideWorldImporters.Purchasing.Suppliers_Archive SA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SA.ValidFrom AND SA.ValidTo
        ) S ON
            S.SupplierID = PO.SupplierID LEFT JOIN
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName,
                SI.QuantityPerOuter
            FROM
                WideWorldImporters.Warehouse.StockItems SI 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SI.ValidFrom AND SI.ValidTo

            UNION

            SELECT
                SIA.StockItemID,
                SIA.StockItemName,
                SIA.QuantityPerOuter
            FROM
                WideWorldImporters.Warehouse.StockItems_Archive SIA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI ON
            SI.StockItemID = POL.StockItemID LEFT JOIN
        (
            SELECT
                PT.PackageTypeID,
                PT.PackageTypeName
            FROM
                WideWorldImporters.Warehouse.PackageTypes PT
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PT.ValidFrom AND PT.ValidTo

            UNION

            SELECT
                PTA.PackageTypeID,
                PTA.PackageTypeName
            FROM
                WideWorldImporters.Warehouse.PackageTypes_Archive PTA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PTA.ValidFrom AND PTA.ValidTo
        ) PT ON
            PT.PackageTypeID = POL.PackageTypeID
    WHERE
        PO.LastEditedWhen <= (SELECT TOP 1 cutofftime FROM cutoff) OR
        POL.LastEditedWhen <= (SELECT TOP 1 cutofftime FROM cutoff)

)

SELECT
    *
FROM
    purchases