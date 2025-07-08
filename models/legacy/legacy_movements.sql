WITH cutoff AS (
    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}
),

movements AS (

    SELECT
        SIT.TransactionOccurredWhen,
        SI.StockItemID,
        SI.StockItemName,
        C.CustomerID,
        C.CustomerName,
        S.SupplierID,
        S.SupplierName,
        TT.TransactionTypeID,
        TT.TransactionTypeName,
        SIT.StockItemTransactionID,
        SIT.InvoiceID,
        SIT.PurchaseOrderID,
        SIT.Quantity
    FROM
        WideWorldImporters.Warehouse.StockItemTransactions SIT LEFT JOIN
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName
            FROM
                WideWorldImporters.Warehouse.StockItems SI 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN Si.ValidFrom AND SI.ValidTo

            UNION

            SELECT
                SIA.StockItemID,
                SIA.StockItemName
            FROM
                WideWorldImporters.Warehouse.StockItems_Archive SIA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI ON
            SI.StockItemID = SIT.StockItemID LEFT JOIN
        (
            SELECT
                C.CustomerID,
                C.CustomerName
            FROM
                WideWorldImporters.Sales.Customers C 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo

            UNION

            SELECT
                CA.CustomerID,
                CA.CustomerName
            FROM
                WideWorldImporters.Sales.Customers_Archive CA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo
        ) C ON 
            C.CustomerID = SIT.CustomerID LEFT JOIN
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
            S.SupplierID = SIT.SupplierID LEFT JOIN
        (
            SELECT
                TT.TransactionTypeID,
                TT.TransactionTypeName
            FROM
                WideWorldImporters.Application.TransactionTypes TT 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN TT.ValidFrom AND TT.ValidTo

            UNION

            SELECT
                TTA.TransactionTypeID,
                TTA.TransactionTypeName
            FROM
                WideWorldImporters.Application.TransactionTypes_Archive TTA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN TTA.ValidFrom AND TTA.ValidTo
        ) TT ON 
            TT.TransactionTypeID = SIT.TransactionTypeID
    WHERE
        SIT.LastEditedWhen <= (SELECT TOP 1 cutofftime FROM cutoff)

),

final AS (

    SELECT
        *
    FROM
        movements

)

SELECT 
    CAST(final.TransactionOccurredWhen AS DATE) AS date_key,
    final.StockItemTransactionID AS wwi_stock_item_transaction_id,
    final.StockItemID AS wwi_stock_item_id,
    final.CustomerID AS wwi_customer_id,
    final.SupplierID AS wwi_supplier_id,
    final.TransactionTypeID AS wwi_transaction_type_id,
    final.InvoiceID AS wwi_invoice_id,
    final.PurchaseOrderID AS wwi_purchase_order_id,
    final.quantity AS quantity
FROM
    final