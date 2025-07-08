WITH cutoff AS (

    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}

),

transactions AS (

    SELECT
        CT.TransactionDate AS date_key,
        CT.CustomerTransactionID AS wwi_customer_transaction_id,
        CAST(NULL AS NUMBER) AS wwi_supplier_transaction_id,
        C.CustomerID AS wwi_customer_id,
        BC.CustomerID AS wwi_bill_to_customer_id,
        CAST(NULL AS NUMBER) AS wwi_supplier_id,
        TT.TransactionTypeID AS wwi_transaction_type_id,
        PM.PaymentMethodID AS wwi_payment_method_id,
        CT.InvoiceID AS wwi_invoice_id,
        CAST(NULL AS NUMBER) AS wwi_purchase_order_id,
        CAST(NULL AS STRING) AS supplier_invoice_number,
        CT.AmountExcludingTax AS total_excluding_tax,
        CT.TaxAmount AS tax_amount,
        CT.TransactionAmount AS total_including_tax,
        CT.OutstandingBalance AS outstanding_balance,
        CT.IsFinalized AS is_finalized
    FROM
        WideWorldImporters.Sales.CustomerTransactions CT LEFT JOIN
        WideWorldImporters.Sales.Invoices I ON
            I.InvoiceID = CT.InvoiceID LEFT JOIN
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
            C.CustomerID = COALESCE(I.CustomerID, CT.CustomerID) LEFT JOIN
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
        ) BC ON
            BC.CustomerID = CT.CustomerID LEFT JOIN
        (
            SELECT
                TT.TransactionTypeID,
                TT.TransactionTypeName
            FROM
                WideWorldImporters.Application.TransactionTypes  TT 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN TT.ValidFrom AND TT.ValidTo

            UNION 

            SELECT
                TTA.TransactionTypeID,
                TTA.TransactionTypeName
            FROM
                WideWorldImporters.Application.TransactionTypes_Archive  TTA 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN TTA.ValidFrom AND TTA.ValidTo
        ) TT ON
            TT.TransactionTypeID = CT.TransactionTypeID LEFT JOIN
        (
            SELECT
                PM.PaymentMethodID,
                PM.PaymentMethodName
            FROM
                WideWorldImporters.Application.PaymentMethods PM 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PM.ValidFrom AND PM.ValidTo

            UNION 

            SELECT
                PMA.PaymentMethodID,
                PMA.PaymentMethodName
            FROM
                WideWorldImporters.Application.PaymentMethods_Archive PMA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PMA.ValidFrom AND PMA.ValidTo
        ) PM ON
            PM.PaymentMethodID = CT.PaymentMethodID
    WHERE
        CT.LastEditedWhen <= (SELECT TOP 1 cutofftime FROM cutoff)

    UNION ALL

    SELECT
        ST.TransactionDate AS date_key,
        CAST(NULL AS NUMBER) AS customer_transaction_id,
        ST.SupplierTransactionID AS supplier_transaction_id,
        CAST(NULL AS NUMBER) AS wwi_customer_id,
        CAST(NULL AS NUMBER) AS wwi_bill_to_customer_id,
        S.SupplierID AS wwi_supplier_id,
        TT.TransactionTypeID AS wwi_transaction_type_id,
        PM.PaymentMethodID AS wwi_payment_method_id,
        CAST(NULL AS NUMBER) AS wwi_invoice_id,
        ST.PurchaseOrderID AS wwi_purchase_order_id,
        ST.SupplierInvoiceNumber AS supplier_invoice_number,
        ST.AmountExcludingTax AS total_excluding_tax,
        ST.TaxAmount AS tax_amount,
        ST.TransactionAmount AS total_including_tax,
        ST.OutstandingBalance AS outstanding_balance,
        ST.IsFinalized AS is_finalized
    FROM
        WideWorldImporters.Purchasing.SupplierTransactions ST LEFT JOIN
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
            S.SupplierID = ST.SupplierID LEFT JOIN
        (
            SELECT
                TT.TransactionTypeID,
                TT.TransactionTypeName
            FROM
                WideWorldImporters.Application.TransactionTypes  TT 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN TT.ValidFrom AND TT.ValidTo

            UNION 

            SELECT
                TTA.TransactionTypeID,
                TTA.TransactionTypeName
            FROM
                WideWorldImporters.Application.TransactionTypes_Archive  TTA 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN TTA.ValidFrom AND TTA.ValidTo
        ) TT ON
            TT.TransactionTypeID = ST.TransactionTypeID LEFT JOIN
        (
            SELECT
                PM.PaymentMethodID,
                PM.PaymentMethodName
            FROM
                WideWorldImporters.Application.PaymentMethods PM 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PM.ValidFrom AND PM.ValidTo

            UNION 

            SELECT
                PMA.PaymentMethodID,
                PMA.PaymentMethodName
            FROM
                WideWorldImporters.Application.PaymentMethods_Archive PMA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PMA.ValidFrom AND PMA.ValidTo
        ) PM ON
            PM.PaymentMethodID = ST.PaymentMethodID
    WHERE
        ST.LastEditedWhen <= (SELECT TOP 1 cutofftime FROM cutoff)

),

final AS (

    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                date_key,
                wwi_customer_transaction_id,
                wwi_supplier_transaction_id
        ) AS transaction_key,
        *
    FROM
        transactions
)

SELECT
    *
FROM
    final
