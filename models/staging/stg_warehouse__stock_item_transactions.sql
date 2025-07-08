WITH source AS (

    SELECT
        *
    FROM
        {{ source('base_warehouse', 'stockitemtransactions') }}

),

transformed AS (

    SELECT
        CAST(TransactionOccurredWhen AS DATE) AS stock_item_transaction_occurred_when,
        StockItemTransactionID AS stock_item_transaction_id,
		InvoiceID AS invoice_id,
		PurchaseOrderID AS purchase_order_id,
		Quantity AS stock_item_transaction_quantity,
		StockItemID AS stock_item_id,
        CustomerID AS customer_id,
        SupplierID AS supplier_id,
        TransactionTypeID AS transaction_type_id,
        LastEditedWhen AS stock_item_transaction_last_edited_when
    FROM
        source

)

SELECT
    *
FROM
    transformed