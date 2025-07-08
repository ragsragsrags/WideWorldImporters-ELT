WITH source AS (

    SELECT
        *
    FROM
        {{ source('purchasing', 'suppliertransactions') }}

),

transformed AS (

    SELECT
        SupplierTransactionID AS supplier_transaction_id,
        SupplierID AS supplier_id,
        TransactionTypeID AS transaction_type_id,
        PurchaseOrderID AS purchase_order_id,
        PaymentMethodID AS payment_method_id,
        SupplierInvoiceNumber AS supplier_transaction_invoice_number,
        TransactionDate AS supplier_transaction_date,
        AmountExcludingTax AS supplier_transaction_amount_excluding_tax,
        TaxAmount AS supplier_transaction_tax_amount,
        TransactionAmount AS supplier_transaction_amount,
        OutstandingBalance AS supplier_transaction_outstanding_balance,
        FinalizationDate AS supplier_transaction_finalization_date,
        IsFinalized AS supplier_transaction_is_finalized,
        LastEditedBy AS supplier_transaction_last_edited_by,
        LastEditedWhen AS supplier_transaction_last_edited_when
    FROM
        source

)

SELECT
    *
FROM 
    transformed