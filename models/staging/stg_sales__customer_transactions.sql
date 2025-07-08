WITH source AS (

    SELECT
        *
    FROM
        {{ source('sales', 'customertransactions') }}

),

transformed AS (

    SELECT
        CustomerTransactionID AS customer_transaction_id,
        CustomerID AS customer_id,
        TransactionTypeID AS transaction_type_id,
        InvoiceID AS invoice_id,
        PaymentMethodID AS payment_method_id,
        TransactionDate AS customer_transaction_date,
        AmountExcludingTax AS customer_transaction_amount_excluding_tax,
        TaxAmount AS customer_transaction_tax_amount,
        TransactionAmount AS customer_transaction_amount,
        OutstandingBalance AS customer_transaction_outstanding_balance,
        FinalizationDate AS customer_transaction_finalization_date,
        IsFinalized AS customer_transaction_is_finalized,
        LastEditedBy AS customer_transaction_last_edited_by,
        LastEditedWhen AS customer_transaction_last_edited_when
    FROM
        source

)

SELECT
    *
FROM
    transformed