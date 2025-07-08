WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_transactions') }}

),

customer_transactions AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__fct_transactions', 
            'stg_sales__customer_transactions', 
            '', 
            'customer_transaction_last_edited_when',
            '',
            'customer_transaction_id' ) 
    }}

),

supplier_transactions AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__fct_transactions', 
            'stg_purchasing__supplier_transactions', 
            '', 
            'supplier_transaction_last_edited_when',
            '',
            'supplier_transaction_id' ) 
    }}

),

fct_sales AS (
    
    SELECT 
        wwi_invoice_id,
        wwi_customer_id
    FROM
        {{ ref('fct_sales') }}
    WHERE
        wwi_invoice_id IN (
            SELECT
                invoice_id
            FROM
                customer_transactions
        ) 
    GROUP BY
        wwi_invoice_id,
        wwi_customer_id

),

final AS (

    SELECT
        customer_transactions.customer_transaction_id,
        CAST(NULL AS NUMBER) AS supplier_transaction_id,
        COALESCE(fct_sales.wwi_customer_id, customer_transactions.customer_id) AS customer_id,
        customer_transactions.customer_id AS bill_to_customer_id,
        CAST(NULL AS NUMBER) AS supplier_id,
        customer_transactions.transaction_type_id,
        customer_transactions.invoice_id,
        CAST(NULL AS NUMBER) AS purchase_order_id,
        customer_transactions.payment_method_id,
        CAST(NULL AS STRING) AS supplier_invoice_number,
        customer_transactions.customer_transaction_date AS transaction_date,
        customer_transactions.customer_transaction_amount_excluding_tax AS total_excluding_tax,
        customer_transactions.customer_transaction_tax_amount AS tax_amount,
        customer_transactions.customer_transaction_amount AS total_including_tax,
        customer_transactions.customer_transaction_outstanding_balance AS outstanding_balance,
        customer_transactions.customer_transaction_finalization_date AS finalization_date,
        customer_transactions.customer_transaction_is_finalized AS is_finalized,
        customer_transactions.customer_transaction_last_edited_by AS last_edited_by,
        customer_transactions.customer_transaction_last_edited_when AS last_edited_when
    FROM
        customer_transactions LEFT JOIN
        fct_sales ON
             customer_transactions.invoice_id = fct_sales.wwi_invoice_id

    UNION ALL

    SELECT
        NULL AS customer_transaction_id,
        supplier_transactions.supplier_transaction_id,
        CAST(NULL AS NUMBER) AS customer_id,
        CAST(NULL AS NUMBER) AS bill_to_customer_id,
        supplier_transactions.supplier_id,        
        supplier_transactions.transaction_type_id,
        CAST(NULL AS NUMBER) AS invoice_id,
        supplier_transactions.purchase_order_id,
        supplier_transactions.payment_method_id,
        supplier_transactions.supplier_transaction_invoice_number AS invoice_number,
        supplier_transactions.supplier_transaction_date AS transaction_dte,
        supplier_transactions.supplier_transaction_amount_excluding_tax AS total_excluding_tax,
        supplier_transactions.supplier_transaction_tax_amount AS tax_amount,
        supplier_transactions.supplier_transaction_amount AS total_including_tax,
        supplier_transactions.supplier_transaction_outstanding_balance AS outstanding_balance,
        supplier_transactions.supplier_transaction_finalization_date AS finalization_date,
        supplier_transactions.supplier_transaction_is_finalized AS is_finalized,
        supplier_transactions.supplier_transaction_last_edited_by AS last_edited_by,
        supplier_transactions.supplier_transaction_last_edited_when AS last_edited_when
    FROM
        supplier_transactions

)

SELECT
    *
FROM
    final