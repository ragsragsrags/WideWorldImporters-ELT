WITH transactions AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__fct_transactions') }}      

),

customers AS (
    
    SELECT
        *
    FROM
        {{ ref('dim_customers') }}

),

suppliers AS (

    SELECT
        *
    FROM
        {{ ref('dim_suppliers') }}

),

transaction_types AS (

    SELECT
        *
    FROM
        {{ ref('dim_transaction_types') }}

),

payment_methods AS (

    SELECT
        *
    FROM
        {{ ref('dim_payment_methods') }}

),

dates AS (

    SELECT
        *
    FROM
        {{ ref('dim_dates') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (
            ORDER BY 
                transactions.transaction_date,
                transactions.customer_transaction_id,
                transactions.supplier_transaction_id
        ) AS transaction_key,
        transactions.transaction_date AS date_key,
        customers.customer_key,
        bill_to_customers.customer_key AS bill_to_customer_key,
        suppliers.supplier_key,
        transaction_types.transaction_type_key,
        payment_methods.payment_method_key,
        transactions.customer_transaction_id AS wwi_customer_transaction_id,
        transactions.supplier_transaction_id AS wwi_supplier_transaction_id,
        customers.wwi_customer_id,
        bill_to_customers.wwi_customer_id AS wwi_bill_to_customer_id,
        suppliers.wwi_supplier_id,
        transaction_types.wwi_transaction_type_id,
        payment_methods.wwi_payment_method_id,
        transactions.invoice_id AS wwi_invoice_id,
        transactions.purchase_order_id AS wwi_purchase_order_id,
        transactions.supplier_invoice_number,
        transactions.total_excluding_tax,
        transactions.tax_amount,
        transactions.total_including_tax,
        transactions.outstanding_balance,
        transactions.is_finalized
    FROM
        transactions LEFT JOIN 
        dates ON
            transactions.transaction_date = dates.date LEFT JOIN
        customers ON
            transactions.customer_id = customers.wwi_customer_id LEFT JOIN
        customers AS bill_to_customers ON
            transactions.bill_to_customer_id = bill_to_customers.wwi_customer_id LEFT JOIN
        suppliers ON
            transactions.supplier_id = suppliers.wwi_supplier_id LEFT JOIN
        transaction_types ON
            transactions.transaction_type_id = transaction_types.wwi_transaction_type_id LEFT JOIN
        payment_methods ON
            transactions.payment_method_id = payment_methods.wwi_payment_method_id    

)

SELECT
    *
FROM
    final