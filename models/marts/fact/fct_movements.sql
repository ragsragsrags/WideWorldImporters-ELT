WITH movements AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__fct_movements') }}      

),

stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

customers AS (

    SELECT
        *
    FROM
        {{ ref('dim_customers') }}

),

dates AS (

    SELECT
        *
    FROM
        {{ ref('dim_dates') }}

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

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY movements.stock_item_transaction_id) AS movement_key,
        dates.date AS date_key,
        stock_items.stock_item_key,
        transaction_types.transaction_type_key,
        movements.stock_item_transaction_id AS wwi_stock_item_transaction_id,
        stock_items.wwi_stock_item_id,
        customers.wwi_customer_id,
        suppliers.wwi_supplier_id,
        transaction_types.wwi_transaction_type_id,
        movements.invoice_id AS wwi_invoice_id,
        movements.purchase_order_id AS wwi_purchase_order_id,
        movements.stock_item_transaction_quantity AS quantity
    FROM
        movements LEFT JOIN
        dates ON
            movements.stock_item_transaction_occurred_when = dates.date LEFT JOIN
        stock_items ON
            movements.stock_item_id = stock_items.wwi_stock_item_id LEFT JOIN
        customers ON
            movements.customer_id = customers.wwi_customer_id LEFT JOIN
        suppliers ON
            movements.supplier_id = suppliers.wwi_supplier_id LEFT JOIN
        transaction_types ON
            movements.transaction_type_id = transaction_types.wwi_transaction_type_id
)

SELECT 
    * 
FROM 
    final