WITH purchase_orders AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__fct_purchases') }}      

),

purchase_order_lines AS (

    SELECT
        *
    FROM
        {{ ref("stg_purchasing__purchase_order_lines") }}
    WHERE
        purchase_order_id IN (
            SELECT
                purchase_order_id
            FROM
                purchase_orders
        )

),

package_types AS (

    SELECT
        *
    FROM
        {{ ref('__merged__package_types') }}

),

stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

suppliers AS (

    SELECT
        *
    FROM
        {{ ref('dim_suppliers') }}

),

dates AS (

    SELECT
        *
    FROM
        {{ ref('dim_dates') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY purchase_orders.purchase_order_id) AS purchase_key,
        dates.date AS date_key,
        suppliers.supplier_key,
        stock_items.stock_item_key,
        purchase_orders.purchase_order_id AS wwi_purchase_order_id,
        purchase_order_lines.purchase_order_line_id AS wwi_purchase_order_line_id,
        suppliers.wwi_supplier_id,
        stock_items.wwi_stock_item_id,
        purchase_order_lines.purchase_order_line_ordered_outers AS ordered_outers,
        (
            purchase_order_lines.purchase_order_line_ordered_outers * 
            stock_items.quantity_per_outer
        ) AS ordered_quantity,
        purchase_order_lines.purchase_order_line_received_outers AS received_outers,
        package_types.package_type_name AS package,
        purchase_order_lines.purchase_order_line_is_finaled AS is_order_finalized
    FROM
        purchase_orders JOIN
        purchase_order_lines ON
            purchase_orders.purchase_order_id = purchase_order_lines.purchase_order_id LEFT JOIN
        dates ON
            purchase_orders.purchase_order_date = dates.date LEFT JOIN
        package_types ON
            purchase_order_lines.package_type_id = package_types.package_type_id LEFT JOIN
        stock_items ON
            purchase_order_lines.stock_item_id = stock_items.wwi_stock_item_id LEFT JOIN
        suppliers ON
            purchase_orders.supplier_id = suppliers.wwi_supplier_id 
                
)

SELECT
    *
FROM
    final