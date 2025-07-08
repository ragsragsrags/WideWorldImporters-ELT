WITH orders AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__fct_orders') }}      

),

order_lines AS (

    SELECT
        *
    FROM
        {{ ref("stg_sales__order_lines") }}
    WHERE
        order_id IN (
            SELECT
                order_id
            FROM
                orders
        )

),

package_types AS (

    SELECT
        *
    FROM
        {{ ref('__merged__package_types') }}

),

cities AS (

    SELECT
        *
    FROM
        {{ ref('dim_cities') }}

),

customers AS (

    SELECT
        *
    FROM
        {{ ref('dim_customers') }}

),

stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

employees AS (

    SELECT
        *
    FROM
        {{ ref('dim_employees') }}

),

dates AS (

    SELECT
        *
    FROM
        {{ ref('dim_dates') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY orders.order_id) AS order_key,
        cities.city_key,
        customers.customer_key,
        stock_items.stock_item_key,
        dates.date AS order_date_key,
        pick_dates.date AS picked_date_key,
        employees.employee_key AS sales_person_key,
        picker.employee_key AS picker_key,
        orders.order_id AS wwi_order_id,
        order_lines.order_line_id AS wwi_order_line_id,
        orders.backorderorder_order_id AS wwi_backorder_id,
        customers.wwi_delivery_city_id AS wwi_city_id,
        customers.wwi_customer_id,
        stock_items.wwi_stock_item_id,
        employees.wwi_employee_id AS wwi_sales_person_id,
        picker.wwi_employee_id AS wwi_picker_id,
        order_lines.order_line_description AS description,
        package_types.package_type_name AS package,
        order_lines.order_line_quantity AS quantity,
        order_lines.order_line_unit_price AS unit_price,
        order_lines.order_line_tax_rate AS tax_rate,
        order_lines.order_line_total_excluding_tax AS total_excluding_tax,
        order_lines.order_line_tax_amount AS tax_amount,
        order_lines.order_line_total_including_tax AS total_including_tax
    FROM
        orders JOIN
        order_lines ON
            orders.order_id = order_lines.order_id LEFT JOIN
        customers ON
            orders.customer_id = customers.wwi_customer_id LEFT JOIN
        cities ON
            customers.wwi_delivery_city_id = cities.wwi_city_id LEFT JOIN
        stock_items ON
            order_lines.stock_item_id = stock_items.wwi_stock_item_id LEFT JOIN
        dates ON
            orders.order_date = dates.date LEFT JOIN
        dates AS pick_dates ON
            orders.order_picking_completed_when = pick_dates.date LEFT JOIN
        employees ON
            orders.sales_person_person_id = employees.wwi_employee_id LEFT JOIN
        employees AS picker ON
            orders.picked_by_person_id = picker.wwi_employee_id LEFT JOIN
        package_types ON
            order_lines.package_type_id = package_types.package_type_id 
                
)

SELECT
    *
FROM
    final