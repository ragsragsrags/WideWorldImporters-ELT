WITH invoices AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__fct_sales') }}      

),

invoice_lines AS (

    SELECT
        *
    FROM
        {{ ref("stg_sales__invoice_lines") }}
    WHERE
        invoice_id IN (
            SELECT
                invoice_id
            FROM
                invoices
        )

),

stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

package_types AS (

    SELECT
        *
    FROM
        {{ ref('__merged__package_types') }}

),

customers AS (

    SELECT
        *
    FROM
        {{ ref('dim_customers') }}

),

cities AS (

    SELECT
        *
    FROM 
        {{ ref('dim_cities') }}

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
        ROW_NUMBER() OVER (ORDER BY invoices.invoice_id) AS sale_key,
        cities.city_key,
        customers.customer_key,
        bill_customers.customer_key AS bill_to_customer_key,
        stock_items.stock_item_key,
        dates.date AS invoice_date_key,
        delivery_dates.date AS delivery_date_key,
        employees.employee_key AS sales_person_key,
        invoices.invoice_id AS wwi_invoice_id,
        invoice_lines.invoice_line_id AS wwi_invoice_line_id,
        cities.wwi_city_id,
        customers.wwi_customer_id,
        bill_customers.wwi_customer_id AS wwi_bill_to_customer_id,
        stock_items.wwi_stock_item_id,
        employees.wwi_employee_id AS wwi_sales_person_id,
        invoice_lines.invoice_line_description AS description,
        package_types.package_type_name AS package,
        invoice_lines.invoice_line_quantity AS quantity,
        invoice_lines.invoice_line_unit_price AS unit_price,
        invoice_lines.invoice_line_tax_rate AS tax_rate,
        invoice_lines.invoice_line_total_excluding_tax AS total_excluding_tax,
        invoice_lines.invoice_line_tax_amount AS tax_amount,
        invoice_lines.invoice_line_profit AS profit,
        invoice_lines.invoice_line_total_including_tax as total_including_tax,
        CASE 
            WHEN stock_items.is_chiller_stock = FALSE THEN invoice_lines.invoice_line_quantity 
            ELSE 0
        END AS total_dry_items,
        CASE
            WHEN stock_items.is_chiller_stock = TRUE THEN invoice_lines.invoice_line_quantity 
            ELSE 0
        END AS total_chiller_items
    FROM
        invoices JOIN
        invoice_lines ON
            invoices.invoice_id = invoice_lines.invoice_id LEFT JOIN
        stock_items ON
            invoice_lines.stock_item_id = stock_items.wwi_stock_item_id LEFT JOIN
        dates ON
            invoices.invoice_date = dates.date LEFT JOIN
        dates AS delivery_dates ON
            invoices.invoice_confirmed_delivery_time = delivery_dates.date LEFT JOIN
        package_types ON
            invoice_lines.package_type_id = package_types.package_type_id LEFT JOIN
        customers ON
            invoices.customer_id = customers.wwi_customer_id LEFT JOIN
        cities ON
            customers.wwi_delivery_city_id = cities.wwi_city_id LEFT JOIN
        customers AS bill_customers ON
            invoices.bill_to_customer_id = bill_customers.wwi_customer_id LEFT JOIN
         employees ON
             invoices.sales_person_person_id = employees.wwi_employee_id
                
)

SELECT
    *
FROM
    final