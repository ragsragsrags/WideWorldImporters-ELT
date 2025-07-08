WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_sales') }}

),

stock_items AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__fct_sales', 
            'stg_warehouse__stock_items', 
            'stg_warehouse__stock_items_archive', 
            'stock_item_valid_from',
            'stock_item_valid_to',
            'stock_item_id' ) 
    }}

),

package_types AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__fct_sales', 
            'stg_warehouse__package_types', 
            'stg_warehouse__package_types_archive', 
            'package_type_valid_from',
            'package_type_valid_to',
            'package_type_id' ) 
    }}

),

customers AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__fct_sales', 
            'stg_sales__customers', 
            'stg_sales__customers_archive', 
            'customer_valid_from',
            'customer_valid_to',
            'customer_id' ) 
    }}

),

changed_sales AS (

    SELECT DISTINCT
		invoices.* 
    FROM 
		{{ ref('stg_sales__invoices') }} AS invoices JOIN 
		{{ ref('stg_sales__invoice_lines') }} AS invoice_lines ON 
			invoices.invoice_id = invoice_lines.invoice_id JOIN
        cutofftimes ON
            1 = 1
    WHERE 
		(
			invoices.invoice_last_edited_when > cutofftimes.last_cutofftime OR
			invoice_lines.invoice_line_last_edited_when > cutofftimes.last_cutofftime OR
			(
				cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
				(
					invoices.invoice_last_edited_when >= cutofftimes.cutofftime OR
					invoice_lines.invoice_line_last_edited_when >= cutofftimes.cutofftime
				)
			) OR
			invoice_lines.package_type_id IN (
                SELECT 
                    package_type_id
                FROM 
                    package_types
            ) OR
			invoice_lines.stock_item_id IN (
                SELECT 
                    stock_item_id
                FROM 
                    stock_items
            ) OR
			invoices.customer_id IN (
                SELECT 
                    customer_id
                FROM 
                    customers
            ) OR
			invoices.bill_to_customer_id IN (
                SELECT 
                    customer_id
                FROM 
                    customers
            )
		) AND
		(
			invoices.invoice_last_edited_when <= cutofftimes.cutofftime OR
			invoice_lines.invoice_line_last_edited_when <= cutofftimes.cutofftime
		)

)

SELECT
    *
FROM
    changed_sales