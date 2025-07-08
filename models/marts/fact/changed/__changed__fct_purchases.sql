WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_purchases') }}

),

changed_packaged_types AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__fct_purchases', 
            'stg_warehouse__package_types', 
            'stg_warehouse__package_types_archive', 
            'package_type_valid_from',
            'package_type_valid_to',
            'package_type_id' ) 
    }}

),

changed_stock_items AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__fct_purchases', 
            'stg_warehouse__stock_items', 
            'stg_warehouse__stock_items_archive', 
            'stock_item_valid_from',
            'stock_item_valid_to',
            'stock_item_id' ) 
    }}

),

changed_purchases AS (

    SELECT DISTINCT
		purchase_orders.* 
    FROM 
		{{ ref('stg_purchasing__purchase_orders') }} AS purchase_orders JOIN 
		{{ ref('stg_purchasing__purchase_order_lines') }} AS purchase_order_lines ON 
			purchase_orders.purchase_order_id = purchase_order_lines.purchase_order_id JOIN
        cutofftimes ON
            1 = 1
    WHERE 
		(
			purchase_orders.purchase_order_last_edited_when > cutofftimes.last_cutofftime OR
			purchase_order_lines.purchase_order_line_last_edited_when > cutofftimes.last_cutofftime OR
			(
				cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
				(
					purchase_orders.purchase_order_last_edited_when >= cutofftimes.cutofftime OR
					purchase_order_lines.purchase_order_line_last_edited_when >= cutofftimes.cutofftime
				)
			) OR
			purchase_order_lines.package_type_id IN (
                SELECT 
                    package_type_id
                FROM 
                    changed_packaged_types
            ) OR
			purchase_order_lines.stock_item_id IN (
                SELECT 
                    stock_item_id
                FROM 
                    changed_stock_items
            )
		) AND
		(
			purchase_orders.purchase_order_last_edited_when <= cutofftimes.cutofftime OR
			purchase_order_lines.purchase_order_line_last_edited_when <= cutofftimes.cutofftime
		)

)

SELECT
    *
FROM
    changed_purchases