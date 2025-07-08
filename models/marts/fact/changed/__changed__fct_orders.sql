WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_orders') }}

),

changed_packaged_types AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__fct_orders', 
            'stg_warehouse__package_types', 
            'stg_warehouse__package_types_archive', 
            'package_type_valid_from',
            'package_type_valid_to',
            'package_type_id' ) 
    }}

),

changed_orders AS (

    SELECT DISTINCT
		orders.* 
    FROM 
		{{ ref('stg_sales__orders') }} AS orders JOIN 
		{{ ref('stg_sales__order_lines') }} AS order_lines ON 
			orders.order_id = order_lines.order_id JOIN
        cutofftimes ON
            1 = 1
    WHERE 
		(
			orders.order_last_edited_when > cutofftimes.last_cutofftime OR
			order_lines.order_line_last_edited_when > cutofftimes.last_cutofftime OR
			(
				cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
				(
					orders.order_last_edited_when >= cutofftimes.cutofftime OR
					order_lines.order_line_last_edited_when >= cutofftimes.cutofftime
				)
			) OR
			order_lines.package_type_id IN (
                SELECT 
                    package_type_id
                FROM 
                    changed_packaged_types
            )
		) AND
		(
			orders.order_last_edited_when <= cutofftimes.cutofftime OR
			order_lines.order_line_last_edited_when <= cutofftimes.cutofftime
		)

)

SELECT
    *
FROM
    changed_orders