WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_stock_items') }}

),

changed_package_types AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_stock_items', 
            'stg_warehouse__package_types', 
            'stg_warehouse__package_types_archive', 
            'package_type_valid_from',
            'package_type_valid_to',
            'package_type_id' ) 
    }}

),

changed_colors AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_stock_items', 
            'stg_warehouse__colors', 
            'stg_warehouse__colors_archive', 
            'color_valid_from',
            'color_valid_to',
            'color_id' )
    }}

),

stock_items AS (

    SELECT
        stock_items.*
    FROM
        {{ ref('stg_warehouse__stock_items') }} AS stock_items JOIN
        cutofftimes ON
            1 = 1
    WHERE
        (
            stock_items.stock_item_valid_from > cutofftimes.last_cutofftime OR
            (
                cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                stock_items.stock_item_valid_from >= cutofftimes.last_cutofftime  
            ) OR
            EXISTS (
                SELECT
                    1
                FROM
                    changed_package_types
                WHERE
                    changed_package_types.package_type_id IN (
                        stock_items.stock_item_unit_package_type_id,
                        stock_items.stock_item_outer_package_type_id
                    )
            ) OR
            EXISTS (
                SELECT
                    1
                FROM
                    changed_colors
                WHERE
                    changed_colors.color_id = stock_items.stock_item_color_id
            ) 
        ) AND
        stock_items.stock_item_valid_from <= cutofftimes.cutofftime

),

stock_items_archive AS (

    SELECT
        stock_items_archive.*
    FROM
        {{ ref('stg_warehouse__stock_items_archive') }} AS stock_items_archive JOIN
        cutofftimes ON
            1 = 1
    WHERE
        (
            stock_items_archive.stock_item_valid_from > cutofftimes.last_cutofftime OR
            (
                cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                stock_items_archive.stock_item_valid_from >= cutofftimes.last_cutofftime  
            ) OR
            EXISTS (
                SELECT
                    1
                FROM
                    changed_package_types
                WHERE
                    changed_package_types.package_type_id IN (
                        stock_items_archive.stock_item_unit_package_type_id,
                        stock_items_archive.stock_item_outer_package_type_id
                    )
            ) OR
            EXISTS (
                SELECT
                    1
                FROM
                    changed_colors
                WHERE
                    changed_colors.color_id = stock_items_archive.stock_item_color_id
            ) 
        ) AND
        stock_items_archive.stock_item_valid_from <= cutofftimes.cutofftime

),

stock_items_archive_max_valid_dates AS (

    SELECT
        stock_item_id,
        MAX(stock_item_valid_from) AS stock_item_valid_from,
        MAX(stock_item_valid_to) AS stock_item_valid_to
    FROM
        stock_items_archive
    GROUP BY
        stock_item_id

),

final AS (

    SELECT
        stock_items.*
    FROM
        stock_items
    
    UNION ALL

    SELECT
        stock_items_archive.*
    FROM
        stock_items_archive 
    WHERE
        EXISTS (
            SELECT
                *
            FROM
                stock_items_archive_max_valid_dates 
            WHERE
                stock_items_archive_max_valid_dates.stock_item_id = stock_items_archive.stock_item_id AND
                stock_items_archive_max_valid_dates.stock_item_valid_from = stock_items_archive.stock_item_valid_from AND
                stock_items_archive_max_valid_dates.stock_item_valid_to = stock_items_archive.stock_item_valid_to
        ) AND
        NOT EXISTS (
            SELECT
                1
            FROM
                stock_items
            WHERE
                stock_items.stock_item_id = stock_items_archive.stock_item_id       
        )

)

SELECT 
    *
FROM
    final