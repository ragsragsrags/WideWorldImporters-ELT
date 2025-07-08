WITH stock_items AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__dim_stock_items') }}      

),

package_types AS (

    SELECT
        *
    FROM
        {{ ref('__merged__package_types') }}

),

colors AS (

    SELECT
        *
    FROM
        {{ ref('__merged__colors') }}

),

final AS (
        
    SELECT
        ROW_NUMBER() OVER (ORDER BY stock_items.stock_item_id) AS stock_item_key,
        stock_items.stock_item_id AS wwi_stock_item_id,
        stock_items.stock_item_name AS stock_item,
        CASE 
            WHEN colors.color_name IS NOT NULL THEN colors.color_name
            ELSE 'N/A'
        END AS color,
        package_types.package_type_name AS selling_package,
        buyer_package_types.package_type_name AS buying_package,
        stock_items.stock_item_brand AS brand,
        stock_items.stock_item_size AS size,
        stock_items.stock_item_lead_time_days AS lead_time_days,
        stock_items.stock_item_quantity_per_outer AS quantity_per_outer,
        stock_items.stock_item_is_chiller_stock AS is_chiller_stock,
        stock_items.stock_item_barcode AS barcode,
        stock_items.stock_item_tax_rate AS tax_rate,
        stock_items.stock_item_unit_price AS unit_price,
        stock_items.stock_item_recommended_retail_price AS recommended_retail_price,
        stock_items.stock_item_typical_weight_per_unit AS typical_weight_per_unit,
        stock_items.stock_item_photo AS photo
    FROM
        stock_items LEFT JOIN
        package_types ON
            stock_items.stock_item_unit_package_type_id = package_types.package_type_id LEFT JOIN
        package_types AS buyer_package_types ON
            stock_items.stock_item_outer_package_type_id = buyer_package_types.package_type_id LEFT JOIN
        colors ON
            stock_items.stock_item_color_id = colors.color_id 

)

SELECT 
    * 
FROM 
    final