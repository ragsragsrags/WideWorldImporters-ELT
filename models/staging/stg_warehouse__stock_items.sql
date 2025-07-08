WITH source AS (

    SELECT
        *
    FROM
        {{ source('base_warehouse', 'stockitems') }}
    
),

final AS (

    SELECT
        StockItemID AS stock_item_id,
        StockItemName AS stock_item_name,
        ColorID AS stock_item_color_id,
        UnitPackageID AS stock_item_unit_package_type_id,
        OuterPackageID AS stock_item_outer_package_type_id,
        Brand AS stock_item_brand,
        Size AS stock_item_size,
        LeadTimeDays AS stock_item_lead_time_days,
        QuantityPerOuter AS stock_item_quantity_per_outer,
        IsChillerStock AS stock_item_is_chiller_stock,
        Barcode AS stock_item_barcode,
        TaxRate AS stock_item_tax_rate,
        UnitPrice AS stock_item_unit_price,
        RecommendedRetailPrice AS stock_item_recommended_retail_price,
        TypicalWeightPerUnit AS stock_item_typical_weight_per_unit,
        Photo AS stock_item_photo,
        ValidFrom AS stock_item_valid_from,
        ValidTo AS stock_item_valid_to
    FROM
        source

)

SELECT 
    * 
FROM 
    final