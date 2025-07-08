WITH cutoff AS (

    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}

),

stock_items AS (

    SELECT
        SI.StockItemID,
        SI.StockItemName,
        C.ColorName,
        SP.PackageTypeName AS SellerPackageTypeName,
        BP.PackageTypeName AS BuyerPackageTypeName,
        SI.Brand,
        SI.Size,
        SI.LeadTimeDays,
        SI.QuantityPerOuter,
        SI.IsChillerStock,
        SI.Barcode,
        SI.TaxRate,
        SI.UnitPrice,
        SI.RecommendedRetailPrice,
        SI.TypicalWeightPerUnit,
        SI.Photo
    FROM
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName,
                SI.ColorID,
                SI.OuterPackageID,
                SI.UnitPackageID,
                SI.Brand,
                SI.Size,
                SI.LeadTimeDays,
                SI.QuantityPerOuter,
                SI.IsChillerStock,
                SI.Barcode,
                SI.TaxRate,
                SI.UnitPrice,
                SI.RecommendedRetailPrice,
                SI.TypicalWeightPerUnit,
                SI.Photo
            FROM
                WideWorldImporters.Warehouse.StockItems SI
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SI.ValidFrom AND SI.ValidTo

            UNION

            SELECT
                SIA.StockItemID,
                SIA.StockItemName,
                SIA.ColorID,
                SIA.OuterPackageID,
                SIA.UnitPackageID,
                SIA.Brand,
                SIA.Size,
                SIA.LeadTimeDays,
                SIA.QuantityPerOuter,
                SIA.IsChillerStock,
                SIA.Barcode,
                SIA.TaxRate,
                SIA.UnitPrice,
                SIA.RecommendedRetailPrice,
                SIA.TypicalWeightPerUnit,
                SIA.Photo
            FROM
                WideWorldImporters.Warehouse.StockItems_Archive SIA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI LEFT JOIN
        (
            SELECT
                C.ColorID,
                C.ColorName
            FROM
                WideWorldImporters.Warehouse.Colors C 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo

            UNION

            SELECT
                CA.ColorID,
                CA.ColorName
            FROM
                WideWorldImporters.Warehouse.Colors_Archive CA 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo
        ) C ON
            C.ColorID = SI.ColorID LEFT JOIN
        (
            SELECT
                PT.PackageTypeID,
                PT.PackageTypeName
            FROM
                WideWorldImporters.Warehouse.PackageTypes PT 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PT.ValidFrom AND PT.ValidTo

            UNION

            SELECT
                PTA.PackageTypeID,
                PTA.PackageTypeName
            FROM
                WideWorldImporters.Warehouse.PackageTypes_Archive PTA 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PTA.ValidFrom AND PTA.ValidTo
        ) SP ON
            SP.PackageTypeID = SI.UnitPackageID LEFT JOIN
        (
            SELECT
                PT.PackageTypeID,
                PT.PackageTypeName
            FROM
                WideWorldImporters.Warehouse.PackageTypes PT 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PT.ValidFrom AND PT.ValidTo

            UNION

            SELECT
                PTA.PackageTypeID,
                PTA.PackageTypeName
            FROM
                WideWorldImporters.Warehouse.PackageTypes_Archive PTA 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PTA.ValidFrom AND PTA.ValidTo
        ) BP ON
            BP.PackageTypeID = SI.OuterPackageID

),

final AS (

    SELECT
        StockItemID AS wwi_stock_item_id,
        StockItemName AS stock_item,
        CASE 
            WHEN ColorName IS NOT NULL THEN ColorName
            ELSE 'N/A'
        END AS color,
        SellerPackageTypeName AS selling_package,
        BuyerPackageTypeName AS buying_package,
        Brand AS brand,
        Size AS size,
        LeadTimeDays AS lead_time_days,
        QuantityPerOuter AS quantity_per_outer,
        IsChillerStock AS is_chiller_stock,
        Barcode AS barcode,
        TaxRate AS tax_rate,
        UnitPrice AS unit_price,
        RecommendedRetailPrice AS recommended_retail_price,
        TypicalWeightPerUnit AS typical_weight_per_unit,
        Photo AS photo
    FROM
        stock_items 

)

SELECT
    *
FROM 
    final