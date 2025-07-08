WITH cutoff AS (
    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}
),

sales AS (
    SELECT
        CAST(I.InvoiceDate AS DATE) AS invoice_date_key,
        CAST(I.ConfirmedDeliveryTime AS DATE) AS delivery_date_key,
        I.InvoiceID AS wwi_invoice_id,
        IL.InvoiceLineID AS wwi_invoice_line_id,
        C.CityID AS wwi_city_id,
        CU.CustomerID AS wwi_customer_id,
        BCU.CustomerID AS wwi_bill_to_customer_id,
        SI.StockItemID AS wwi_stock_item_id,
        I.SalespersonPersonID AS wwi_sales_person_id,
        IL.Description AS description,
        PT.PackageTypeName AS package,
        IL.Quantity AS quantity,
        IL.UnitPrice AS unit_price,
        IL.TaxRate AS tax_rate,
        IL.ExtendedPrice - IL.TaxAmount AS total_excluding_tax,
        IL.TaxAmount AS tax_amount,
        IL.LineProfit AS profit,
        IL.ExtendedPrice AS total_including_tax,
        CASE 
            WHEN SI.IsChillerStock = 0 THEN IL.Quantity 
            ELSE 0 
        END AS total_dry_items,
        CASE 
            WHEN SI.IsChillerStock <> 0 THEN IL.Quantity 
            ELSE 0 
        END AS total_chiller_items
    FROM
        WideWorldImporters.Sales.Invoices I LEFT JOIN
        WideWorldImporters.Sales.InvoiceLines IL ON
            IL.InvoiceID = I.InvoiceID LEFT JOIN
        (
            SELECT
                C.CustomerID,
                C.CustomerName,
                C.DeliveryCityID
            FROM
                WideWorldImporters.Sales.Customers C
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo

            UNION

            SELECT
                CA.CustomerID,
                CA.CustomerName,
                CA.DeliveryCityID
            FROM
                WideWorldImporters.Sales.Customers_Archive CA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo
        ) CU ON
            CU.CustomerID = I.CustomerID LEFT JOIN
        (
            SELECT
                C.CityID,
                C.CityName
            FROM
                WideWorldImporters.Application.Cities C
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo

            UNION

            SELECT
                CA.CityID,
                CA.CityName
            FROM
                WideWorldImporters.Application.Cities_Archive CA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo
        ) C ON
            C.CityID = CU.DeliveryCityID LEFT JOIN
        (
            SELECT
                C.CustomerID,
                C.CustomerName
            FROM
                WideWorldImporters.Sales.Customers C
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo

            UNION

            SELECT
                CA.CustomerID,
                CA.CustomerName
            FROM
                WideWorldImporters.Sales.Customers_Archive CA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo
        ) BCU ON
            BCU.CustomerID = I.BillToCustomerID LEFT JOIN
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName,
                SI.IsChillerStock
            FROM
                WideWorldImporters.Warehouse.StockItems SI
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SI.ValidFrom AND SI.ValidTo

            UNION

            SELECT
                SIA.StockItemID,
                SIA.StockItemName,
                SIA.IsChillerStock
            FROM
                WideWorldImporters.Warehouse.StockItems_Archive SIA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI ON
            SI.StockItemID = IL.StockItemID LEFT JOIN
        (
            SELECT
                P.PersonID,
                P.FullName
            FROM
                WideWorldImporters.Application.People P
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN P.ValidFrom AND P.ValidTo

            UNION

            SELECT
                PA.PersonID,
                PA.FullName
            FROM
                WideWorldImporters.Application.People_Archive PA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PA.ValidFrom AND PA.ValidTo
        ) SP ON
            SP.PersonID = I.SalespersonPersonID LEFT JOIN
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
        ) PT ON
            PT.PackageTypeID = IL.PackageTypeID
    WHERE
        I.LastEditedWhen <= (SELECT TOP 1 cutofftime FROM cutoff) OR
        IL.LastEditedWhen <= (SELECT TOP 1 cutofftime FROM cutoff)
)

SELECT
    *
FROM
    sales
