WITH cutoff AS (
    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}
),

orders AS (

    SELECT
        O.OrderID AS wwi_order_id,
        OL.OrderLineID AS wwi_order_line_id,
        C.DeliveryCityID AS wwi_city_id,
        C.CustomerID AS wwi_customer_id,
        SI.StockItemID AS wwi_stock_item_id,
        CAST(O.OrderDate AS DATE) AS order_date_key,
        CAST(O.PickingCompletedWhen AS DATE) AS picked_date_key,
        O.SalespersonPersonID AS wwi_sales_person_id,
        O.PickedByPersonID AS wwi_picker_id,
        O.BackorderOrderID AS wwi_backorder_id,
        OL.Description AS description,
        PT.PackageTypeName AS package,
        OL.Quantity AS quantity,
        OL.UnitPrice AS unit_price,
        OL.TaxRate AS tax_rate,
        ROUND(OL.Quantity * OL.UnitPrice, 2) AS total_excluding_tax,
        ROUND(OL.Quantity * OL.UnitPrice * ol.TaxRate / 100.0, 2) AS tax_amount,
        ROUND(OL.Quantity * OL.UnitPrice, 2) + ROUND(OL.Quantity * ol.UnitPrice * OL.TaxRate / 100.0, 2) AS total_including_tax
    FROM
        WideWorldImporters.Sales.Orders O LEFT JOIN
        WideWorldImporters.Sales.OrderLines OL ON
            OL.OrderID = O.OrderID LEFT JOIN 
        (
            SELECT
                C.CustomerID,
                C.DeliveryCityID,
                C.CustomerName
            FROM
                WideWorldImporters.Sales.Customers C
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo

            UNION

            SELECT
                CA.CustomerID,
                CA.DeliveryCityID,
                CA.CustomerName
            FROM
                WideWorldImporters.Sales.Customers_Archive CA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo
        ) C ON
            C.CustomerID = O.CustomerID LEFT JOIN
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
        ) CI ON
            CI.CityID = C.DeliveryCityID LEFT JOIN
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName
            FROM
                WideWorldImporters.Warehouse.StockItems SI 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SI.ValidFrom AND SI.ValidTo

            UNION

            SELECT
                SIA.StockItemID,
                SIA.StockItemName
            FROM
                WideWorldImporters.Warehouse.StockItems_Archive SIA 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI ON
            SI.StockItemID = OL.StockItemID LEFT JOIN
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
        ) P ON
            P.PersonID = O.SalespersonPersonID LEFT JOIN
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
        ) P2 ON
            P2.PersonID = O.PickedByPersonID LEFT JOIN
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
            PT.PackageTypeID = OL.PackageTypeID
    WHERE
        O.LastEditedWhen <= (SELECT TOP 1 cutofftime FROM cutoff) OR
	OL.LastEditedWhen <= (SELECT TOP 1 cutofftime FROM cutoff)

)

SELECT
    *
FROM
    orders