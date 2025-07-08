WITH source AS (

    SELECT
        *
    FROM
        {{ source('sales', 'orderlines') }}

),

transformed AS (

    SELECT
        OrderLineID AS order_line_id,
        OrderID AS order_id,
        StockItemID AS stock_item_id,
        Description AS order_line_description,
        PackageTypeID AS package_type_id,
        Quantity AS order_line_quantity,
        UnitPrice AS order_line_unit_price,
        TaxRate AS order_line_tax_rate,
        PickedQuantity AS order_line_picked_quantity,
        PickingCompletedWhen AS order_line_picking_completed_when,
        ROUND(Quantity * UnitPrice, 2) AS order_line_total_excluding_tax,
        ROUND((Quantity * UnitPrice * TaxRate) / 100.0, 2) AS order_line_tax_amount,
        (
			ROUND(Quantity * UnitPrice, 2) + 
			ROUND((Quantity * UnitPrice * TaxRate) / 100.0, 2)
		) AS order_line_total_including_tax,
        LastEditedBy AS order_line_last_edited_by,
        LastEditedWhen AS order_line_last_edited_when
    FROM
        source

)

SELECT
    *
FROM
    transformed