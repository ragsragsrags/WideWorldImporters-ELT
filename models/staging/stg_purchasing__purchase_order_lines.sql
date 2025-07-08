WITH source AS (

    SELECT
        *
    FROM
        {{ source('purchasing', 'purchaseorderlines') }}

),

transformed AS (

    SELECT
        PurchaseOrderLineID AS purchase_order_line_id,
        PurchaseOrderID AS purchase_order_id,
        StockItemID AS stock_item_id,
        OrderedOuters AS purchase_order_line_ordered_outers,
        Description AS purchase_order_line_description,
        ReceivedOuters AS purchase_order_line_received_outers,
        PackageTypeID AS package_type_id,
        IsOrderLineFinalized AS purchase_order_line_is_finaled,
        LastEditedWhen AS purchase_order_line_last_edited_when
    FROM
        source

)

SELECT
    *
FROM
    transformed