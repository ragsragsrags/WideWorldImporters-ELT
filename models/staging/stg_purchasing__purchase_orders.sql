WITH source AS (

    SELECT
        *
    FROM
        {{ source('purchasing', 'purchaseorders') }}

),

transformed AS (

    SELECT
        PurchaseOrderID AS purchase_order_id,
        OrderDate AS purchase_order_date,
        SupplierID AS supplier_id,
        LastEditedWhen AS purchase_order_last_edited_when
    FROM
        source

)

SELECT
    *
FROM
    transformed