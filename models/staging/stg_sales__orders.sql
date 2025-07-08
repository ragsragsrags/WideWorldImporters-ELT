WITH source AS (

    SELECT
        *
    FROM
        {{ source('sales', 'orders') }}

),

transformed AS (

    SELECT
        OrderID AS order_id,
        CustomerID AS customer_id,
        SalespersonPersonID AS sales_person_person_id,
        PickedByPersonID AS picked_by_person_id,
        ContactPersonID AS contact_person_id,
        BackorderOrderID AS backorderorder_order_id,
        CAST(OrderDate AS DATE) AS order_date,
        ExpectedDeliveryDate AS order_expected_delivery_date,
        CustomerPurchaseOrderNumber AS order_customer_purchase_order_number,
        IsUndersupplyBackordered AS order_is_under_supply_backordered,
        Comments AS order_comments,
        DeliveryInstructions AS order_delivery_instructions,
        InternalComments AS order_internal_comments,
        CAST(PickingCompletedWhen AS DATE) AS order_picking_completed_when,
        LastEditedBy AS order_last_edited_by,
        LastEditedWhen AS order_last_edited_when
    FROM
        source

)

SELECT
    *
FROM
    transformed