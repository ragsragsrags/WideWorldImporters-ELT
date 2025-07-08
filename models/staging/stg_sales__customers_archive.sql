WITH source AS (

    SELECT
        *
    FROM
        {{ source('sales', 'customers_archive') }}

),

transformed AS (

    SELECT
        CustomerID AS customer_id,
        CustomerName AS customer_name,
        BillToCustomerID AS bill_to_customer_id,
        CustomerCategoryID AS customer_category_id,
        BuyingGroupID AS buying_group_id,
        PrimaryContactPersonID AS primary_contact_person_id,
        DeliveryCityID AS delivery_city_id,
        DeliveryPostalCode AS customer_delivery_postal_code,
        ValidFrom AS customer_valid_from,
        ValidTo AS customer_valid_to
    FROM
        source

)

SELECT
    *
FROM
    transformed