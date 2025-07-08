WITH source AS (

    SELECT
        *
    FROM
        {{ source('purchasing', 'suppliers') }}

),

transformed AS (

    SELECT
        SupplierID AS supplier_id,
        SupplierName AS supplier_name,
        SupplierCategoryID AS supplier_category_id,
        PrimaryContactPersonID AS supplier_primary_contact_person_id,
        SupplierReference AS supplier_reference,
        PaymentDays AS supplier_payment_days,
        DeliveryPostalCode AS supplier_delivery_postal_code,
        ValidFrom AS supplier_valid_from,
        ValidTo AS supplier_valid_to
    FROM
        source

)

SELECT
    *
FROM
    transformed