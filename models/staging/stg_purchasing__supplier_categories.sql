WITH source AS (

    SELECT
        *
    FROM
        {{ source('purchasing', 'suppliercategories') }}

),

transformed AS (

    SELECT
        SupplierCategoryID AS supplier_category_id,
        SupplierCategoryName AS supplier_category_name,
        ValidFrom AS supplier_category_valid_from,
        ValidTo AS supplier_category_valid_to
    FROM
        source

)

SELECT
    *
FROM
    transformed