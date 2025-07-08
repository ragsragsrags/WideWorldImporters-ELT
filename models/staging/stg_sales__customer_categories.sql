WITH source AS (

    SELECT
        *
    FROM
        {{ source('sales', 'customercategories') }}

),

transformed AS (

    SELECT
        CustomerCategoryID AS customer_category_id,
        CustomerCategoryName AS customer_category_name,
        ValidFrom AS customer_category_valid_from,
        ValidTo AS customer_category_valid_to
    FROM
        source

)

SELECT
    *
FROM
    transformed