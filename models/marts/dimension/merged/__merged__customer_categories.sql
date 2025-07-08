WITH cutofftime AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_cutofftime') }}

),

customer_categories AS (

    SELECT
        *
    FROM
        {{ ref('stg_sales__customer_categories') }} AS customer_categories
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN customer_categories.customer_category_valid_from AND customer_categories.customer_category_valid_to
        )

),

customer_categories_archive AS (

    SELECT
        *
    FROM
        {{ ref('stg_sales__customer_categories_archive') }} AS customer_categories_archive
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN customer_categories_archive.customer_category_valid_from AND customer_categories_archive.customer_category_valid_to
        )

),

final AS (
    
    SELECT
        customer_categories.*
    FROM
        customer_categories
    
    UNION ALL

    SELECT
        customer_categories_archive.*
    FROM
        customer_categories_archive

)

SELECT 
    *
FROM
    final