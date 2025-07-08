WITH cutofftime AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_cutofftime') }}

),

supplier_categories AS (

    SELECT
        *
    FROM
        {{ ref('stg_purchasing__supplier_categories') }} AS supplier_categories
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN supplier_categories.supplier_category_valid_from AND supplier_categories.supplier_category_valid_to
        )

),

supplier_categories_archive AS (
    
    SELECT
        *
    FROM
        {{ ref('stg_purchasing__supplier_categories_archive') }} AS supplier_categories_archive
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN supplier_categories_archive.supplier_category_valid_from AND supplier_categories_archive.supplier_category_valid_to
        )

),

final AS (
    
    SELECT
        supplier_categories.*
    FROM
        supplier_categories

    UNION ALL

    SELECT
        supplier_categories_archive.*
    FROM
        supplier_categories_archive

)

SELECT 
    *
FROM
    final