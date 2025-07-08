WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_suppliers') }}

),

changed_supplier_categories AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_suppliers', 
            'stg_purchasing__supplier_categories', 
            'stg_purchasing__supplier_categories_archive', 
            'supplier_category_valid_from',
            'supplier_category_valid_to',
            'supplier_category_id' ) 
    }}

),

changed_people AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_suppliers', 
            'stg_application__people', 
            'stg_application__people_archive', 
            'people_valid_from',
            'people_valid_to',
            'people_id' ) 
    }}

),

suppliers AS (

    SELECT
        suppliers.*
    FROM
        {{ ref('stg_purchasing__suppliers') }} AS suppliers JOIN
        cutofftimes ON
            1 = 1
    WHERE
        (
            suppliers.supplier_valid_from > cutofftimes.last_cutofftime OR
            (
                cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                suppliers.supplier_valid_from >= cutofftimes.last_cutofftime  
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_supplier_categories
                WHERE
                    changed_supplier_categories.supplier_category_id = suppliers.supplier_category_id
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_people
                WHERE
                    changed_people.people_id = suppliers.supplier_primary_contact_person_id
            )
        ) AND
        suppliers.supplier_valid_from <= cutofftimes.cutofftime

),

suppliers_archive AS (

    SELECT
        suppliers_archive.*
    FROM
        {{ ref('stg_purchasing__suppliers_archive') }} AS suppliers_archive JOIN
        cutofftimes ON
            1 = 1
    WHERE
        (
            suppliers_archive.supplier_valid_from > cutofftimes.last_cutofftime OR
            (
                cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                suppliers_archive.supplier_valid_from >= cutofftimes.last_cutofftime  
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_supplier_categories
                WHERE
                    changed_supplier_categories.supplier_category_id = suppliers_archive.supplier_category_id
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_people
                WHERE
                    changed_people.people_id = suppliers_archive.supplier_primary_contact_person_id
            )
        ) AND
        suppliers_archive.supplier_valid_from <= cutofftimes.cutofftime

),

suppliers_archive_max_valid_dates AS (

    SELECT
        supplier_id,
        MAX(supplier_valid_from) AS supplier_valid_from,
        MAX(supplier_valid_to) AS supplier_valid_to
    FROM
        suppliers
    GROUP BY
        supplier_id

),

final AS (

    SELECT
        suppliers.*
    FROM
        suppliers
    
    UNION ALL

    SELECT
        suppliers_archive.*
    FROM
        suppliers_archive 
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                suppliers_archive_max_valid_dates
            WHERE
                suppliers_archive_max_valid_dates.supplier_id = suppliers_archive.supplier_id AND
                suppliers_archive_max_valid_dates.supplier_valid_from = suppliers_archive.supplier_valid_from AND
                suppliers_archive_max_valid_dates.supplier_valid_to = suppliers_archive.supplier_valid_to
        ) AND
        NOT EXISTS (
            SELECT
                1
            FROM
                suppliers
            WHERE
                suppliers.supplier_id = suppliers_archive.supplier_id       
        )

)

SELECT
    *
FROM
    final