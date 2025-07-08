WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_customers') }}

),

changed_buying_groups AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_customers', 
            'stg_sales__buying_groups', 
            'stg_sales__buying_groups_archive', 
            'buying_group_valid_from',
            'buying_group_valid_to',
            'buying_group_id' ) 
    }}

),

changed_customer_categories AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_customers', 
            'stg_sales__customer_categories', 
            'stg_sales__customer_categories_archive', 
            'customer_category_valid_from',
            'customer_category_valid_to',
            'customer_category_id' )
    }}

),

changed_people AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_customers', 
            'stg_application__people', 
            'stg_application__people_archive', 
            'people_valid_from',
            'people_valid_to',
            'people_id' )
    }}

),

customers AS (

    SELECT
        customers.*
    FROM
        {{ ref('stg_sales__customers') }} AS customers JOIN
        cutofftimes ON
            1 = 1
    WHERE
        (
            customers.customer_valid_from > cutofftimes.last_cutofftime OR
            (
                cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                customers.customer_valid_from >= cutofftimes.last_cutofftime  
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_buying_groups
                WHERE
                    changed_buying_groups.buying_group_id = customers.buying_group_id
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_customer_categories
                WHERE
                    changed_customer_categories.customer_category_id = customers.customer_category_id
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_people
                WHERE
                    changed_people.people_id = customers.primary_contact_person_id
            )
        ) AND
        customers.customer_valid_from <= cutofftimes.cutofftime

),

customers_archive AS (

    SELECT
        customers_archive.*
    FROM
        {{ ref('stg_sales__customers_archive') }} AS customers_archive JOIN
        cutofftimes ON
            1 = 1
    WHERE
        (
            customers_archive.customer_valid_from > cutofftimes.last_cutofftime OR
            (
                cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                customers_archive.customer_valid_from >= cutofftimes.last_cutofftime  
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_buying_groups
                WHERE
                    changed_buying_groups.buying_group_id = customers_archive.buying_group_id
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_customer_categories
                WHERE
                    changed_customer_categories.customer_category_id = customers_archive.customer_category_id
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_people
                WHERE
                    changed_people.people_id = customers_archive.primary_contact_person_id
            )
        ) AND
        customers_archive.customer_valid_from <= cutofftimes.cutofftime

),

customers_archive_max_valid_dates AS (

    SELECT
        customer_id,
        MAX(customer_valid_from) AS customer_valid_from,
        MAX(customer_valid_to) AS customer_valid_to
    FROM
        customers_archive
    GROUP BY
        customer_id

),

final AS (

    SELECT
        customers.*
    FROM
        customers
    
    UNION ALL

    SELECT
        customers_archive.*
    FROM
        customers_archive 
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                customers_archive_max_valid_dates
            WHERE
                customers_archive_max_valid_dates.customer_id = customers_archive.customer_id AND
                customers_archive_max_valid_dates.customer_valid_from = customers_archive.customer_valid_from AND
                customers_archive_max_valid_dates.customer_valid_to = customers_archive.customer_valid_to
        ) AND
        NOT EXISTS (
            SELECT
                1
            FROM
                customers
            WHERE
                customers.customer_id = customers_archive.customer_id       
        )

)

SELECT 
    *
FROM
    final