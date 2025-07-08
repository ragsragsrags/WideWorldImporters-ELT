WITH cutofftime AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_cutofftime') }}

),

buying_groups AS (

    SELECT
        *
    FROM
        {{ ref('stg_sales__buying_groups') }} AS buying_groups
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN buying_groups.buying_group_valid_from AND buying_groups.buying_group_valid_to
        )

),

buying_groups_archive AS (

    SELECT
        *
    FROM
        {{ ref('stg_sales__buying_groups_archive') }} AS buying_groups_archive
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN buying_groups_archive.buying_group_valid_from AND buying_groups_archive.buying_group_valid_to
        )

),

final AS (
    
    SELECT
        buying_groups.*
    FROM
        buying_groups
    
    UNION ALL

    SELECT
        buying_groups_archive.*
    FROM
        buying_groups_archive

)

SELECT 
    *
FROM
    final