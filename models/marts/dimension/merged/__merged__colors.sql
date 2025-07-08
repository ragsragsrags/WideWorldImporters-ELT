WITH cutofftime AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_cutofftime') }}

),

colors AS (

    SELECT
        *
    FROM
        {{ ref('stg_warehouse__colors') }} AS colors
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN colors.color_valid_from AND colors.color_valid_to
        )

),

colors_archive AS (

    SELECT
        *
    FROM
        {{ ref('stg_warehouse__colors_archive') }} AS colors_archive
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN colors_archive.color_valid_from AND colors_archive.color_valid_to
        )

),

final AS (
    
    SELECT
        colors.*
    FROM
        colors
    
    UNION ALL

    SELECT
        colors_archive.*
    FROM
        colors_archive

)

SELECT 
    *
FROM
    final