WITH cutofftime AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_cutofftime') }}

),

countries AS (

    SELECT
        *
    FROM
        {{ ref('stg_application__countries') }} AS countries
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN countries.country_valid_from AND countries.country_valid_to
        )

),

countries_archive AS (

    SELECT
        *
    FROM
        {{ ref('stg_application__countries_archive') }} AS countries_archive
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN countries_archive.country_valid_from AND countries_archive.country_valid_to
        )

),

final AS (
    
    SELECT
        countries.*
    FROM
        countries
    
    UNION ALL

    SELECT
        countries_archive.*
    FROM
        countries_archive

)

SELECT 
    *
FROM
    final