WITH cutofftime AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_cutofftime') }}

),

people AS (

    SELECT
        *
    FROM
        {{ ref('stg_application__people') }} AS people
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN people.people_valid_from AND people.people_valid_to
        )

),

people_archive AS (

    SELECT
        *
    FROM
        {{ ref('stg_application__people_archive') }} AS people_archive
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN people_archive.people_valid_from AND people_archive.people_valid_to
        )

),

final AS (
    
    SELECT
        people.*
    FROM
        people
    
    UNION ALL

    SELECT
        people_archive.*
    FROM
        people_archive

)

SELECT 
    *
FROM
    final