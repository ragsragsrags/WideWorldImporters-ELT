WITH cutofftime AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_cutofftime') }}

),

states AS (

    SELECT
        *
    FROM
        {{ ref('stg_application__stateprovinces') }} AS states
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN states.stateprovince_valid_from AND states.stateprovince_valid_to
        )

),

states_archive AS (
    
    SELECT
        *
    FROM
        {{ ref('stg_application__stateprovinces_archive') }} AS states_archive
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN states_archive.stateprovince_valid_from AND states_archive.stateprovince_valid_to
        )

),

final AS (
    
    SELECT
        states.*
    FROM
        states

    UNION ALL

    SELECT
        states_archive.*
    FROM
        states_archive

)

SELECT 
    *
FROM
    final