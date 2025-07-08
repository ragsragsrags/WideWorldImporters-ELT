WITH configured_cutofftime AS (
    
    SELECT  
        * 
    FROM
        {{ source('staging', 'eltcutoff') }}

),

run_datetime AS (
    SELECT
        *
    FROM
        {{ ref('__run_datetime') }}
),

final AS (

    SELECT
        COALESCE(
            (
                SELECT TOP 1
                    configured_cutofftime.cutofftime
                FROM
                    configured_cutofftime         
            ),
            (
                SELECT TOP 1
                    run_datetime.run_datetime
                FROM
                    run_datetime
            )
        ) AS cutofftime
)

SELECT 
    *
FROM
    final
