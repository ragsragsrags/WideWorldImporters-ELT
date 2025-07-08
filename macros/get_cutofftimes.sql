{%- macro get_cutofftimes(database, table) -%}
    WITH cutofftime AS (
        
        SELECT  
            * 
        FROM
            {{ ref('stg_cutofftime') }}
    
    ),

    init_cutofftime AS (        
        
        SELECT
            * 
        FROM
            {{ source('staging', 'init_eltcutoff') }}
    
    ),

    last_cutofftime AS (
    
        SELECT 
            * 
        FROM
            {{ 
                table_exists(
                    database, 
                    table, 
                    'CAST(NULL AS DATETIME) AS cutofftime'
                ) 
            }}
    
    ),

    final AS (
    
        SELECT
        (
            SELECT TOP 1 
                cutofftime 
            FROM 
                cutofftime
        ) AS cutofftime,
        (
            SELECT TOP 1 
                cutofftime 
            FROM 
                init_cutofftime
        ) AS init_cutofftime,
        COALESCE(
            (
                SELECT TOP 1 
                    cutofftime 
                FROM 
                    last_cutofftime 
            ),
            (
                SELECT TOP 1 
                    cutofftime 
                FROM 
                    init_cutofftime
            )
        ) AS last_cutofftime

    )

    SELECT 
        * 
    FROM 
        final

{%- endmacro -%}
