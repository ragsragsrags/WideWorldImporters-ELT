WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_employees') }}

),

people AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_employees', 
            'stg_application__people', 
            'stg_application__people_archive', 
            'people_valid_from',
            'people_valid_to',
            'people_id' ) 
    }}    

),

final AS (

    SELECT
        *
    FROM
        people
    WHERE
        people_is_employee = TRUE

)

SELECT
    *
FROM 
    final