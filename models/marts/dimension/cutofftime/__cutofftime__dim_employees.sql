WITH employees_count AS (

    SELECT 
        COUNT(*)
    FROM
        {{ ref('dim_employees') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_employees') }}

)

SELECT
    *
FROM
    cutofftimes