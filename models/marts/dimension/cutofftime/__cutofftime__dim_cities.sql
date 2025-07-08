WITH cities_count AS (

    SELECT 
        COUNT(*)
    FROM
        {{ ref('dim_cities') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_cities') }}

)

SELECT
    *
FROM
    cutofftimes