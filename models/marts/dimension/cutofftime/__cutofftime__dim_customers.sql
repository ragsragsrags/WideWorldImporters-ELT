WITH customers_count AS (
    
    SELECT
        COUNT(*)
    FROM
        {{ ref('dim_customers') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_customers') }}

)

SELECT
    *
FROM
    cutofftimes