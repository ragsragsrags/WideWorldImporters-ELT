WITH suppliers_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('dim_suppliers') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_suppliers') }}

)

SELECT
    *
FROM
    cutofftimes