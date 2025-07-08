WITH stock_items_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('dim_stock_items') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_stock_items') }}

)

SELECT
    *
FROM
    cutofftimes