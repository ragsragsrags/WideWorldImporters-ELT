WITH orders_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('fct_orders') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_orders') }}

)

SELECT
    *
FROM
    cutofftimes