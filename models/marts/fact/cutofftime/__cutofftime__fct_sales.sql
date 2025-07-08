WITH sales_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('fct_sales') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_sales') }}

)

SELECT
    *
FROM
    cutofftimes