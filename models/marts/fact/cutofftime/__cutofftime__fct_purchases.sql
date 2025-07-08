WITH purchases_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('fct_purchases') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_purchases') }}

)

SELECT
    *
FROM
    cutofftimes