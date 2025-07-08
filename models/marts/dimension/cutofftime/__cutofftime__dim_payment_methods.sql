WITH payment_methods_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('dim_payment_methods') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_payment_methods') }}

)

SELECT
    *
FROM
    cutofftimes