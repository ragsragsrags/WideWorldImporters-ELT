WITH transaction_types AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('dim_transaction_types') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_transaction_types') }}

)

SELECT
    *
FROM
    cutofftimes