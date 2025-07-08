WITH transactions_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('fct_transactions') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_transactions') }}

)

SELECT
    *
FROM
    cutofftimes