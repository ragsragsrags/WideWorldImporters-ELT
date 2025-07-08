WITH stock_holdings_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('fct_stock_holdings') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_stock_holdings') }}

)

SELECT
    *
FROM
    cutofftimes