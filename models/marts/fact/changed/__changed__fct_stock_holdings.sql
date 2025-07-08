WITH stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

changed_stock_holdings AS (

    SELECT DISTINCT
		stock_item_holdings.* 
    FROM 
		{{ ref('stg_warehouse__stock_item_holdings') }} AS stock_item_holdings 
    WHERE
        stock_item_id IN (
            SELECT
                wwi_stock_item_id
            FROM 
                stock_items
        )    

)

SELECT
    *
FROM
    changed_stock_holdings