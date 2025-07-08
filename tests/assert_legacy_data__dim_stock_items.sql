WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_stock_items', 'dim_stock_items', 'wwi_stock_item_id')
    }}

)

SELECT
    *
FROM
    compare_result
