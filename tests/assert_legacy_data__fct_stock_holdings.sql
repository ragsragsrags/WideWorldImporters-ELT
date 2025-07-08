WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_stock_holdings', 'fct_stock_holdings', 'wwi_stock_item_id')
    }}

)

SELECT
    *
FROM
    compare_result
