WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_movements', 'fct_movements', 'wwi_stock_item_transaction_id')
    }}

)

SELECT
    *
FROM
    compare_result
