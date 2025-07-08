WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_orders', 'fct_orders', 'wwi_order_id')
    }}

)

SELECT
    *
FROM
    compare_result
