WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_purchases', 'fct_purchases', 'wwi_purchase_order_id')
    }}

)

SELECT
    *
FROM
    compare_result
