WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_payment_methods', 'dim_payment_methods', 'wwi_payment_method_id')
    }}

)

SELECT
    *
FROM
    compare_result
