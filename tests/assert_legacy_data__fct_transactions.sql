WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_transactions', 'fct_transactions', 'transaction_key')
    }}

)

SELECT
    *
FROM
    compare_result
