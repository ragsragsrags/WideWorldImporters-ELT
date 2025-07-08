WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_transaction_types', 'dim_transaction_types', 'wwi_transaction_type_id')
    }}

)

SELECT
    *
FROM
    compare_result
