WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_customers', 'dim_customers', 'wwi_customer_id')
    }}

)

SELECT
    *
FROM
    compare_result
