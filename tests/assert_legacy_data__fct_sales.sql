WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_sales', 'fct_sales', 'wwi_invoice_id')
    }}

)

SELECT
    *
FROM
    compare_result
