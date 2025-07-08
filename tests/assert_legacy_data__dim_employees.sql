WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_employees', 'dim_employees', 'wwi_employee_id')
    }}

)

SELECT
    *
FROM
    compare_result
