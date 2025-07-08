WITH compare_result AS (

    {{ 
        test_existing_legacy('legacy_cities', 'dim_cities', 'wwi_city_id')
    }}

)

SELECT
    *
FROM
    compare_result
