WITH cities AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__dim_cities') }}      

),

states AS (

    SELECT
        *
    FROM
        {{ ref('__merged__stateprovinces') }}

),

countries AS (

    SELECT
        *
    FROM
        {{ ref('__merged__countries') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY cities.city_id) AS city_key,
        cities.city_id AS wwi_city_id,
        cities.city_name AS city,
        states.stateprovince_name AS state_province,
        countries.country_name AS country,
        countries.country_continent AS continent,
        states.stateprovince_sales_territory as sales_territory,
        countries.country_region AS region,
        countries.country_sub_region AS sub_region,
        cities.city_location AS location,
        cities.city_latest_recorded_population AS latest_recorded_population
    FROM
        cities join
        states ON
            cities.stateprovince_id = states.stateprovince_id JOIN
        countries ON
            states.country_id = countries.country_id

)

SELECT 
    * 
FROM 
    final
    