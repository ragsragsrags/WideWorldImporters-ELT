WITH source AS (
    
    SELECT 
        * 
    FROM
        {{ source('application', 'cities') }}
),

transformed AS (

    SELECT
        source.CityID AS city_id,
        source.CityName AS city_name,
        source.StateProvinceID AS stateprovince_id,
        source.Location AS city_location,
        source.LatestRecordedPopulation AS city_latest_recorded_population,
        source.ValidFrom AS city_valid_from,
        source.ValidTo AS city_valid_to
    FROM
        source

)

SELECT 
    *
FROM
    transformed