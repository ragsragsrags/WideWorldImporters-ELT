WITH source AS (
    
    SELECT 
        * 
    FROM
        {{ source('application', 'countries_archive') }}

),

transformed AS (

    SELECT
        source.CountryID AS country_id,
        source.CountryName AS country_name,
        source.Continent AS country_continent,
        source.Region AS country_region,
        source.SubRegion AS country_sub_region,
        source.ValidFrom AS country_valid_from,
        source.ValidTo AS country_valid_to
    FROM
        source

)

SELECT 
    *
FROM
    transformed