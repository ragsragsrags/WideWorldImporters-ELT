WITH source AS (
    
    SELECT 
        * 
    FROM
        {{ source('application', 'stateprovinces_archive') }}

),

transformed AS (

    SELECT
        source.StateProvinceID AS stateprovince_id,
        source.StateProvinceName AS stateprovince_name,
        source.CountryID AS country_id,
        source.SalesTerritory AS stateprovince_sales_territory,
        source.ValidFrom AS stateprovince_valid_from,
        source.ValidTo AS stateprovince_valid_to
    FROM
        source

)

SELECT 
    *
FROM
    transformed