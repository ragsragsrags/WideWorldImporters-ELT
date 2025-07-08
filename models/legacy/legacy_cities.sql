WITH cutoff AS (
    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}
),

cities AS (
    SELECT
    	C.CityID,
    	C.CityName,
    	C.Location,
    	C.LatestRecordedPopulation,
    	SP.StateProvinceName,
    	CA.CountryName,
    	CA.Continent,
    	SP.SalesTerritory,
    	CA.Region,
    	CA.Subregion
    FROM
    	(
    		SELECT
    			C.CityID,
    			C.CityName,
    			C.Location,
    			C.LatestRecordedPopulation,
    			C.StateProvinceID
    		FROM
    			WideWorldImporters.Application.Cities C
    		WHERE
    			(SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo 
    
    		UNION
    
    		SELECT
    			CA.CityID,
    			CA.CityName,
    			CA.Location,
    			CA.LatestRecordedPopulation,
    			CA.StateProvinceID
    		FROM
    			WideWorldImporters.Application.Cities_Archive CA
    		WHERE
    			(SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo 
    	) C LEFT JOIN
    	(
    		SELECT
    			SP.StateProvinceID,
    			SP.CountryID,
    			SP.StateProvinceName,
    			SP.SalesTerritory
    		FROM
    			WideWorldImporters.Application.StateProvinces SP
    		WHERE
    			(SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SP.ValidFrom AND SP.ValidTo 
    
    		UNION
    
    		SELECT
    			SPA.StateProvinceID,
    			SPA.CountryID,
    			SPA.StateProvinceName,
    			SPA.SalesTerritory
    		FROM
    			WideWorldImporters.Application.StateProvinces_Archive SPA
    		WHERE
    			(SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SPA.ValidFrom AND SPA.ValidTo
    	) SP ON
    		SP.StateProvinceID = C.StateProvinceID LEFT JOIN
    	(
    		SELECT
    			C.CountryID,
    			C.CountryName,
    			C.Continent,
    			C.Region,
    			C.Subregion
    		FROM
    			WideWorldImporters.Application.Countries C
    		WHERE
    			(SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo 
    
    		UNION
    
    		SELECT
    			CA.CountryID,
    			CA.CountryName,
    			CA.Continent,
    			CA.Region,
    			CA.Subregion
    		FROM
    			WideWorldImporters.Application.Countries_Archive CA
    		WHERE
    			(SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo
    	) CA ON
    		CA.CountryID = SP.CountryID
)

select 
    CityID AS wwi_city_id,
    CityName AS city,
    StateProvinceName AS state_province,
    Location AS location,
    CountryName AS country,
    Continent AS continent,
    SalesTerritory AS sales_territory,
    Region AS region,
    SubRegion AS sub_region,
    LatestRecordedPopulation AS latest_recorded_population
from 
    cities