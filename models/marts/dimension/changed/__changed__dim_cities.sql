WITH changed_states AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_cities', 
            'stg_application__stateprovinces', 
            'stg_application__stateprovinces_archive', 
            'stateprovince_valid_from',
            'stateprovince_valid_to',
            'stateprovince_id' ) 
    }}

),

changed_countries AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_cities', 
            'stg_application__countries', 
            'stg_application__countries_archive', 
            'country_valid_from',
            'country_valid_to',
            'country_id' ) 
    }}

),

merged_states AS (

    SELECT
        *
    FROM
        {{ ref('__merged__stateprovinces') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_cities') }}

),

cities AS (
    
    SELECT
        cities.*
    FROM
        {{ ref('stg_application__cities') }} AS cities join
        merged_states ON
            cities.stateprovince_id = merged_states.stateprovince_id JOIN
        cutofftimes ON
            1 = 1
    WHERE
        (
            cities.city_valid_from > cutofftimes.last_cutofftime OR
            (
                cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                cities.city_valid_from >= cutofftimes.last_cutofftime  
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_states
                WHERE
                    changed_states.stateprovince_id = cities.stateprovince_id
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_countries
                WHERE
                    changed_countries.country_id = merged_states.country_id
            )
        ) AND
        cities.city_valid_from <= cutofftimes.cutofftime 

),

cities_archive AS (

    SELECT
        cities_archive.*
    FROM
        {{ ref('stg_application__cities_archive') }} AS cities_archive JOIN
        merged_states ON
            cities_archive.stateprovince_id = merged_states.stateprovince_id JOIN
        cutofftimes ON
            1 = 1
    WHERE 
        (
            cities_archive.city_valid_from > cutofftimes.last_cutofftime OR
            (
                cutofftimes.cutofftime = cutofftimes.init_cutofftime AND
                cities_archive.city_valid_from >= cutofftimes.last_cutofftime  
            ) 
            OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_states
                WHERE
                    changed_states.stateprovince_id = cities_archive.stateprovince_id
            ) OR
            EXISTS(
                SELECT
                    1
                FROM
                    changed_countries
                WHERE
                    changed_countries.country_id = merged_states.country_id
            )
        ) AND
        cities_archive.city_valid_from <= cutofftimes.cutofftime

),

cities_archive_max_valid_dates AS (

    SELECT
        city_id,
        MAX(cities_archive.city_valid_from) AS city_valid_from,
        MAX(cities_archive.city_valid_to) AS city_valid_to 
    FROM
        cities_archive
    GROUP BY
        city_id

),

final AS (

    SELECT
        cities.*
    FROM
        cities

    UNION ALL

    SELECT
        *
    FROM
        cities_archive  
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                cities_archive_max_valid_dates 
            WHERE
                cities_archive_max_valid_dates.city_id = cities_archive.city_id AND
                cities_archive_max_valid_dates.city_valid_from = cities_archive.city_valid_from AND
                cities_archive_max_valid_dates.city_valid_to = cities_archive.city_valid_to
        ) AND
        NOT EXISTS (
            SELECT
                1
            FROM
                cities
            WHERE
                cities.city_id = cities_archive.city_id
        ) 
)

SELECT
    *
FROM
    final