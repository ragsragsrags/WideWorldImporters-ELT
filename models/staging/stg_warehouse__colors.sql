WITH source AS (

    SELECT
        *
    FROM
        {{ source('base_warehouse', 'colors') }}
    
),

final AS (

    SELECT
        ColorID AS color_id,
        ColorName AS color_name,
        ValidFrom AS color_valid_from,
        ValidTo AS color_valid_to
    FROM
        source

)

SELECT 
    * 
FROM 
    final