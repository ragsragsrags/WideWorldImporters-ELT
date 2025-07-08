WITH source AS (

    SELECT
        *
    FROM
        {{ source('base_warehouse', 'packagetypes') }}
    
),

final AS (

    SELECT
        PackageTypeID AS package_type_id,
        PackageTypeName AS package_type_name,
        ValidFrom AS package_type_valid_from,
        ValidTo AS package_type_valid_to
    FROM
        source

)

SELECT 
    * 
FROM 
    final