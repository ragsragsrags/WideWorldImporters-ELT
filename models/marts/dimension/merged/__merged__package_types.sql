WITH cutofftime AS (

    SELECT 
        * 
    FROM 
        {{ ref('stg_cutofftime') }}

),

package_types AS (

    SELECT
        *
    FROM
        {{ ref('stg_warehouse__package_types') }} AS package_types
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN package_types.package_type_valid_from AND package_types.package_type_valid_to
        )

),

package_types_archive AS (

    SELECT
        *
    FROM
        {{ ref('stg_warehouse__package_types_archive') }} AS package_types_archive
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                cutofftime
            WHERE
                cutofftime.cutofftime BETWEEN package_types_archive.package_type_valid_from AND package_types_archive.package_type_valid_to
        )

),

final AS (
    
    SELECT
        package_types.*
    FROM
        package_types
    
    UNION ALL

    SELECT
        package_types_archive.*
    FROM
        package_types_archive

)

SELECT 
    *
FROM
    final