WITH source AS (

    SELECT
        *
    FROM
        {{ source('application', 'people') }}
    
),

final AS (

    SELECT
        PersonID AS people_id,
        FullName AS people_fullname,
        PreferredName AS people_preferred_name,
        IsEmployee AS people_is_employee,
        IsSalesPerson AS people_is_sales_person,
        Photo AS people_photo,
        ValidFrom AS people_valid_from,
        ValidTo AS people_valid_to
    FROM
        source

)

SELECT 
    * 
FROM 
    final