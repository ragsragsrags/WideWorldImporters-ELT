WITH suppliers AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__dim_suppliers') }}      

),

supplier_categories AS (

    SELECT
        *
    FROM
        {{ ref('__merged__supplier_categories') }}

),

people AS (

    SELECT
        *
    FROM
        {{ ref('__merged__people') }}

),

final AS (
        
    SELECT
        ROW_NUMBER() OVER (ORDER BY suppliers.supplier_id) AS supplier_key,
        suppliers.supplier_id AS wwi_supplier_id,
        suppliers.supplier_name AS supplier,
        supplier_categories.supplier_category_name AS category,
        people.people_fullname AS primary_contact,
        suppliers.supplier_reference,
        suppliers.supplier_payment_days AS payment_days,
        suppliers.supplier_delivery_postal_code AS postal_code
    FROM
        suppliers LEFT JOIN
        supplier_categories ON
            suppliers.supplier_category_id = supplier_categories.supplier_category_id LEFT JOIN
        people ON
            suppliers.supplier_primary_contact_person_id = people.people_id
)

SELECT 
    * 
FROM 
    final