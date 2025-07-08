WITH employees AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__dim_employees') }}      

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY employees.people_id) AS employee_key,
        employees.people_id AS wwi_employee_id,
        employees.people_fullname AS employee,
        employees.people_preferred_name AS preferred_name,
        employees.people_is_sales_person AS is_sales_person,
        employees.people_photo AS photo
    FROM
        employees

)

SELECT 
    * 
FROM 
    final