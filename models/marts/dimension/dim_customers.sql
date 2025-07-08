WITH customers AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__dim_customers') }}      

),

buying_groups AS (

    SELECT
        *
    FROM
        {{ ref('__merged__buying_groups') }}

),

customer_categories AS (

    SELECT
        *
    FROM
        {{ ref('__merged__customer_categories') }}

),

people AS (

    SELECT
        *
    FROM
        {{ ref('__merged__people') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY customers.customer_id) AS customer_key,
        customers.customer_id AS wwi_customer_id,
        customers.customer_name AS customer,
        bill_to_customer.customer_name AS bill_to_customer,
        customer_categories.customer_category_name AS category,
        buying_groups.buying_group_name AS buying_group,
        people.people_fullname AS primary_contact,
        customers.customer_delivery_postal_code AS postal_code,
        customers.delivery_city_id AS wwi_delivery_city_id
    FROM
        customers LEFT JOIN
        customers AS bill_to_customer ON
            customers.bill_to_customer_id = bill_to_customer.customer_id LEFT JOIN
        buying_groups ON
            customers.buying_group_id = buying_groups.buying_group_id LEFT JOIN
        customer_categories ON
            customers.customer_category_id = customer_categories.customer_category_id LEFT JOIN
        people ON
            customers.primary_contact_person_id = people.people_id 

)

SELECT 
    * 
FROM 
    final