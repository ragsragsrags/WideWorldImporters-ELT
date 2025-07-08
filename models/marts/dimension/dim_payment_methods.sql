WITH payment_methods AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__dim_payment_methods') }}      

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY payment_methods.payment_method_id) AS payment_method_key,
        payment_methods.payment_method_id AS wwi_payment_method_id,
        payment_methods.payment_method_name AS payment_method
    FROM
        payment_methods

)

SELECT 
    * 
FROM 
    final