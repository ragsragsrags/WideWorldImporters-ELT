WITH transaction_types AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init__dim_transaction_types') }}      

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY transaction_types.transaction_type_id) AS transaction_type_key,
        transaction_types.transaction_type_id AS wwi_transaction_type_id,
        transaction_types.transaction_type_name AS transaction_type
    FROM
        transaction_types

)

SELECT 
    * 
FROM 
    final