WITH changed_transaction_types AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__dim_transaction_types') }}

),

init_transaction_types AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__dim_transaction_types", 
            "stg_application__transaction_types") }}

),

final AS (

    SELECT
        *
    FROM
        changed_transaction_types

    UNION ALL

    SELECT
        *
    FROM
        init_transaction_types
    WHERE
        init_transaction_types.transaction_type_id NOT IN  (
            SELECT
                changed_transaction_types.transaction_type_id
            FROM
                changed_transaction_types
        ) 

)

SELECT 
    * 
FROM 
    final