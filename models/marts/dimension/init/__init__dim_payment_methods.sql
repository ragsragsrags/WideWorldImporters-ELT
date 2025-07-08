WITH changed_payment_methods AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__dim_payment_methods') }}

),

init_payment_methods AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__dim_payment_methods", 
            "stg_application__payment_methods") }}

),

final AS (

    SELECT
        *
    FROM
        changed_payment_methods

    UNION ALL

    SELECT
        *
    FROM
        init_payment_methods
    WHERE
        init_payment_methods.payment_method_id NOT IN  (
            SELECT
                changed_payment_methods.payment_method_id
            FROM
                changed_payment_methods
        ) 

)

SELECT 
    * 
FROM 
    final