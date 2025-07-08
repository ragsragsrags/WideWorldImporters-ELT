WITH changed_transactions AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__fct_transactions') }}

),

init_transactions AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__fct_transactions", 
            "__changed__fct_transactions") }}

),

final AS (

    SELECT
        *
    FROM
        changed_transactions

    UNION ALL

    SELECT
        *
    FROM
        init_transactions
    WHERE
        NOT EXISTS  (
            SELECT
                1
            FROM
                changed_transactions
            WHERE
                EQUAL_NULL(changed_transactions.customer_transaction_id, init_transactions.customer_transaction_id) AND
                EQUAL_NULL(changed_transactions.supplier_transaction_id, init_transactions.supplier_transaction_id)
        ) 

)

SELECT 
    * 
FROM 
    final