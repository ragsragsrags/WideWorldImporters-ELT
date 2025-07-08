WITH changed_sales AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__fct_sales') }}

),

init_sales AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__fct_sales", 
            "stg_sales__invoices") }}

),

final AS (

    SELECT
        *
    FROM
        changed_sales

    UNION ALL

    SELECT
        *
    FROM
        init_sales
    WHERE
        init_sales.invoice_id NOT IN  (
            SELECT
                changed_sales.invoice_id
            FROM
                changed_sales
        ) 

)

SELECT 
    * 
FROM 
    final