WITH changed_suppliers AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__dim_suppliers') }}

),

init_suppliers AS (
    
    SELECT
        *
    FROM
        {{ table_exists_by_stage_view(
            "warehouse", 
            "__init__dim_suppliers", 
            "stg_purchasing__suppliers") }}

),

final AS (

    SELECT
        *
    FROM
        changed_suppliers

    UNION ALL

    SELECT
        *
    FROM
        init_suppliers
    WHERE
        init_suppliers.supplier_id NOT IN  (
            SELECT
                changed_suppliers.supplier_id
            FROM
                changed_suppliers
        ) 

)

SELECT 
    * 
FROM 
    final