WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_movements') }}

),

changed_movements AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__fct_movements', 
            'stg_warehouse__stock_item_transactions', 
            '', 
            'stock_item_transaction_last_edited_when',
            '',
            'stock_item_transaction_id' ) 
    }}

)

SELECT
    *
FROM
    changed_movements