WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_transaction_types') }}

),

transaction_type AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_transaction_types', 
            'stg_application__transaction_types', 
            'stg_application__transaction_types_archive', 
            'transaction_type_valid_from',
            'transaction_type_valid_to',
            'transaction_type_id' ) 
    }}

)

SELECT
    *
FROM 
    transaction_type