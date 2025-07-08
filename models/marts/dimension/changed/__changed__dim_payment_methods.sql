WITH cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_payment_methods') }}

),

payment_methods AS (

    {{ 
        get_table_changes(
            'warehouse', 
            '__cutofftime__dim_payment_methods', 
            'stg_application__payment_methods', 
            'stg_application__payment_methods_archive', 
            'payment_method_valid_from',
            'payment_method_valid_to',
            'payment_method_id' ) 
    }}

)

SELECT
    *
FROM 
    payment_methods