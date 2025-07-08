WITH source AS (
    
    SELECT 
        * 
    FROM
        {{ source('application', 'transactiontypes_archive') }}

),

transformed AS (

    SELECT
        source.TransactionTypeID AS transaction_type_id,
        source.TransactionTypeName AS transaction_type_name,
        source.ValidFrom AS transaction_type_valid_from,
        source.ValidTo AS transaction_type_valid_to
    FROM
        source

)

SELECT 
    *
FROM
    transformed