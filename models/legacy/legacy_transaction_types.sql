WITH cutoff AS (

    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}

),

transaction_types AS (

    SELECT
        TT.TransactionTypeID,
        TT.TransactionTypeName
    FROM
        WideWorldImporters.Application.TransactionTypes TT
    WHERE
        (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN TT.ValidFrom AND TT.ValidTo

    UNION 

    SELECT
        TTA.TransactionTypeID,
        TTA.TransactionTypeName
    FROM
        WideWorldImporters.Application.TransactionTypes_Archive TTA
    WHERE
        (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN TTA.ValidFrom AND TTA.ValidTo

),

final AS (

    SELECT
        TransactionTypeID AS wwi_transaction_type_id,
        TransactionTypeName AS transaction_type
    FROM
        transaction_types

)

SELECT
    *
FROM
    final