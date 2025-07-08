WITH cutoff AS (
    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}
),

payment_methods AS (

    SELECT
        PM.PaymentMethodID AS wwi_payment_method_id,
        PM.PaymentMethodName AS payment_method
    FROM
        WideWorldImporters.Application.PaymentMethods PM
    WHERE
        (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PM.ValidFrom AND PM.ValidTo

    UNION

    SELECT
        PMA.PaymentMethodID,
        PMA.PaymentMethodName
    FROM
        WideWorldImporters.Application.PaymentMethods_Archive PMA
    WHERE
    	(SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PMA.ValidFrom AND PMA.ValidTo

)

SELECT
    *
FROM
    payment_methods