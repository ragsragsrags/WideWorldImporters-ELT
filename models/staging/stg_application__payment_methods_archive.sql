WITH source AS (

    SELECT
        *
    FROM
        {{ source('application', 'paymentmethods_archive') }}
    
),

final AS (

    SELECT
        PaymentMethodID AS payment_method_id,
        PaymentMethodName AS payment_method_name,
        ValidFrom AS payment_method_valid_from,
        ValidTo AS payment_method_valid_to
    FROM
        source

)

SELECT 
    * 
FROM 
    final