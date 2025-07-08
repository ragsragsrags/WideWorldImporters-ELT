WITH source AS (

    SELECT
        *
    FROM
        {{ source('sales', 'buyinggroups') }}

),

transformed AS (

    SELECT
        BuyingGroupID AS buying_group_id,
        BuyingGroupName AS buying_group_name,
        ValidFrom AS buying_group_valid_from,
        ValidTo AS buying_group_valid_to
    FROM
        source

)

SELECT
    *
FROM
    transformed