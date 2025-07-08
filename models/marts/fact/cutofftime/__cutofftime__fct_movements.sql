WITH movements_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('fct_movements') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__fct_movements') }}

)

SELECT
    *
FROM
    cutofftimes