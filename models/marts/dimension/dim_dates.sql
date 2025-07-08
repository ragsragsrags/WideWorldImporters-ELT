SELECT
    *
FROM
    {{ ref('__init__dim_dates') }}
ORDER BY
    date