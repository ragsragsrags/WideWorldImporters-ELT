WITH dates_count AS (

    SELECT
        COUNT(*)
    FROM
        {{ ref('dim_dates') }}

),

cutofftimes AS (

    {{ get_cutofftimes('warehouse', '__cutofftime__dim_dates') }}

),

final AS (

    SELECT
        CAST(CAST(cutofftime AS DATE) AS TIMESTAMP_NTZ) AS cutofftime,
        CAST(CAST(init_cutofftime AS DATE) AS TIMESTAMP_NTZ) AS init_cutofftime,
        CAST(CAST(last_cutofftime AS DATE) AS TIMESTAMP_NTZ) AS last_cutofftime
    FROM
        cutofftimes

)

SELECT
    *
FROM
    final