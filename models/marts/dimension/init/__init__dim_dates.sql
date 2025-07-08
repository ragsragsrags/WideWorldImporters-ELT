WITH changed_dates AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed__dim_dates') }}

),

init_dates AS (
    
    SELECT
        *
    FROM
        {{ table_exists(
            "warehouse", 
            "__init__dim_dates", 
            "CAST(NULL AS TIMESTAMP_NTZ) AS date, 
            CAST(NULL AS NUMBER) AS day_number,
            CAST(NULL AS NUMBER) AS day,
            CAST(NULL AS VARCHAR) AS month,
            CAST(NULL AS VARCHAR) AS short_month,
            CAST(NULL AS NUMBER) AS calendar_month_number,
            CAST(NULL AS VARCHAR) AS calendar_month_label, 
            CAST(NULL AS NUMBER) AS calendar_year,
            CAST(NULL AS VARCHAR) AS calendar_year_label,
            CAST(NULL AS NUMBER) AS fiscal_month_number,
            CAST(NULL AS VARCHAR) AS fiscal_month_label,
            CAST(NULL AS NUMBER) AS fiscal_year,
            CAST(NULL AS VARCHAR) AS fiscal_year_label,
            CAST(NULL AS NUMBER) AS iso_week_number
            ") }}

),

final AS (

    SELECT
        *
    FROM
        changed_dates

    UNION

    SELECT
        *
    FROM
        init_dates 

)

SELECT 
    * 
FROM
    final