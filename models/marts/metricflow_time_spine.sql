-- metricflow_time_spine.sql
WITH days AS (
    --for BQ adapters use "DATE('01/01/2000','mm/dd/yyyy')"
    {{ dbt_date.get_base_dates(n_dateparts=365*13, datepart="day") }}
),

cast_to_date AS (
    SELECT 
        CAST(date_day AS DATE) AS date_day
    FROM 
        days
)

SELECT 
    * 
FROM 
    cast_to_date