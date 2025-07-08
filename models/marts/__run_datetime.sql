WITH run_datetime AS (
    
    SELECT 
        GETDATE() AS run_datetime

)

SELECT
    *
FROM
    run_datetime