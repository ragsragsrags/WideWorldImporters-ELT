WITH cutoff AS (
    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}
),

employees AS (

    SELECT 
        P.PersonID AS wwi_employee_id,
        P.FullName AS employee,
        P.PreferredName AS preferred_name,
        P.IsSalesperson AS is_sales_person,
        P.Photo
    FROM 
        WideWorldImporters.Application.People P 
    WHERE
        P.IsEmployee = 1 AND
        (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN P.ValidFrom AND P.ValidTo

    UNION

    SELECT 
        PA.PersonID,
        PA.FullName,
        PA.PreferredName,
        PA.IsSalesperson,
        PA.Photo
    FROM 
        WideWorldImporters.Application.People_Archive PA 
    WHERE
        PA.IsEmployee = 1 AND
        (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PA.ValidFrom AND PA.ValidTo
)

SELECT 
    *
FROM
    employees