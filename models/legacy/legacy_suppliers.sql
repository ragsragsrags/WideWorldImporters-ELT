WITH cutoff AS (

    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}

),

suppliers AS (

    SELECT
        S.SupplierID,
        S.SupplierName,
        SC.SupplierCategoryName,
        P.FullName,
        S.SupplierReference,
        S.PaymentDays,
        S.DeliveryPostalCode
    FROM
        (
            SELECT 
                S.SupplierID,
                S.SupplierCategoryID,
                S.PrimaryContactPersonID,
                S.SupplierName,
                S.SupplierReference,
                S.PaymentDays,
                S.DeliveryPostalCode
            FROM 
                WideWorldImporters.Purchasing.Suppliers S
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN S.ValidFrom AND S.ValidTo

            UNION

            SELECT
                SA.SupplierID,
                SA.SupplierCategoryID,
                SA.PrimaryContactPersonID,
                SA.SupplierName,
                SA.SupplierReference,
                SA.PaymentDays,
                SA.DeliveryPostalCode
            FROM
                WideWorldImporters.Purchasing.Suppliers_Archive SA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SA.ValidFrom AND SA.ValidTo
        ) S LEFT JOIN
        (
            SELECT 
                SC.SupplierCategoryID,
                SC.SupplierCategoryName
            FROM
                WideWorldImporters.Purchasing.SupplierCategories SC 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SC.ValidFrom AND SC.ValidTo

            UNION

            SELECT
                SCA.SupplierCategoryID,
                SCA.SupplierCategoryName
            FROM
                WideWorldImporters.Purchasing.SupplierCategories_Archive SCA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN SCA.ValidFrom AND SCA.ValidTo
        ) SC ON
            SC.SupplierCategoryID = S.SupplierCategoryID LEFT JOIN
        (
            SELECT
                P.PersonID,
                P.FullName
            FROM
                WideWorldImporters.Application.People P
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN P.ValidFrom AND P.ValidTo

            UNION

            SELECT
                PA.PersonID,
                PA.FullName
            FROM
                WideWorldImporters.Application.People_Archive PA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN PA.ValidFrom AND PA.ValidTo
        ) P ON
            P.PersonID = S.PrimaryContactPersonID

),

final AS (

    SELECT
        SupplierID AS wwi_supplier_id,
        SupplierName AS supplier,
        SupplierCategoryName AS category,
        FullName AS primary_contact,
        SupplierReference AS supplier_reference,
        PaymentDays AS payment_days,
        DeliveryPostalCode AS postal_code
    FROM
        suppliers

)

SELECT
    *
FROM
    final