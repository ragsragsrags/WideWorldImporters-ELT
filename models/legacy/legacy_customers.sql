WITH cutoff AS (
    SELECT 
        *
    FROM
        {{ ref('stg_cutofftime') }}
),

customers AS (
    SELECT
        C.CustomerID AS wwi_customer_id,
        C.CustomerName AS customer ,
        BC.CustomerName AS bill_to_customer,
        CC.CustomerCategoryName AS category,
        BG.BuyingGroupName AS buying_group,
        PA.FullName AS primary_contact,
        C.DeliveryPostalCode AS postal_code
    FROM
        (
            SELECT 
                C.CustomerID,
                C.BillToCustomerID,
                C.CustomerCategoryID,
                C.PrimaryContactPersonID,
                C.BuyingGroupID,
                C.CustomerName,
                C.DeliveryPostalCode
            FROM
                WideWorldImporters.Sales.Customers C
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo

            UNION

            SELECT 
                CA.CustomerID,
                CA.BillToCustomerID,
                CA.CustomerCategoryID,
                CA.PrimaryContactPersonID,
                CA.BuyingGroupID,
                CA.CustomerName,
                CA.DeliveryPostalCode
            FROM
                WideWorldImporters.Sales.Customers_Archive CA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo
        ) C LEFT JOIN
        (
            SELECT 
                C.CustomerID,
                C.CustomerName,
                C.DeliveryPostalCode
            FROM
                WideWorldImporters.Sales.Customers C
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN C.ValidFrom AND C.ValidTo

            UNION

            SELECT 
                CA.CustomerID,
                CA.CustomerName,
                CA.DeliveryPostalCode
            FROM
                WideWorldImporters.Sales.Customers_Archive CA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CA.ValidFrom AND CA.ValidTo
        ) BC ON
            BC.CustomerID = C.BillToCustomerID LEFT JOIN
        (
            SELECT
                CC.CustomerCategoryID,
                CC.CustomerCategoryName
            FROM
                WideWorldImporters.sales.CustomerCategories CC 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CC.ValidFrom AND CC.ValidTo

            UNION

            SELECT
                CCA.CustomerCategoryID,
                CCA.CustomerCategoryName
            FROM
                WideWorldImporters.sales.CustomerCategories_Archive CCA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN CCA.ValidFrom AND CCA.ValidTo
        ) CC ON
            CC.CustomerCategoryID = C.CustomerCategoryID LEFT JOIN
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
        ) PA ON
            PA.PersonID = C.PrimaryContactPersonID LEFT JOIN
        (
            SELECT
                BG.BuyingGroupID,
                BG.BuyingGroupName
            FROM
                WideWorldImporters.Sales.BuyingGroups BG 
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN BG.ValidFrom AND BG.ValidTo

            UNION

            SELECT
                BGA.BuyingGroupID,
                BGA.BuyingGroupName
            FROM
                WideWorldImporters.Sales.BuyingGroups_Archive BGA
            WHERE
                (SELECT TOP 1 cutofftime FROM cutoff) BETWEEN BGA.ValidFrom AND BGA.ValidTo
        ) BG ON
            BG.BuyingGroupID = C.BuyingGroupID
)

SELECT
    *
FROM
    customers