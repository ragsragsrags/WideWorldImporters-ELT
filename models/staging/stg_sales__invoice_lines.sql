WITH source AS (

    SELECT
        *
    FROM
        {{ source('sales', 'invoicelines') }}

),

transformed AS (

    SELECT
        InvoiceLineID AS invoice_line_id,
        InvoiceID AS invoice_id,
        StockItemID AS stock_item_id,
        Description AS invoice_line_description,
        PackageTypeID AS package_type_id,
        Quantity AS invoice_line_quantity,
        UnitPrice AS invoice_line_unit_price,
        TaxRate AS invoice_line_tax_rate,
        ExtendedPrice - TaxAmount AS invoice_line_total_excluding_tax,
        TaxAmount AS invoice_line_tax_amount,
        LineProfit AS invoice_line_profit,
        ExtendedPrice AS invoice_line_total_including_tax,
        LastEditedBy AS invoice_line_last_edited_by,
        LastEditedWhen AS invoice_line_last_edited_when
    FROM
        source

)

SELECT
    *
FROM
    transformed