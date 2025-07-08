WITH source AS (

    SELECT
        *
    FROM
        {{ source('sales', 'invoices') }}

),

transformed AS (

    SELECT
        InvoiceID AS invoice_id,
        CustomerID AS customer_id,
        BillToCustomerID AS bill_to_customer_id,
        OrderID AS order_id,
        DeliveryMethodID AS delivery_method_id,
        ContactPersonID AS contact_person_id,
        AccountsPersonID AS accounts_person_id,
        SalespersonPersonID AS sales_person_person_id,
        PackedByPersonID AS packed_by_person_id,
        CAST(InvoiceDate AS DATE) AS invoice_date,
        CustomerPurchaseOrderNumber AS invoice_customer_purchase_order_number,
        IsCreditNote AS invoice_is_credit_note,
        CreditNoteReason AS invoice_credit_note_reason,
        Comments AS invoice_comments,
        DeliveryInstructions AS invoice_delivery_instructions,
        InternalComments AS invoice_internal_comments,
        TotalDryItems AS invoice_total_dry_items,
        TotalChillerItems AS invoice_total_chiller_items,
        DeliveryRun AS invoice_delivery_run,
        RunPosition AS invoice_run_position,
        ReturnedDeliveryData AS invoice_returned_delivery_data,
        CAST(ConfirmedDeliveryTime AS DATE) AS invoice_confirmed_delivery_time,
        ConfirmedReceivedBy AS invoice_confirmed_received_by,
        LastEditedBy AS invoice_last_edited_by,
        LastEditedWhen AS invoice_last_edited_when
    FROM
        source

)

SELECT
    *
FROM
    transformed