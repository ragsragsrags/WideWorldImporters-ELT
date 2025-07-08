{% set old_etl_relation=ref('legacy_sales') %} 

{% set dbt_relation=ref('fct_sales') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_invoice_id"
) }}