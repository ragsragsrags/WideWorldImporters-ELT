{% set old_etl_relation=ref('legacy_purchases') %} 

{% set dbt_relation=ref('fct_purchases') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_purchase_order_id"
) }}