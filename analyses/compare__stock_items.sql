{% set old_etl_relation=ref('legacy_stock_items') %} 

{% set dbt_relation=ref('dim_stock_items') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_stock_item_id"
) }}