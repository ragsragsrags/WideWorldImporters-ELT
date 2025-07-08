{% set old_etl_relation=ref('legacy_stock_holdings') %} 

{% set dbt_relation=ref('fct_stock_holdings') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_stock_item_id"
) }}