{% set old_etl_relation=ref('legacy_movements') %} 

{% set dbt_relation=ref('fct_movements') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_stock_item_transaction_id"
) }}