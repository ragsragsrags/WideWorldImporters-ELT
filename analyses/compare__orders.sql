{% set old_etl_relation=ref('legacy_orders') %} 

{% set dbt_relation=ref('fct_orders') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_order_line_id"
) }}