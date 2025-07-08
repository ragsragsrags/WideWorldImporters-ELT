{% set old_etl_relation=ref('legacy_transactions') %} 

{% set dbt_relation=ref('fct_transactions') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="transaction_key"
) }}