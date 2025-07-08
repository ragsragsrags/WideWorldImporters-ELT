{% set old_etl_relation=ref('legacy_transaction_types') %} 

{% set dbt_relation=ref('dim_transaction_types') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_transaction_type_id"
) }}