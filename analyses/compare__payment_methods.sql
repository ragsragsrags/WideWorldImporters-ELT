{% set old_etl_relation=ref('legacy_payment_methods') %} 

{% set dbt_relation=ref('dim_payment_methods') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_payment_method_id"
) }}