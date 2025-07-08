{% set old_etl_relation=ref('legacy_suppliers') %} 

{% set dbt_relation=ref('dim_suppliers') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_supplier_id"
) }}