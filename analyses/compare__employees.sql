{% set old_etl_relation=ref('legacy_employees') %} 

{% set dbt_relation=ref('dim_employees') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_employee_id"
) }}