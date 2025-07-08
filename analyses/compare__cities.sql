{% set old_etl_relation=ref('legacy_cities') %} 

{% set dbt_relation=ref('dim_cities') %}  {{ 

audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="wwi_city_id"
) }}