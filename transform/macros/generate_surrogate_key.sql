-- macros/generate_surrogate_key.sql
-- ====================================
-- Thin wrapper around dbt_utils.generate_surrogate_key.
-- Exists here so we can override behaviour if needed without
-- changing every model that calls it.
--
-- Usage: {{ dbt_utils.generate_surrogate_key(['col1', 'col2']) }}

{% macro generate_surrogate_key(field_list) %}
    {{ return(dbt_utils.generate_surrogate_key(field_list)) }}
{% endmacro %}
