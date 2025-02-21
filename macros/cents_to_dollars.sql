{% macro cent_to_dollars(column_name,precision=2) -%}
round({{column_name}} / 100 , {{precision}})
{%- endmacro %}