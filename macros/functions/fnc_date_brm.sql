{% macro fnc_date_brm(column_name) -%}
  convert_timezone('Etc/GMT','America/Mexico_City', TO_TIMESTAMP( {{column_name}} ) )
{%- endmacro %}