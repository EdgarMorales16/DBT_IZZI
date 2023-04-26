{% macro fnc_translate(column_name) -%}
  translate({{column_name}},'áéíóúáéíóú','aeiouaeiou')
{%- endmacro %}	