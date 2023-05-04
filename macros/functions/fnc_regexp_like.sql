{% macro fnc_regexp_like(column_name) -%}
    regexp_like(  {{column_name}} ,'[0-9áéíóúÁÉÍÓÚ/|*¡?=()&%$#"!-,. ]')
{%- endmacro %}  