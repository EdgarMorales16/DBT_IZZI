{% macro fnc_date(column_name) -%}  -- {{column_name}} 
    convert_timezone('Etc/GMT','America/Mexico_City', {{column_name}})
{%- endmacro %}