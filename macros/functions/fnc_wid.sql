{% macro fnc_wid(orig_id,column_name) -%}
  sha2(array_to_string(array_construct({{orig_id}}||{{column_name}}), '^^^'))
{%- endmacro %}