{% macro fnc_var_from_date(var_from_date) -%}

    {%- if var('var_from_date', false) -%}
        '{{ var('var_from_date') }}'
    {%- else -%}
        current_date() -10
    {%- endif -%}

{%- endmacro %}