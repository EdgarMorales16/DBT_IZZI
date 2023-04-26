{% macro fnc_var_to_date(var_to_date) -%}

    {%- if var('var_to_date', false) -%}
        '{{ var('var_to_date') }}'
    {%- else -%}
        current_date()
    {%- endif -%}

{%- endmacro %}