{% macro fnc_var_rework(var_rework) -%}

    {%- if var('var_rework', false) -%}
        '{{ var('var_to_date') }}'
    {%- else -%}
        'N'
    {%- endif -%}

{%- endmacro %}