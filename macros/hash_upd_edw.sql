{% macro hash_upd_edw(table_name) %}

{%- set source_name = 'raw_edw' -%}
{%- set source_relation = source(source_name, table_name) -%}
{%- set columns = adapter.get_columns_in_relation(source_relation) -%}
{% set column_names = columns | map(attribute='name') %}

{% set base_model_sql %}
sha2( 
    {%- for column in column_names %} 
        {%- if column not in (
                            'DW_FECHA_CREACION', 
                            'DW_CREADO_POR',
                            'DW_FECHA_ACTUALIZACION',
                            'DW_ACTUALIZADO_POR',
                            'DWJOB_ID'
                            ) -%}
            {{ column }}
            {%- if not loop.last -%}
                ||
            {%- endif %}
        {% endif %}
    {%- endfor -%}
)
{% endset %}

{% if execute %}

{{ log(base_model_sql, info=True) }}
{% do return(base_model_sql) %}

{% endif %}
{% endmacro %}

       