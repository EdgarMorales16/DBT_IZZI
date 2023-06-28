{# ####### #}
{# MACRO 1 #}
{# ####### #}

{% macro carga_historico(table_name, campo) %}

{%- set source_name = 'raw_edw' -%}
{%- set source_relation = source(source_name, table_name) -%}
{%- set columns = adapter.get_columns_in_relation(source_relation) -%}
{% set column_names = columns | map(attribute='name') %}
{% set buscar = campo %}

{%- for column in column_names  -%}
        {%- if column == buscar -%}
            {{ column }}
        {%- endif -%}
{%- endfor %}


{% endmacro %}

{# ####### #}
{# MACRO 2 #}
{# ####### #}

{% macro carga_historico_2(table_name, table_name_delta) %}

{%- set source_name = 'raw_edw' -%}
{%- set source_relation = source(source_name, table_name_delta) -%}
{%- set columns = adapter.get_columns_in_relation(source_relation) -%}
{% set column_names = columns | map(attribute='name') %}

{%- for column in column_names  -%}
            {{ column }} {{ carga_historico(table_name,column) }}
{%- endfor -%}

{%- endmacro -%}

{# ####### #}
{# MACRO 3 #}
{# ####### #}

{% macro carga_historico_3(table_name, table_name_delta) %}

{%- set source_name = 'raw_edw' -%}
{%- set source_relation = source(source_name, table_name) -%}
{%- set columns = adapter.get_columns_in_relation(source_relation) -%}
{% set column_names = columns | map(attribute='name') %}

{%- set source_relation_delta = source(source_name, table_name_delta) -%}
{%- set columns_delta = adapter.get_columns_in_relation(source_relation_delta) -%}
{% set column_names_delta = columns_delta | map(attribute='name') %}


{%- for column in column_names %} 

    {%- for column_delta in column_names_delta %} 
        {{ column_delta }}        
    {%- endfor -%}    

{%- endfor -%}

{% endmacro %}

{#

{%- for column in column_names  %}
        {% if column == buscar %}
            {{ column }}
        {%- else -%}
            null {{ column }}
        {% endif %}
{%- endfor %}

{% set base_model_sql %}

    {%- for column in column_names %} 
        {%- for column_delta in column_names_delta %}
            {{ column[1] }}
         
    {%- endfor %}

{% endset %}

{% if execute %} 

{{ log(base_model_sql, info=True) }}  
{% do return(base_model_sql) %}

{% endif %}




       
{% macro carga_historico_2(table_name_delta) %}

{%- set source_name = 'raw_edw' -%}
{%- set source_relation_delta = source(source_name, table_name_delta) -%}
{%- set columns_delta = adapter.get_columns_in_relation(source_relation_delta) -%}
{% set column_names_delta = columns_delta | map(attribute='name') %}
{% set tabla_maestra = {{ carga_historico('stg_ttc_mt_clientes_crm') }}  %}

{%- for column_delta in column_names_delta recursive  %} 
       {{ column_delta }}
{%- endfor %}

{{ tabla_maestra }}


{% endmacro %}

       
       #}