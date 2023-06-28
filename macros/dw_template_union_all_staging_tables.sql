{%- macro union_all_staging
(
v_staging_tables=[],
exclude_dw_columns=true
) 
-%}  

{%- set lista_campos_unicos = [] -%}

{%- for table in v_staging_tables -%} 
{%- set lista_campos = genera_columnas_staging(table, exclude_dw_columns) -%}

{%- set lista_campos_unicos = lista_campos_unicos.append(lista_campos) -%}

{%- endfor -%} 

{%- set v_campos_target = lista_campos_unicos | sum(start = []) | unique | list -%}

{%- for table in v_staging_tables -%} 
{{ genera_select_staging(table,v_campos_target) }}
{% if not loop.last %} union all {% endif %} 
{%- endfor -%} 

{%- endmacro -%}

{%- macro genera_columnas_staging(
    table,
    exclude_dw_columns=true
    ) 
-%}

{# {%- set columns = ( get_columns_in_relation(table) | map(attribute="column") | map("lower") | list ) -%} #}

{%- set source_name = 'raw_edw' -%}
{%- set source_relation = source(source_name, table) -%}
{%- set columns_name = adapter.get_columns_in_relation(source_relation) -%}
{% set columns = columns_name | map(attribute="column") | map("lower") | list  %}


{% if exclude_dw_columns %}

{% set dw_cols = [
    "dw_fecha_creacion",
    "dw_creado_por",
    "dw_fecha_actualizacion",
    "dw_actualizado_por",
    "dwjob_id",
] %}

{% set columns = columns | reject(
    "in",
    dw_cols,
) | list %}

{% endif %}

{{ return(columns) }}    

{%- endmacro -%}



{% macro genera_select_staging(
    table,
    v_campos_target
) %}


{%- set source_name = 'raw_edw' -%}
{%- set source_relation = source(source_name, table) -%}
{%- set columns_name = adapter.get_columns_in_relation(source_relation) -%}
{% set columns_orig = columns_name | map(attribute="column") | map("lower") | list  %}
{% set llave_hash='{{ fnc_wid("orig_id","1") }}' %}

{%- set result -%} 

select 
{% for column_dest in v_campos_target -%} 
{%- if column_dest in columns_orig | list -%}{{ column_dest }}{%- if not loop.last -%},{%- endif -%} 

{%- else -%}
    {%- if (column_dest | string).startswith("wid") -%}  {{ llave_hash }} as {{ column_dest }} {%- if not loop.last -%},{%- endif -%}
    {%- else -%}
    null as {{ column_dest }}{%- if not loop.last -%},{%- endif -%} 
    {%- endif -%}
{%- endif %}
{%- endfor %}
from {{table}}
where 1=1
{# where {{ '<cambiar_orig_id> = nvl({{ fnc_var_orig_id(var("var_orig_id")) }}, <cambiar_orig_id>)' | as_text }} #}
{%- endset -%}

{{ return ( result ) }}  
 
{% endmacro %}