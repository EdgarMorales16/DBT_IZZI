-- ----------------------------------
-- MACRO union_all_staging
-- ----------------------------------
{%- macro union_all_staging_jemv
(
v_staging_tables=[],
exclude_dw_columns=true
) 
-%}  

{%- set lista_campos_unicos = [] -%}

{%- for table in v_staging_tables -%} 
{%- set lista_campos = genera_columnas_staging_jemv(table, exclude_dw_columns) -%}

{%- set lista_campos_unicos = lista_campos_unicos.append(lista_campos) -%}

{%- endfor -%} 

{%- set v_campos_target = lista_campos_unicos | sum(start = []) | unique | list -%}

{{ v_campos_target }}

{%- endmacro -%}

-- ----------------------------------
-- MACRO genera_columnas_staging_jemv
-- ----------------------------------
{%- macro genera_columnas_staging_jemv(
    table,
    exclude_dw_columns=true
    ) 
-%}

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