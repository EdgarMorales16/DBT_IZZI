{#Macro que consulta la tabla de control: ttc_ctl_inventario_entidades para revisar los orig_id válido por entidad#}
{%- macro dw_registro_no_match (
    table_name
) -%}

{%- set orig_id_query -%}
select distinct orig_id from ttc_ctl_inventario_entidades
where 1=1
and upper(entidad) = upper('{{table_name | replace ("_h","") }}')
order by 1
{%- endset -%}

{%- set results = run_query(orig_id_query) -%}

{%- if execute -%}
{# Return the first column #}
{%- set results_list = results.columns[0].values() -%}
{%- else -%}
{%- set results_list = [] -%}
{%- endif -%}

{#Por cada orig_id que arroje la consulta, se invoca la macro para generar el select#}
{%- for org in results_list -%} 
{{dw_insert_no_match (table_name,org)}}
{% if not loop.last -%}union all{% endif %} 
{%- endfor -%}

{%- endmacro -%}

{#Macro que genera la consulta de select de manera dinamica para cada entidad por orig_id#}
{%- macro dw_insert_no_match(
    table_name,
    orig_id
) -%}

{#Evalua que le parametro de la tabla no venga vacio#}
{%- if table_name is not defined -%}
{{ exceptions.raise_compiler_error("Macro is missing the 'table' parameter.") }}
{%- endif -%}

{#Evalua que la tabla no sea efimera, caso contrario manda mensaje de error#}
{%- if (table_name | string).startswith("__dbt__CTE__") -%}
{{ exceptions.raise_compiler_error("Cannot get columns from a ephemeral model.") }}
{%- endif -%}

{%- if execute -%}
{%- set columns = (get_columns_in_relation(table_name)) -%}

{% set result %} 
select 
{% for column in columns -%} 
{#Filtramos los registros de control para no duplicarlos en el insert#}
{%- if not column.column.startswith("DW") -%}
{#Si el campo es un WID tipo varchar, genera una llave hash generica#}
{%- if column.column.startswith("WID") and column.is_string() -%}
sha2('{{orig_id}}'||'1') as {{ column.column }}
{#Si el campo es el row_id, genera el row_id concatenando el orig_id + 1#}
{%- elif column.column == "ROW_ID" -%}
'{{orig_id}}'||'1' as {{ column.column }}
{#Si el campo es el orig_id, inserta el valor del orig_id#}
{%- elif column.column == "ORIG_ID" -%}
{{orig_id}} as {{ column.column }}
{#Evalua si el tipo de dato de la columna es varchar, en ese caso pasa el nombre de la columna como valor del campo#}
{%- elif column.is_string() -%}
'SIN {{ column.column | replace ("_"," ") }}' as {{ column.column }}
{#Evalua si el tipo de dato de la columna es number, en ese caso el valor es 1#}
{%- elif column.is_numeric() -%}
-1 as {{ column.column }}
{#Si el valor de la columna no es varchar ni number, se asume que es date y se pone el valor 01/01/1990 como default#}
{%- else -%}
{#to_date('01/01/1990','dd/mm/yyyy') as {{ column.column }}#}
null as {{ column.column }}
{%- endif -%}
{%- if not loop.last -%},{%- endif -%} 
{%- endif -%}

{%- endfor -%} 

{%- endset -%}

{{ return(result) }}

{%- endif -%}

{%- endmacro -%}