{%- macro genera_source_stmt (
    source_name, 
    table_name,
    v_fnc_date,
    v_fnc_translate,
    v_wid="false",
    v_wid_orig="",
    v_wid_row_name="",
    v_wid_alias=""
 ) -%}

{%- set lista_repedida = v_fnc_date | select ("in", v_fnc_translate, ) -%}

{%- if lista_repedida | list | length() != 0 -%}
{{ raise_exception("No se puede utilizar el mismo campo para aplicar dos funciones diferentes.") }}
{%- endif -%}

{%- if source_name is not defined -%}
{{ raise_exception("El parametro 'source_name' es requerido.") }}
{%- endif -%}

{%- if table_name is not defined -%}
{{ raise_exception("El parametro 'table_name' es requerido.") }}
{%- endif -%}

{%- set source_relation = source(source_name, table_name) -%}

{%- set column_names = get_columns_in_relation(source_relation) -%} 

{% set result %} 

with 
source_model as (select * from {{ source(source_name, table_name) }}),

model_columns as (
select 
{% if v_wid == "true" -%}
{% if v_wid_orig | length() != 0 and v_wid_row_name | length() != 0 and v_wid_alias | length() != 0-%}
{{fnc_wid ( v_wid_orig, v_wid_row_name )}} as {{ v_wid_alias }},
{% else %}
{{ raise_exception("Si el parametro v_wid es 'true', se requieren los parametros v_wid_orig, v_wid_row_name y v_wid_alias para poder construir la llave ") }}
{% endif -%}
{% endif -%}
{% for column in column_names -%} {{val_column(column, v_fnc_date, v_fnc_translate)}}{% if not loop.last %},{% endif %} 
{% endfor -%} 
from source_model)

select * from model_columns
{% endset -%}

{{return(result)}}

{%- endmacro -%}




{%- macro val_column (
    column,
    v_fnc_date,
    v_fnc_translate
) -%}

{%- if column.dtype | lower != 'timestamp_ntz' and column.column | lower in v_fnc_date | list -%}
{{ raise_exception("No se puede aplicar la función 'fnc_date' a un tipo de dato diferente a 'date'") }}
{%- endif -%}

{%- if column.dtype | lower != 'varchar' and column.column | lower in v_fnc_translate | list -%}
{{ raise_exception("No se puede aplicar la función 'fnc_translate' a un tipo de dato diferente a 'varchar'") }}
{%- endif -%}

{%- set column_name = column.column | lower -%}

{%- if column.column | lower in v_fnc_date | list -%}
{{fnc_date ( column_name )}} as {{ column_name }},
{{ column_name }} as {{ column_name }}_raw
{%- elif column.column | lower in v_fnc_translate | list -%}
{{fnc_translate ( column_name )}} as {{ column_name }}
{%- else -%}
{{column_name}} as {{ column_name }}
{%- endif -%}

{% endmacro %}




{%- macro raise_exception(mensaje) -%}

{{ exceptions.raise_compiler_error(mensaje) }}
    
{%- endmacro -%}