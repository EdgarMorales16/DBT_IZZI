select {{ carga_historico_2('stg_ttc_mt_clientes_salesforce','stg_ttc_mt_clientes_crm') }}
from {{ source('raw_edw', 'stg_ttc_mt_clientes_crm') }}


{%- macro prueba_genera_source (
    source_name, 
    table_name,
    v_fnc_date,
    v_fnc_translate
 ) -%}

{%- set lista = v_fnc_date | select ("in", v_fnc_translate, ) -%}

{%- if lista | list | length() != 0 -%}
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
{% for column in column_names -%} 
{{val_column(column, v_fnc_date, v_fnc_translate)}}{% if not loop.last %},{% endif %} 
{% endfor -%} 
from source_model)

select * from model_columns
{%- endset -%}

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
{{fnc_date ( column_name )}} as {{ column_name }}
{%- elif column.column | lower in v_fnc_translate | list -%}
{{fnc_translate ( column_name )}} as {{ column_name }}
{%- else -%}
{{column_name}} as {{ column_name }}
{%- endif -%}

{% endmacro %}

{%- macro raise_exception(mensaje) -%}

{{ exceptions.raise_compiler_error(mensaje) }}
    
{%- endmacro -%}

{{prueba_genera_source(source_name="raw_crm",table_name="ttc_s_order_type",v_fnc_date=["created", "last_upd"], v_fnc_translate=["order_cat_cd","name"])}}