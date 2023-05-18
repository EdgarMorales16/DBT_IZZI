{% macro is_incremental_upd (table_name, column_name ) -%}
    
    select delta.* 
    from {{table_name}} delta, 
    {{ this }} historico 
    where 1 = 1 
    and (delta. {{column_name}} = historico. {{column_name}} 
    and delta.{{column_name}}_upd != historico.{{column_name}}_upd)
        
        union 

    select * 
    from {{table_name}}  
    where {{column_name}} NOT IN ( (select {{column_name}} from {{ this }}) )    

{%- endmacro %}