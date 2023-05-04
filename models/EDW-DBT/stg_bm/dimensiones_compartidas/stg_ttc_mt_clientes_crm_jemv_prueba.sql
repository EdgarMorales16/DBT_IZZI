{{ config(
    materialized = 'incremental',
    unique_key = 'widmt_cliente'
) }}

with ttc_mt_clientes_crm_jemv as (
    
    select delta.* 
    from {{ source('raw_edw', 'ttc_mt_clientes_crm_jemv') }} delta,
    {{ this }} historico 
             
    {% if is_incremental() -%}

        where 1 = 1 
        and ( (delta.widmt_cliente = historico.widmt_cliente 
             and delta.widmt_cliente_upd != historico.widmt_cliente_upd))
    {%- endif %}
        
        union 

    select * from {{ source('raw_edw', 'ttc_mt_clientes_crm_jemv') }} 
             
    {% if is_incremental() %}   
        where widmt_cliente NOT IN ( (select widmt_cliente from {{ this }}) )    
    {% endif %}

)

select *
from ttc_mt_clientes_crm_jemv