{{
config(
    materialized = 'incremental',
    unique_key = 'widmt_cliente'
)}}

with
    {# bloque 1: CTE Objetos#}
    stg_ttc_mt_clientes_crm as (select * from {{ ref("stg_ttc_mt_clientes_crm") }}),
    stg_ttc_mt_clientes_brm as (select * from {{ ref("stg_ttc_mt_clientes_brm") }}),

    {# bloque 2: Union all#}
    ttc_mt_clientes as 
    (
        select *
        from stg_ttc_mt_clientes_crm
            union all
        select *
        from stg_ttc_mt_clientes_brm
    ),

    {# bloque 3: carga incremental
    final as 
    (
    select delta.* 
    from ttc_mt_clientes delta,
    {{ this }} historico 
             
    {% if is_incremental() -%}

        where 1 = 1 
        and ( (delta.widmt_cliente = historico.widmt_cliente 
             and delta.widmt_cliente_upd != historico.widmt_cliente_upd))
    {%- endif %}
        
        union 

    select * from ttc_mt_clientes
             
    {% if is_incremental() %}   
        where widmt_cliente NOT IN ( (select widmt_cliente from {{ this }}) )    
    {% endif %}
    )
    #}   

    {# bloque 3: carga incremental#}
    final as 
    ( 
        {{ is_incremental_upd('ttc_mt_clientes','widmt_cliente') }}
    )

select *
from final
