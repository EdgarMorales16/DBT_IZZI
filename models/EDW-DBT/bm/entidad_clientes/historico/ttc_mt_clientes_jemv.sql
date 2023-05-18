{{
    config(
        alias='ttc_mt_clientes_jemv',
        tags=["clientes"],
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key= "widmt_cliente",
    ) 
}}

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

    {# bloque 3: carga incremental#}
    final as 
    ( 
       {{ is_incremental_upd('ttc_mt_clientes','widmt_cliente') }} 
    )

select *
from final
