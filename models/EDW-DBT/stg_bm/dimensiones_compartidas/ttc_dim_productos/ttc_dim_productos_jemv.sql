{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key= "widdim_producto",
        merge_exclude_columns = 'categoria'
    ) 
}}

with
    {# bloque 1: CTE Objetos#}
    ttc_dim_productos_crm as (select * from {{ ref("stg_ttc_dim_productos_crm") }}),
    ttc_dim_productos_brm as (select * from {{ ref("stg_ttc_dim_productos_brm") }}),
    

    {# bloque 2: Union all#}
    ttc_dim_productos as 
    (
        select *
        from ttc_dim_productos_crm
            union all
        select *
        from ttc_dim_productos_brm
    ),  

    {# bloque 3: carga incremental#}
    final as 
    ( 
        select * from ttc_dim_productos
    )

select *
from final