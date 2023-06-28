{{
    config(
        alias='ttc_mt_clientes_h',
        tags=["clientes"],
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="widmt_cliente",
        on_schema_change='append_new_columns',
        merge_exclude_columns=["dw_fecha_creacion","dw_creado_por"]
    )
}}

with
    {# bloque 1: CTE Objetos#}
    stg_ttc_mt_ordenes as (select * from {{ ref("stg_ttc_mt_clientes") }}),
    {# bloque 3: Final#}
    final as 
    (
        {{ union_all_staging(["stg_ttc_mt_clientes"]) }}
            union all
        {{dw_registro_no_match (this.identifier)}}
        
    )

    select {{ dw_control_columns() }}, *
from final