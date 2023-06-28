{{
    config(
        alias='stg_ttc_mt_clientes', 
        tags=["clientes"],
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key="orig_id",
        on_schema_change='append_new_columns'
    )
}}

with
    {# bloque 1: CTE Objetos#}

    stg_ttc_mt_clientes_crm as (select * from {{ ref("stg_ttc_mt_clientes_crm") }}),
    stg_ttc_mt_clientes_brm as (select * from {{ ref("stg_ttc_mt_clientes_brm") }}),
    stg_ttc_mt_clientes_salesforce as (select * from {{ ref("stg_ttc_mt_clientes_salesforce") }}),

    {# bloque 2: Union all #}

    ttc_mt_clientes as 
    (
        {{ union_all_staging(["stg_ttc_mt_clientes_crm",
                              "stg_ttc_mt_clientes_brm",
                              "stg_ttc_mt_clientes_salesforce"]) }}
    ),

    {# bloque 3: Final #}

    final as 
    (
        select * from ttc_mt_clientes
    )

select {{ dw_control_columns() }}, *
from final