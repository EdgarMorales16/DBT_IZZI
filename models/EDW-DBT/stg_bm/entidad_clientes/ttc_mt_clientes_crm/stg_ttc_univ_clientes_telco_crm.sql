{{
    config(
        materialized="table"
    )
}}

with
    {# bloque 1: CTE tablas#}
    ttc_s_org_ext as ( select *  from {{ ref("stg_ttc_s_org_ext") }} ),   
    ttc_s_org_ext_x as ( select *  from {{ ref("stg_ttc_s_org_ext_x") }} ),
    ttc_s_org_ext_xm as ( select *  from {{ ref("stg_ttc_s_org_ext_xm") }} ), 
    ttc_s_order_item as ( select *  from {{ ref("stg_ttc_s_order_item") }} ),
    ttc_s_order_item_x as ( select *  from {{ ref("stg_ttc_s_order_item_x") }} ),
    ttc_s_order as ( select *  from {{ ref("stg_ttc_s_order") }} ),
    ttc_s_order_x as ( select *  from {{ ref("stg_ttc_s_order_x") }} ),
    ttc_s_addr_per as ( select * from {{ ref("stg_ttc_s_addr_per") }}),
    ttc_s_indust as ( select * from {{ ref("stg_ttc_s_indust") }}),

    {# bloque 2: CTE filtros#}
    u_s_org_ext as (
        select row_id
        from ttc_s_org_ext
        where
            1 = 1
            and (
                ( created::date between '2023-03-23' and '2023-03-23' )
                or ( last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_portabilidad as (
        select soe.row_id
        from ttc_s_org_ext soe
        inner join ttc_s_order so on soe.ROW_ID = so.ACCNT_ID
        inner join ttc_s_order_x sox on so.ROW_ID = sox.PAR_ROW_ID 
        inner join ttc_s_order_item soi on so.ROW_ID = soi.ORDER_ID
        inner join ttc_s_order_item_x soix on soi.ROW_ID = soix.PAR_ROW_ID
        where 1 = 1
        and upper(so.STATUS_CD) in ('COMPLETA','FINALIZADA')
        and upper(sox.ATTRIB_04) = 'PORTABILIDAD'
        and upper(soi.X_CARRIER) <> 'IZZI'
        and soix.X_TELEPHONE_NUMBER IS NOT NULL
        and ( 
            ( so.created::date between '2023-03-23' and '2023-03-23' )
            or ( so.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
        )
    ),
    u_s_org_ext_x as (
        select par_row_id as row_id
        from ttc_s_org_ext_x
        where
            1 = 1
            and (
                ( created::date between '2023-03-23' and '2023-03-23' )
                or ( last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_s_org_ext_xm as (
        select par_row_id as row_id
        from ttc_s_org_ext_xm
        where
            1 = 1
            and (
                ( created::date between '2023-03-23' and '2023-03-23' )
                or ( last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_s_addr_per as (
        select soe.row_id
        from ttc_s_org_ext soe
        inner join ttc_s_addr_per ad on soe.PR_ADDR_ID = ad.row_id
        where
            1 = 1
            and (
                ( ad.created::date between '2023-03-23' and '2023-03-23' )
                or ( ad.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_s_indust as (
        select soe.row_id
        from ttc_s_org_ext soe
        inner join ttc_s_indust ind on soe.PR_INDUST_ID = ind.row_id
        where
            1 = 1
            and (
                ( ind.created::date between '2023-03-23' and '2023-03-23' )
                or ( ind.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),

    {# bloque 3: CTE Tranformaciones#}
    univ_s_clientes as (
        select row_id
        from u_s_org_ext
            union
        select row_id
        from u_portabilidad
            union
        select row_id
        from u_s_org_ext_x
            union 
         select row_id
        from u_s_org_ext_xm
            union 
         select row_id
        from u_s_addr_per
            union 
         select row_id
        from u_s_indust
    ),

    {# bloque 4: CTE Final#}
    final as (select * from univ_s_clientes)

select *
from final
