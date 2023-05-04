{#
{{
    config(
        materialized="table"
    )
}}
#}

with    
    {# bloque 1: CTE tablas#} 
    {# CRM #} 
    ttc_s_org_ext as ( select *  from {{ ref("stg_ttc_s_org_ext") }} ),   
    ttc_s_org_ext_x as ( select *  from {{ ref("stg_ttc_s_org_ext_x") }} ),
    ttc_s_org_ext_xm as ( select *  from {{ ref("stg_ttc_s_org_ext_xm") }} ), 
    ttc_s_order_item as ( select *  from {{ ref("stg_ttc_s_order_item") }} ),
    ttc_s_order_item_x as ( select *  from {{ ref("stg_ttc_s_order_item_x") }} ),
    ttc_s_order as ( select *  from {{ ref("stg_ttc_s_order") }} ),
    ttc_s_order_x as ( select *  from {{ ref("stg_ttc_s_order_x") }} ),
    ttc_s_addr_per as ( select * from {{ ref("stg_ttc_s_addr_per") }}),
    ttc_s_con_addr as ( select * from {{ ref("stg_ttc_s_con_addr") }}),
    ttc_s_indust as ( select * from {{ ref("stg_ttc_s_indust") }}),
    ttc_s_inv_prof as ( select * from {{ ref("stg_ttc_s_inv_prof") }}),
    ttc_cx_tt_admision as ( select * from {{ ref("stg_ttc_cx_tt_admision") }}),
    ttc_cx_bz_cls_sgt as ( select * from {{ ref("stg_ttc_cx_bz_cls_sgt") }}),    
    {# BRM #}
    ttc_collections_scenario_t as ( select * from {{ ref("stg_ttc_collections_scenario_t") }}),
    ttc_account_t as ( select * from {{ ref("stg_ttc_account_t") }}),
    ttc_billinfo_t as ( select * from {{ ref("stg_ttc_billinfo_t") }}),
    ttc_payinfo_t as ( select * from {{ ref("stg_ttc_payinfo_t") }}),
    ttc_payinfo_cc_t as ( select * from {{ ref("stg_ttc_payinfo_cc_t") }}),

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
    u_s_org_ext_x as 
    (
        select par_row_id as row_id
        from ttc_s_org_ext_x
        where
            1 = 1
            and (
                ( created::date between '2023-03-23' and '2023-03-23' )
                or ( last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_s_org_ext_xm as 
    (
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
    u_s_inv_prof as (
        select sip.accnt_id as row_id
        from ttc_s_inv_prof sip
        where
            1 = 1
            and (
                ( sip.created::date between '2023-03-23' and '2023-03-23' )
                or ( sip.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_cx_tt_admision as (
        select soe.row_id
        from ttc_s_org_ext soe
        inner join ttc_cx_tt_admision cx on soe.name = cx.tt_cuenta
        where
            1 = 1
            and (
                ( cx.created::date between '2023-03-23' and '2023-03-23' )
                or ( cx.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_cx_tt_admision as (
        select soe.row_id
        from ttc_s_org_ext soe
        inner join ttc_cx_tt_admision cx on soe.name = cx.tt_cuenta
        where
            1 = 1
            and (
                ( cx.created::date between '2023-03-23' and '2023-03-23' )
                or ( cx.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_collections_scenario_t as (
        select soe.row_id
        from ttc_s_org_ext soe
        inner join ttc_account_t acc on soe.name = acc.account_no
        inner join ttc_collections_scenario_t cst on acc.POID_ID0 = cst.ACCOUNT_OBJ_ID0
        where
            1 = 1
            and (
                ( cst.created_t::date between '2023-03-23' and '2023-03-23' )
                or ( cst.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_payinfo_t as (
        select soe.row_id
        from ttc_s_org_ext soe
        inner join ttc_account_t  acc on soe.NAME = acc.ACCOUNT_NO 
        inner join ttc_billinfo_t binfot on acc.POID_ID0 = binfot.ACCOUNT_OBJ_ID0
        inner join ttc_payinfo_t pit on binfot.PAYINFO_OBJ_ID0 = pit.POID_ID0
        inner join ttc_payinfo_cc_t picc on pit.POID_ID0 = picc.OBJ_ID0
        where
            1 = 1
            and (
                ( pit.created_t::date between '2023-03-23' and '2023-03-23' )
                or ( pit.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
                or ( picc.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_cx_bz_cls_sgt as (
        select soe.row_id
        from ttc_s_org_ext soe
        inner join ttc_cx_bz_cls_sgt cx_bz on soe.NAME = cx_bz.CLIENTE_ID
        where
            1 = 1
            and (
                ( cx_bz.created::date between '2023-03-23' and '2023-03-23' )
                or ( cx_bz.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
            )
    ),
    u_s_con_addr as (
        select soe.row_id
        from ttc_s_org_ext soe
        inner join ttc_s_con_addr c_addr on soe.PR_ADDR_ID = c_addr.ADDR_PER_ID
                                        and soe.ROW_ID = c_addr.ACCNT_ID
        where
            1 = 1
            and (
                ( c_addr.created::date between '2023-03-23' and '2023-03-23' )
                or ( c_addr.last_upd_gg::date between '2023-03-23' and '2023-03-23' )
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
            union
         select row_id
         from u_s_inv_prof
            union 
         select row_id
        from u_cx_tt_admision
            union 
         select row_id
        from u_collections_scenario_t
            union 
         select row_id
        from u_payinfo_t
            union 
         select row_id
        from u_cx_bz_cls_sgt
            union
         select row_id
        from u_s_con_addr
    ),

    {# bloque 4: CTE Final#}
    final as (select * from univ_s_clientes)

select *
from final
