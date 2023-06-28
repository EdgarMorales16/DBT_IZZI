{{
    config(
        alias='stg_ttc_mt_clientes_brm_attb_calculado',
        tags=["clientes"],
        materialized="table"
    )
}}

with 
    
    {# bloque 1: CTE tablas#}                               
    ttc_univ_clientes_telco_brm as ( select *  from {{ ref("stg_ttc_univ_clientes_telco_brm") }} ),
    ttc_account_t as ( select *  from {{ ref("stg_ttc_account_t") }} ),
    ttc_profile_t as ( select *  from {{ ref("stg_ttc_profile_t") }} ),
    ttc_cv_profile_account_t as ( select *  from {{ ref("stg_ttc_cv_profile_account_t") }} ),
    ttc_billinfo_t as ( select *  from {{ ref("stg_ttc_billinfo_t") }} ),
    ttc_collections_scenario_t as ( select *  from {{ ref("stg_ttc_collections_scenario_t") }} ),
    ttc_s_org_ext as ( select *  from {{ ref("stg_ttc_s_org_ext") }} ),
    ttc_s_addr_per as ( select *  from {{ ref("stg_ttc_s_addr_per") }} ),
    ttc_dim_region as ( select *  from {{ ref("stg_ttc_dim_region") }} ),
    ttc_account_phones_t as ( select *  from {{ ref("stg_ttc_account_phones_t") }} ),
    ttc_account_phones_t_2 as ( select *  from {{ ref("stg_ttc_account_phones_t") }} ),

    {# bloque 2: CTE filtros#}

    tmp_ttc_account_phones_t as (
        select ap2.obj_id0,
               ap.phone tel_fijo,
               ap2.phone tel_cel
        from ttc_account_phones_t ap
        inner join ttc_account_phones_t_2 ap2 on ap.obj_id0 = ap2.obj_id0
            and (
                ap.phone is not null 
                    or ap2.phone is not null
                )
        where 1 = 1
        and ap.rec_id = 1
        and ap.rec_id2 = 2
        and ap2.rec_id = 5
    ),
    tmp_ttc_dim_region as (
        select *
        from ttc_dim_region
        where 1=1
        and orig_id = 5
    ),

    {# bloque 3: CTE Tranformaciones#}
    stg_ttc_mt_attb_calculados as (
        select 
            account_t.poid_id0,
            max(pat.class)                          class,
            max(pat.subclass)                       subclass,
            max(pat.ind_unity)                      ind_unity,
            max(pat.rpt_cuenta)                     rpt_cuenta,
            max(pat.company)                        company,
            max(billinfo_t.actg_cycle_dom)          actg_cycle_dom,
            max(billinfo_t.pay_type)                pay_type,
            min(cst.entry_t)                        entry_t,
            nvl(dr.id_trans,'GRL')                  x_hub,
            nvl(dr.codigo_rpt ,s_addr_per.x_tt_rpt_codigo) x_tt_rpt_codigo,
            apt.tel_fijo,
            apt.tel_cel              
        from ttc_account_t account_t
        left outer join ttc_profile_t profile_t on account_t.poid_id0 = profile_t.account_obj_id0
        left outer join ttc_cv_profile_account_t pat on account_t.poid_id0 = pat.obj_id0
        left outer join ttc_billinfo_t billinfo_t on account_t.poid_id0 = billinfo_t.account_obj_id0
        left outer join ttc_collections_scenario_t cst on billinfo_t.scenario_obj_id0 = cst.poid_id0
            and billinfo_t.poid_id0 = cst.billinfo_obj_id0
        left outer join ttc_s_org_ext s_org_ext on account_t.account_no = s_org_ext.name
        left outer join ttc_s_addr_per s_addr_per on s_org_ext.pr_addr_id = s_addr_per.row_id
        left outer join tmp_ttc_dim_region dr on s_addr_per.x_tt_rpt_codigo = dr.codigo_rpt
            and s_addr_per.x_hub = dr.id_trans
        left outer join tmp_ttc_account_phones_t apt on account_t.poid_id0 = apt.obj_id0
        where 1=1
        and account_t.poid_id0 in (
            select row_id
            from ttc_univ_clientes_telco_brm            
        ) 
        group by 
            account_t.poid_id0,
            nvl(dr.id_trans,'GRL'),
            nvl(dr.codigo_rpt ,s_addr_per.x_tt_rpt_codigo),
            apt.tel_fijo,
            apt.tel_cel 
    ),

{# bloque 4: CTE Final#}
    final as (select * from stg_ttc_mt_attb_calculados)

select {{ dw_control_columns() }}, 
       *
from final

