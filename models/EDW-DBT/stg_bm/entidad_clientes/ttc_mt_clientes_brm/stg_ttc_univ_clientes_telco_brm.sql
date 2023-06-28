{{
    config(
        alias='stg_ttc_univ_clientes_telco_brm',
        tags=["clientes"],
        materialized="table"
    )
}}

with
    {# bloque 1: CTE tablas#}
    ttc_account_t as (select * from {{ ref("stg_ttc_account_t") }}),
    ttc_profile_t as (select * from {{ ref("stg_ttc_profile_t") }}),
    ttc_account_nameinfo_t as (select * from {{ ref("stg_ttc_account_nameinfo_t") }}),
    ttc_account_phones_t as (select * from {{ ref("stg_ttc_account_phones_t") }}),
    ttc_billinfo_t as (select * from {{ ref("stg_ttc_billinfo_t") }}),
    ttc_collections_scenario_t as (select * from {{ ref("stg_ttc_collections_scenario_t") }}),
    ttc_cv_profile_account_t as (select * from {{ ref("stg_ttc_cv_profile_account_t") }}),
    
    {# bloque 2: CTE filtros#}
    u_account_t as (
        select poid_id0 row_id
        from ttc_account_t
        where 1 = 1
        and ( 
                ( created_t::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                      and {{ fnc_var_to_date(var("var_to_date")) }} )
                or ( last_upd_gg::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                                and {{ fnc_var_to_date(var("var_to_date")) }} )
            )
    ),
    u_profile_t as (
        select ttc_profile_t.account_obj_id0 row_id
        from ttc_profile_t
        inner join ttc_cv_profile_account_t on ttc_profile_t.poid_id0 = ttc_cv_profile_account_t.obj_id0
        where 1 = 1
        and ttc_cv_profile_account_t.last_upd_gg::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                                           and {{ fnc_var_to_date(var("var_to_date")) }} 
    ),
    u_account_nameinfo_t as (
        select obj_id0 row_id
        from ttc_account_nameinfo_t
        where 1 = 1
        and last_upd_gg::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                  and {{ fnc_var_to_date(var("var_to_date")) }} 
    ),
    u_account_phones_t as (
        select obj_id0 row_id
        from ttc_account_phones_t
        where 1 = 1
        and last_upd_gg::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                  and {{ fnc_var_to_date(var("var_to_date")) }} 
    ),
    u_billinfo_t as (
        select account_obj_id0 row_id
        from ttc_billinfo_t
        where 1 = 1
        and ( 
                ( created_t::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                      and {{ fnc_var_to_date(var("var_to_date")) }} )
                or ( last_upd_gg::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                                and {{ fnc_var_to_date(var("var_to_date")) }} )
            )
    ),
    U_collections_scenario_t as (
        select account_obj_id0 row_id
        from ttc_collections_scenario_t
        where 1 = 1
        and ( 
                ( created_t::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                      and {{ fnc_var_to_date(var("var_to_date")) }} )
                or ( last_upd_gg::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                                and {{ fnc_var_to_date(var("var_to_date")) }} )
            )
    ),
    
    {# bloque 3: CTE Tranformaciones#}
    univ_clientes_brm as (
        select row_id
        from u_account_t
            union
        select row_id
        from u_profile_t
            union
        select row_id
        from u_account_nameinfo_t
            union
        select row_id
        from u_account_phones_t 
            union
        select row_id
        from u_billinfo_t
            union
        select row_id
        from U_collections_scenario_t
    ),

    {# bloque 4: CTE Final#}
    final as (select * from univ_clientes_brm)

select *
from final
