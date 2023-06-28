{{
    config(
        alias='stg_ttc_univ_clientes_salesforce',
        tags=["clientes"],
        materialized="table"
    )
}}

with
    {# bloque 1: CTE tablas#}
    salesforce_account as (select * from {{ ref("stg_salesforce_account") }}),
    salesforce_asset as (select * from {{ ref("stg_salesforce_asset") }}),
    
    {# bloque 2: CTE filtros#}
    u_account as (
        select id
        from salesforce_account
        where 1 = 1
        and upper(nvl(type,'CONSUMER')) = 'CONSUMER'
        and ( 
                ( createddate::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                        and {{ fnc_var_to_date(var("var_to_date")) }} )
                or ( systemmodstamp::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                           and {{ fnc_var_to_date(var("var_to_date")) }} )
                or ( lastmodifieddate::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                           and {{ fnc_var_to_date(var("var_to_date")) }} )
            )
    ),
    u_asset as (    
        select distinct(account.id) id
        from salesforce_asset asset
        inner join salesforce_account account on asset.VLOCITY_CMT__BILLINGACCOUNTID__C = account.ID
        where 1 = 1
        and ( 
                ( asset.createddate::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                          and {{ fnc_var_to_date(var("var_to_date")) }} )
                or ( asset.systemmodstamp::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                              and {{ fnc_var_to_date(var("var_to_date")) }} )
                or ( asset.lastmodifieddate::date between {{ fnc_var_from_date(var("var_from_date")) }} 
                                                and {{ fnc_var_to_date(var("var_to_date")) }} )
                    )
    ),
    

    {# bloque 3: CTE Tranformaciones#}
    univ_clientes_salesforce as (
        select id row_id
        from u_account
        union
        select id
        from u_asset
    ),

    {# bloque 4: CTE Final#}
    final as (select * from univ_clientes_salesforce)

select *
from final
