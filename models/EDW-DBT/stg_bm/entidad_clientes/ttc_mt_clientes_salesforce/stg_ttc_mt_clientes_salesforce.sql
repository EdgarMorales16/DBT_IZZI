{{
    config(
        alias='stg_ttc_mt_clientes_salesforce',
        tags=["clientes"],
        materialized="table"
    )
}}
with 
    
    {# bloque 1: CTE tablas#} 
    ttc_univ_clientes_salesforce as ( select *  from {{ ref("stg_ttc_univ_clientes_salesforce") }} ),
    salesforce_account as ( select * from {{ ref('stg_salesforce_account') }} ),
    salesforce_asset as ( select * from {{ ref('stg_salesforce_asset') }} ),
    ttc_s_org_ext as ( select * from {{ ref('stg_ttc_s_org_ext') }} ),
    ttc_s_addr_per as ( select * from {{ ref('stg_ttc_s_addr_per') }} ),
    ttc_dim_region as (select * from {{ ref("stg_ttc_dim_region") }}),
    ttc_ctl_hub_dir as (select * from {{ ref("stg_ttc_ctl_hub_dir") }}),

    {# bloque 2: CTE filtros#}
    tmp_ttc_dim_region as (
        select * from ttc_dim_region where 1 = 1 and orig_id = 16
    ),
    tmp_salesforce_asset as (
        select *
        from (
            select 
                asset.vlocity_cmt__billingaccountid__c row_id,
                CASE 
                    WHEN asset.operador__c IS NULL 
                    THEN 'altan' 
                    ELSE asset.operador__c 
                END operador_movil,                   
                ROW_NUMBER() OVER (PARTITION BY asset.vlocity_cmt__billingaccountid__c 
                                ORDER BY CASE WHEN asset.operador__c IS NULL 
                                                    THEN 'altan' 
                                                    ELSE asset.operador__c 
                                                END DESC) orden
            from salesforce_asset asset
            where 1=1 and status = 'Activo'
        )
        where 1=1
        and orden = 1
    ),

    {# bloque 3: CTE Tranformaciones#}
    ttc_mt_clientes_salesforce as  
    (
        select 
            account.widmt_cliente,
            account.id row_id,
            16 orig_id,
            account.rpt__c codigo_rpt,
            account.tipo_de_cliente__c tipo_cliente,
            account.subtipo_de_cliente__c subtipo_cte,
            case
                when UPPER(account.vlocity_cmt__Status__c) = 'ACTIVE' 
                then 'Activo'
                else account.vlocity_cmt__Status__c
            end estatus_cte,
            account.createddate fecha_creacion,
            account.lastmodifieddate fecha_actualizacion,
            account.name nombres,
            account.name nombre_comercial,
            account.vlocity_cmt__billingemailaddress__c email,
            account.phone tel_fijo,
            'N' flg_sky,
            account.parentid cuenta_padre,
            account.siebel_number__c cliente_crm,
            tmp_salesforce_asset.operador_movil
        from salesforce_account account
        left join ttc_s_org_ext s_org_ext on account.siebel_number__c = s_org_ext.name
        left join ttc_s_addr_per s_addr_per on s_org_ext.pr_addr_id = s_addr_per.row_id
        left join ttc_ctl_hub_dir on s_addr_per.x_hub = ttc_ctl_hub_dir.x_hub
            and s_addr_per.x_tt_rpt_codigo = ttc_ctl_hub_dir.codigo_rpt
        left join tmp_ttc_dim_region on UPPER(s_addr_per.x_hub) = tmp_ttc_dim_region.id_trans  
            and  s_addr_per.x_tt_rpt_codigo =  tmp_ttc_dim_region.codigo_rpt 
        left join tmp_salesforce_asset on account.id = tmp_salesforce_asset.row_id
        where 1 = 1
        and account.id in (
            select row_id 
            from ttc_univ_clientes_salesforce  
        )
    ),

    {# bloque 4: CTE Final#}
    final as (select * from ttc_mt_clientes_salesforce)

select {{ dw_control_columns() }}, 
       *
from final