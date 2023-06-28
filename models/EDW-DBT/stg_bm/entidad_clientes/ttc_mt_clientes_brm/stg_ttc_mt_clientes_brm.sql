{{
    config(
        alias='stg_ttc_mt_clientes_brm',
        tags=["clientes"],
        materialized="table"
    )
}}


with 
    
    {# bloque 1: CTE tablas#}                               
    ttc_univ_clientes_telco_brm as ( select *  from {{ ref("stg_ttc_univ_clientes_telco_brm") }} ),
    ttc_account_t as ( select *  from {{ ref("stg_ttc_account_t") }} ),
    ttc_mt_clientes_brm_attb_calculado as ( select *  from {{ ref("stg_ttc_mt_clientes_brm_attb_calculado") }} ), 
    ttc_dim_region as ( select *  from {{ ref("stg_ttc_dim_region") }} ),
    ttc_account_nameinfo_t as (select * from {{ ref('stg_ttc_account_nameinfo_t') }}),

    {# bloque 2: CTE filtros#}
    tmp_ttc_dim_region as (
        select *
        from ttc_dim_region
        where 1 = 1
        and orig_id = 5
    ),
    tmp_ttc_account_nameinfo_t as(
        select *
        from ttc_account_nameinfo_t
        where 1=1
        and rec_id = 1
    ),

    {# bloque 3: CTE Tranformaciones#}
    ttc_mt_clientes_telco_brm as 
    (
        select 
            nvl( tmp_ttc_dim_region.widdim_region, {{ fnc_wid('3','1') }} ) widdim_region,
            account_t.widmt_cliente,
            to_char(account_t.poid_id0) row_id,
            5 orig_id,
            account_t.account_no cliente,
            tmp_ttc_dim_region.id_trans,
            tmp_ttc_dim_region.codigo_rpt,
            trim (
                  case 
                      when attb.class IS NULL 
                          then 'SIN TIPO CLIENTE'
                          else UPPER(attb.class) 
                      end 
                  )  tipo_cliente,
            trim (
                  case 
                      when attb.subclass IS NULL 
                          then 'SIN SUBTIPO CLIENTE' 
                          else UPPER(attb.subclass) 
                  END 
                  ) subtipo_cte,
            trim( UPPER(
                    case 
                        when account_t.status =10100 then 'ACTIVO'
                        when account_t.status =10102 then 'INACTIVO'
                        when account_t.status =10103 then 'CERRADO'
                        when account_t.status =10105 then 'OTRO'
                        when account_t.status =0 then 'INDEFINIDO'
                        when account_t.status IS NULL then 'SIN_ESTATUS' 
                        else 'NO MATCH' end
                        )
                ) estatus_cte,
            case 
                when attb.pay_type =10001 then 'EFECTIVO'
                when attb.pay_type =10003 then 'TARJETA CREDITO'
                when attb.pay_type =10007 then 'SUBORDINADO'
                when attb.pay_type = 10009  then '10009'
                when attb.pay_type = 0  then 'INDEFINIDO'
                when attb.pay_type IS NULL then 'SIN METODO PAGO'
                else 'NO MATCH' 
            end metodo_pago,
            account_t.created_t fecha_creacion,
            account_t.mod_t fecha_actualizacion,
            account_t.created_t fecha_instalacion,
            account_t.account_no cta_facturacion,
            TRIM(UPPER(
                    CASE 
                        when  account_t.status =10100 then 'ACTIVO'
                        when  account_t.status=10102 then 'INACTIVO'
                        when  account_t.status=10103 then 'CERRADO'
                        when  account_t.status=10105 then 'OTRO'
                        when  account_t.status=0 then 'INDEFINIDO'
                        when  account_t.status IS NULL then 'SIN ESTATUS' 
                        else 'NO MATCH'
                    end)
                ) estatus_cta_fac,
            ant.first_name ||' '|| ant.middle_name nombres,
            account_t.vat_cert rfc,
            nvl(attb.actg_cycle_dom, 0) ciclo_facturacion,
            attb.tel_fijo,
            attb.tel_cel,
            'SIN PERFIL FACTURACION' perfil_facturacion,
            'SIN CATEGORIA' categoria,
            UPPER(NVL(TO_CHAR(
                DECODE(attb.ind_unity,
                        0, 'Legacy', 
                        1, 'IZZI', 
                        2, 'Lego', 
                        3, 'WIZZ PLUS', 
                        4, 'WIZZ',
                        'Desconocido'
                        )
                    ), 'SIN_OFERTA_COMERCIAL'
                )) oferta_comercial,
            'SIN CANAL INGRESO' canal_ingreso,
            attb.company compania,
            case
                when UPPER (TRIM (ant.first_name||' '|| ant.middle_name)) = 'CORPORACION NOVAVISION'
                    and length (account_t.account_no) = 12
                then 'Y'
                else 'N'
            end flg_sky,
            to_char(1) giro_negocio,
            attb.entry_t fecha_collections,
            case
                when attb.entry_t IS NULL 
                    then 'N'
                    else 'Y' 
            end flg_collections            
        from ttc_account_t account_t  
        left join tmp_ttc_account_nameinfo_t ant on account_t.poid_id0 = ant.obj_id0
        left join ttc_mt_clientes_brm_attb_calculado attb on account_t.poid_id0 = attb.poid_id0
        left join tmp_ttc_dim_region on (
                case  
                    when nvl(UPPER(attb.rpt_cuenta), 0) != nvl(UPPER(attb.x_tt_rpt_codigo),0) 
                        then 'GRL'
                        else UPPER(attb.x_hub)
                end
            ) = UPPER(tmp_ttc_dim_region.id_trans)
            AND UPPER(attb.rpt_cuenta) = UPPER(tmp_ttc_dim_region.codigo_rpt) 
        where 1 = 1
        and account_t.poid_id0 in (
            select row_id
            from ttc_univ_clientes_telco_brm
        )
    ),


    {# bloque 4: CTE Final#}
    final as (select * from ttc_mt_clientes_telco_brm)

select {{ dw_control_columns() }}, 
       *
from final

