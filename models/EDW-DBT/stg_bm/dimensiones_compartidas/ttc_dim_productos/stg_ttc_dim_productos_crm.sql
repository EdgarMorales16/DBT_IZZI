{{ config(
    materialized = 'table'
) }}

with 

    {# bloque 1: CTE tablas#} 
    ttc_s_prod_int as ( select * from {{ ref('stg_ttc_s_prod_int') }} ),

    {# bloque 2: CTE filtros#}
    {# bloque 3: CTE Tranformaciones#}
    ttc_dim_productos_crm as (
    select 
        s_prod_int.widdim_producto,
        3 orig_id,
        s_prod_int.row_id,
        s_prod_int.name producto,
        nvl(s_prod_int.sub_type_cd,'SIN NIVEL') nivel,
        case 
            when s_prod_int.prod_catg_cd ='/service/basic_video' then 'VIDEO'
            when s_prod_int.prod_catg_cd ='/service/email' then 'EMAIL'
            when s_prod_int.prod_catg_cd ='/service/ip/cable' then 'INTERNET'
            when s_prod_int.prod_catg_cd ='/service/ip/cable/device' then 'EQUIPO INTERNET'
            when s_prod_int.prod_catg_cd ='/service/ip/enlace' then 'ENLACE DEDICADO'
            when s_prod_int.prod_catg_cd ='/service/ip/enlace/device' then 'EQUIPO ENLACE DEDICADO'
            when s_prod_int.prod_catg_cd ='/service/ip/mobile' then 'INTERNET MOVIL'
            when s_prod_int.prod_catg_cd ='/service/iptv' then 'VEO'
            when s_prod_int.prod_catg_cd ='/service/negocio/ip' then 'INTERNET EMP'
            when s_prod_int.prod_catg_cd ='/service/negocio/ip/device' then 'RENTA EQUIPO INTERNET EMP'
            when s_prod_int.prod_catg_cd ='/service/negocio/pbx' then 'TELEFONIA EMP'
            when s_prod_int.prod_catg_cd ='/service/negocio/pbx/device' then 'EQUIPO TELEFONIA EMP'
            when s_prod_int.prod_catg_cd ='/service/negocio/video' then 'VIDEO EMP'
            when s_prod_int.prod_catg_cd ='/service/negocio/video/device' then 'RENTA EQUIPO VIDEO EMP'
            when s_prod_int.prod_catg_cd ='/service/receiver' then 'MICRONODO'
            when s_prod_int.prod_catg_cd ='/service/store/computer' then 'VENTA EQUIPO COMPUTO'
            when s_prod_int.prod_catg_cd ='/service/store/tablet' then 'VENTA TABLET'
            when s_prod_int.prod_catg_cd ='/service/store/video' then 'VENTA TABLET'
            when s_prod_int.prod_catg_cd ='/service/telephony' then 'TELEFONIA'
            when s_prod_int.prod_catg_cd ='/service/telephony/pbx' then 'TELEFONIA'
            when s_prod_int.prod_catg_cd ='/service/telephony/pbx/device' then 'EQUIPO TELEFONIA'
            when s_prod_int.prod_catg_cd ='/service/telephony/voicemail' then 'CORREO DE VOZ'
            when s_prod_int.prod_catg_cd ='/service/video' then 'VIDEO'
            when s_prod_int.prod_catg_cd ='/service/video/iptv' then 'VEO'
            when s_prod_int.category_cd = 'servicio/unity/internet' AND s_prod_int.prod_catg_cd IS NULL then 'INTERNET'
            when s_prod_int.category_cd = 'servicio/unity/video' AND s_prod_int.prod_catg_cd IS NULL then 'VIDEO'
            when s_prod_int.category_cd = 'servicio/unity/telefonia' AND s_prod_int.prod_catg_cd IS NULL then 'TELEFONIA FIJA'
            when s_prod_int.prod_catg_cd = '/service/ip/stream' then 'VIDEO'
            else upper(s_prod_int.category_cd)
            end categoria
    from ttc_s_prod_int s_prod_int
    where 1 = 1
    ),

    {# bloque 4: CTE Final#}
    final as (select * from ttc_dim_productos_crm)

select {{ dw_control_columns() }},
       *
from final

