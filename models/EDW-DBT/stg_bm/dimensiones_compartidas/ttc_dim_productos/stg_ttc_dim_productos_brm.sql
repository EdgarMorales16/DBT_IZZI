{{ config(
    materialized = 'table'
) }}

with 

    {# bloque 1: CTE tablas#} 
    ttc_s_prod_int as ( select * from {{ ref('stg_ttc_s_prod_int') }} ),
    ttc_s_prod_int_x as ( select * from {{ ref('stg_ttc_s_prod_int_x') }} ),

    {# bloque 2: CTE filtros#}
    {# bloque 3: CTE Tranformaciones#}
    ttc_dim_productos_brm as (
    select 
        '5'||s_prod_int.widdim_producto widdim_producto,
        5 orig_id,
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
        end categoria,
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
            when s_prod_int.category_cd = 'servicio/unity/internet' and s_prod_int.prod_catg_cd IS NULL then 'INTERNET'
            when s_prod_int.category_cd = 'servicio/unity/video' and s_prod_int.prod_catg_cd IS NULL then 'VIDEO'
            when s_prod_int.category_cd = 'servicio/unity/telefonia' and s_prod_int.prod_catg_cd IS NULL then 'TELEFONIA FIJA'
            when s_prod_int.prod_catg_cd = '/service/ip/stream' then 'VIDEO'
            else upper(s_prod_int.category_cd)
        end categoria_origen,
        s_prod_int.type familia,
        s_prod_int.sub_type_cd tipo,
        null segmento_ofertado,
        null play,
        case 
            when (s_prod_int.prod_catg_cd ='/service/basic_video' 
                and upper(s_prod_int.sub_type_cd) = 'PRINCIPAL CON EQUIPO'
                and (upper(s_prod_int.name)) not like 'EXTENSION%' 
                and (upper(s_prod_int.name)) not like 'COMPLEMENTO%' 
                and (upper(s_prod_int.type)) IN 
                ('PAQUETE BASICO','ACCESO INTERNET','SERVICIO INTERNET','BASICO ANALOGICO','RENTA TELEFONICA'))THEN 'BAS' 
            when (s_prod_int.prod_catg_cd ='/service/ip/cable' 
                and upper(s_prod_int.sub_type_cd) = 'PRINCIPAL CON EQUIPO' 
                and (upper(s_prod_int.name)) not like 'EXTENSION%' 
                and (upper(s_prod_int.name)) not like 'COMPLEMENTO%' 
                and (upper(s_prod_int.type)) 
                IN ('PAQUETE BASICO','ACCESO INTERNET','SERVICIO INTERNET','BASICO ANALOGICO','RENTA TELEFONICA')) THEN 'MB'
            when (s_prod_int.prod_catg_cd ='/service/telephony'
                and upper(s_prod_int.sub_type_cd) = 'PRINCIPAL CON EQUIPO'
                and (upper(s_prod_int.name)) not like 'EXTENSION%' 
                and (upper(s_prod_int.name)) not like 'COMPLEMENTO%' 
                and (upper(s_prod_int.type)) 
                IN ('PAQUETE BASICO','ACCESO INTERNET','SERVICIO INTERNET','BASICO ANALOGICO','RENTA TELEFONICA')) THEN 'TEL'
            when (s_prod_int.prod_catg_cd ='/service/telephony/pbx' 
                and upper(s_prod_int.sub_type_cd) = 'PRINCIPAL CON EQUIPO' 
                and (upper(s_prod_int.name)) not like 'EXTENSION%' 
                and (upper(s_prod_int.name)) not like 'COMPLEMENTO%' 
                and (upper(s_prod_int.type)) 
                IN ('PAQUETE BASICO','ACCESO INTERNET','SERVICIO INTERNET','BASICO ANALOGICO','RENTA TELEFONICA')) THEN 'TEL'
            when (s_prod_int.prod_catg_cd ='/service/video' 
                and upper(s_prod_int.sub_type_cd) = 'PRINCIPAL CON EQUIPO' 
                and (upper(s_prod_int.name)) not like 'EXTENSION%'
                and (upper(s_prod_int.name)) not like 'COMPLEMENTO%' 
                and (upper(s_prod_int.type)) 
                IN ('PAQUETE BASICO','ACCESO INTERNET','SERVICIO INTERNET','BASICO ANALOGICO','RENTA TELEFONICA')) THEN 'BAS'
            when (s_prod_int.sub_type_cd = 'Principal con Equipo' 
                and s_prod_int.type = 'Renta Telefonica'
                and (upper(s_prod_int.name)) not like 'EXTENSION%' 
                and (upper(s_prod_int.name)) not like '%COMPLEMENTO%') THEN 'TEL'
            when (s_prod_int.sub_type_cd = 'Principal con Equipo' 
                and (s_prod_int.type = 'Acceso Internet' OR s_prod_int.type = 'Servicio Internet')
                and (upper(s_prod_int.name)) not like 'EXTENSION%' 
                and (upper(s_prod_int.name)) not like '%COMPLEMENTO%')  THEN 'MB'
            when (s_prod_int.sub_type_cd = 'Principal con Equipo' 
                and s_prod_int.type = 'Paquete Básico' 
                and (upper(s_prod_int.name)) not like 'EXTENSION%' 
                and (upper(s_prod_int.name)) not like '%COMPLEMENTO%' )THEN 'BAS'
                else  null
        end agrupador,
        null producto_homologado,
        s_prod_int.category_cd categoria_producto,
        null id_homologado,
        s_prod_int.part_num,
        null flg_rgu,
        null producto_negocio_finanza,
        null producto_negocio_grupo_cx,
        case 
            when
                upper(s_prod_int.sub_type_cd) = 'PRINCIPAL CON EQUIPO'
                and upper(s_prod_int.category_cd) = 'INTERNET'
                and upper(s_prod_int.prod_catg_cd) = '/SERVICE/IP/MOBILE'
            then  ltrim(replace(upper(s_prod_int.name),'FLEX ',' ')||'M')
        end vel_int,
        null complemento,
        null grupo_tier_video,
        null tipo_cargo,
        null precio,
        null tier_calculado,
        null flg_complemento,
        CASE 
            WHEN
                UPPER(s_prod_int.sub_type_cd) = 'PRINCIPAL CON EQUIPO'
                AND UPPER(s_prod_int.category_cd) = 'INTERNET'
                AND UPPER(s_prod_int.prod_catg_cd) = '/SERVICE/IP/MOBILE'
            THEN 'Y'
            ELSE 'N'
        END FLG_IM,
        NULL CATEGORIA_FACTURACION,
        case 
            when upper(s_prod_int.name) like '%ANUAL%' 
            then 'Y' 
            else 'N' 
        end flg_anual,
        null flg_promocion,
        null duracion_promo,
        case 
            when upper(s_prod_int_x.ATTRIB_51) = 'FTTH' 
                then 'Y' 
                else 'N' 
        END flg_ftth, 
        null flg_consumo,
        null flg_extension,
        null flg_svod,
        null flg_a_la_carta,
        null producto_negocio_an,
        null producto_negocio_an_complemento,
        null oferta

    from ttc_s_prod_int s_prod_int
    left join ttc_s_prod_int_x s_prod_int_x on s_prod_int.row_id = s_prod_int_x.par_row_id
    where 1 = 1
    ),

    {# bloque 4: CTE Final#}
    final as (select * from ttc_dim_productos_brm)

select {{ dw_control_columns() }},
       *
from final
 
