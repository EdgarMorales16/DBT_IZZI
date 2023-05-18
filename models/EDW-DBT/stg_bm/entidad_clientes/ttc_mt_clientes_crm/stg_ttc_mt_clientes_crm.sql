{{ config(
    materialized = 'table'
) }}

with 
    
    {# bloque 1: CTE tablas#} 
    ttc_univ_clientes_telco_crm as ( select *  from {{ ref("stg_ttc_univ_clientes_telco_crm") }} ),
    ttc_s_org_ext as ( select *  from {{ ref("stg_ttc_s_org_ext") }} ),
    ttc_s_org_ext_x as ( select *  from {{ ref("stg_ttc_s_org_ext_x") }} ),
    ttc_s_addr_per as ( select *  from {{ ref("stg_ttc_s_addr_per") }} ),
    ttc_s_con_addr as ( select *  from {{ ref("stg_ttc_s_con_addr") }} ),
    ttc_ctl_hub_dir as (select * from {{ ref("stg_ttc_ctl_hub_dir") }}),
    ttc_dim_region as (select * from {{ ref("stg_ttc_dim_region") }}),
    ttc_dim_region_operativa as (select * from {{ ref("stg_ttc_dim_region_operativa") }}),
    ttc_s_pri_lst as ( select * from {{ ref("stg_ttc_s_pri_lst") }}),
    ttc_s_lst_of_val as ( select * from {{ ref("stg_ttc_s_lst_of_val") }}),
    ttc_s_indust as ( select * from {{ ref("stg_ttc_s_indust") }}),
    ttc_cat_domiciliacion_clientes as ( select * from {{ ref("stg_ttc_cat_domiciliacion_clientes") }}),

    {# bloque 2: CTE filtros#}

    tmp_ttc_dim_region as (
        select * from ttc_dim_region where 1 = 1 and orig_id = 3
    ),

    tmp_ttc_dim_region_operativa as (
        select * from ttc_dim_region_operativa where 1 = 1 and orig_id = 3
    ),

    tmp_ttc_s_lst_of_val as (
        select * from ttc_s_lst_of_val where 1=1 and upper(ttc_s_lst_of_val.TYPE) = 'TT_LST_RPT'
    ),

    {# bloque 3: CTE Tranformaciones#}
    ttc_mt_clientes_telco_crm as 
    (
        select 
            nvl(case 
                    when ( s_addr_per.X_HUB is null and s_addr_per.X_TT_RPT_CODIGO is null) then '1'
                    when ( {{ fnc_regexp_like('s_addr_per.X_HUB') }} ) then '1' 
                    when ( s_addr_per.X_HUB = ttc_ctl_hub_dir.X_HUB 
                        and s_addr_per.X_TT_RPT_CODIGO = ttc_ctl_hub_dir.CODIGO_RPT)  then '1' 
                    WHEN length(s_addr_per.X_HUB) IN (2,3) then TO_CHAR(ttc_dim_region.WIDDIM_REGION) 
                    else '19'
            end,'19') WIDDIM_REGION,
            NVL(ttc_dim_region_operativa.WIDDIM_REGION_OPERATIVA,'178') WIDDIM_REGION_OPERATIVA,
            NVL(s_con_addr.WIDMT_DIRECCION, {{ fnc_wid('3','1') }} ) WIDMT_DIRECCION,
            NVL(S_PRI_LST.WIDCAT_LISTA_PRECIO, {{ fnc_wid('3','1') }} )  WIDCAT_LISTA_PRECIO,
            NULL WIDDIM_PAQUETE,
            NULL WIDDIM_PAQUETE_COMERCIAL_IFT,
            NULL WIDDIM_SKU,
            '3'||TRUNC(S_ORG_EXT.NAME)||S_ORG_EXT.CUST_STAT_CD WIDMT_CLIENTE_UPD,
            NVL('3'||S_ORG_EXT.WIDMT_CLIENTE, {{ fnc_wid('3','1') }} ) WIDMT_CLIENTE,
            3||S_ORG_EXT.ROW_ID ROW_ID,
            3 ORIG_ID,
            NVL(S_ORG_EXT.NAME,1) CLIENTE,
            S_ADDR_PER.X_HUB ID_TRANS,
            S_ADDR_PER.X_TT_RPT_CODIGO CODIGO_RPT ,
            S_LST_OF_VAL.VAL RPT_CUENTA,
            DECODE(S_ORG_EXT.OU_TYPE_CD,NULL,'SIN TIPO CLIENTE', S_ORG_EXT.OU_TYPE_CD)  TIPO_CLIENTE,
            DECODE(S_ORG_EXT.EXPERTISE_CD,NULL,'SIN SUBTIPO CLIENTE', UPPER( {{ fnc_translate('S_ORG_EXT.EXPERTISE_CD') }} )) SUBTIPO_CTE,
            DECODE(S_ORG_EXT.CUST_STAT_CD,NULL,'SIN ESTATUS', UPPER( {{ fnc_translate('S_ORG_EXT.CUST_STAT_CD') }} )) ESTATUS_CTE,
            NULL METODO_PAGO,
            S_ORG_EXT.CREATED FECHA_CREACION,
            S_ORG_EXT.LAST_UPD FECHA_ACTUALIZACION,
            S_ORG_EXT.CUST_SINCE_DT FECHA_INSTALACION,
            NULL PLAY,
            NVL(S_ORG_EXT.NAME,1) CTA_FACTURACION,
            DECODE(S_ORG_EXT.CUST_STAT_CD,NULL,'SIN ESTATUS', UPPER( {{ fnc_translate('S_ORG_EXT.CUST_STAT_CD')  }} )) ESTATUS_CTA_FAC,
            S_ORG_EXT.X_PRIMER_NOMBRE||' '|| S_ORG_EXT.X_SEGUNDO_NOMBRE NOMBRES,
            S_ORG_EXT.X_APELL_PATERNO AP_PATERNO,
            S_ORG_EXT.X_APELL_MATERNO AP_MATERNO,
            S_ORG_EXT.X_COMERC_NOMBRE NOMBRE_COMERCIAL,
            S_ORG_EXT.DUNS_NUM RFC,
            NVL(S_ORG_EXT_X.X_TT_CICLO_FACT,0) CICLO_FACTURACION,
            NULL EMAIL, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL TEL_FIJO, -- PUSH -- STG_TTC_MT_CLIENTES_TEL_FIJO
            NULL TEL_CEL, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL TEL_CABLE,
            S_ORG_EXT_X.X_TT_FLG_CTE_MIGRADO FLG_CTE_MIGRADO,
            S_ORG_EXT_X.X_TT_CTA_MIGRADA CTA_MIGRADA,
            S_ORG_EXT.MAIN_PH_NUM TEL_PRINCIPAL,
            NULL PERFIL_FACTURACION, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            (CASE 
                    WHEN (S_ORG_EXT.X_CALIF_CLIENTE < 4.5) THEN 'ROJO' 
                    WHEN (S_ORG_EXT.X_CALIF_CLIENTE >= 4.5 AND S_ORG_EXT.X_CALIF_CLIENTE <6.5) THEN 'AMARILLO'
                    ELSE 'VERDE' 
                END) CATEGORIA,
            S_ORG_EXT.SUPRESS_CALL_FLG ACEPTA_LLAMADA,
            NULL ACEPTA_SMS, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL ACEPTA_WHATSAPP, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL TARJETA_DEBITO, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL TARJETA_CREDITO, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL FECHA_EXP_TARJETA_CREDITO, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL OFERTA_COMERCIAL, -- PUSH -- STG_TTC_MT_CLIENTES_OFERTA_COMERCIA
            UPPER(NVL(S_ORG_EXT.ROUTE, 'SIN CANAL INGRESO')) CANAL_INGRESO, 
            S_ORG_EXT.X_COMERC_SUC SUCURSAL,
            S_ORG_EXT_X.X_TT_DIA_VENCIMIENTO DIA_VENCIMIENTO_PAGO,
            NULL FECHA_MIGRACION,
            NULL COMPANIA,
            S_ORG_EXT.X_TT_ADVANCE_PYMT_AMT CANTIDAD_A_PAGAR,
            NVL(S_ORG_EXT.X_TT_ADVANCE_PYMT_STATUS,'SIN SITUACION ANTICIPO') SITUACION_ANTICIPO,
            NVL(UPPER(S_INDUST.NAME),'SIN_LINEA_NEGOCIO') LINEA_NEGOCIO,
            NVL(UPPER(S_ORG_EXT.SVC_CVRG_STAT_CD),'SIN_ESQUEMA_INSTALACION') ESQUEMA_INSTALACION,
            NVL(UPPER(S_ORG_EXT.ADL_STATUS),'SIN_SUBESTADO') SUBESTADO,
            NVL(UPPER(S_ORG_EXT_X.X_TT_TYPE_CONTR),'SIN_TIPO_CONTRIBUYENTE') TIPO_CONTRIBUYENTE,
            S_ORG_EXT.VISIT_FREQUENCY NO_HABITACIONES,
            NULL SITUACION_ADMISION, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL FECHA_ADMISION, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            'N' FLG_SKY,
            NULL CTA_CLIENTE,
            CASE 
                WHEN S_ORG_EXT_X.X_TT_CTA_MIGRADA IS NOT NULL AND S_ORG_EXT.NAME < '50000000' THEN 'MIGRACION POR GOTEO'
                WHEN S_ORG_EXT_X.X_TT_CTA_MIGRADA IS NOT NULL AND S_ORG_EXT.NAME >= '50000000' THEN 'MIGRACION MASIVO'   
                ELSE 'NATIVO TELCO' 
            END  PLATAFORMA_ORIGEN,
            S_ORG_EXT.TLR_INTG_RET_CD CREDENCIAL_ELECTOR,
            S_ORG_EXT.BRANCH_TYPE_CD GIRO_NEGOCIO, 
            NULL FECHA_COLLECTION, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL FLG_COLLECTIONS, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL FLG_EXENTO_COLLECTION,
            S_ORG_EXT_X.X_TT_PREPAGO_FLG FLG_PREPAGO,
            S_ORG_EXT_X.X_TT_TIPO_PREPAGO TIPO_PREPAGO, 
            NULL FECHA_ULTIMA_CANCELACION,
            NULL NUM_TELEFONICO_MIGRADO, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            S_ORG_EXT.CORP_STOCK_SYMBOL CURP,
            S_ORG_EXT.DOM_ULT_DUNS_NUM LICENCIA_CONDUCIR,
            S_ORG_EXT.X_EDO_CTA_DEVUELTO_MEN EDO_CTA_DEVUELTO_MENSAJERIA,
            TRUNC( months_between(current_date, S_ORG_EXT.CUST_SINCE_DT ) / 12 ) || 
                ' Años, ' ||
                    trunc(mod(months_between(current_date,S_ORG_EXT.CUST_SINCE_DT),12)) || 
                        ' Meses, ' ||
                            trunc(mod(months_between(current_date,S_ORG_EXT.CUST_SINCE_DT),12)*30 -                                                 (trunc(mod(months_between(current_date,S_ORG_EXT.CUST_SINCE_DT),12))*30)) 
                                || ' Dias' ANTIGUEDAD,
            CASE 
            WHEN S_ORG_EXT.OU_TYPE_CD = 'Residencial'
            THEN CASE 
                        WHEN S_ORG_EXT.X_SEGMENT_COLLECTION >= 0 AND S_ORG_EXT.X_SEGMENT_COLLECTION <= 1.59 THEN 'Riesgo Alto'
                        WHEN S_ORG_EXT.X_SEGMENT_COLLECTION >= 1.6 AND S_ORG_EXT.X_SEGMENT_COLLECTION <= 2.49 THEN 'Riesgo Medio'
                        WHEN S_ORG_EXT.X_SEGMENT_COLLECTION >= 2.5 AND S_ORG_EXT.X_SEGMENT_COLLECTION <= 3 THEN 'Riesgo Bajo'
                        ELSE 'Sin segmentar'
                    END
            ELSE 'No aplica'
            END SEGMENTO_COBRANZA,
                S_ORG_EXT.X_TT_BRM_ACCOUNT_CREATION SINCRONIZACION_BRM,
            S_ORG_EXT.X_TT_BRM_ACCOUNT_ERR_DESC DESCRIPCION_ERROR,
            S_ORG_EXT.X_TT_ADVANCE_PYMT_CURRNT_NEW CANTIDAD_PAGADA,
            S_ORG_EXT.VISIT_PERIOD SALUDO,
            NULL CUENTA_PADRE, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            S_ORG_EXT.REFERENCE_START_DT FECHA_CONSULTA_SALDOS,
            S_ORG_EXT_X.X_TT_FLG_UPG FLG_ACTUALIZACION_DATOS,
            S_ORG_EXT_X.X_TT_FLG_RFC FLG_RFC,
            S_ORG_EXT_X.X_TT_FOLIO_SEG FOLIO_SEG,
            S_ORG_EXT_X.X_TT_NUM_SEG NUM_SEG,
            S_ORG_EXT_X.X_TT_OS_SEG ORDEN_SERVICIO_SEG,
            S_ORG_EXT.X_TT_CASH_CAMBACEO PAGO_ANTICIPADO_VENDEDOR,
            S_ORG_EXT_X.ATTRIB_48 NUMERO_EMPLEADO,
            S_ORG_EXT_X.X_TT_EMP_GTLV NUMERO_EMPRESA_EMPLEADO,
            S_ORG_EXT_X.ATTRIB_49 CENTRO_COSTOS,
            S_ORG_EXT.MISC_FLG FLG_PAGO_CON_TARJETA,
            S_ORG_EXT.PO_PAY_MAX_AMT PAGO_INICIAL_VTS,
            NULL DIRECCION_FISCAL, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            S_ORG_EXT_X.X_TT_FLG_BLACKLIST BLACKLIST,
            S_ORG_EXT_X.X_TT_FLG_BLK_LST_CAT CAT_BLACKLIST,
            S_ORG_EXT_X.X_TT_FLG_BLK_LST_COM COMENTARIOS_BLACKLIST,
            S_ORG_EXT_X.X_TT_GROUP_BESTEL GRUPO_NEGOCIOS,
            S_ORG_EXT.X_TENDENCIA TENDENCIA,
            NULL FOLIO, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            S_ORG_EXT_X.ATTRIB_17 PREVENTIVO_QUEJA,
            CASE 
                WHEN S_ORG_EXT.OU_TYPE_CD = 'Centrex PYME' AND S_ORG_EXT.EXPERTISE_CD = 'Cuenta SIP' 
                    THEN S_ORG_EXT.CG_SVP_STATUS 
            END ESTADO_OPERATIVO_SIP,
            NULL CALCULO_CANAL_INGRESO,
            NULL FLG_FTTH,
            UPPER(S_ORG_EXT_X.X_ATTRIB_76) PAQUETE_CONTRATADO,
            NULL FECHA_INGRESO_NETFLIX, -- REVISAR -- NETFLIX_NFX_ACCOUNT_INFO
            NULL ESTATUS_NETFLIX, -- REVISAR -- NETFLIX_NFX_ACCOUNT_INFO
            NULL SESIONES_NETFLIX, -- REVISAR -- NETFLIX_NFX_ACCOUNT_INFO
            NULL CATEGORIA_NETFLIX, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_NETFL
            NULL FECHA_DOMICILIACION, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL CANAL_INGRESO_DOMICILIACION, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL INSTITUCION_BANCARIA, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            NULL CLIENTE_CRM, 
            NULL FLG_CTE_BENEFICIO, 
            NULL FECHA_COSECHA_CTE_BENEFICIO,
            NULL SEGMENTO_CTE, -- PUSH -- STG_TTC_MT_CLIENTES_ATRIBUTOS_CALCULADOS
            S_ORG_EXT_X.ATTRIB_19 SALDO_RESTANTE,
            S_ORG_EXT_X.ATTRIB_15 SALDO_TOTAL_DIFERIDO,
            S_ORG_EXT_X.ATTRIB_18 CONTEO_MENSUALIDAD_PAGADA,
            UPPER(S_ORG_EXT_X.X_TT_CONTING_FLAG_AZUL) FLG_CONTING_AZUL,
            UPPER(S_ORG_EXT_X.X_TT_SALDO_TOTAL_DIF) FLG_PAGO_DIFERIDO,
            NULL FLG_DOMICILIACION, -- PUSH -- TTC_TMP_DOMICILIACION
            NULL DIAS_DOMICILIADO, -- PUSH -- TTC_TMP_DOMICILIACION
            NULL PERIODO_DOMICILIACION, -- PUSH -- TTC_TMP_DOMICILIACION
            NULL FECHA_INSTALACION_CALCULADO, -- PUSH -- TTC_TMP_DOMICILIACION
            NULL FECHA_DOMICILIACION_PERFIL_FAC, -- PUSH -- TTC_TMP_DOMICILIACION
            NULL FECHA_PORTADO,
            NULL FLG_PORTABILIDAD,
            NULL FLG_MOVIL,
            NULL FLG_PORTABILIDAD_MOVIL,
            CASE 
                WHEN 
                    DECODE(S_ORG_EXT.EXPERTISE_CD,NULL,'SIN SUBTIPO CLIENTE', UPPER( {{ fnc_translate('S_ORG_EXT.EXPERTISE_CD') }} )) = 'BESTEL' 
                    OR NVL(S_ORG_EXT.NAME,1) IN  ('-999666332', '-999667358', '-999667359')
                    OR NVL(S_ORG_EXT.NAME,1) LIKE '-9%'
                    OR NVL(S_ORG_EXT.NAME,1) LIKE '-8881%'
                    THEN 'N' 
                    ELSE 'Y' END FLG_CLIENTE_MASIVO,
            NULL ESTATUS_MORA,
            S_ORG_EXT_X.X_TT_D_ACTIVE FECHA_ACTIVACION_DISNEY, 
            NVL(TTC_CAT_DOMICILIACION_CLIENTES.FLG_DOMICILIACION_CLIENTES, 'N') FLG_DESCARTA_DOMICILIACION,
            NULL FLG_PAGO_ANUAL, -- PUSH -- STG_CUENTAS_FLG_ANUAL
            NULL VELOCIDAD_CONTRATADA, -- PUSH  -- STG_TTC_MT_CLIENTES_VELOCIDAD_INTER
            NULL TIPO_DOMICILIACION, -- PUSH -- TTC_TMP_DOMICILIACION -- STG_TTC_MT_CLIENTES_VELOCIDAD_INTER_DOMICILIACION
            NULL OPERADOR_MOVIL, -- PUSH -- STG_TTC_MT_CLIENTES_OPERADOR_MOVIL
            S_ORG_EXT_X.X_TT_VIX_STA ACTIVACION_VIX
        {# -------------- #}
        {# INICIO DE FROM #}        
        from ttc_s_org_ext s_org_ext
        left join ttc_s_addr_per s_addr_per ON s_org_ext.PR_ADDR_ID = s_addr_per.ROW_ID  
        left join tmp_ttc_dim_region ttc_dim_region ON UPPER(s_addr_per.X_HUB) = ttc_dim_region.ID_TRANS
                                                and s_addr_per.X_TT_RPT_CODIGO = ttc_dim_region.CODIGO_RPT  
        left join ttc_ctl_hub_dir ON s_addr_per.X_HUB = ttc_ctl_hub_dir.X_HUB
                       and s_addr_per.X_TT_RPT_CODIGO = ttc_ctl_hub_dir.CODIGO_RPT     
        left join tmp_ttc_dim_region_operativa ttc_dim_region_operativa
                                    ON NVL(UPPER(TRIM(s_addr_per.X_HUB)),'SIN HUB') = ttc_dim_region_operativa.HUB
        left join ttc_s_con_addr s_con_addr ON s_org_ext.ROW_ID = s_con_addr.ACCNT_ID
                                          AND s_org_ext.PR_ADDR_ID = s_con_addr.ADDR_PER_ID 
        left join ttc_s_pri_lst S_PRI_LST ON S_ORG_EXT.CURR_PRI_LST_ID = S_PRI_LST.ROW_ID   
        left join tmp_ttc_s_lst_of_val S_LST_OF_VAL ON S_ADDR_PER.X_TT_RPT_CODIGO = S_LST_OF_VAL.NAME     
        left join ttc_s_org_ext_x S_ORG_EXT_X ON S_ORG_EXT.ROW_ID = S_ORG_EXT_X.PAR_ROW_ID  
        left join ttc_s_indust S_INDUST ON S_ORG_EXT.PR_INDUST_ID = S_INDUST.ROW_ID
        left join ttc_cat_domiciliacion_clientes  
                                    ON S_ORG_EXT.NAME = TTC_CAT_DOMICILIACION_CLIENTES.CLIENTE                                                                                                                                                                                                                                           
        where 1 = 1
        and s_org_ext.row_id in (
                                select row_id
                                from ttc_univ_clientes_telco_crm
                                where 1=1
                                and row_id in 
                                            (
                                            '1-1TVUN7UQ',
                                            '1-1TVRZN1A',
                                            '1-1TVW8Y3O',
                                            '1-1TVN5ZHQ',
                                            '1-1TW2J9HS',
                                            '1-1TWGECB2',
                                            '1-1TW7E0TK',
                                            '1-1TW1G2VA',
                                            '1-1TVM9L0E',
                                            '1-1TW3IA86'
                                            ,'1-1TX756NI'
                                            ,'1-1TX4DVNI'
                                            ,'1-1TXB3E2Y'
                                            ) 
                                ) 
        ),


    {# bloque 4: CTE Final#}
    final as (select * from ttc_mt_clientes_telco_crm)

select {{ dw_control_columns() }}, 
       *
from final

