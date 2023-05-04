SELECT DW_FECHA_CREACION,
       DW_FECHA_ACTUALIZACION,
       WIDCAT_DOMICILIACION_CLIENTES,
       ORIG_ID,
       CLIENTE,
       FECHA_CREACION,
       FLG_DOMICILIACION_CLIENTES
       
  FROM {{ source('raw_edw','ttc_cat_domiciliacion_clientes') }}
 WHERE 1 = 1