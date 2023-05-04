SELECT DIM_FECHA_CREACION,
       DIM_FECHA_ACTUALIZACION,
       WIDDIM_REGION,
       ORIG_ID,
       ID_TRANS,
       CODIGO_RPT,
       AGRUPADOR_BI,
       REGION,
       SUBREGION,
       HUB_CM,
       RPT,
       PLAZA,
       FLG_EVALUACION_TC,
       OFERTA_PLAZA,
       RPT_OPERATIVO,
       PLAZA_WR

  FROM {{ source('raw_edw','ttc_dim_region')}} 