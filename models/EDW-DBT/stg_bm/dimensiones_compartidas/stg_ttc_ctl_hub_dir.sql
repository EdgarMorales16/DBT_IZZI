SELECT DW_CREACION_FC,
       DW_ULTIMO_FC,
       ROW_ID,
       ORIG_ID,
       ID,
       X_HUB,
       CODIGO_RPT

  FROM {{ source('raw_edw','ttc_ctl_hub_dir') }} 