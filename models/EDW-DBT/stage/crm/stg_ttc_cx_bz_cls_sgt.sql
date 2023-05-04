SELECT ROW_ID,
       CREATED,
       CREATED_BY,
       LAST_UPD,
       LAST_UPD_BY,
       MODIFICATION_NUM,
       CONFLICT_ID,
       DB_LAST_UPD,
       CLIENTE_ID,
       DB_LAST_UPD_SRC,
       NOMBRE_SEGMENTACION,
       LAST_UPD_GG
       
  FROM {{ source('raw_crm','ttc_cx_bz_cls_sgt') }} 