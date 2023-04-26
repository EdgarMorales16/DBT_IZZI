SELECT ROW_ID,
       CREATED,
       CREATED_BY,
       LAST_UPD,
       LAST_UPD_BY,
       MODIFICATION_NUM,
       CONFLICT_ID,
       DB_LAST_UPD,
       DB_LAST_UPD_SRC,
       TT_CUENTA,
       TT_EDO_ADMISION,
       TT_ORDEN_SERVICIO,
       TT_FULL_NAME,
       LAST_UPD_GG
  FROM {{ source('raw_crm', 'ttc_cx_tt_admision') }}