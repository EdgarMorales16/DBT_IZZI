SELECT ROW_ID,
       CREATED,
       CREATED_BY,
       LAST_UPD,
       LAST_UPD_BY,
       DCKING_NUM,
       MODIFICATION_NUM,
       CONFLICT_ID,
       NAME,
       NEWS_ACTIVE_FLG,
       SIC,
       SUB_TYPE,
       LANG_ID,
       PRESCRIBER_FLG,
       DESC_TEXT,
       INTEGRATION_ID,
       NAICS_INDUS_CD,
       NAICS_INDUS_DESC,
       PAR_INDUST_ID,
       QUERY_TEXT,
       DB_LAST_UPD,
       DB_LAST_UPD_SRC,
       TREND_TEXT,
       LAST_UPD_GG

FROM {{ source('raw_crm','ttc_s_indust') }}        