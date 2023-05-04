SELECT {{ fnc_wid('3','ROW_ID') }} WIDCAT_LISTA_PRECIO,
       ROW_ID,
       CREATED,
       CREATED_BY,
       LAST_UPD,
       LAST_UPD_BY,
       DCKING_NUM,
       MODIFICATION_NUM,
       CONFLICT_ID,
       ACTIVE_FLG,
       BU_ID,
       EFF_START_DT,
       ENTERPRISE_FLG,
       NAME,
       SUBTYPE_CD,
       TAXABLE_FLG,
       CURCY_CD,
       EFF_END_DT,
       FIXED_TAX_RATE,
       VARIABLE_TAX_RATE,
       COST_LST_ID,
       DESC_TEXT,
       DFLT_COST_METH_CD,
       FRGHT_CD,
       INTEGRATION_ID,
       PAYMENT_TERM_ID,
       PERIOD_ID,
       PRIMDL_ID,
       PRI_LST_CD,
       SHIP_METH_CD,
       DB_LAST_UPD,
       DB_LAST_UPD_SRC,
       PRI_WF_PROC_NAME,
       LAST_UPD_GG

  FROM {{ source('raw_crm','ttc_s_pri_lst') }}
 WHERE 1 = 1