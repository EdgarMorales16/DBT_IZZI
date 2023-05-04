SELECT {{ fnc_wid('3','ROW_ID') }} WIDMT_DIRECCION,
       ROW_ID,
       CREATED,
       CREATED_BY,
       LAST_UPD,
       LAST_UPD_BY,
       MODIFICATION_NUM,
       CONFLICT_ID,
       ACTIVE_FLG,
       ADDR_PER_ID,
       BL_ADDR_FLG,
       FRAUD_FLG,
       MAIN_ADDR_FLG,
       RELATION_TYPE_CD,
       SHIP_ADDR_FLG,
       DEA_EXPR_DT,
       END_DT,
       FRI_1ST_CLOSE_TM,
       FRI_1ST_OPEN_TM,
       FRI_2ND_CLOSE_TM,
       FRI_2ND_OPEN_TM,
       MON_1ST_CLOSE_TM,
       MON_1ST_OPEN_TM,
       MON_2ND_CLOSE_TM,
       MON_2ND_OPEN_TM,
       NUM_MONTHS_AT_ADDR,
       SAT_1ST_CLOSE_TM,
       SAT_1ST_OPEN_TM,
       SAT_2ND_CLOSE_TM,
       SAT_2ND_OPEN_TM,
       START_DT,
       SUN_1ST_CLOSE_TM,
       SUN_1ST_OPEN_TM,
       SUN_2ND_CLOSE_TM,
       SUN_2ND_OPEN_TM,
       THU_1ST_CLOSE_TM,
       THU_1ST_OPEN_TM,
       THU_2ND_CLOSE_TM,
       THU_2ND_OPEN_TM,
       TUE_1ST_CLOSE_TM,
       TUE_1ST_OPEN_TM,
       TUE_2ND_CLOSE_TM,
       TUE_2ND_OPEN_TM,
       WED_1ST_CLOSE_TM,
       WED_1ST_OPEN_TM,
       WED_2ND_CLOSE_TM,
       WED_2ND_OPEN_TM,
       ACCNT_ID,
       ADDR_MAIL_CD,
       ADDR_TYPE_CD,
       BRICK_ID,
       BU_ID,
       CONTACT_ID,
       DEA_NUM,
       DFLT_SHIP_PRIO_CD,
       EMAIL_ADDR,
       FAX_PH_NUM,
       MAIL_TYPE_CD,
       OCCUPANCY_CD,
       ORG_GROUP_ID,
       PH_NUM,
       TRNSPRT_ZONE_CD,
       ADDRESSEE,
       ALIGNMENT_FLG,
       DB_LAST_UPD,
       DB_LAST_UPD_SRC,
       YEARLY_END_DT,
       YEARLY_START_DT,
       LAST_UPD_GG

  FROM {{ source('raw_crm','ttc_s_con_addr') }}