SELECT POID_DB,
       POID_ID0,
       POID_TYPE,
       POID_REV,
       {{ fnc_date_brm('CREATED_T') }} CREATED_T,
       {{ fnc_date_brm('MOD_T') }} MOD_T,
       READ_ACCESS,
       WRITE_ACCESS,
       CONFIG_SCENARIO_OBJ_DB,
       CONFIG_SCENARIO_OBJ_ID0,
       CONFIG_SCENARIO_OBJ_TYPE,
       CONFIG_SCENARIO_OBJ_REV,
       ACCOUNT_OBJ_DB,
       ACCOUNT_OBJ_ID0,
       ACCOUNT_OBJ_TYPE,
       ACCOUNT_OBJ_REV,
       AGENT_OBJ_DB,
       AGENT_OBJ_ID0,
       AGENT_OBJ_TYPE,
       AGENT_OBJ_REV,
       OVERDUE_T,
       OVERDUE_AMOUNT,
       ENTRY_T,
       ENTRY_AMOUNT,
       EXIT_T,
       EXIT_AMOUNT,
       REASON,
       BILLINFO_OBJ_DB,
       BILLINFO_OBJ_ID0,
       BILLINFO_OBJ_TYPE,
       BILLINFO_OBJ_REV,
       LAST_UPD_GG,
       EXTERNAL_USER
  FROM {{ source('raw_brm','ttc_collections_scenario_t') }}
 WHERE 1 = 1