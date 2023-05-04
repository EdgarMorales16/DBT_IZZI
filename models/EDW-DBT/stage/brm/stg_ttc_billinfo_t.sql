SELECT POID_DB,
       POID_ID0,
       POID_TYPE,
       POID_REV,
       {{ fnc_date_brm('CREATED_T') }} CREATED_T,
       {{ fnc_date_brm('MOD_T') }} MOD_T,
       READ_ACCESS,
       WRITE_ACCESS,
       BILL_INFO_ID,
       BILLING_STATE,
       BILLING_STATUS,
       BILLING_STATUS_FLAGS,
       STATUS,
       STATUS_FLAGS,
       ACCOUNT_OBJ_DB,
       ACCOUNT_OBJ_ID0,
       ACCOUNT_OBJ_TYPE,
       ACCOUNT_OBJ_REV,
       ACTG_CYCLE_DOM,
       ACTG_LAST_T,
       ACTG_NEXT_T,
       ACTG_FUTURE_T,
       BILL_ACTGCYCLES_LEFT,
       BILL_WHEN,
       LAST_BILL_T,
       LAST_BILL_OBJ_DB,
       LAST_BILL_OBJ_ID0,
       LAST_BILL_OBJ_TYPE,
       LAST_BILL_OBJ_REV,
       ACTUAL_LAST_BILL_T,
       ACTUAL_LAST_BILL_OBJ_DB,
       ACTUAL_LAST_BILL_OBJ_ID0,
       ACTUAL_LAST_BILL_OBJ_TYPE,
       ACTUAL_LAST_BILL_OBJ_REV,
       NEXT_BILL_T,
       NEXT_BILL_OBJ_DB,
       NEXT_BILL_OBJ_ID0,
       NEXT_BILL_OBJ_TYPE,
       NEXT_BILL_OBJ_REV,
       FUTURE_BILL_T,
       BILL_OBJ_DB,
       BILL_OBJ_ID0,
       BILL_OBJ_TYPE,
       BILL_OBJ_REV,
       CURRENCY,
       CURRENCY_SECONDARY,
       PARENT_FLAGS,
       SPONSOR_FLAGS,
       SPONSOREE_FLAGS,
       PENDING_RECV,
       AR_BILLINFO_OBJ_DB,
       AR_BILLINFO_OBJ_ID0,
       AR_BILLINFO_OBJ_TYPE,
       AR_BILLINFO_OBJ_REV,
       PARENT_BILLINFO_OBJ_DB,
       PARENT_BILLINFO_OBJ_ID0,
       PARENT_BILLINFO_OBJ_TYPE,
       PARENT_BILLINFO_OBJ_REV,
       PAYINFO_OBJ_DB,
       PAYINFO_OBJ_ID0,
       PAYINFO_OBJ_TYPE,
       PAYINFO_OBJ_REV,
       PAY_TYPE,
       SCENARIO_OBJ_DB,
       SCENARIO_OBJ_ID0,
       SCENARIO_OBJ_TYPE,
       SCENARIO_OBJ_REV,
       EXEMPT_FROM_COLLECTIONS,
       BILLING_SEGMENT,
       ACCOUNT_SUPPRESSED,
       NUM_SUPPRESSED_CYCLES,
       SUPPRESSION_CYCLES_LEFT,
       PAYMENT_EVENT_OBJ_DB,
       PAYMENT_EVENT_OBJ_ID0,
       PAYMENT_EVENT_OBJ_TYPE,
       PAYMENT_EVENT_OBJ_REV,
       COLLECTION_DATE,
       EVENT_POID_LIST,
       BUSINESS_PROFILE_OBJ_DB,
       BUSINESS_PROFILE_OBJ_ID0,
       BUSINESS_PROFILE_OBJ_TYPE,
       BUSINESS_PROFILE_OBJ_REV,
       OBJECT_CACHE_TYPE,
       EFFECTIVE_T,
       BAL_GRP_OBJ_DB,
       BAL_GRP_OBJ_ID0,
       BAL_GRP_OBJ_TYPE,
       BAL_GRP_OBJ_REV,
       ACTG_TYPE,
       ASSOC_BUS_PROFILE_OBJ_LIST,
       LAST_UPD_GG

  FROM {{source('raw_brm','ttc_billinfo_t') }}