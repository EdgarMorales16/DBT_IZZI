SELECT OBJ_ID0,
       REC_ID,
       ADDRESS,
       CITY,
       COUNTRY,
       DEBIT_EXP,
       DEBIT_NUM,
       NAME,
       STATE,
       ZIP,
       LAST_UPD_GG,
       CARD_TYPE

  FROM {{ source('raw_brm','ttc_payinfo_cc_t') }}