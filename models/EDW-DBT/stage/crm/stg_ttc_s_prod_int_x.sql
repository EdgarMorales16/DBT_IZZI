select ROW_ID,
       CREATED,
       CREATED_BY,
       LAST_UPD,
       LAST_UPD_BY,
       MODIFICATION_NUM,
       CONFLICT_ID,
       PAR_ROW_ID,
       ATTRIB_08,
       ATTRIB_09,
       ATTRIB_10,
       ATTRIB_11,
       ATTRIB_12,
       ATTRIB_13,
       ATTRIB_14,
       ATTRIB_15,
       ATTRIB_16,
       ATTRIB_17,
       ATTRIB_18,
       ATTRIB_19,
       ATTRIB_20,
       ATTRIB_21,
       ATTRIB_22,
       ATTRIB_23,
       ATTRIB_24,
       ATTRIB_25,
       ATTRIB_26,
       ATTRIB_27,
       ATTRIB_28,
       ATTRIB_29,
       ATTRIB_30,
       ATTRIB_31,
       ATTRIB_32,
       ATTRIB_33,
       ATTRIB_48,
       ATTRIB_49,
       ATTRIB_50,
       ATTRIB_54,
       ATTRIB_55,
       ATTRIB_56,
       ATTRIB_57,
       ATTRIB_58,
       ATTRIB_59,
       ATTRIB_60,
       ATTRIB_01,
       ATTRIB_02,
       ATTRIB_03,
       ATTRIB_04,
       ATTRIB_05,
       ATTRIB_06,
       ATTRIB_07,
       ATTRIB_34,
       ATTRIB_35,
       ATTRIB_36,
       ATTRIB_37,
       ATTRIB_38,
       ATTRIB_39,
       ATTRIB_40,
       ATTRIB_41,
       ATTRIB_42,
       ATTRIB_43,
       ATTRIB_44,
       ATTRIB_45,
       ATTRIB_46,
       ATTRIB_47,
       ATTRIB_51,
       ATTRIB_52,
       ATTRIB_53,
       DB_LAST_UPD,
       DB_LAST_UPD_SRC,
       TT_COPY_CHIP_ID_FLG,
       TT_COPY_SMART_ID_FLG,
       TT_TECHNOLOGY_FTTH,
       LAST_UPD_GG

from {{ source('raw_crm', 'ttc_s_prod_int_x') }}