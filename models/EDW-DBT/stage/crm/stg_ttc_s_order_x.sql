select
    row_id,
    created,
    created_by,
    last_upd,
    last_upd_by,
    modification_num,
    conflict_id,
    par_row_id,
    attrib_08,
    attrib_09,
    attrib_10,
    attrib_11,
    attrib_12,
    attrib_13,
    attrib_14,
    attrib_15,
    attrib_16,
    attrib_17,
    attrib_18,
    attrib_19,
    attrib_20,
    attrib_21,
    attrib_22,
    attrib_23,
    attrib_24,
    attrib_25,
    attrib_26,
    attrib_27,
    attrib_28,
    attrib_29,
    attrib_30,
    attrib_31,
    attrib_32,
    attrib_33,
    attrib_55,
    attrib_56,
    attrib_57,
    attrib_58,
    attrib_59,
    attrib_60,
    attrib_01,
    attrib_02,
    attrib_03,
    {{ fnc_translate("ATTRIB_04") }} attrib_04,
    attrib_04_raw,
    attrib_05,
    attrib_06,
    attrib_07,
    attrib_34,
    attrib_35,
    attrib_36,
    attrib_37,
    attrib_38,
    attrib_39,
    attrib_40,
    attrib_41,
    attrib_42,
    attrib_43,
    attrib_44,
    attrib_45,
    attrib_46,
    attrib_47,
    attrib_48,
    attrib_49,
    attrib_50,
    attrib_51,
    attrib_52,
    attrib_53,
    attrib_54,
    x_attrib_61,
    x_attrib_62,
    db_last_upd,
    db_last_upd_src,
    x_attrib_63,
    x_attrib_64,
    x_attrib_65,
    x_attrib_66,
    x_attrib_67,
    x_attrib_68,
    x_attrib_69,
    x_attrib_70,
    x_attrib_71,
    x_attrib_72,
    x_attrib_73,
    x_attrib_74,
    x_attrib_75,
    x_attrib_76,
    x_attrib_77,
    x_attrib_78,
    x_attrib_79,
    x_attrib_80,
    x_attrib_81,
    x_attrib_82,
    x_attrib_83,
    x_attrib_84,
    x_attrib_85,
    x_attrib_86,
    x_attrib_87,
    x_attrib_88,
    x_attrib_89,
    x_attrib_90,
    x_attrib_91,
    x_attrib_92,
    x_attrib_93,
    x_attrib_94,
    x_attrib_95,
    x_attrib_96,
    x_attrib_97,
    x_attrib_99,
    x_attrib_100,
    x_attrib_101,
    x_attrib_102,
    x_attrib_98,
    x_attrib_103,
    x_attrib_104,
    x_attrib_105,
    tt_program_flag,
    tt_req_v_tec,
    x_tt_supp_order_type,
    x_tt_dlvr_dt,
    x_tt_stat,
    x_tt_supp_order,
    tt_reenv_flg,
    x_tt_cont_env,
    x_tt_hor_seg,
    x_tt_id_plat_voz,
    x_tt_num_tel,
    x_tt_proxy_prim,
    x_tt_pto_proxy_prim,
    x_tt_pto_reg_prim,
    x_tt_reg_primario,
    x_tt_usuario,
    x_tt_rqst_dt_wa,
    x_tt_rqst_hra_wa,
    x_attrib_106,
    x_tt_ord_pending_train_ords,
    x_tt_port_int_flg,
    x_tt_stat_mat,
    x_tt_pass_om,
    tt_order_outputs,
    x_attrib_107,
    x_attrib_108,
    x_attrib_109,
    tt_bucket_capacity,
    x_tt_usuario_ok_cliente,
    last_upd_gg,
    x_tt_folio_ofsc,
    tt_fch_assign_ofsc,
    x_tt_trail_order,
    x_tt_soa_order,
    tt_confirm_ord_flg,
    x_tt_grp_name_eq,
    x_tt_combo_name,
    x_tt_dom_flg,
    x_tt_lydeorder_id,
    x_tt_lydeorder_stat,
    x_tt_tv_almacen,
    x_tt_tv_guia,
    x_tt_tv_serialnum,
    x_tt_tv_transportista,
    x_tt_st_dlvr_dt,
    x_tt_tv_ref_pago,
    x_tt_combo_desc,
    x_tt_combo_price,
    x_tt_ins_exp_cost,
    x_tt_ins_exp_flg
from {{ source("raw_crm", "ttc_s_order_x") }}
