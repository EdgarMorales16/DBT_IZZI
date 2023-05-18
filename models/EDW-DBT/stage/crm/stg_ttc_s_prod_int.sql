select
    {{ fnc_wid('3','row_id') }} widdim_producto,
    row_id,
    created,
    created_by,
    last_upd,
    last_upd_by,
    dcking_num,
    modification_num,
    conflict_id,
    active_flg,
    add_to_quote_flg,
    bu_id,
    cg_competitor_flg,
    commisioned_flg,
    compensatable_flg,
    crt_ast_rec_flg,
    enterprise_flg,
    featured_flg,
    model_prod_flg,
    {{ fnc_translate('name') }} name,
    orderable_flg,
    postn_bl_prod_flg,
    sales_prod_flg,
    sales_srvc_flg,
    service_flg,
    serv_inst_flg,
    ship_flg,
    target_vrsn_flg,
    tax_subcomp_flg,
    transferable_flg,
    ctrl_subs_flg,
    accrual_rate,
    alc_below_sfty_flg,
    aloc_assets_flg,
    aprx_cost_amt,
    apx_cst_exch_dt,
    auto_allocate_flg,
    auto_subst_flg,
    avg_call_cst,
    avg_call_cst_dt,
    avg_op_life,
    avg_profit,
    capacity,
    cary_cost,
    cary_cost_dt,
    case_conv_factor,
    case_pack,
    configuration_flg,
    cutoff_flg,
    deliv_interval,
    dest_cost,
    disp_cmpnt_flg,
    disp_cmpnt_pri_flg,
    efficiency_rating,
    eff_end_dt,
    eff_start_dt,
    exch_dt,
    fru_flg,
    gross_mgn,
    inventory_flg,
    item_size,
    load_added,
    lock_flg,
    lot_active_flg,
    max_order_units,
    min_order_units,
    model_yr,
    msrp,
    mtbf,
    mttr,
    num_occurrence,
    order_cst,
    order_cst_dt,
    product_level,
    prod_upd_dt,
    req_appl_flg,
    req_referral_flg,
    reservable_flg,
    rtrn_defective_flg,
    rxavpr_exch_dt,
    rx_avg_price,
    serialized_flg,
    skip_flg,
    taxable_flg,
    tool_flg,
    units_bckord,
    units_bckord_as_of,
    units_invent,
    units_invent_as_of,
    unit_conv_factor,
    addl_lead_time,
    alias_name,
    apx_cst_curcy_cd,
    assoc_level,
    atm_type_cd,
    avgcallcst_curcycd,
    body_style_cd,
    book_appr_id,
    build,
    cary_cost_curcy_cd,
    category_cd,
    cfg_model_id,
    cg_pr_ctlg_cat_id,
    class_partnum_id,
    config_rule_file,
    critical_cd,
    cs_path_id,
    curcy_cd,
    data_src,
    def_mod_prod_id,
    desc_text,
    detail_type_cd,
    dflt_procsys_id,
    divn_cd,
    doors_type_cd,
    down_time,
    drive_train_cd,
    engine_type_cd,
    fabric_cd,
    fuel_type_cd,
    gender_cd,
    integration_id,
    intl_bld_lang_cd,
    invst_catg_cd,
    invst_obj_cd,
    lead_tm,
    life_cycle_cd,
    make_cd,
    model,
    model_cd,
    movement_class,
    mtbf_uom_cd,
    mttr_uom_cd,
    objective_desc,
    onl_pageset_id,
    order_cst_curcy_cd,
    part_num,
    par_prod_int_id,
    plan_status,
    pm_dept_postn_id,
    preappr_state_id,
    pref_carrier_cd,
    pref_ship_meth_cd,
    price_type_cd,
    prod_assembly_lvl,
    prod_attrib01_cd,
    prod_attrib02_cd,
    prod_attrib03_cd,
    prod_attrib04_cd,
    prod_catg_cd,
    prod_cd,
    prod_cls_num,
    prod_dist_cd,
    prod_global_uid,
    prod_image_id,
    prod_lcycle_status,
    prod_opt1_mix_id,
    prod_opt2_mix_id,
    prod_option1_id,
    prod_option2_id,
    prod_supply_chain,
    prod_type_cd,
    profit_rank_cd,
    pr_con_id,
    pr_equiv_prod_id,
    pr_erng_id,
    pr_fulfl_invloc_id,
    pr_indust_id,
    pr_postn_id,
    pr_prod_ln_id,
    pr_season_id,
    pr_trgt_mkt_seg_id,
    quality_cd,
    ref_number_1,
    ref_number_2,
    ref_number_3,
    ref_number_4,
    ref_number_5,
    region_id,
    req_data_id,
    risk,
    rule_attrib1,
    rule_attrib2,
    rxavpr_curcy_cd,
    schedule_num_cd,
    seq_cd,
    service_terms,
    silhouette_cd,
    status_cd,
    strategy,
    strength,
    sub_type,
    sub_type_cd,
    tgt_cust_type_cd,
    tgt_region_cd,
    thmbnl_image_id,
    transmission_cd,
    trim_cd,
    type,
    uom_cd,
    valtn_sys_id,
    value_class,
    vendr_ou_id,
    vendr_part_num,
    version,
    xa_class_id,
    x_categoria_2_linea,
    x_prod_cont,
    apply_ec_rule_flg,
    approval_flg,
    asset_ref_expr,
    auto_ungroup_flg,
    bar_code_num,
    billable_flg,
    billing_type_cd,
    cmpnd_flg,
    comments,
    crt_agreement_flg,
    crt_inst_flg,
    db_last_upd,
    db_last_upd_src,
    design_reg_flg,
    dpndncy_vldtn_flg,
    exp_lead_days,
    gtin,
    incl_all_crse_flg,
    incl_crse_num,
    inclsv_elig_rl_flg,
    invntry_integ_id,
    leaf_level_flg,
    loy_actual_dist,
    loy_dist_uom,
    loy_exp_lead_time,
    loy_exp_uom,
    loy_expr_basis_cd,
    loy_fromaprt_cd,
    loy_num_year,
    loy_period_dur,
    loy_period_type_cd,
    loy_seq_type_cd,
    loy_series_prefix,
    loy_sug_points,
    loy_sug_price,
    loy_sug_pttype_id,
    loy_sug_r_points,
    loy_sug_r_price,
    loy_sug_rpttype_id,
    loy_toaprt_cd,
    loy_vch_exp_day_cd,
    loy_vch_exp_mth_cd,
    loy_vchrexp_prddur,
    loy_vexp_pertyp_cd,
    need_ph_num_flg,
    net_elmt_type_cd,
    paymnt_type_cd,
    permitted_type,
    pr_ship_carprio_id,
    pr_src_id,
    promo_instance_cd,
    promo_type_cd,
    reason_txt,
    reference_type_cd,
    reqd_service_flg,
    rollup_level,
    rollup_trgmkt_flg,
    root_prod_id,
    score_num,
    serv_length_uom_cd,
    service_length,
    subscn_dur_day_num,
    trgt_mkt_id,
    twobarcodes_flg,
    unique_asset_flg,
    x_principal_ext,
    x_tt_sap_id,
    x_puertos,
    x_oferta_valida,
    x_tt_sociedad,
    x_izzi_tv_app,
    x_tt_chan_serv_type,
    x_one_chrg_lego,
    x_tt_override_flg,
    x_tt_exc_orden,
    x_tt_no_delete,
    x_tt_no_man_choise,
    x_tt_msj69,
    x_tt_wifi_solut_comp,
    x_tt_equip_ofsc,
    last_upd_gg,
    x_tt_no_rec_acometida
from {{ source('raw_crm', 'ttc_s_prod_int') }}