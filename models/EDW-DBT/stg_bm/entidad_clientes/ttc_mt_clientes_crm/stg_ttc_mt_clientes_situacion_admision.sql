with    
    {# bloque 1: CTE tablas#} 

    ttc_cx_tt_admision as ( select * from {{ ref("stg_ttc_cx_tt_admision") }} ),
    ttc_s_org_ext ( select * from {{ ref("stg_ttc_s_org_ext") }} )
    ttc_univ_clientes_telco_crm ( select * from {{ ref("stg_ttc_univ_clientes_telco_crm") }} )

    {# bloque 2: CTE filtros#}
    u_ttc_cx_tt_admision as 
    (
        select *
        from ttc_cx_tt_admision,
         
        where 1 = 1
        and  (SELECT 1 FROM ttc_univ_clientes_telco_crm 
                            WHERE 1=1
                            AND TTC_S_ORG_EXT.ROW_ID = UNIV.ROW_ID)
    )
            