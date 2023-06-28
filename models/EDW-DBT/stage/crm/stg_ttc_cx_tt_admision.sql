select
    row_id,
    created,
    created_by,
    last_upd,
    last_upd_by,
    modification_num,
    conflict_id,
    db_last_upd,
    db_last_upd_src,
    tt_cuenta,
    tt_edo_admision,
    tt_orden_servicio,
    tt_full_name,
    last_upd_gg
from {{ source("raw_crm", "ttc_cx_tt_admision") }}
