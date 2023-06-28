select
    row_id,
    created,
    created_by,
    last_upd,
    last_upd_by,
    modification_num,
    conflict_id,
    db_last_upd,
    cliente_id,
    db_last_upd_src,
    nombre_segmentacion,
    last_upd_gg
from {{ source("raw_crm", "ttc_cx_bz_cls_sgt") }}
