select 
    obj_id0,
    rec_id,
    rec_id2,
    phone,
    type,
    last_upd_gg
from {{ source('raw_brm', 'ttc_account_phones_t') }}