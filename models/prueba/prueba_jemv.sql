select  
    {{ hash_upd_edw('stg_ttc_mt_clientes_crm' ) }} widmt_cliente_upd
from {{ source('raw_edw', 'stg_ttc_mt_clientes_crm') }}
