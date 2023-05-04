SELECT DIM_FECHA_CREACION,
       WIDDIM_REGION_OPERATIVA,
       ORIG_ID,
       HUB,
       DIVISION_INTERNA,
       REGION_INTERNA,
       PLAZA_INTERNA,
       DIVISION_EXTERNA,
       REGION_EXTERNA,
       PLAZA_EXTERNA,
       SUBREGION_INTERNA
       
  FROM {{ source('raw_edw','ttc_dim_region_operativa') }}
 WHERE 1 = 1