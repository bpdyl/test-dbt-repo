{{ config(
    materialized='view',
    alias='V_DWH_D_PRD_UDA_ITM_MTX',
    schema='DW_DWH_V',
    tags=['d_prd_uda_itm_mtx_ld'],
) }}
SELECT
  SRC.*
FROM {{ref('DWH_D_PRD_UDA_ITM_MTX')}} SRC